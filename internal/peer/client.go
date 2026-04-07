package peer

import (
	"context"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/t4db/t4/internal/wal"
)

// FollowerRetryInterval is the backoff between consecutive stream reconnect
// attempts. Exported so the leader's watchLoop can use the same value when
// computing how long to poll S3 after a follower disconnect.
const FollowerRetryInterval = 2 * time.Second

// LeaderLivenessTTL is the maximum age of a lock record's LastSeenNano for
// which a follower will back off from attempting TakeOver. The leader refreshes
// LastSeenNano at most every FollowerRetryInterval while it has connected
// followers, so a record younger than this means the leader was alive recently.
// Using 3× the touch interval gives tolerance for timing jitter and S3 latency.
const LeaderLivenessTTL = 3 * FollowerRetryInterval // 6 seconds

// Client is the follower-side peer client.
//
// It maintains a single persistent gRPC ClientConn to the leader that is
// shared by both the WAL stream (Follow) and write forwarding (ForwardWrite).
// gRPC multiplexes both over a single HTTP/2 connection.
type Client struct {
	leaderAddr string
	nodeID     string
	maxRetries int                              // consecutive failures before ErrLeaderUnreachable (0 = unlimited)
	tlsCreds   credentials.TransportCredentials // nil = plaintext

	connMu sync.Mutex
	conn   *grpc.ClientConn // lazily initialised; nil after Close
}

// NewClient creates a Client that will connect to leaderAddr.
// maxRetries is the number of consecutive connection failures before Follow
// returns ErrLeaderUnreachable. Use 0 for unlimited retries.
// tlsCreds may be nil for plaintext (only safe on a trusted network).
func NewClient(leaderAddr, nodeID string, maxRetries int, tlsCreds credentials.TransportCredentials) *Client {
	return &Client{leaderAddr: leaderAddr, nodeID: nodeID, maxRetries: maxRetries, tlsCreds: tlsCreds}
}

// Close releases the underlying gRPC connection.
func (c *Client) Close() {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

// getConn returns the shared persistent ClientConn, creating it on first use.
func (c *Client) getConn() (*grpc.ClientConn, error) {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if c.conn != nil {
		return c.conn, nil
	}
	creds := c.tlsCreds
	if creds == nil {
		creds = insecure.NewCredentials()
	}
	conn, err := grpc.NewClient(
		c.leaderAddr,
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(Codec{})),
	)
	if err != nil {
		return nil, err
	}
	c.conn = conn
	return conn, nil
}

// Follow streams WAL entries from the leader starting at fromRev. For each
// batch it first calls walFn to durably append the entries to the follower WAL,
// then ACKs the highest revision in the batch to the leader, then calls applyFn
// to update the follower's local state. fromRev advances automatically once the
// WAL append succeeds. Batches contain all entries that arrived between
// consecutive applyFn calls, amortising WAL fsyncs across multiple revisions.
//
// Follow reconnects on transient errors. It returns:
//   - ctx.Err() on context cancellation.
//   - ErrResyncRequired when the leader's buffer no longer covers fromRev.
//   - ErrLeaderUnreachable after maxRetries consecutive connection failures.
//   - ErrLeaderShutdown when the leader sent a graceful shutdown signal.
func (c *Client) Follow(ctx context.Context, fromRev int64, walFn func([]wal.Entry) error, applyFn func([]wal.Entry) error) error {
	consecutiveFailures := 0
	for {
		nextRev, err := c.followOnce(ctx, fromRev, walFn, applyFn)

		if ctx.Err() != nil {
			return ctx.Err()
		}
		if IsResyncRequired(err) {
			logrus.Errorf("peer: leader requires resync from rev=%d: %v", fromRev, err)
			return err
		}
		// Leader is shutting down: skip retry wait and signal caller to elect now.
		if IsLeaderShutdown(err) {
			logrus.Infof("peer: leader sent graceful shutdown — starting election immediately")
			return err
		}

		if nextRev > fromRev {
			consecutiveFailures = 0
		} else {
			consecutiveFailures++
		}
		fromRev = nextRev

		if c.maxRetries > 0 && consecutiveFailures >= c.maxRetries {
			logrus.Errorf("peer: leader unreachable after %d attempts", consecutiveFailures)
			return ErrLeaderUnreachable
		}

		logrus.Warnf("peer: stream error (attempt %d): %v", consecutiveFailures, err)
		select {
		case <-time.After(FollowerRetryInterval):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// followOnce makes one streaming attempt using the shared connection.
// walFn must durably append the batch to the follower's local WAL.
// applyFn updates follower local state after the ACK has been sent.
// Returns the next fromRev (highest WAL-durable revision + 1) on any error.
func (c *Client) followOnce(ctx context.Context, fromRev int64, walFn func([]wal.Entry) error, applyFn func([]wal.Entry) error) (int64, error) {
	conn, err := c.getConn()
	if err != nil {
		return fromRev, err
	}

	// Cancel the stream context on return so the receiver goroutine below
	// exits cleanly when followOnce returns for any reason.
	streamCtx, streamCancel := context.WithCancel(ctx)
	defer streamCancel()

	stream, err := NewWalStreamClient(conn).Follow(streamCtx, &FollowRequest{
		FromRevision: fromRev,
		NodeID:       c.nodeID,
	})
	if err != nil {
		return fromRev, err
	}

	logrus.Infof("peer: connected to leader %s (fromRev=%d)", c.leaderAddr, fromRev)

	// entryC buffers entries received from the stream so the main loop can
	// drain multiple entries per batch, amortising WAL fsyncs (AppendBatch
	// does one fsync for the whole batch rather than one per entry).
	entryC := make(chan wal.Entry, 512)
	recvErrC := make(chan error, 1)

	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				recvErrC <- err
				return
			}
			if msg.Shutdown {
				recvErrC <- ErrLeaderShutdown
				return
			}
			select {
			case entryC <- MsgToEntry(msg):
			case <-streamCtx.Done():
				recvErrC <- streamCtx.Err()
				return
			}
		}
	}()

	for {
		// Block until at least one entry or an error.
		var batch []wal.Entry
		select {
		case e := <-entryC:
			batch = append(batch, e)
		case err := <-recvErrC:
			return fromRev, err
		}

		// Drain any additional entries that are already buffered so we can
		// process them in a single AppendBatch (one fsync for the lot).
	drain:
		for {
			select {
			case e := <-entryC:
				batch = append(batch, e)
			default:
				break drain
			}
		}

		batchStartRev := fromRev
		if err := walFn(batch); err != nil {
			return batchStartRev, err
		}
		fromRev = batch[len(batch)-1].Revision + 1

		// ACK the highest revision in the WAL-durable batch. The leader only
		// needs the maximum ACK'd revision to advance its quorum counter, so
		// one ACK per batch is enough regardless of how many entries it contained.
		if err := stream.SendAck(batch[len(batch)-1].Revision); err != nil {
			return fromRev, err
		}
		if err := applyFn(batch); err != nil {
			return fromRev, err
		}
	}
}

// GoodBye notifies the leader that this follower is shutting down gracefully.
// The leader will skip split-brain fencing when this follower's stream closes.
// Best-effort: errors are logged but not returned.
func (c *Client) GoodBye(ctx context.Context) {
	conn, err := c.getConn()
	if err != nil {
		logrus.Warnf("peer: goodbye: connect: %v", err)
		return
	}
	if _, err := NewWalStreamClient(conn).GoodBye(ctx, &GoodByeRequest{NodeID: c.nodeID}); err != nil {
		logrus.Warnf("peer: goodbye: rpc: %v", err)
	}
}

// ForwardWrite sends a write operation to the leader and returns its response.
// This is a unary RPC over the same connection as the WAL stream.
func (c *Client) ForwardWrite(ctx context.Context, req *ForwardRequest) (*ForwardResponse, error) {
	conn, err := c.getConn()
	if err != nil {
		return nil, err
	}
	return NewWalStreamClient(conn).Forward(ctx, req)
}
