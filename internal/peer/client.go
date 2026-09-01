package peer

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/trace"
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
	tp         trace.TracerProvider             // nil = inherit the global provider

	connMu sync.Mutex
	conn   *grpc.ClientConn // lazily initialised; nil after Close

	log peerLogger
}

// NewClient creates a Client that will connect to leaderAddr.
// maxRetries is the number of consecutive connection failures before Follow
// returns ErrLeaderUnreachable. Use 0 for unlimited retries.
// tlsCreds may be nil for plaintext (only safe on a trusted network).
// tp propagates trace context on forwarded writes so a client span connects
// through to the leader's commit spans; nil inherits the global provider,
// which is a no-op when none is configured.
func NewClient(leaderAddr, nodeID string, maxRetries int, tlsCreds credentials.TransportCredentials, log peerLogger, tp trace.TracerProvider) *Client {
	if log == nil {
		log = stdlibPeerLogger{}
	}
	return &Client{leaderAddr: leaderAddr, nodeID: nodeID, maxRetries: maxRetries, tlsCreds: tlsCreds, tp: tp, log: log}
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
	var otelOpts []otelgrpc.Option
	if c.tp != nil {
		otelOpts = append(otelOpts, otelgrpc.WithTracerProvider(c.tp))
	}
	conn, err := grpc.NewClient(
		c.leaderAddr,
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(Codec{})),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler(otelOpts...)),
	)
	if err != nil {
		return nil, err
	}
	c.conn = conn
	return conn, nil
}

// Follow streams WAL entries from the leader starting at fromRev. Despite the
// historical name, fromRev is the next WAL sequence to request. The leader
// may send entry messages ahead of its own local WAL fsync; followers stage
// those entries in memory and only make them durable/visible after a matching
// commit message arrives. On commit, the follower appends the committed batch
// to its WAL, ACKs the highest committed sequence back to the leader, then
// applies the batch locally.
//
// Follow reconnects on transient errors. It returns:
//   - ctx.Err() on context cancellation.
//   - ErrResyncRequired when the leader's buffer no longer covers fromRev.
//   - ErrLeaderUnreachable after maxRetries consecutive connection failures.
//   - ErrLeaderShutdown when the leader sent a graceful shutdown signal.
func (c *Client) Follow(ctx context.Context, fromRev int64, walFn func([]wal.Entry) error, applyFn func([]wal.Entry) error) error {
	consecutiveFailures := 0
	for {
		nextSeq, err := c.followOnce(ctx, fromRev, walFn, applyFn)

		if ctx.Err() != nil {
			return ctx.Err()
		}
		if IsResyncRequired(err) {
			c.log.Errorf("peer: leader requires resync from rev=%d: %v", fromRev, err)
			return err
		}
		// Leader is shutting down: skip retry wait and signal caller to elect now.
		if IsLeaderShutdown(err) {
			c.log.Infof("peer: leader sent graceful shutdown — starting election immediately")
			return err
		}

		if nextSeq > fromRev {
			consecutiveFailures = 0
		} else {
			consecutiveFailures++
		}
		fromRev = nextSeq

		if c.maxRetries > 0 && consecutiveFailures >= c.maxRetries {
			c.log.Debugf("peer: leader unreachable after %d attempts", consecutiveFailures)
			return ErrLeaderUnreachable
		}

		c.log.Debugf("peer: stream error (attempt %d): %v", consecutiveFailures, err)
		select {
		case <-time.After(FollowerRetryInterval):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// followOnce makes one streaming attempt using the shared connection.
// walFn must durably append a committed batch to the follower's local WAL.
// applyFn updates follower local state after the ACK has been sent.
// Returns the next fromRev (highest committed sequence + 1) on any error.
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

	c.log.Infof("peer: connected to leader %s (fromRev=%d)", c.leaderAddr, fromRev)

	// msgC buffers entry and commit messages received from the stream.
	msgC := make(chan *WalEntryMsg, 512)
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
			case msgC <- msg:
			case <-streamCtx.Done():
				recvErrC <- streamCtx.Err()
				return
			}
		}
	}()

	var staged []wal.Entry
	for {
		// Block until at least one message or an error.
		var msg *WalEntryMsg
		select {
		case msg = <-msgC:
		case err := <-recvErrC:
			return fromRev, err
		}

		// Drain any additional messages already buffered so we can process
		// one or more commit notifications in a single pass.
		msgs := []*WalEntryMsg{msg}
	drain:
		for {
			select {
			case msg = <-msgC:
				msgs = append(msgs, msg)
			default:
				break drain
			}
		}

		for _, msg := range msgs {
			if !msg.Commit {
				staged = append(staged, MsgToEntry(msg))
				continue
			}
			startRev := msg.CommitStartRevision
			if startRev == 0 {
				startRev = msg.CommitRevision
			}
			if msg.CommitRevision < fromRev {
				continue
			}
			drop := 0
			for drop < len(staged) && staged[drop].Sequence() < startRev {
				drop++
			}
			if drop > 0 {
				staged = staged[drop:]
			}

			cut := 0
			for cut < len(staged) && staged[cut].Sequence() <= msg.CommitRevision {
				cut++
			}
			if cut == 0 || staged[0].Sequence() != startRev || staged[cut-1].Sequence() != msg.CommitRevision {
				return fromRev, ErrResyncRequired
			}
			batch := staged[:cut]
			batchStartRev := startRev
			if batch[0].Sequence() != batchStartRev {
				return fromRev, ErrResyncRequired
			}
			for i, e := range batch {
				if e.Sequence() != batchStartRev+int64(i) {
					return fromRev, ErrResyncRequired
				}
			}
			if err := walFn(batch); err != nil {
				return batchStartRev, err
			}
			if batch[len(batch)-1].Sequence()+1 > fromRev {
				fromRev = batch[len(batch)-1].Sequence() + 1
			}
			if err := stream.SendAck(batch[len(batch)-1].Sequence()); err != nil {
				return fromRev, err
			}
			if err := applyFn(batch); err != nil {
				return fromRev, err
			}
			staged = staged[cut:]
		}
	}
}

// GoodBye notifies the leader that this follower is shutting down gracefully.
// The leader will skip split-brain fencing when this follower's stream closes.
// Best-effort: errors are logged but not returned.
func (c *Client) GoodBye(ctx context.Context) {
	conn, err := c.getConn()
	if err != nil {
		c.log.Warnf("peer: goodbye: connect: %v", err)
		return
	}
	if _, err := NewWalStreamClient(conn).GoodBye(ctx, &GoodByeRequest{NodeID: c.nodeID}); err != nil {
		c.log.Warnf("peer: goodbye: rpc: %v", err)
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
