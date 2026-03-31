package peer

import (
	"context"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/makhov/strata/internal/wal"
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

// Follow streams WAL entries from the leader starting at fromRev, calling
// applyFn for each entry. fromRev advances automatically as entries are applied.
//
// Follow reconnects on transient errors. It returns:
//   - ctx.Err() on context cancellation.
//   - ErrResyncRequired when the leader's buffer no longer covers fromRev.
//   - ErrLeaderUnreachable after maxRetries consecutive connection failures.
//   - ErrLeaderShutdown when the leader sent a graceful shutdown signal.
func (c *Client) Follow(ctx context.Context, fromRev int64, applyFn func(wal.Entry) error) error {
	consecutiveFailures := 0
	for {
		nextRev, err := c.followOnce(ctx, fromRev, applyFn)

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
// Returns the next fromRev (last applied revision + 1) on any error.
func (c *Client) followOnce(ctx context.Context, fromRev int64, applyFn func(wal.Entry) error) (int64, error) {
	conn, err := c.getConn()
	if err != nil {
		return fromRev, err
	}

	stream, err := NewWalStreamClient(conn).Follow(ctx, &FollowRequest{
		FromRevision: fromRev,
		NodeID:       c.nodeID,
	})
	if err != nil {
		return fromRev, err
	}

	logrus.Infof("peer: connected to leader %s (fromRev=%d)", c.leaderAddr, fromRev)

	for {
		msg, err := stream.Recv()
		if err != nil {
			return fromRev, err
		}
		if msg.Shutdown {
			return fromRev, ErrLeaderShutdown
		}
		e := MsgToEntry(msg)
		if err := applyFn(e); err != nil {
			return fromRev, err
		}
		fromRev = e.Revision + 1
		// ACK after durable local WAL write (applyFn writes to WAL before Pebble).
		// Best-effort: a send error just causes reconnect, which is safe.
		if err := stream.SendAck(e.Revision); err != nil {
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
