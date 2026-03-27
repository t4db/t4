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
		case <-time.After(2 * time.Second):
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
		e := MsgToEntry(msg)
		if err := applyFn(e); err != nil {
			return fromRev, err
		}
		fromRev = e.Revision + 1
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
