package peer

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

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

// HeartbeatInterval is how often the leader and each follower send each other
// a heartbeat on the Follow stream when both support it.
const HeartbeatInterval = 500 * time.Millisecond

// HeartbeatTimeout is how long either side of a Follow stream waits without
// hearing anything from the other before it gives up on the stream. It
// catches partitions that drop packets without breaking the connection.
const HeartbeatTimeout = 4 * HeartbeatInterval

// errLeaderSilent ends a Follow attempt when the leader stopped sending
// heartbeats; the follower retries like after any other stream failure.
var errLeaderSilent = status.Error(codes.Unavailable, "leader heartbeat timeout")

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

	// leaderGone, if set, is consulted after each failed Follow attempt; see
	// SetLeaderGoneCheck.
	leaderGone func(context.Context) bool

	// leaderHeartbeats is set once the leader has sent a heartbeat. From
	// then on every Follow attempt expects them from the start, so an
	// attempt on a dead connection times out instead of hanging.
	leaderHeartbeats atomic.Bool

	// heardTerm and heardNano record the leader's term and when (local
	// clock) this follower last received a heartbeat from it.
	heardTerm atomic.Uint64
	heardNano atomic.Int64

	log peerLogger
}

// NewClient creates a Client that will connect to leaderAddr.
// maxRetries is the number of consecutive connection failures before Follow
// returns ErrLeaderUnreachable. Use 0 for unlimited retries.
// tlsCreds may be nil for plaintext (only safe on a trusted network).
// tp propagates trace context on forwarded writes so a client span connects
// through to the leader's commit spans. A nil tp installs no stats handler:
// otelgrpc tags every RPC whether or not anything is sampled, and a follower
// forwards every write through this client. Pass otel.GetTracerProvider()
// explicitly to inherit the global provider.
func NewClient(leaderAddr, nodeID string, maxRetries int, tlsCreds credentials.TransportCredentials, log peerLogger, tp trace.TracerProvider) *Client {
	if log == nil {
		log = stdlibPeerLogger{}
	}
	return &Client{leaderAddr: leaderAddr, nodeID: nodeID, maxRetries: maxRetries, tlsCreds: tlsCreds, tp: tp, log: log}
}

// LastHeard returns the term of the leader this client last received a
// heartbeat from and when. term is 0 if it never received one.
func (c *Client) LastHeard() (term uint64, at time.Time) {
	nano := c.heardNano.Load()
	if nano == 0 {
		return 0, time.Time{}
	}
	return c.heardTerm.Load(), time.Unix(0, nano)
}

// SetLastHeard seeds LastHeard, for a client that replaces another one
// following the same leader.
func (c *Client) SetLastHeard(term uint64, at time.Time) {
	c.heardTerm.Store(term)
	c.heardNano.Store(at.UnixNano())
}

// SetLeaderGoneCheck installs a check that Follow runs after each failed
// attempt. When it reports that the leader has left for good, Follow returns
// ErrLeaderShutdown at once instead of retrying a dead address. Must not be
// called while Follow runs.
func (c *Client) SetLeaderGoneCheck(f func(context.Context) bool) {
	c.leaderGone = f
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

// resetConn drops the shared connection so the next call dials afresh. Used
// when the connection has gone silent: gRPC may not notice that it is dead.
func (c *Client) resetConn() {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if c.conn != nil {
		_ = c.conn.Close()
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
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(Codec{})),
	}
	if c.tp != nil {
		dialOpts = append(dialOpts, grpc.WithStatsHandler(
			otelgrpc.NewClientHandler(otelgrpc.WithTracerProvider(c.tp))))
	}
	conn, err := grpc.NewClient(c.leaderAddr, dialOpts...)
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

		if consecutiveFailures > 0 && c.leaderGone != nil && c.leaderGone(ctx) {
			c.log.Infof("peer: leader %s has left — starting election immediately", c.leaderAddr)
			return ErrLeaderShutdown
		}

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

	// msgC buffers entry and commit messages received from the stream.
	msgC := make(chan *WalEntryMsg, 512)

	// Once this leader is known to send heartbeats, a watchdog cancels the
	// attempt when it goes silent. It runs from before the stream opens:
	// on a dead connection opening the stream can block too.
	var lastRecv atomic.Int64 // unix nanos of the last message from the leader; 0 until the first
	streamStart := time.Now()
	var silent atomic.Bool
	go func() {
		t := time.NewTicker(HeartbeatInterval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				// Buffered messages mean the leader was heard; the main
				// loop below just has not caught up with them yet.
				ref := streamStart
				if heard := lastRecv.Load(); heard != 0 {
					ref = time.Unix(0, heard)
				}
				if c.leaderHeartbeats.Load() && len(msgC) == 0 && time.Since(ref) > HeartbeatTimeout {
					silent.Store(true)
					streamCancel()
					return
				}
			case <-streamCtx.Done():
				return
			}
		}
	}()
	// silenced turns an error caused by the watchdog into errLeaderSilent
	// and drops the connection, which gRPC may not know is dead.
	silenced := func(err error) error {
		if !silent.Load() || ctx.Err() != nil {
			return err
		}
		c.log.Warnf("peer: no heartbeat from leader %s for %v — reconnecting", c.leaderAddr, HeartbeatTimeout)
		c.resetConn()
		return errLeaderSilent
	}

	stream, err := NewWalStreamClient(conn).Follow(streamCtx, &FollowRequest{
		FromRevision: fromRev,
		NodeID:       c.nodeID,
		Heartbeats:   true,
	})
	if err != nil {
		return fromRev, silenced(err)
	}

	c.log.Infof("peer: connected to leader %s (fromRev=%d)", c.leaderAddr, fromRev)

	// ACKs double as this follower's heartbeats: every HeartbeatInterval it
	// repeats its latest ACK, which the leader treats as a no-op. It sends
	// them only while it hears the leader, so the leader hearing this
	// follower proves the follower heard it less than HeartbeatTimeout
	// earlier; the leader's lease relies on that. gRPC forbids concurrent
	// sends on one stream, hence sendMu.
	var sendMu sync.Mutex
	var ackedSeq atomic.Int64
	ackedSeq.Store(fromRev - 1)
	sendAck := func(seq int64) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.SendAck(seq)
	}
	go func() {
		t := time.NewTicker(HeartbeatInterval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				heard := lastRecv.Load()
				if heard == 0 || time.Since(time.Unix(0, heard)) >= HeartbeatTimeout {
					continue
				}
				if err := sendAck(ackedSeq.Load()); err != nil {
					return
				}
			case <-streamCtx.Done():
				return
			}
		}
	}()

	recvErrC := make(chan error, 1)
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				recvErrC <- err
				return
			}
			lastRecv.Store(time.Now().UnixNano())
			if msg.Shutdown {
				recvErrC <- ErrLeaderShutdown
				return
			}
			if msg.Heartbeat {
				c.leaderHeartbeats.Store(true)
				c.heardTerm.Store(msg.Term)
				c.heardNano.Store(time.Now().UnixNano())
				continue
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
			return fromRev, silenced(err)
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
			if err := sendAck(batch[len(batch)-1].Sequence()); err != nil {
				return fromRev, silenced(err)
			}
			ackedSeq.Store(batch[len(batch)-1].Sequence())
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
