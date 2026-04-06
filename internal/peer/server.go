package peer

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/strata-db/strata/internal/metrics"
	"github.com/strata-db/strata/internal/wal"
)

type WaitMode string

const (
	WaitNone   WaitMode = "none"
	WaitQuorum WaitMode = "quorum"
	WaitAll    WaitMode = "all"
)

// Server is the leader-side WAL streaming + write-forwarding server.
//
// It maintains:
//   - A bounded ring buffer of recent entries for follower catch-up.
//   - A map of per-follower channels for live fan-out.
//   - followerAckRevs tracking each follower's last ACK'd revision (quorum commit).
//   - A ForwardHandler that processes write RPCs forwarded by followers.
//   - A DisconnectC channel that receives a notification whenever any follower
//     disconnects unexpectedly. Graceful disconnects (preceded by a GoodBye RPC)
//     do not signal DisconnectC because there is no split-brain risk from a
//     follower that voluntarily shut down.
//
// Thread safety: Broadcast and Follow both hold mu.
type Server struct {
	mu              sync.Mutex
	buf             *entryBuffer
	followers       map[string]chan *wal.Entry
	followerAckRevs map[string]int64 // last ACK'd revision per follower
	maxBroadcastRev int64            // highest revision sent via Broadcast
	forwardHandler  ForwardHandler

	// ackNotify is a buffered-1 channel. A non-blocking send is made whenever
	// any follower ACKs an entry or disconnects, waking WaitForFollowers.
	ackNotify chan struct{}

	// startRev is the first revision this leader will ever write — i.e.
	// db.CurrentRevision()+1 at the moment becomeLeader ran.  A follower that
	// connects with FromRevision < startRev has missed entries that are only
	// in S3 (never in this leader's ring buffer) and must re-sync from S3
	// before it can consume the live stream.
	startRev int64

	// gracefulGoodbyes tracks followers that sent a GoodBye RPC before
	// disconnecting. Their stream disconnect will not trigger DisconnectC.
	gracefulGoodbyes map[string]struct{}

	// shutdownC is closed by BroadcastShutdown to signal all active Follow
	// loops that the leader is shutting down gracefully.
	shutdownC chan struct{}

	// DisconnectC receives a struct{} whenever any follower disconnects
	// unexpectedly (i.e., without a prior GoodBye). The leader uses this to
	// immediately fence writes and check the S3 lock. Capacity 1 so sends
	// never block and rapid-fire disconnects coalesce into a single check.
	DisconnectC chan struct{}
}

// NewServer creates a Server with a ring buffer of capacity cap.
func NewServer(cap int) *Server {
	return &Server{
		buf:              newEntryBuffer(cap),
		followers:        make(map[string]chan *wal.Entry),
		followerAckRevs:  make(map[string]int64),
		ackNotify:        make(chan struct{}, 1),
		gracefulGoodbyes: make(map[string]struct{}),
		shutdownC:        make(chan struct{}),
		DisconnectC:      make(chan struct{}, 1),
	}
}

// ConnectedFollowers returns the number of followers currently streaming
// from this leader.
func (s *Server) ConnectedFollowers() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.followers)
}

// SetStartRev records the first revision this leader owns — callers pass
// db.CurrentRevision()+1 immediately after becomeLeader completes its S3
// replay.  Followers that connect with FromRevision < startRev are missing
// entries that will never appear in the ring buffer; they must re-sync.
func (s *Server) SetStartRev(rev int64) {
	s.mu.Lock()
	s.startRev = rev
	s.mu.Unlock()
}

// SetForwardHandler registers the handler that processes forwarded writes.
// Must be called before the gRPC server starts accepting connections.
func (s *Server) SetForwardHandler(h ForwardHandler) {
	s.mu.Lock()
	s.forwardHandler = h
	s.mu.Unlock()
}

// Broadcast appends e to the buffer and fans it out to all connected followers.
// Called by the leader after every successful appendAndApply.
func (s *Server) Broadcast(e *wal.Entry) {
	s.mu.Lock()
	s.buf.push(e)
	if e.Revision > s.maxBroadcastRev {
		s.maxBroadcastRev = e.Revision
	}
	var toKick []string
	for id, ch := range s.followers {
		select {
		case ch <- e:
		default:
			// Channel full: close it so Follow returns an error and the follower
			// reconnects from its last applied revision, re-fetching the gap
			// from the ring buffer. Silently dropping the entry and continuing
			// would leave the follower with a permanent hole.
			logrus.Warnf("peer: follower %q too slow — disconnecting to force resync at rev=%d", id, e.Revision)
			toKick = append(toKick, id)
		}
	}
	for _, id := range toKick {
		close(s.followers[id])
		delete(s.followers, id)
	}
	s.mu.Unlock()
}

// notifyACK wakes any goroutine waiting in WaitForFollowers.
func (s *Server) notifyACK() {
	select {
	case s.ackNotify <- struct{}{}:
	default:
	}
}

// WaitForFollowers blocks until enough followers connected at call time have
// ACK'd a revision >= rev according to mode, or until all remaining candidates
// disconnect. New followers that connect after this call are not included.
//
// Returns ctx.Err() if the context is cancelled before quorum is reached.
// Returns nil immediately if no followers are connected.
//
// This is called by the commitLoop after WAL.AppendBatch and before db.Apply
// to implement quorum commit: the leader only commits to Pebble once a majority
// has the entry durably in their WAL.
func (s *Server) WaitForFollowers(ctx context.Context, rev int64, mode WaitMode) error {
	// Snapshot which followers must ACK this revision.
	s.mu.Lock()
	if len(s.followers) == 0 {
		s.mu.Unlock()
		return nil
	}
	target := requiredFollowerACKs(len(s.followers), mode)
	required := make(map[string]struct{}, len(s.followers))
	for id := range s.followers {
		required[id] = struct{}{}
	}
	s.mu.Unlock()
	if target == 0 {
		return nil
	}

	for {
		s.mu.Lock()
		acked := 0
		pending := 0
		for id := range required {
			if _, connected := s.followers[id]; connected {
				if s.followerAckRevs[id] >= rev {
					acked++
					if acked >= target {
						s.mu.Unlock()
						return nil
					}
				} else {
					pending++
				}
			}
			// If the follower disconnected, it's no longer required.
		}
		s.mu.Unlock()

		if acked >= target || pending == 0 {
			return nil
		}
		select {
		case <-s.ackNotify:
			// Something changed — re-check.
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func requiredFollowerACKs(connected int, mode WaitMode) int {
	switch mode {
	case WaitNone:
		return 0
	case WaitAll:
		return connected
	case WaitQuorum, "":
		// Majority of the current cluster, counting the leader as already durable.
		return (connected + 1) / 2
	default:
		return (connected + 1) / 2
	}
}

// MinFollowerAppliedRev returns the minimum ACK'd revision across all currently
// connected followers. Used by the leader to determine the safe WAL GC boundary:
// WAL segments are only deleted once all connected followers have applied them.
//
// Returns math.MaxInt64 if no followers are connected (leader can GC freely).
func (s *Server) MinFollowerAppliedRev() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.followers) == 0 {
		return math.MaxInt64
	}
	min := int64(math.MaxInt64)
	for id := range s.followers {
		if rev := s.followerAckRevs[id]; rev < min {
			min = rev
		}
	}
	return min
}

func (s *Server) Follow(req *FollowRequest, stream WalStream_FollowServer) error {
	// Atomically snapshot the buffer and register the live channel.
	// Holding the lock here means Broadcast also blocks, so entries that arrive
	// during "snapshot + register" will be in the channel — no gap.
	s.mu.Lock()
	// A follower whose FromRevision is below startRev has missed entries that
	// were committed by a prior leader and replayed from S3 by this leader —
	// those entries are in Pebble but will never appear in the ring buffer.
	// The follower must re-sync from S3 before it can consume the live stream.
	if s.startRev > 0 && req.FromRevision < s.startRev {
		s.mu.Unlock()
		logrus.Warnf("peer: follower %q needs resync (fromRev=%d < leaderStartRev=%d)",
			req.NodeID, req.FromRevision, s.startRev)
		if metrics.FollowerResyncsTotal != nil {
			metrics.FollowerResyncsTotal.WithLabelValues("behind_leader_start").Inc()
		}
		return ErrResyncRequired
	}
	snapshot, ok := s.buf.since(req.FromRevision)
	if !ok {
		s.mu.Unlock()
		if metrics.FollowerResyncsTotal != nil {
			metrics.FollowerResyncsTotal.WithLabelValues("ring_buffer_miss").Inc()
		}
		return ErrResyncRequired
	}
	ch := make(chan *wal.Entry, 512)
	s.followers[req.NodeID] = ch
	var maxSent int64
	if len(snapshot) > 0 {
		maxSent = snapshot[len(snapshot)-1].Revision
	} else {
		maxSent = req.FromRevision - 1
	}
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.followers, req.NodeID)
		delete(s.followerAckRevs, req.NodeID)
		graceful := false
		if _, ok := s.gracefulGoodbyes[req.NodeID]; ok {
			delete(s.gracefulGoodbyes, req.NodeID)
			graceful = true
		}
		// Only trigger split-brain fencing for unexpected disconnects.
		// A graceful GoodBye means the follower is shutting down intentionally
		// and will not attempt a TakeOver.
		if !graceful {
			select {
			case s.DisconnectC <- struct{}{}:
			default:
			}
		}
		s.mu.Unlock()
		// Remove the lag metric so disconnected followers don't linger in dashboards.
		if metrics.FollowerLag != nil {
			metrics.FollowerLag.DeleteLabelValues(req.NodeID)
		}
		// Wake WaitForFollowers: this follower is no longer required.
		s.notifyACK()
	}()

	logrus.Infof("peer: follower %q connected (fromRev=%d, snapshot=%d entries)", req.NodeID, req.FromRevision, len(snapshot))

	// Spawn a goroutine to read ACK messages from the follower on the bidi
	// stream. The main goroutine continues sending WalEntryMsgs concurrently.
	// gRPC allows one goroutine to Send and another to Recv on the same stream.
	go func() {
		for {
			ack := new(AckMsg)
			if err := stream.RecvMsg(ack); err != nil {
				return // stream closed or context done
			}
			s.mu.Lock()
			if ack.Revision > s.followerAckRevs[req.NodeID] {
				s.followerAckRevs[req.NodeID] = ack.Revision
			}
			lag := s.maxBroadcastRev - s.followerAckRevs[req.NodeID]
			if lag < 0 {
				lag = 0
			}
			s.mu.Unlock()
			if metrics.FollowerLag != nil {
				metrics.FollowerLag.WithLabelValues(req.NodeID).Set(float64(lag))
			}
			s.notifyACK()
		}
	}()

	for _, e := range snapshot {
		if err := stream.Send(EntryToMsg(e)); err != nil {
			return err
		}
	}

	for {
		select {
		case e, ok := <-ch:
			if !ok {
				// Channel was closed by Broadcast because the follower was too
				// slow. Return a retriable error so the client reconnects and
				// re-fetches the missed entries from the ring buffer.
				return fmt.Errorf("follower stream closed: too slow, reconnect required")
			}
			if e.Revision <= maxSent {
				continue
			}
			maxSent = e.Revision
			if err := stream.Send(EntryToMsg(e)); err != nil {
				return err
			}
		case <-s.shutdownC:
			// Leader is shutting down gracefully. Send a shutdown signal to the
			// follower so it starts a TakeOver immediately.
			msg := &WalEntryMsg{Shutdown: true}
			_ = stream.Send(msg) // best-effort; follower will also detect stream close
			logrus.Infof("peer: sent shutdown signal to follower %q", req.NodeID)
			return nil
		case <-stream.Context().Done():
			logrus.Infof("peer: follower %q disconnected", req.NodeID)
			return stream.Context().Err()
		}
	}
}

// GoodBye implements WalStreamServer. Called by a follower before graceful
// shutdown. Recording the nodeID here prevents the subsequent stream disconnect
// from triggering split-brain fencing machinery.
func (s *Server) GoodBye(_ context.Context, req *GoodByeRequest) (*GoodByeResponse, error) {
	s.mu.Lock()
	s.gracefulGoodbyes[req.NodeID] = struct{}{}
	s.mu.Unlock()
	logrus.Infof("peer: follower %q sent goodbye (graceful shutdown)", req.NodeID)
	return &GoodByeResponse{}, nil
}

// BroadcastShutdown sends a shutdown signal to all connected followers so they
// start a TakeOver election immediately without waiting for retry exhaustion.
// Called by the leader during graceful shutdown, before stopping the gRPC server.
func (s *Server) BroadcastShutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-s.shutdownC:
		// already closed
	default:
		close(s.shutdownC)
		logrus.Infof("peer: broadcasting shutdown to %d follower(s)", len(s.followers))
	}
}
func (s *Server) Forward(ctx context.Context, req *ForwardRequest) (*ForwardResponse, error) {
	s.mu.Lock()
	h := s.forwardHandler
	s.mu.Unlock()
	if h == nil {
		return nil, status.Error(codes.Unavailable, "leader not ready")
	}
	return h.HandleForward(ctx, req)
}

// ── entry ring buffer ─────────────────────────────────────────────────────────

type entryBuffer struct {
	entries []*wal.Entry
	cap     int
}

func newEntryBuffer(cap int) *entryBuffer { return &entryBuffer{cap: cap} }

func (b *entryBuffer) push(e *wal.Entry) {
	b.entries = append(b.entries, e)
	if len(b.entries) > b.cap {
		b.entries = b.entries[len(b.entries)-b.cap:]
	}
}

func (b *entryBuffer) since(fromRev int64) ([]*wal.Entry, bool) {
	if len(b.entries) == 0 {
		return nil, true
	}
	minRev := b.entries[0].Revision
	if fromRev < minRev {
		return nil, false
	}
	for i, e := range b.entries {
		if e.Revision >= fromRev {
			out := make([]*wal.Entry, len(b.entries)-i)
			copy(out, b.entries[i:])
			return out, true
		}
	}
	return nil, true
}
