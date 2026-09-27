package peer

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/t4db/t4/internal/metrics"
	"github.com/t4db/t4/internal/wal"
)

// ServerOptions returns the transport settings for the peer gRPC server.
//
// Keepalive: heartbeats keep a healthy connection busy, so pings go out only
// when the follower has gone quiet. They catch the case heartbeats cannot: a
// Follow loop blocked in Send because a partitioned follower stopped reading,
// where only closing the connection unblocks it.
//
// ConnectionTimeout bounds the handshake of a new connection. The default of
// 120 s lets a connection that never completes it, as through a partition,
// hold up Server.Stop, and with it a leader stepping down, for two minutes.
// 10 s leaves room for a TLS handshake over a slow link.
func ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    time.Second, // gRPC's minimum
			Timeout: HeartbeatTimeout,
		}),
		grpc.ConnectionTimeout(10 * time.Second),
	}
}

// errFollowerSilent ends a follower's stream when it stopped sending
// heartbeats; the disconnect then releases writes waiting for its ACK.
var errFollowerSilent = status.Error(codes.Unavailable, "follower heartbeat timeout")

// AckProgressTimeout is how long a follower may go without ACKing further
// while it has entries outstanding. Its heartbeats show that it is connected,
// not that it is keeping up: a follower whose disk stalls keeps repeating its
// last ACK. Dropping it releases writes waiting for it.
const AckProgressTimeout = 5 * time.Second

// errFollowerStalled ends the stream of a follower that stopped ACKing.
var errFollowerStalled = status.Error(codes.Unavailable, "follower stopped acknowledging")

// member is what the leader knows about a follower of its term, kept after
// the follower's stream ends.
type member struct {
	connected  bool
	heartbeats bool      // the current or last stream exchanges heartbeats
	lastHeard  time.Time // last heartbeat or ACK on the current stream; zero until the first
	departedAt time.Time // when the last stream ended; zero while connected
	graceful   bool      // the last stream ended with a GoodBye
	baseSeq    int64     // the follower had every entry up to this when its stream opened
	lastAck    int64     // highest sequence ACKed, kept after the stream ends
}

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
	pending         []*wal.Entry
	followers       map[string]chan *WalEntryMsg
	followerAckRevs map[string]int64 // last ACK'd sequence per follower
	maxBroadcastRev int64            // highest sequence sent via Broadcast
	forwardHandler  ForwardHandler

	// ackNotify is a buffered-1 channel. A non-blocking send is made whenever
	// any follower ACKs an entry or disconnects, waking WaitForFollowers.
	ackNotify chan struct{}

	// startRev is the first sequence this leader will ever write — i.e.
	// db.LastSequence()+1 at the moment becomeLeader ran.  A follower that
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

	// term is the leader's term, carried by heartbeats so that followers
	// know whose liveness they are tracking.
	term uint64

	// members tracks every follower that has streamed from this leader.
	members map[string]*member

	log peerLogger
}

// NewServer creates a Server with a ring buffer of capacity cap.
func NewServer(cap int, log peerLogger) *Server {
	if log == nil {
		log = stdlibPeerLogger{}
	}
	return &Server{
		buf:              newEntryBuffer(cap),
		followers:        make(map[string]chan *WalEntryMsg),
		followerAckRevs:  make(map[string]int64),
		ackNotify:        make(chan struct{}, 1),
		gracefulGoodbyes: make(map[string]struct{}),
		shutdownC:        make(chan struct{}),
		DisconnectC:      make(chan struct{}, 1),
		members:          make(map[string]*member),
		log:              log,
	}
}

// ConnectedFollowers returns the number of followers currently streaming
// from this leader.
func (s *Server) ConnectedFollowers() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.followers)
}

// SetStartRev records the first sequence this leader owns — callers pass
// db.LastSequence()+1 immediately after becomeLeader completes its S3
// replay.  Followers that connect with FromRevision < startRev are missing
// entries that will never appear in the ring buffer; they must re-sync.
func (s *Server) SetStartRev(rev int64) {
	s.mu.Lock()
	s.startRev = rev
	s.mu.Unlock()
}

// SetTerm records the leader's term, which heartbeats carry.
func (s *Server) SetTerm(term uint64) {
	s.mu.Lock()
	s.term = term
	s.mu.Unlock()
}

// SlowSafe reports whether every follower that may still consider this
// leader its own is demonstrably hearing it: each connected follower
// exchanges heartbeats and was heard within recent, and no follower's stream
// ended without a GoodBye within departedWindow. Followers only send while they hear the
// leader, so while SlowSafe holds none of them can be about to take over.
func (s *Server) SlowSafe(now time.Time, recent, departedWindow time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.members {
		if m.connected {
			if !m.heartbeats || m.lastHeard.IsZero() || now.Sub(m.lastHeard) > recent {
				return false
			}
		} else if !m.graceful && now.Sub(m.departedAt) < departedWindow {
			return false
		}
	}
	return true
}

// MaxSeqWithout returns the highest sequence that a follower of this leader
// may hold without holding seq: the ACK of each connected follower that has
// not ACKed seq, and the last ACK of each follower whose stream ended. It
// returns -1 if every follower that ever streamed has ACKed seq.
func (s *Server) MaxSeqWithout(seq int64) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	max := int64(-1)
	for id, m := range s.members {
		has := m.lastAck
		if m.baseSeq > has {
			has = m.baseSeq
		}
		if m.connected {
			if ack := s.followerAckRevs[id]; ack > has {
				has = ack
			}
			if has >= seq {
				continue
			}
		}
		if has > max {
			max = has
		}
	}
	return max
}

// WaitForAll waits until every follower connected at call time has ACKed seq
// or disconnected, or until timeout. It reports whether they all did.
func (s *Server) WaitForAll(ctx context.Context, seq int64, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		s.mu.Lock()
		done := true
		for id := range s.followers {
			if s.followerAckRevs[id] < seq {
				done = false
				break
			}
		}
		s.mu.Unlock()
		if done {
			return true
		}
		select {
		case <-s.ackNotify:
		case <-timer.C:
			return false
		case <-ctx.Done():
			return false
		}
	}
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
	s.pending = append(s.pending, e)
	var toKick []string
	for id, ch := range s.followers {
		select {
		case ch <- EntryToMsg(e):
		default:
			// Channel full: close it so Follow returns an error and the follower
			// reconnects from its last applied revision, re-fetching the gap
			// from the ring buffer. Silently dropping the entry and continuing
			// would leave the follower with a permanent hole.
			s.log.Warnf("peer: follower %q too slow — disconnecting to force resync at seq=%d", id, e.Sequence())
			toKick = append(toKick, id)
		}
	}
	for _, id := range toKick {
		close(s.followers[id])
		delete(s.followers, id)
	}
	s.mu.Unlock()
}

// BroadcastCommit tells followers that all entries up to rev are now
// committed by the leader and may be made visible locally.
func (s *Server) BroadcastCommit(startRev, rev int64) {
	s.mu.Lock()
	// Move only the committed revision range into the replay buffer.
	keep := s.pending[:0]
	for _, e := range s.pending {
		switch {
		case e.Sequence() < startRev:
			// An older uncommitted entry was superseded by a later committed
			// range. Drop it so reconnect snapshots never replay aborted writes.
		case e.Sequence() <= rev:
			s.buf.push(e)
			if e.Sequence() > s.maxBroadcastRev {
				s.maxBroadcastRev = e.Sequence()
			}
		default:
			keep = append(keep, e)
		}
	}
	s.pending = keep
	var toKick []string
	msg := &WalEntryMsg{Commit: true, CommitStartRevision: startRev, CommitRevision: rev}
	for id, ch := range s.followers {
		select {
		case ch <- msg:
		default:
			s.log.Warnf("peer: follower %q too slow for commit signal — disconnecting to force resync at rev=%d", id, rev)
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

// ReplicationSatisfiable reports whether the followers connected right now
// could meet mode's ACK target. When it returns false a committed write is
// durable only where the leader itself puts it: nothing is replicated, so
// WaitForFollowers will return without having proven anything.
//
// WaitNone is always satisfiable — the operator has explicitly opted out of
// replication durability, and it is not this call's job to override that.
func (s *Server) ReplicationSatisfiable(mode WaitMode) bool {
	if mode == WaitNone {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return requiredFollowerACKs(len(s.followers), mode) > 0
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

// MinFollowerAppliedRev returns the minimum ACK'd sequence across all currently
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
		s.log.Warnf("peer: follower %q needs resync (fromRev=%d < leaderStartRev=%d)",
			req.NodeID, req.FromRevision, s.startRev)
		metrics.FollowerResyncsTotal.WithLabelValues("behind_leader_start").Inc()
		return ErrResyncRequired
	}
	snapshot, ok := s.buf.since(req.FromRevision)
	if !ok {
		s.mu.Unlock()
		metrics.FollowerResyncsTotal.WithLabelValues("ring_buffer_miss").Inc()
		return ErrResyncRequired
	}
	ch := make(chan *WalEntryMsg, 512)
	s.followers[req.NodeID] = ch
	m := s.members[req.NodeID]
	if m == nil {
		m = &member{}
		s.members[req.NodeID] = m
	}
	*m = member{connected: true, heartbeats: req.Heartbeats, baseSeq: req.FromRevision - 1, lastAck: m.lastAck}
	term := s.term
	var maxSent int64
	if len(snapshot) > 0 {
		maxSent = snapshot[len(snapshot)-1].Sequence()
	} else {
		maxSent = req.FromRevision - 1
	}
	s.mu.Unlock()

	defer func() {
		owned := false
		graceful := false
		s.mu.Lock()
		if cur, ok := s.followers[req.NodeID]; ok && cur == ch {
			owned = true
			if ack := s.followerAckRevs[req.NodeID]; ack > m.lastAck {
				m.lastAck = ack
			}
			m.connected = false
			m.departedAt = time.Now()
			delete(s.followers, req.NodeID)
			delete(s.followerAckRevs, req.NodeID)
			if _, ok := s.gracefulGoodbyes[req.NodeID]; ok {
				delete(s.gracefulGoodbyes, req.NodeID)
				graceful = true
			}
			// A follower that said goodbye is shutting down and will not
			// take over, so it does not hold the leader in fast mode; it
			// still counts for the election fence.
			m.graceful = graceful
			// Only trigger split-brain fencing for unexpected disconnects.
			// A graceful GoodBye means the follower is shutting down intentionally
			// and will not attempt a TakeOver.
			if !graceful {
				select {
				case s.DisconnectC <- struct{}{}:
				default:
				}
			}
		}
		s.mu.Unlock()
		if owned {
			// Remove the lag metric so disconnected followers don't linger in dashboards.
			metrics.FollowerLag.DeleteLabelValues(req.NodeID)
			// Wake WaitForFollowers: this follower is no longer required.
			s.notifyACK()
		}
	}()

	s.log.Infof("peer: follower %q connected (fromRev=%d, snapshot=%d entries)", req.NodeID, req.FromRevision, len(snapshot))

	var lastHeard atomic.Int64 // unix nanos of the follower's last message
	lastHeard.Store(time.Now().UnixNano())

	// Spawn a goroutine to read ACK messages from the follower on the bidi
	// stream. The main goroutine continues sending WalEntryMsgs concurrently.
	// gRPC allows one goroutine to Send and another to Recv on the same stream.
	go func() {
		for {
			ack := new(AckMsg)
			if err := stream.RecvMsg(ack); err != nil {
				return // stream closed or context done
			}
			now := time.Now()
			lastHeard.Store(now.UnixNano())
			s.mu.Lock()
			if cur, ok := s.followers[req.NodeID]; !ok || cur != ch {
				s.mu.Unlock()
				return
			}
			m.lastHeard = now
			if ack.Revision > s.followerAckRevs[req.NodeID] {
				s.followerAckRevs[req.NodeID] = ack.Revision
			}
			lag := s.maxBroadcastRev - s.followerAckRevs[req.NodeID]
			if lag < 0 {
				lag = 0
			}
			s.mu.Unlock()
			metrics.FollowerLag.WithLabelValues(req.NodeID).Set(float64(lag))
			s.notifyACK()
		}
	}()

	// A follower that asked for heartbeats gets one at once, which tells it
	// this leader sends them, and then one every HeartbeatInterval. It
	// repeats its latest ACK as often, so silence means it is gone. The
	// ticker also drives the ACK progress check for every follower.
	tick := time.NewTicker(HeartbeatInterval)
	defer tick.Stop()
	if req.Heartbeats {
		if err := stream.Send(&WalEntryMsg{Heartbeat: true, Term: term}); err != nil {
			return err
		}
	}
	ackSeen, lastProgress := int64(-1), time.Now()

	for _, e := range snapshot {
		if err := stream.Send(EntryToMsg(e)); err != nil {
			return err
		}
	}
	if len(snapshot) > 0 {
		if err := stream.Send(&WalEntryMsg{
			Commit:              true,
			CommitStartRevision: snapshot[0].Sequence(),
			CommitRevision:      snapshot[len(snapshot)-1].Sequence(),
		}); err != nil {
			return err
		}
	}

	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				// Channel was closed by Broadcast because the follower was too
				// slow. Return a retriable error so the client reconnects and
				// re-fetches the missed entries from the ring buffer.
				return fmt.Errorf("follower stream closed: too slow, reconnect required")
			}
			// Invariant: non-commit messages are produced by EntryToMsg and
			// must carry a non-zero sequence ID.
			if !msg.Commit && msg.ID <= maxSent {
				continue
			}
			if !msg.Commit {
				maxSent = msg.ID
			}
			if err := stream.Send(msg); err != nil {
				return err
			}
		case now := <-tick.C:
			s.mu.Lock()
			ack := s.followerAckRevs[req.NodeID]
			s.mu.Unlock()
			if ack > ackSeen || ack >= maxSent {
				ackSeen, lastProgress = ack, now
			} else if stalled := now.Sub(lastProgress); stalled > AckProgressTimeout {
				s.log.Warnf("peer: follower %q made no ACK progress for %v (acked=%d, sent=%d) — closing its stream",
					req.NodeID, stalled.Round(time.Millisecond), ack, maxSent)
				return errFollowerStalled
			}
			if !req.Heartbeats {
				continue
			}
			if silent := now.Sub(time.Unix(0, lastHeard.Load())); silent > HeartbeatTimeout {
				s.log.Warnf("peer: no heartbeat from follower %q for %v — closing its stream", req.NodeID, silent.Round(time.Millisecond))
				return errFollowerSilent
			}
			if err := stream.Send(&WalEntryMsg{Heartbeat: true, Term: term}); err != nil {
				return err
			}
		case <-s.shutdownC:
			// Leader is shutting down gracefully. Send a shutdown signal to the
			// follower so it starts a TakeOver immediately.
			msg := &WalEntryMsg{Shutdown: true}
			_ = stream.Send(msg) // best-effort; follower will also detect stream close
			s.log.Infof("peer: sent shutdown signal to follower %q", req.NodeID)
			return nil
		case <-stream.Context().Done():
			s.log.Infof("peer: follower %q disconnected", req.NodeID)
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
	s.log.Infof("peer: follower %q sent goodbye (graceful shutdown)", req.NodeID)
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
		s.log.Infof("peer: broadcasting shutdown to %d follower(s)", len(s.followers))
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
	// fromRev is the next WAL sequence requested by the follower; the name is
	// retained to match the FollowRequest wire field.
	if len(b.entries) == 0 {
		return nil, true
	}
	minRev := b.entries[0].Sequence()
	if fromRev < minRev {
		return nil, false
	}
	for i, e := range b.entries {
		if e.Sequence() >= fromRev {
			out := make([]*wal.Entry, len(b.entries)-i)
			copy(out, b.entries[i:])
			return out, true
		}
	}
	return nil, true
}
