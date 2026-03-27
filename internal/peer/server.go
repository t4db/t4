package peer

import (
	"context"
	"sync"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/makhov/strata/internal/wal"
)

// Server is the leader-side WAL streaming + write-forwarding server.
//
// It maintains:
//   - A bounded ring buffer of recent entries for follower catch-up.
//   - A map of per-follower channels for live fan-out.
//   - A ForwardHandler that processes write RPCs forwarded by followers.
//
// Thread safety: Broadcast and Follow both hold mu.
type Server struct {
	mu             sync.Mutex
	buf            *entryBuffer
	followers      map[string]chan *wal.Entry
	forwardHandler ForwardHandler
}

// NewServer creates a Server with a ring buffer of capacity cap.
func NewServer(cap int) *Server {
	return &Server{
		buf:       newEntryBuffer(cap),
		followers: make(map[string]chan *wal.Entry),
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
	s.buf.push(e)
	for id, ch := range s.followers {
		select {
		case ch <- e:
		default:
			logrus.Warnf("peer: follower %q too slow — dropping entry rev=%d; it will reconnect", id, e.Revision)
		}
	}
	s.mu.Unlock()
}

// Follow implements WalStreamServer. It is called per follower connection.
func (s *Server) Follow(req *FollowRequest, stream WalStream_FollowServer) error {
	// Atomically snapshot the buffer and register the live channel.
	// Holding the lock here means Broadcast also blocks, so entries that arrive
	// during "snapshot + register" will be in the channel — no gap.
	s.mu.Lock()
	snapshot, ok := s.buf.since(req.FromRevision)
	if !ok {
		s.mu.Unlock()
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
		s.mu.Unlock()
	}()

	logrus.Infof("peer: follower %q connected (fromRev=%d, snapshot=%d entries)", req.NodeID, req.FromRevision, len(snapshot))

	for _, e := range snapshot {
		if err := stream.Send(EntryToMsg(e)); err != nil {
			return err
		}
	}

	for {
		select {
		case e := <-ch:
			if e.Revision <= maxSent {
				continue
			}
			maxSent = e.Revision
			if err := stream.Send(EntryToMsg(e)); err != nil {
				return err
			}
		case <-stream.Context().Done():
			logrus.Infof("peer: follower %q disconnected", req.NodeID)
			return stream.Context().Err()
		}
	}
}

// Forward implements WalStreamServer. It proxies a write from a follower to
// the ForwardHandler (the leader Node).
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
