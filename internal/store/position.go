package store

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
)

// Positions (docs/design/takeover-fence.md). A node's position in the log is
// the sequence of its last applied entry and that entry's term. The store
// keeps two: the position of everything applied (lastSeq, lastTerm), and the
// node's leader-known position, advanced only by entries a leader streamed to
// it or it wrote as leader (Apply), never by object-storage catch-up (Recover)
// or a checkpoint restore. A takeover candidate is fenced by the latter.

// Position is a point in the log: an entry's term and sequence. Positions
// order by term, then sequence.
type Position struct {
	Term uint64
	Seq  int64
}

// Less reports whether p is before q.
func (p Position) Less(q Position) bool {
	if p.Term != q.Term {
		return p.Term < q.Term
	}
	return p.Seq < q.Seq
}

var metaLastTermKey = []byte{prefixMeta, 't', 'e', 'r', 'm'}

// streamPosKey is the meta key of nodeID's leader-known position. It is keyed
// by node ID so that a checkpoint written by another node, which carries that
// node's key, does not give a restoring node a position it never received.
func streamPosKey(nodeID string) []byte {
	return append([]byte{prefixMeta, 's', 'p', '/'}, nodeID...)
}

func encodePosition(p Position) []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b[:8], p.Term)
	binary.BigEndian.PutUint64(b[8:], uint64(p.Seq))
	return b
}

func decodePosition(b []byte) (Position, error) {
	if len(b) != 16 {
		return Position{}, fmt.Errorf("store: position is %d bytes, want 16", len(b))
	}
	return Position{Term: binary.BigEndian.Uint64(b[:8]), Seq: int64(binary.BigEndian.Uint64(b[8:]))}, nil
}

// LastPosition returns the position of the last applied entry.
func (s *Store) LastPosition() Position {
	// Sequence first: the term read after it is at least the term of the
	// entry at that sequence.
	seq := atomic.LoadInt64(&s.lastSeq)
	return Position{Term: s.lastTerm.Load(), Seq: seq}
}

// SetNodeID makes Apply record the leader-known position of nodeID, and loads
// it. Until it is called, Apply records none and LeaderKnownPosition is zero.
func (s *Store) SetNodeID(nodeID string) error {
	key := streamPosKey(nodeID)
	pos := Position{}
	v, closer, err := s.db.Get(key)
	switch {
	case err == nil:
		pos, err = decodePosition(v)
		_ = closer.Close()
		if err != nil {
			return err
		}
	case err != pebble.ErrNotFound:
		return fmt.Errorf("store: read leader-known position: %w", err)
	}
	s.posMu.Lock()
	s.streamKey, s.streamPos = key, pos
	s.posMu.Unlock()
	return nil
}

// LeaderKnownPosition returns the position of the last entry this node applied
// from a leader's stream or wrote as leader.
func (s *Store) LeaderKnownPosition() Position {
	s.posMu.Lock()
	defer s.posMu.Unlock()
	return s.streamPos
}

// SetLeaderKnownPosition overwrites the leader-known position, durably. It is
// for keeping a node's own position across a checkpoint restore, which
// replaces the store underneath it.
func (s *Store) SetLeaderKnownPosition(p Position) error {
	s.posMu.Lock()
	defer s.posMu.Unlock()
	if s.streamKey == nil {
		return fmt.Errorf("store: leader-known position without a node ID")
	}
	if err := s.db.Set(s.streamKey, encodePosition(p), pebble.Sync); err != nil {
		return fmt.Errorf("store: write leader-known position: %w", err)
	}
	s.streamPos = p
	return nil
}

// recordPositions adds the meta writes for a batch whose last entry is at tip:
// the last applied term when tip is the new end of the log, and, for entries a
// leader streamed or wrote (fromLeader), the leader-known position. It returns
// the leader-known position to publish once the batch commits, or ok false.
func (s *Store) recordPositions(b *pebble.Batch, tip Position, fromLeader bool) (known Position, ok bool, err error) {
	if tip.Seq >= atomic.LoadInt64(&s.lastSeq) {
		if err := b.Set(metaLastTermKey, encodeUint64(tip.Term), pebble.NoSync); err != nil {
			return Position{}, false, fmt.Errorf("store: set last term: %w", err)
		}
	}
	if !fromLeader {
		return Position{}, false, nil
	}
	s.posMu.Lock()
	key, cur := s.streamKey, s.streamPos
	s.posMu.Unlock()
	if key == nil || !cur.Less(tip) {
		return Position{}, false, nil
	}
	if err := b.Set(key, encodePosition(tip), pebble.NoSync); err != nil {
		return Position{}, false, fmt.Errorf("store: set leader-known position: %w", err)
	}
	return tip, true, nil
}

func encodeUint64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}
