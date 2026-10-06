package store

import (
	"sort"

	"github.com/cockroachdb/pebble"
)

// Pinned reads served from the history ring. Each builds the same undo map
// undoAfter builds from the Pebble log, but from memory: the ring merge
// costs O(changed keys since the pinned revision) and each changed key then
// needs one point lookup for its pre-pin record — versus scanning every log
// record written since the pin. Coverage gaps (ring disabled, evicted,
// corrupt anchor) report errUndoChain, which callers already treat as "use
// the slower path".

// undoFromHist builds the undo map for a read pinned at rev from the history
// ring, filtered to prefix and fromKey.
func (s *Store) undoFromHist(snap pebble.Reader, prefix, fromKey string, rev int64) (map[string]*KeyValue, error) {
	ring := s.hist.Load()
	if ring == nil {
		return nil, errUndoChain
	}
	merged, ok := ring.changesSince(prefix, fromKey, rev)
	if !ok {
		return nil, errUndoChain
	}
	undo := make(map[string]*KeyValue, len(merged))
	lk := make([]byte, logKeyScratchSize)
	for key, c := range merged {
		prev, err := histStateAt(snap, lk, c, rev)
		if err != nil {
			return nil, err
		}
		undo[key] = prev
	}
	return undo, nil
}

// histStateAt returns the state at rev of the key whose first change after
// rev is c: nil when c created it, else the record c anchors to.
func histStateAt(snap pebble.Reader, lk []byte, c histChange, rev int64) (*KeyValue, error) {
	if c.prevRev == 0 {
		if !c.create {
			return nil, errUndoChain
		}
		return nil, nil
	}
	if c.prevRev > rev {
		// By construction the earliest change after rev anchors at or
		// below rev; treat a violation as corruption and unwind.
		return nil, errUndoChain
	}
	prev, err := logEntryAt(snap, lk, c.key, c.prevRev)
	if err != nil || prev == nil {
		return nil, errUndoChain
	}
	return prev, nil
}

func (s *Store) listAtFromHist(prefix string, opts ReadOptions, rev int64) ([]*KeyValue, error) {
	snap := s.db.NewSnapshot()
	defer func() { _ = snap.Close() }()
	undo, err := s.undoFromHist(snap, prefix, opts.FromKey, rev)
	if err != nil {
		return nil, err
	}
	return listFromUndo(snap, prefix, opts, undo)
}

func (s *Store) countAtFromHist(prefix, fromKey string, rev int64) (int64, error) {
	snap := s.db.NewSnapshot()
	defer func() { _ = snap.Close() }()
	undo, err := s.undoFromHist(snap, prefix, fromKey, rev)
	if err != nil {
		return 0, err
	}
	return countFromUndo(snap, prefix, fromKey, undo)
}

func (s *Store) getAtFromHist(key string, rev int64) (*KeyValue, error) {
	ring := s.hist.Load()
	if ring == nil {
		return nil, errUndoChain
	}
	snap := s.db.NewSnapshot()
	defer func() { _ = snap.Close() }()

	ie, err := idxEntryFrom(snap, key)
	if err != nil {
		return nil, err
	}
	if cur := ie.rev; cur != 0 && cur <= rev {
		// Unchanged since rev.
		return logEntryForIdx(snap, make([]byte, logKeyScratchSize), key, ie)
	}
	merged, ok := ring.changesSince(key, "", rev)
	if !ok {
		return nil, errUndoChain
	}
	c, changed := merged[key]
	if !changed {
		if ie.rev != 0 {
			// Live at a revision after rev, yet the ring has no change after rev.
			return nil, errUndoChain
		}
		return nil, nil // never existed at or after rev
	}
	return histStateAt(snap, make([]byte, logKeyScratchSize), c, rev)
}

// ── merge helpers shared with the Pebble undo path ─────────────────────────

// listFromUndo merges a HEAD listing of prefix with the undo map: unchanged
// keys keep their HEAD state, keys the map replaced take their state at the
// pinned revision, keys absent then are dropped.
func listFromUndo(snap pebble.Reader, prefix string, opts ReadOptions, undo map[string]*KeyValue) ([]*KeyValue, error) {
	// Dropping the changed keys removes at most len(undo) entries from the
	// head of the listing, so that many extra suffice to fill the limit.
	limit := opts.Limit
	if limit > 0 {
		limit += int64(len(undo))
	}
	head, err := listCurrentFrom(snap, prefix, opts.FromKey, limit, opts.KeysOnly)
	if err != nil {
		return nil, err
	}
	out := head[:0]
	for _, kv := range head {
		if _, changed := undo[kv.Key]; !changed {
			out = append(out, kv)
		}
	}
	for _, kv := range undo {
		if kv != nil {
			out = append(out, kv)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	if opts.Limit > 0 && int64(len(out)) > opts.Limit {
		out = out[:opts.Limit]
	}
	return out, nil
}

// countFromUndo counts keys live at the pinned revision from the HEAD count
// plus the undo map: a changed key counted at HEAD is removed, a changed key
// present at the revision is added.
func countFromUndo(snap pebble.Reader, prefix, fromKey string, undo map[string]*KeyValue) (int64, error) {
	n, err := countCurrentFrom(snap, prefix, fromKey)
	if err != nil {
		return 0, err
	}
	for key, kv := range undo {
		cur, err := idxRevFrom(snap, key)
		if err != nil {
			return 0, err
		}
		if cur != 0 {
			n--
		}
		if kv != nil {
			n++
		}
	}
	return n, nil
}
