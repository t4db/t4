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

// undoAnchor reads the first change to a key after rev and reports whether the
// key existed at rev, and whether the undo chain is intact enough to say.
//
// This is the one place the rules live, for both the paths that need the
// record at rev and the paths that only need to know it was there:
//
//	prevRev == 0, created  → absent at rev
//	prevRev == 0, not      → nothing anchors the chain; unwind
//	prevRev > rev          → by construction the earliest change after rev
//	                         anchors at or below rev, so this is corruption
//	otherwise              → live at rev, at revision prevRev
func undoAnchor(prevRev int64, create bool, rev int64) (existed, ok bool) {
	if prevRev == 0 {
		return false, create
	}
	if prevRev > rev {
		return false, false
	}
	return true, true
}

// histStateAt returns the state at rev of the key whose first change after
// rev is c: nil when c created it, else the record c anchors to.
func histStateAt(snap pebble.Reader, lk []byte, c histChange, rev int64) (*KeyValue, error) {
	existed, ok := undoAnchor(c.prevRev, c.create, rev)
	if !ok {
		return nil, errUndoChain
	}
	if !existed {
		return nil, nil
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

// presenceFromHist is undoFromHist for a count: which keys changed after rev,
// and whether each was live then, without reading any of their records.
func (s *Store) presenceFromHist(prefix, fromKey string, rev int64) (map[string]bool, error) {
	ring := s.hist.Load()
	if ring == nil {
		return nil, errUndoChain
	}
	merged, ok := ring.changesSince(prefix, fromKey, rev)
	if !ok {
		return nil, errUndoChain
	}
	present := make(map[string]bool, len(merged))
	for key, c := range merged {
		existed, ok := undoAnchor(c.prevRev, c.create, rev)
		if !ok {
			return nil, errUndoChain
		}
		present[key] = existed
	}
	return present, nil
}

func (s *Store) countAtFromHist(prefix, fromKey string, rev int64) (int64, error) {
	present, err := s.presenceFromHist(prefix, fromKey, rev)
	if err != nil {
		return 0, err
	}
	snap := s.db.NewSnapshot()
	defer func() { _ = snap.Close() }()
	return countFromPresence(snap, prefix, fromKey, present)
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

// countFromPresence counts keys live at the pinned revision from the HEAD
// count plus which changed keys were there: a changed key counted at HEAD is
// removed, a changed key present at the revision is added.
//
// A count only needs that one bit per changed key, and undoAnchor answers it
// from the change alone. Building the records instead — which is what this
// took before, through histStateAt or undoAfter — read one log entry per
// changed key and copied its whole value only to test the result against nil.
// For a txn-written record that lookup also missed logKey(rev) and fell back
// to scanning every sub-op at that revision, so the cost per changed key grew
// with the size of the transaction that wrote it. A pinned CountOnly over 5000
// keys with 500 changed since the pin took 11.9ms against 0.26ms at HEAD.
//
// The bit this gives up with the records: a prevRev the log has no entry for
// used to surface as errUndoChain and send the caller down the slower path,
// and now goes unnoticed, leaving a count one out. Compaction keeps the
// anchored version (see undoAfter), so that state is corruption rather than
// something a healthy store reaches, and every path that reads values still
// checks it.
func countFromPresence(snap pebble.Reader, prefix, fromKey string, present map[string]bool) (int64, error) {
	n, err := countCurrentFrom(snap, prefix, fromKey)
	if err != nil {
		return 0, err
	}
	for key, existed := range present {
		cur, err := idxRevFrom(snap, key)
		if err != nil {
			return 0, err
		}
		if cur != 0 {
			n--
		}
		if existed {
			n++
		}
	}
	return n, nil
}
