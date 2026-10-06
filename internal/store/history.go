package store

import "sync"

// A historyRing is a bounded in-memory index of recent changes. For every
// revision it remembers which keys that revision touched and, for each key,
// the revision where the key's state just before the change lives (0 when
// the key did not exist before it). It is the in-memory source of the undo
// map that undoAfter otherwise builds by scanning the Pebble log region from
// rev+1 to HEAD: a revision-pinned read covered by the ring costs
// O(changed keys since the pin) in-memory merge plus one point lookup per
// changed key, instead of O(all writes since the pin) of log scanning.
//
// Entries hold no values, only key names and revisions, so a revision in the
// ring costs as much as the names of the keys it changed.
type historyRing struct {
	mu   sync.Mutex
	cap  int
	revs []histRev
}

// histRev is one revision's changes in apply order.
type histRev struct {
	rev     int64
	changes []histChange
}

// histChange records that key changed, its previous state living at prevRev
// (0 when the key was created by the change). create mirrors the log record's
// create flag so a zero prevRev on a non-create can be told apart from "the
// key did not exist" and treated as a broken chain, as undoAfter does.
type histChange struct {
	key     string
	prevRev int64
	create  bool
}

func newHistoryRing(cap int) *historyRing {
	if cap <= 0 {
		cap = 1
	}
	return &historyRing{cap: cap}
}

// append records a revision's changes. Revisions arrive in apply order; a
// repeat of the newest revision is a WAL term-rewrite during replay and
// replaces it. Revisions the cap evicts are dropped from the front.
func (h *historyRing) append(rev int64, changes []histChange) {
	h.mu.Lock()
	if n := len(h.revs); n > 0 {
		if last := h.revs[n-1].rev; last == rev {
			h.revs[n-1] = histRev{rev: rev, changes: changes}
		} else if last < rev {
			h.revs = append(h.revs, histRev{rev: rev, changes: changes})
		} else {
			h.mu.Unlock()
			return // out of order; ignore defensively
		}
	} else {
		h.revs = append(h.revs, histRev{rev: rev, changes: changes})
	}
	if len(h.revs) > h.cap {
		h.revs = h.revs[len(h.revs)-h.cap:]
	}
	h.mu.Unlock()
}

// changesSince merges the ring into an undo map for a read pinned at rev:
// every key matching prefix and fromKey that changed after rev maps to its
// first change after rev, whose prevRev is the revision of its state at rev
// (0 when the key did not exist at rev).
// The first (oldest) change to a key wins, as in undoAfter's ascending log
// scan. Non-matching keys are skipped during the walk, so the map never
// holds changes outside the read's range even when the window is large.
//
// The coverage check and the merge run under one lock so a concurrent
// eviction cannot produce a window with a hole. Coverage failure reports
// ok=false and the caller falls back to the Pebble undo/replay paths.
//
// The merge horizon may extend past the reader's snapshot HEAD: a key's
// earliest change after rev is by construction the one with an anchor at or
// below rev, and entries for later revisions never predate it, so extra
// changes cannot corrupt the map.
func (h *historyRing) changesSince(prefix, fromKey string, rev int64) (map[string]histChange, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.revs) == 0 || h.revs[0].rev > rev+1 {
		return nil, false
	}
	m := make(map[string]histChange, 64)
	for _, hr := range h.revs {
		if hr.rev <= rev {
			continue
		}
		for _, c := range hr.changes {
			if len(c.key) < len(prefix) || c.key[:len(prefix)] != prefix || c.key < fromKey {
				continue
			}
			if _, ok := m[c.key]; !ok {
				m[c.key] = c
			}
		}
	}
	return m, true
}

// reset drops every retained revision. It clears in place rather than
// swapping the ring so a reader holding the ring pointer sees the reset too.
func (h *historyRing) reset() {
	h.mu.Lock()
	h.revs = nil
	h.mu.Unlock()
}

// floor returns the oldest retained revision, for tests and diagnostics.
func (h *historyRing) floor() (int64, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.revs) == 0 {
		return 0, false
	}
	return h.revs[0].rev, true
}
