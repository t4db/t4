package store

import (
	"testing"

	"github.com/t4db/t4/internal/wal"
)

// ── Compaction fixes ─────────────────────────────────────────────────────────

// TestCompactPreservesLiveKeyAtOnlyRevision is the regression test for the
// original compaction bug: applyCompact must not delete a log entry that is
// still the current (and only) revision of a live key, even when that revision
// falls below the compaction target.
func TestCompactPreservesLiveKeyAtOnlyRevision(t *testing.T) {
	s := openMem(t)

	// Create "k" at rev 1.  Its only log entry is at rev 1.
	apply(t, s, createEntry(1, "k", []byte("v1")))

	// Compact at revision 1 — everything ≤ 1 is nominally eligible for
	// removal, but rev 1 is the current revision of "k" so it must be kept.
	apply(t, s, wal.Entry{
		Revision:     2,
		Term:         1,
		Op:           wal.OpCompact,
		PrevRevision: 1,
	})

	if s.CompactRevision() != 1 {
		t.Errorf("CompactRevision: want 1 got %d", s.CompactRevision())
	}

	kv, err := s.Get("k")
	if err != nil {
		t.Fatalf("Get after compact: %v", err)
	}
	if kv == nil {
		t.Fatal("key 'k' must still be readable after compaction — its current revision was incorrectly deleted")
	}
	if string(kv.Value) != "v1" {
		t.Errorf("value after compact: want v1 got %q", kv.Value)
	}
}

// TestCompactWithMultipleKeysAtSoleRevision verifies the fix for multiple keys
// that each only have a single log entry inside the compaction window.
func TestCompactWithMultipleKeysAtSoleRevision(t *testing.T) {
	s := openMem(t)

	apply(t, s,
		createEntry(1, "/ns/a", []byte("a")),
		createEntry(2, "/ns/b", []byte("b")),
		createEntry(3, "/ns/c", []byte("c")),
	)

	// Compact at rev 3 — all three entries are ≤ 3 but each is the current
	// (and only) revision of its key, so all must be preserved.
	apply(t, s, wal.Entry{
		Revision:     4,
		Term:         1,
		Op:           wal.OpCompact,
		PrevRevision: 3,
	})

	for _, key := range []string{"/ns/a", "/ns/b", "/ns/c"} {
		kv, err := s.Get(key)
		if err != nil {
			t.Fatalf("Get %q after compact: %v", key, err)
		}
		if kv == nil {
			t.Errorf("key %q is missing after compaction — current revision must not be deleted", key)
		}
	}

	n, err := s.Count("/ns/")
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if n != 3 {
		t.Errorf("Count after compact: want 3 got %d", n)
	}
}

// TestCompactRemovesOldRevisionsKeepsLatest verifies that old (non-current)
// revisions of a key are removed and the current revision is kept.
func TestCompactRemovesOldRevisionsKeepsLatest(t *testing.T) {
	s := openMem(t)

	apply(t, s,
		createEntry(1, "k", []byte("v1")),
		updateEntry(2, "k", []byte("v2"), 1, 1),
		updateEntry(3, "k", []byte("v3"), 1, 2),
	)

	// Compact at rev 2.  Rev 1 is a non-current revision → should be deleted.
	// Rev 2 is also non-current (current is 3) → should be deleted.
	// Rev 3 is the current revision → must be kept.
	apply(t, s, wal.Entry{
		Revision:     4,
		Term:         1,
		Op:           wal.OpCompact,
		PrevRevision: 2,
	})

	kv, err := s.Get("k")
	if err != nil {
		t.Fatalf("Get after compact: %v", err)
	}
	if kv == nil || string(kv.Value) != "v3" {
		t.Errorf("want current value v3 after compact, got %v", kv)
	}
	if s.CompactRevision() != 2 {
		t.Errorf("CompactRevision: want 2 got %d", s.CompactRevision())
	}
}

// TestCompactDeletedKeyIsRemoved verifies that log entries for deleted keys are
// eligible for removal because their index entry is gone.
func TestCompactDeletedKeyIsRemoved(t *testing.T) {
	s := openMem(t)

	apply(t, s,
		createEntry(1, "gone", []byte("v")),
		deleteEntry(2, "gone", 1, 1),
		createEntry(3, "live", []byte("v")),
	)

	// Compact at rev 2.  Rev 1 ("gone" created) and rev 2 ("gone" deleted)
	// are both eligible.  Rev 3 ("live" current revision) must be kept.
	apply(t, s, wal.Entry{
		Revision:     4,
		Term:         1,
		Op:           wal.OpCompact,
		PrevRevision: 2,
	})

	// "gone" must still be absent.
	kv, err := s.Get("gone")
	if err != nil {
		t.Fatalf("Get 'gone': %v", err)
	}
	if kv != nil {
		t.Errorf("deleted key 'gone' should still be absent after compaction, got %v", kv)
	}

	// "live" must still be present.
	kv, err = s.Get("live")
	if err != nil {
		t.Fatalf("Get 'live': %v", err)
	}
	if kv == nil || string(kv.Value) != "v" {
		t.Errorf("live key must survive compaction, got %v", kv)
	}
}

// ── Term-conflict index cleanup ───────────────────────────────────────────────

// TestApplyEntryTermConflictCleansStaleIndex is the regression test for the
// term-conflict index corruption bug.  When two WAL terms both write a different
// key at the same revision (which happens after a leader takeover), applying the
// newer-term entry must not leave a stale index entry pointing to the old key.
func TestApplyEntryTermConflictCleansStaleIndex(t *testing.T) {
	s := openMem(t)

	// Term-1 creates "alpha" at rev 1, then updates it at rev 2.
	// After this, idx["alpha"] = 2.
	if err := s.Recover([]wal.Entry{
		createEntry(1, "alpha", []byte("a-v1")),
		{Revision: 2, Term: 1, Op: wal.OpUpdate, Key: "alpha", Value: []byte("a-v2"), CreateRevision: 1, PrevRevision: 1},
	}); err != nil {
		t.Fatalf("Recover term-1 entries: %v", err)
	}

	// Term-2 takes over and writes a completely different key "beta" at rev 2.
	// This simulates replayRemote applying a newer-term segment that conflicts
	// with an older-term segment at the same revision.
	if err := s.Recover([]wal.Entry{
		{Revision: 2, Term: 2, Op: wal.OpCreate, Key: "beta", Value: []byte("b-v2"), CreateRevision: 2},
	}); err != nil {
		t.Fatalf("Recover term-2 entry: %v", err)
	}

	// "beta" must be readable at its new-term value.
	kv, err := s.Get("beta")
	if err != nil {
		t.Fatalf("Get beta: %v", err)
	}
	if kv == nil || string(kv.Value) != "b-v2" {
		t.Errorf("beta: want b-v2, got %v", kv)
	}

	// "alpha" must not be accessible via a stale index entry pointing at rev 2
	// (which now belongs to "beta").  A stale pointer would return beta's data
	// under alpha's key, or corrupt the store's internal state on the next Get.
	kv, err = s.Get("alpha")
	if err != nil {
		t.Fatalf("Get alpha: %v", err)
	}
	if kv != nil && kv.Revision == 2 {
		t.Errorf("alpha has stale index at rev 2 (rev 2 now belongs to beta); got %+v", kv)
	}
}

// TestApplyEntryTermConflictSameKey verifies that when two terms write the same
// key at the same revision (common case: same key, different value), the newer
// term's value wins and no index corruption occurs.
func TestApplyEntryTermConflictSameKey(t *testing.T) {
	s := openMem(t)

	if err := s.Recover([]wal.Entry{
		{Revision: 1, Term: 1, Op: wal.OpCreate, Key: "k", Value: []byte("t1"), CreateRevision: 1},
	}); err != nil {
		t.Fatalf("Recover term-1: %v", err)
	}

	// Term-2 writes the same key at the same revision with a different value.
	if err := s.Recover([]wal.Entry{
		{Revision: 1, Term: 2, Op: wal.OpCreate, Key: "k", Value: []byte("t2"), CreateRevision: 1},
	}); err != nil {
		t.Fatalf("Recover term-2: %v", err)
	}

	kv, err := s.Get("k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if kv == nil || string(kv.Value) != "t2" {
		t.Errorf("want term-2 value t2, got %v", kv)
	}
	if kv.Revision != 1 {
		t.Errorf("Revision: want 1 got %d", kv.Revision)
	}
}
