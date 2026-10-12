package store

import (
	"errors"
	"fmt"
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

// TestCompactHighCardinalityHistory exercises the workload shape that made
// the former quadratic compaction implementation stall: many distinct keys
// with a second version near the compaction boundary.
func TestCompactHighCardinalityHistory(t *testing.T) {
	s := openMem(t)
	const keys = 10_000

	entries := make([]wal.Entry, 0, keys*2)
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("/registry/nodes/node-%05d", i)
		entries = append(entries, createEntry(int64(i+1), key, []byte("v1")))
	}
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("/registry/nodes/node-%05d", i)
		entries = append(entries, updateEntry(int64(keys+i+1), key, []byte("v2"), int64(i+1), int64(i+1)))
	}
	apply(t, s, entries...)

	apply(t, s, wal.Entry{
		Revision:     keys*2 + 1,
		Term:         1,
		Op:           wal.OpCompact,
		PrevRevision: keys * 2,
	})

	for _, i := range []int{0, keys / 2, keys - 1} {
		key := fmt.Sprintf("/registry/nodes/node-%05d", i)
		kv, err := s.GetAt(key, keys*2)
		if err != nil {
			t.Fatalf("GetAt(%q): %v", key, err)
		}
		if kv == nil || string(kv.Value) != "v2" {
			t.Fatalf("GetAt(%q): got %+v, want v2", key, kv)
		}
	}
}

// ── Revision rewrites ────────────────────────────────────────────────────────

// TestRecoverRejectsRevisionRewrite checks that Recover refuses an entry at a
// revision the store already holds, whether from an earlier call or earlier
// in the same batch, and leaves the store as it was. Undoing a newer term's
// rewrite would need the older term's later records and counters reverted
// too; deleting index entries instead silently lost keys.
func TestRecoverRejectsRevisionRewrite(t *testing.T) {
	setup := func(t *testing.T) *Store {
		s := openMem(t)
		if err := s.Recover([]wal.Entry{
			createEntry(1, "alpha", []byte("a1")),
			updateEntry(2, "alpha", []byte("a2"), 1, 1),
		}); err != nil {
			t.Fatalf("Recover term-1 entries: %v", err)
		}
		return s
	}
	term2 := func(e wal.Entry) wal.Entry { e.Term = 2; return e }

	for name, entries := range map[string][]wal.Entry{
		"other key": {term2(createEntry(2, "beta", []byte("b2")))},
		"same key":  {term2(updateEntry(2, "alpha", []byte("a2'"), 1, 1))},
		"txn": {term2(wal.Entry{Revision: 2, Op: wal.OpTxn, Value: wal.EncodeTxnOps([]wal.TxnSubOp{
			{Op: wal.OpCreate, Key: "beta", Value: []byte("b2"), CreateRevision: 2},
		})})},
		"within batch": {
			createEntry(3, "beta", []byte("b3")),
			term2(createEntry(3, "gamma", []byte("c3"))),
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := setup(t)
			err := s.Recover(entries)
			if !errors.Is(err, errRevisionRewrite) {
				t.Fatalf("Recover: want errRevisionRewrite, got %v", err)
			}
			kv, err := s.Get("alpha")
			if err != nil {
				t.Fatal(err)
			}
			if kv == nil || string(kv.Value) != "a2" {
				t.Fatalf("alpha = %+v, want a2", kv)
			}
			for _, key := range []string{"beta", "gamma"} {
				if kv, err := s.Get(key); err != nil || kv != nil {
					t.Fatalf("%s = %+v, %v; want absent", key, kv, err)
				}
			}
			if got := s.CurrentRevision(); got != 2 {
				t.Fatalf("CurrentRevision = %d, want 2", got)
			}
		})
	}
}

// ── Unknown ops ──────────────────────────────────────────────────────────────

// TestApplyRejectsUnknownOp guards against an older binary misapplying an op
// added by a newer release. Previously any unrecognised op was written as a
// data record at logKey(e.Revision), overwriting that revision's history.
func TestApplyRejectsUnknownOp(t *testing.T) {
	const opFromFuture wal.Op = 99
	s := openMem(t)
	apply(t, s, createEntry(1, "a", []byte("v1")))

	for _, e := range []wal.Entry{
		// Same revision as the data write, like a metadata-only entry.
		{ID: 2, Revision: 1, Term: 1, Op: opFromFuture, Key: "b", Value: []byte("x")},
		{ID: 2, Revision: 2, Term: 1, Op: wal.OpTxn, Value: wal.EncodeTxnOps([]wal.TxnSubOp{
			{Op: wal.OpCreate, Key: "c", Value: []byte("y")},
			{Op: opFromFuture, Key: "b", Value: []byte("x")},
		})},
	} {
		if err := s.Apply([]wal.Entry{e}); !errors.Is(err, wal.ErrUnknownOp) {
			t.Fatalf("Apply op=%d: want ErrUnknownOp, got %v", e.Op, err)
		}
		if err := s.Recover([]wal.Entry{e}); !errors.Is(err, wal.ErrUnknownOp) {
			t.Fatalf("Recover op=%d: want ErrUnknownOp, got %v", e.Op, err)
		}
	}

	if got := s.CurrentRevision(); got != 1 {
		t.Fatalf("CurrentRevision: want 1, got %d", got)
	}
	kv, err := s.GetAt("a", 1)
	if err != nil || kv == nil || string(kv.Value) != "v1" {
		t.Fatalf("GetAt(a, 1): want v1, got %+v err=%v", kv, err)
	}
	for _, k := range []string{"b", "c"} {
		if kv, err := s.Get(k); err != nil || kv != nil {
			t.Fatalf("Get(%s): want absent, got %+v err=%v", k, kv, err)
		}
	}
}

// TestCompactNeverMovesBack checks that the compaction revision only grows. A
// compaction to an earlier revision than the store is compacted to must change
// nothing: the history below the current compaction revision is gone, and a
// read there must fail rather than see what is left of it.
func TestCompactNeverMovesBack(t *testing.T) {
	s := openMem(t)
	apply(t, s,
		createEntry(1, "k", []byte("v1")),
		updateEntry(2, "k", []byte("v2"), 1, 1),
		updateEntry(3, "k", []byte("v3"), 1, 2),
		updateEntry(4, "k", []byte("v4"), 1, 3),
	)
	apply(t, s, wal.Entry{Revision: 4, Term: 1, Op: wal.OpCompact, PrevRevision: 3})
	apply(t, s, wal.Entry{Revision: 4, Term: 1, Op: wal.OpCompact, PrevRevision: 2})

	if got := s.CompactRevision(); got != 3 {
		t.Fatalf("CompactRevision after compacting to 3 then 2 = %d, want 3", got)
	}
	if _, err := s.GetAt("k", 2); !errors.Is(err, ErrCompacted) {
		t.Fatalf("GetAt(k, 2) = %v, want ErrCompacted", err)
	}
	kv, err := s.GetAt("k", 3)
	if err != nil || kv == nil || string(kv.Value) != "v3" {
		t.Fatalf("GetAt(k, 3) = %v, %v, want v3", kv, err)
	}
}
