package store

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/t4db/t4/internal/wal"
)

// Compaction is only observable through the records it leaves behind: a read
// or a watch that still answers says nothing about history that was never
// reclaimed. These tests therefore count physical log records.

// logRecords returns every retained log record as "rev:key", in revision
// order.
func logRecords(t *testing.T, s *Store) []string {
	t.Helper()
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: logLower, UpperBound: logUpper})
	if err != nil {
		t.Fatalf("log iter: %v", err)
	}
	defer func() { _ = iter.Close() }()
	var out []string
	for iter.First(); iter.Valid(); iter.Next() {
		r, err := decodeRecord(iter.Value())
		if err != nil {
			t.Fatalf("decode rev=%d: %v", decodeLogKey(iter.Key()), err)
		}
		out = append(out, fmt.Sprintf("%d:%s", decodeLogKey(iter.Key()), r.key))
	}
	if err := iter.Error(); err != nil {
		t.Fatalf("log scan: %v", err)
	}
	return out
}

// revisionsOf returns the revisions of the retained log records for key.
func revisionsOf(t *testing.T, s *Store, key string) []int64 {
	t.Helper()
	var revs []int64
	for _, rec := range logRecords(t, s) {
		var rev int64
		var k string
		if _, err := fmt.Sscanf(rec, "%d:%s", &rev, &k); err == nil && k == key {
			revs = append(revs, rev)
		}
	}
	return revs
}

func sameRevs(got []int64, want ...int64) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestCompactSeesPrecedingEntriesInSameBatch: applying the same WAL entries
// must retain the same history however they are grouped into batches.
// Compaction used to read the committed database only, so mutations earlier in
// its own batch were invisible to it and survived (#422).
func TestCompactSeesPrecedingEntriesInSameBatch(t *testing.T) {
	entries := []wal.Entry{
		createEntry(1, "k", []byte("v1")),
		updateEntry(2, "k", []byte("v2"), 1, 1),
		createEntry(3, "other", []byte("v1")),
		compactEntry(3, 3),
	}

	for _, tc := range []struct {
		name  string
		apply func(*Store, []wal.Entry) error
	}{
		{"Apply", (*Store).Apply},
		{"Recover", (*Store).Recover},
	} {
		t.Run(tc.name, func(t *testing.T) {
			separate := openMem(t)
			for i := range entries {
				if err := tc.apply(separate, entries[i:i+1]); err != nil {
					t.Fatalf("%s one at a time: %v", tc.name, err)
				}
			}
			together := openMem(t)
			if err := tc.apply(together, entries); err != nil {
				t.Fatalf("%s in one batch: %v", tc.name, err)
			}

			one, all := logRecords(t, separate), logRecords(t, together)
			if fmt.Sprint(one) != fmt.Sprint(all) {
				t.Fatalf("batch grouping changed retained history:\n one at a time: %v\n one batch:     %v", one, all)
			}
			if want := "[2:k 3:other]"; fmt.Sprint(all) != want {
				t.Fatalf("retained history %v, want %s", all, want)
			}
		})
	}
}

// TestCompactReclaimsVersionsBelowPreviousWatermark: the version a compaction
// retains as a key's anchor is below its watermark, so the next window — which
// starts at that watermark — does not contain it. Each compaction used to
// leave one such version behind for every key it touched, forever (#420).
func TestCompactReclaimsVersionsBelowPreviousWatermark(t *testing.T) {
	s := openMem(t)
	var rev int64
	var prev int64
	for i := 0; i < 20; i++ {
		rev++
		if prev == 0 {
			apply(t, s, createEntry(rev, "k", []byte("v")))
		} else {
			apply(t, s, updateEntry(rev, "k", []byte("v"), 1, prev))
		}
		kRev := rev
		prev = rev
		// An unrelated write moves the watermark past k's newest version.
		rev++
		apply(t, s, createEntry(rev, fmt.Sprintf("other-%d", i), []byte("v")))
		apply(t, s, compactEntry(rev, rev))

		if got := revisionsOf(t, s, "k"); !sameRevs(got, kRev) {
			t.Fatalf("after compaction %d at rev %d: k has records %v, want only %d", i, rev, got, kRev)
		}
	}
	// The current value is still readable, and so is the watermark revision.
	kv, err := s.Get("k")
	if err != nil || kv == nil || string(kv.Value) != "v" {
		t.Fatalf("Get k after 20 compactions: %+v err=%v", kv, err)
	}
}

// TestCompactReclaimsTxnWrittenVersions: a version written by a transaction
// lives at logKeyWithSub(rev, sub), which the chain link does not name, so the
// reclaim walk has to find it by scanning the revision.
func TestCompactReclaimsTxnWrittenVersions(t *testing.T) {
	s := openMem(t)
	txn := func(rev int64, ops ...wal.TxnSubOp) wal.Entry {
		return wal.Entry{Revision: rev, Term: 1, Op: wal.OpTxn, Value: wal.EncodeTxnOps(ops)}
	}
	apply(t, s, txn(1,
		wal.TxnSubOp{Op: wal.OpCreate, Key: "a", Value: []byte("1"), CreateRevision: 1},
		wal.TxnSubOp{Op: wal.OpCreate, Key: "k", Value: []byte("1"), CreateRevision: 1},
	))
	apply(t, s, createEntry(2, "other", []byte("v")))
	apply(t, s, compactEntry(2, 2))
	apply(t, s, txn(3, wal.TxnSubOp{Op: wal.OpUpdate, Key: "k", Value: []byte("2"), CreateRevision: 1, PrevRevision: 1}))
	apply(t, s, createEntry(4, "other2", []byte("v")))
	apply(t, s, compactEntry(4, 4))

	if got := revisionsOf(t, s, "k"); !sameRevs(got, 3) {
		t.Fatalf("k records: %v, want only 3", got)
	}
	// The txn sub-op for "a" at rev 1 is still its only version and must stay.
	if got := revisionsOf(t, s, "a"); !sameRevs(got, 1) {
		t.Fatalf("a records: %v, want only 1", got)
	}
	kv, err := s.Get("k")
	if err != nil || kv == nil || string(kv.Value) != "2" {
		t.Fatalf("Get k: %+v err=%v", kv, err)
	}
}

// TestCompactReclaimsDeletedKeyRecords: a deletion below the watermark anchors
// nothing — the key is absent there — so it is reclaimed along with the value
// it deleted. It used to be retained as the key's newest record, so a workload
// churning through unique keys grew the log without bound (#421).
func TestCompactReclaimsDeletedKeyRecords(t *testing.T) {
	s := openMem(t)
	apply(t, s,
		createEntry(1, "file-A", []byte("metadata")),
		deleteEntry(2, "file-A", 1, 1),
		createEntry(3, "unrelated", []byte("v")),
	)
	apply(t, s, compactEntry(3, 3))

	if got := revisionsOf(t, s, "file-A"); len(got) != 0 {
		t.Fatalf("file-A records after compaction past its deletion: %v, want none", got)
	}
	// Reads and a replay at the watermark still agree the key is gone.
	if kv, err := s.Get("file-A"); err != nil || kv != nil {
		t.Fatalf("Get file-A: %+v err=%v", kv, err)
	}
	if kv, err := s.GetAt("file-A", 3); err != nil || kv != nil {
		t.Fatalf("GetAt file-A rev=3: %+v err=%v", kv, err)
	}
	kvs, err := s.ListRange("", ReadOptions{Revision: 3})
	if err != nil {
		t.Fatalf("ListRange at rev 3: %v", err)
	}
	for _, kv := range kvs {
		if kv.Key == "file-A" {
			t.Fatalf("replay at the watermark resurrected file-A: %v", kvs)
		}
	}
}

// TestCompactDeletionAtWatermarkStaysWatchable: the compaction revision itself
// is still watchable (see TestWatchReplayAtCompactRevision), so a deletion
// landing exactly on it is kept — and reclaimed by the next compaction, whose
// window starts at that watermark.
func TestCompactDeletionAtWatermarkStaysWatchable(t *testing.T) {
	s := openMem(t)
	apply(t, s,
		createEntry(1, "k", []byte("v")),
		deleteEntry(2, "k", 1, 1),
	)
	apply(t, s, compactEntry(2, 2))
	if got := revisionsOf(t, s, "k"); !sameRevs(got, 2) {
		t.Fatalf("k records after compaction at its deletion: %v, want only 2", got)
	}

	apply(t, s, createEntry(3, "other", []byte("v")))
	apply(t, s, compactEntry(3, 3))
	if got := revisionsOf(t, s, "k"); len(got) != 0 {
		t.Fatalf("k records after the next compaction: %v, want none", got)
	}
}

// TestCompactRecreatedKeyKeepsItsGeneration: reclaiming a deletion must not
// leave an older value of the same key behind, which a replay would read as
// the key still being there.
func TestCompactRecreatedKeyKeepsItsGeneration(t *testing.T) {
	s := openMem(t)
	apply(t, s, createEntry(1, "k", []byte("v1")))
	apply(t, s, deleteEntry(2, "k", 1, 1))
	apply(t, s, createEntry(3, "k", []byte("v3")))
	apply(t, s, updateEntry(4, "k", []byte("v4"), 3, 3))
	apply(t, s, compactEntry(4, 4))

	if got := revisionsOf(t, s, "k"); !sameRevs(got, 4) {
		t.Fatalf("k records: %v, want only 4", got)
	}
	kvs, err := s.ListRange("", ReadOptions{Revision: 4})
	if err != nil {
		t.Fatalf("ListRange at rev 4: %v", err)
	}
	if len(kvs) != 1 || kvs[0].Key != "k" || string(kvs[0].Value) != "v4" {
		t.Fatalf("replay at the watermark: %+v", kvs)
	}
}

// TestCompactChurnDoesNotGrowLog: creating and deleting distinct keys leaves
// the log holding what the live keyspace needs, not one record per key ever
// written.
func TestCompactChurnDoesNotGrowLog(t *testing.T) {
	s := openMem(t)
	var rev int64
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("part-%03d", i)
		rev++
		createRev := rev
		apply(t, s, createEntry(rev, key, []byte("metadata")))
		rev++
		apply(t, s, deleteEntry(rev, key, createRev, createRev))
		rev++
		apply(t, s, createEntry(rev, "live", []byte("v")))
		apply(t, s, compactEntry(rev, rev))
		// Undo "live" so the next iteration can create it again.
		liveRev := rev
		rev++
		apply(t, s, deleteEntry(rev, "live", liveRev, liveRev))
	}
	// At most a handful of records: the churned keys are all gone, leaving the
	// tail above the last watermark.
	if got := logRecords(t, s); len(got) > 4 {
		t.Fatalf("log holds %d records after 50 create/delete cycles: %v", len(got), got)
	}
}
