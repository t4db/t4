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
