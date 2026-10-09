package store

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble"

	"github.com/t4db/t4/internal/wal"
)

func leased(e wal.Entry, lease int64) wal.Entry {
	e.Lease = lease
	return e
}

func requireLeaseKeys(t *testing.T, s *Store, lease int64, want ...string) {
	t.Helper()
	got, err := s.LeaseKeys(lease)
	if err != nil {
		t.Fatalf("LeaseKeys(%d): %v", lease, err)
	}
	if len(want) == 0 {
		want = nil
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("LeaseKeys(%d) = %q, want %q", lease, got, want)
	}
}

// leaseIdxEntries returns how many raw lease index entries the store holds,
// including any LeaseKeys would filter out as stale.
func leaseIdxEntries(t *testing.T, s *Store) int {
	t.Helper()
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte{prefixLease}, UpperBound: []byte{prefixLease + 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = iter.Close() }()
	n := 0
	for iter.First(); iter.Valid(); iter.Next() {
		n++
	}
	return n
}

func TestLeaseIndexFollowsWrites(t *testing.T) {
	s := openMem(t)
	const l1, l2 = 101, 202

	apply(t, s,
		leased(createEntry(1, "a", nil), l1),
		leased(createEntry(2, "b", nil), l1),
		createEntry(3, "c", nil),
	)
	requireLeaseKeys(t, s, l1, "a", "b")

	// Move a to l2, delete b, attach c to l1, in one txn.
	apply(t, s, wal.Entry{Revision: 4, Term: 1, Op: wal.OpTxn, Value: wal.EncodeTxnOps([]wal.TxnSubOp{
		{Op: wal.OpUpdate, Key: "a", CreateRevision: 1, PrevRevision: 1, Lease: l2},
		{Op: wal.OpDelete, Key: "b", CreateRevision: 2, PrevRevision: 2},
		{Op: wal.OpUpdate, Key: "c", CreateRevision: 3, PrevRevision: 3, Lease: l1},
	})})
	requireLeaseKeys(t, s, l1, "c")
	requireLeaseKeys(t, s, l2, "a")

	// Several writes to one key in one batch: the batch is not readable
	// before it commits, so each must see the lease the previous one set.
	apply(t, s,
		leased(createEntry(5, "d", nil), l1),
		deleteEntry(6, "d", 5, 5),
		leased(createEntry(7, "e", nil), l1),
		leased(updateEntry(8, "e", nil, 7, 7), l2),
		updateEntry(9, "c", nil, 3, 4),
	)
	requireLeaseKeys(t, s, l1)
	requireLeaseKeys(t, s, l2, "a", "e")

	// Compaction past every superseded record drops the stale entries.
	apply(t, s, wal.Entry{Revision: 10, Term: 1, Op: wal.OpCompact, PrevRevision: 9})
	requireLeaseKeys(t, s, l1)
	requireLeaseKeys(t, s, l2, "a", "e")
	if n := leaseIdxEntries(t, s); n != 2 {
		t.Fatalf("lease index holds %d entries after compaction, want 2 (stale entries left behind)", n)
	}
}

// TestLeaseIndexCompactionKeepsEntryAddedInSameBatch covers a compaction that
// drops a record attaching a key to a lease while an earlier entry of the
// same batch attaches it again: the committed state says the key has no
// lease, but the entry is live.
func TestLeaseIndexCompactionKeepsEntryAddedInSameBatch(t *testing.T) {
	s := openMem(t)
	const l1 = 101
	apply(t, s,
		leased(createEntry(1, "k", nil), l1),
		updateEntry(2, "k", nil, 1, 1),
	)
	requireLeaseKeys(t, s, l1)
	apply(t, s,
		leased(updateEntry(3, "k", nil, 1, 2), l1),
		wal.Entry{Revision: 4, Term: 1, Op: wal.OpCompact, PrevRevision: 2},
	)
	requireLeaseKeys(t, s, l1, "k")
}

func TestLeaseIndexRebuiltAfterIndexUnawareWrites(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	const l1 = 101
	if err := s.Apply([]wal.Entry{
		leased(createEntry(1, "a", nil), l1),
		leased(createEntry(2, "b", nil), l1),
		createEntry(3, "c", nil),
	}); err != nil {
		t.Fatal(err)
	}

	// Make the store look as a binary without the lease index leaves it: no
	// lease index entries, a current revision without the maintained flag,
	// and later writes (deleting b, attaching c) that only the key index
	// records.
	if err := s.db.Delete(idxKey("b"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := s.db.DeleteRange([]byte{prefixLease}, []byte{prefixLease + 1}, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := s.Apply([]wal.Entry{leased(updateEntry(4, "c", nil, 3, 3), l1)}); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Set(leaseIdxKey(l1, "b"), nil, pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Set(metaCurrentRevKey, encodeRev(4), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	requireLeaseKeys(t, s, l1, "a", "c")
	if n := leaseIdxEntries(t, s); n != 2 {
		t.Fatalf("lease index holds %d entries after rebuild, want 2", n)
	}
}

// TestLeaseIndexTermConflictDropsDiscardedEntry covers Recover replacing a
// record of an older term: the record is overwritten, not superseded, so no
// compaction would ever drop its lease index entry.
func TestLeaseIndexTermConflictDropsDiscardedEntry(t *testing.T) {
	s := openMem(t)
	const l1 = 101
	if err := s.Recover([]wal.Entry{leased(createEntry(1, "alpha", nil), l1)}); err != nil {
		t.Fatal(err)
	}
	requireLeaseKeys(t, s, l1, "alpha")
	if err := s.Recover([]wal.Entry{{Revision: 1, Term: 2, Op: wal.OpCreate, Key: "beta", CreateRevision: 1}}); err != nil {
		t.Fatal(err)
	}
	requireLeaseKeys(t, s, l1)
	if n := leaseIdxEntries(t, s); n != 0 {
		t.Fatalf("lease index holds %d entries, want 0", n)
	}
}
