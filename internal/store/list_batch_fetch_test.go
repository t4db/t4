package store

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble"

	"github.com/t4db/t4/internal/wal"
)

// A list reads its records in log order through one iterator rather than one
// point lookup per key, so what the index scan produces and what the fetch
// returns are no longer in the same order. These tests pin the properties that
// keeps: every key gets its own record, the result stays in key order, and the
// records the index does not simply point at still resolve.

// TestListFetchesInterleavedRevisionsInKeyOrder writes two collections
// alternately, so the revisions behind one collection's keys are scattered
// through the log rather than contiguous — the case the fetch sorts for.
func TestListFetchesInterleavedRevisionsInKeyOrder(t *testing.T) {
	s := openMem(t)

	const n = 40
	var rev int64
	for i := 0; i < n; i++ {
		rev++
		apply(t, s, createEntry(rev, fmt.Sprintf("/a/k%03d", i), []byte(fmt.Sprint("a", i))))
		rev++
		apply(t, s, createEntry(rev, fmt.Sprintf("/b/k%03d", i), []byte(fmt.Sprint("b", i))))
	}

	for _, prefix := range []string{"/a/", "/b/"} {
		kvs, err := s.ListRange(prefix, ReadOptions{})
		if err != nil {
			t.Fatalf("ListRange(%q): %v", prefix, err)
		}
		if len(kvs) != n {
			t.Fatalf("ListRange(%q) returned %d keys, want %d", prefix, len(kvs), n)
		}
		for i, kv := range kvs {
			wantKey := fmt.Sprintf("%sk%03d", prefix, i)
			wantVal := fmt.Sprint(prefix[1:2], i)
			if kv.Key != wantKey {
				t.Fatalf("%q position %d: key %q, want %q", prefix, i, kv.Key, wantKey)
			}
			if string(kv.Value) != wantVal {
				t.Fatalf("%q key %s: value %q, want %q", prefix, kv.Key, kv.Value, wantVal)
			}
		}
	}
}

// TestListFetchesTxnSubRecords covers records written by a transaction, which
// live at logKeyWithSub(rev, sub) rather than logKey(rev). The last revision's
// sub-keyed records sort above logKey(rev), so an iterator bounded at the
// revision itself would lose them.
func TestListFetchesTxnSubRecords(t *testing.T) {
	s := openMem(t)

	// One revision holding many keys, then a second so the first is not the
	// newest, and a third written as a transaction at the highest revision.
	batch := func(rev int64, keys []string, val string) {
		t.Helper()
		ops := make([]wal.TxnSubOp, 0, len(keys))
		for _, k := range keys {
			ops = append(ops, wal.TxnSubOp{Op: wal.OpCreate, Key: k, Value: []byte(val), CreateRevision: rev})
		}
		apply(t, s, wal.Entry{Revision: rev, Term: 1, Op: wal.OpTxn, Value: wal.EncodeTxnOps(ops)})
	}
	var first, second []string
	for i := 0; i < 20; i++ {
		first = append(first, fmt.Sprintf("/t/a%03d", i))
		second = append(second, fmt.Sprintf("/t/b%03d", i))
	}
	batch(1, first, "one")
	batch(2, second, "two")

	kvs, err := s.ListRange("/t/", ReadOptions{})
	if err != nil {
		t.Fatalf("ListRange: %v", err)
	}
	if len(kvs) != len(first)+len(second) {
		t.Fatalf("got %d keys, want %d", len(kvs), len(first)+len(second))
	}
	for _, kv := range kvs {
		want := "one"
		if kv.Key >= "/t/b" {
			want = "two"
		}
		if string(kv.Value) != want {
			t.Fatalf("key %s: value %q, want %q", kv.Key, kv.Value, want)
		}
	}
	// Key order, which the fetch must preserve however it read the records.
	for i := 1; i < len(kvs); i++ {
		if kvs[i-1].Key >= kvs[i].Key {
			t.Fatalf("result not in key order at %d: %q then %q", i, kvs[i-1].Key, kvs[i].Key)
		}
	}
}

// TestListFallsBackWhenIndexDoesNotPointAtTheRecord is the guard that the
// fetch's fast route is only a fast route. It deletes the log record the index
// points at for one key, and rewrites another's to hold a different key, which
// are the two ways the fast route can miss. Both must still come back with the
// right value, through the per-key path.
func TestListFallsBackWhenIndexDoesNotPointAtTheRecord(t *testing.T) {
	s := openMem(t)

	const n = 12
	for i := 1; i <= n; i++ {
		apply(t, s, createEntry(int64(i), fmt.Sprintf("/f/k%03d", i), []byte(fmt.Sprint("v", i))))
	}

	// Rewrite the record at revision 4 under a key that is not the one the
	// index entry for /f/k004 names, so the decoded key will not match.
	moved := logKey(4)
	v, closer, err := s.db.Get(moved)
	if err != nil {
		t.Fatalf("read record at rev 4: %v", err)
	}
	rec := make([]byte, len(v))
	copy(rec, v)
	_ = closer.Close()
	// The same record stored at a sub-key, so logKey(4) itself is absent and
	// the fast route finds nothing there while the slow route's sub-op scan
	// still can.
	if err := s.db.Set(logKeyWithSub(4, 0), rec, pebble.NoSync); err != nil {
		t.Fatalf("write sub record: %v", err)
	}
	if err := s.db.Delete(moved, pebble.NoSync); err != nil {
		t.Fatalf("delete record at rev 4: %v", err)
	}

	kvs, err := s.ListRange("/f/", ReadOptions{})
	if err != nil {
		t.Fatalf("ListRange: %v", err)
	}
	if len(kvs) != n {
		t.Fatalf("got %d keys, want %d", len(kvs), n)
	}
	for i, kv := range kvs {
		wantKey := fmt.Sprintf("/f/k%03d", i+1)
		wantVal := fmt.Sprint("v", i+1)
		if kv.Key != wantKey {
			t.Fatalf("position %d: key %q, want %q", i, kv.Key, wantKey)
		}
		if string(kv.Value) != wantVal {
			t.Fatalf("key %s: value %q, want %q (the fallback did not fill the result)", kv.Key, kv.Value, wantVal)
		}
	}
}

// TestListPagedFetchesOnlyThePage checks that a limited list still returns the
// first page in key order, since the fetch now runs after the index scan has
// already chosen the page.
func TestListPagedFetchesOnlyThePage(t *testing.T) {
	s := openMem(t)

	const n = 50
	for i := 0; i < n; i++ {
		apply(t, s, createEntry(int64(i+1), fmt.Sprintf("/p/k%03d", i), []byte(fmt.Sprint("v", i))))
	}

	const limit = 10
	seen := 0
	from := ""
	for page := 0; page < n/limit; page++ {
		kvs, err := s.ListRange("/p/", ReadOptions{Limit: limit, FromKey: from})
		if err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		if len(kvs) != limit {
			t.Fatalf("page %d returned %d keys, want %d", page, len(kvs), limit)
		}
		for i, kv := range kvs {
			wantKey := fmt.Sprintf("/p/k%03d", seen+i)
			if kv.Key != wantKey {
				t.Fatalf("page %d position %d: key %q, want %q", page, i, kv.Key, wantKey)
			}
			if got, want := string(kv.Value), fmt.Sprint("v", seen+i); got != want {
				t.Fatalf("page %d key %s: value %q, want %q", page, kv.Key, got, want)
			}
		}
		seen += len(kvs)
		from = kvs[len(kvs)-1].Key + "\x00"
	}
	if seen != n {
		t.Fatalf("walked %d keys, want %d", seen, n)
	}
}

// TestListKeysOnlyNeedsNoLogRead pins the path that skips the fetch: a v2
// index entry carries enough for a keys-only read, so those entries must not
// be queued for a log record at all. Deleting every log record leaves a store
// whose keys-only list still has to work.
func TestListKeysOnlyNeedsNoLogRead(t *testing.T) {
	s := openMem(t)

	const n = 8
	for i := 1; i <= n; i++ {
		apply(t, s, createEntry(int64(i), fmt.Sprintf("/o/k%03d", i), []byte("value")))
	}
	for i := 1; i <= n; i++ {
		if err := s.db.Delete(logKey(int64(i)), pebble.NoSync); err != nil {
			t.Fatalf("delete record %d: %v", i, err)
		}
	}

	kvs, err := s.ListRange("/o/", ReadOptions{KeysOnly: true})
	if err != nil {
		t.Fatalf("keys-only ListRange: %v", err)
	}
	if len(kvs) != n {
		t.Fatalf("got %d keys, want %d", len(kvs), n)
	}
	for i, kv := range kvs {
		if want := fmt.Sprintf("/o/k%03d", i+1); kv.Key != want {
			t.Fatalf("position %d: key %q, want %q", i, kv.Key, want)
		}
		if len(kv.Value) != 0 {
			t.Fatalf("key %s: keys-only returned a value %q", kv.Key, kv.Value)
		}
	}
}

// TestListTakesEveryRecordFromTheBatchedFetch guards the fast route itself.
// Falling back is correct but costs a point lookup per key, so a change that
// makes the batched fetch miss would otherwise give up the speedup in silence
// while every other test still passed.
func TestListTakesEveryRecordFromTheBatchedFetch(t *testing.T) {
	s := openMem(t)

	// Single-op revisions and a transaction's sub-keyed records, which sort
	// above the revision's own log key and are the ones a bound set at the
	// revision would miss.
	var rev int64
	for i := 0; i < 15; i++ {
		rev++
		apply(t, s, createEntry(rev, fmt.Sprintf("/g/a%03d", i), []byte("single")))
	}
	rev++
	ops := make([]wal.TxnSubOp, 0, 15)
	for i := 0; i < 15; i++ {
		ops = append(ops, wal.TxnSubOp{Op: wal.OpCreate, Key: fmt.Sprintf("/g/b%03d", i), Value: []byte("txn"), CreateRevision: rev})
	}
	apply(t, s, wal.Entry{Revision: rev, Term: 1, Op: wal.OpTxn, Value: wal.EncodeTxnOps(ops)})

	before := listLogFallbacks.Load()
	kvs, err := s.ListRange("/g/", ReadOptions{})
	if err != nil {
		t.Fatalf("ListRange: %v", err)
	}
	if len(kvs) != 30 {
		t.Fatalf("got %d keys, want 30", len(kvs))
	}
	if got := listLogFallbacks.Load() - before; got != 0 {
		t.Fatalf("the batched fetch missed %d of %d records and looked them up one at a time", got, len(kvs))
	}
}
