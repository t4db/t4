package store

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble"

	"github.com/t4db/t4/internal/wal"
)

// buildRandomHistory drives s through random single-op writes, txns, deletes
// and compactions over a few prefixes, with leases so stripping is visible.
func buildRandomHistory(t *testing.T, s *Store, rng *rand.Rand) {
	t.Helper()
	live := map[string]*KeyValue{}
	keys := []string{"/a/1", "/a/2", "/a/3", "/a/4", "/b/1", "/b/2", "/c"}
	rev := int64(0)
	sub := func(key string, value []byte, del bool) wal.TxnSubOp {
		old := live[key]
		op := wal.TxnSubOp{Key: key, Value: value, Lease: rng.Int63n(3)}
		switch {
		case del:
			op.Op, op.CreateRevision, op.PrevRevision, op.Version = wal.OpDelete, old.CreateRevision, old.Revision, old.Version
			delete(live, key)
		case old == nil:
			op.Op, op.CreateRevision, op.Version = wal.OpCreate, rev, 1
			live[key] = &KeyValue{Key: key, Revision: rev, CreateRevision: rev, Version: 1}
		default:
			op.Op, op.CreateRevision, op.PrevRevision, op.Version = wal.OpUpdate, old.CreateRevision, old.Revision, old.Version+1
			live[key] = &KeyValue{Key: key, Revision: rev, CreateRevision: old.CreateRevision, Version: old.Version + 1}
		}
		return op
	}
	for step := 0; step < 300; step++ {
		switch n := rng.Intn(10); {
		case n == 0 && rev > 0:
			target := s.CompactRevision() + rng.Int63n(rev-s.CompactRevision()+1)
			apply(t, s, wal.Entry{Revision: rev, Term: 1, Op: wal.OpCompact, PrevRevision: target})
		case n <= 3:
			rev++
			var ops []wal.TxnSubOp
			for _, i := range rng.Perm(len(keys))[:1+rng.Intn(3)] {
				k := keys[i]
				ops = append(ops, sub(k, []byte(fmt.Sprint(rev)), live[k] != nil && rng.Intn(3) == 0))
			}
			apply(t, s, wal.Entry{Revision: rev, Term: 1, Op: wal.OpTxn, Value: wal.EncodeTxnOps(ops)})
		default:
			k := keys[rng.Intn(len(keys))]
			del := live[k] != nil && rng.Intn(3) == 0
			if !del && live[k] == nil && rng.Intn(2) == 0 {
				continue
			}
			rev++
			op := sub(k, []byte(fmt.Sprint(rev)), del)
			apply(t, s, wal.Entry{Revision: rev, Term: 1, Op: op.Op, Key: op.Key, Value: op.Value, Lease: op.Lease,
				CreateRevision: op.CreateRevision, PrevRevision: op.PrevRevision, Version: op.Version})
		}
	}
}

// downgradeIndex rewrites every index value to the v1 (revision-only) format,
// as written by binaries that predate v2 values.
func downgradeIndex(t *testing.T, s *Store) {
	t.Helper()
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte{prefixIdx}, UpperBound: idxUpper})
	if err != nil {
		t.Fatal(err)
	}
	b := s.db.NewBatch()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := b.Set(append([]byte(nil), iter.Key()...), encodeRev(decodeRev(iter.Value())), pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}
	if err := iter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(pebble.NoSync); err != nil {
		t.Fatal(err)
	}
}

// checkKeysOnlyMatchesFull compares keys-only listings with full listings
// stripped to keys-only, at HEAD and at every readable revision.
func checkKeysOnlyMatchesFull(t *testing.T, s *Store) {
	t.Helper()
	for _, prefix := range []string{"/a/", "/b/", ""} {
		for r := int64(0); r <= s.CurrentRevision(); r++ {
			if r > 0 && r < max(s.CompactRevision(), 1) {
				continue
			}
			full, err := s.ListRange(prefix, ReadOptions{Revision: r})
			if err != nil {
				t.Fatalf("ListRange(%q, rev=%d): %v", prefix, r, err)
			}
			for _, kv := range full {
				stripToKeysOnly(kv)
			}
			got, err := s.ListRange(prefix, ReadOptions{Revision: r, KeysOnly: true})
			if err != nil {
				t.Fatalf("ListRange(%q, rev=%d, keys-only): %v", prefix, r, err)
			}
			if !reflect.DeepEqual(got, full) {
				t.Fatalf("ListRange(%q, rev=%d) keys-only mismatch\n got: %+v\nwant: %+v", prefix, r, kvsString(got), kvsString(full))
			}
		}
	}
}

func kvsString(kvs []*KeyValue) []KeyValue {
	out := make([]KeyValue, len(kvs))
	for i, kv := range kvs {
		out[i] = *kv
	}
	return out
}

// TestKeysOnlyListMatchesFull pins that keys-only listings, served from v2
// index values at HEAD, return exactly what full listings do minus the value,
// lease and prev revision, at HEAD and past revisions, for v2 and legacy v1
// index values alike.
func TestKeysOnlyListMatchesFull(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		t.Run(fmt.Sprint("seed=", seed), func(t *testing.T) {
			s := openMem(t)
			buildRandomHistory(t, s, rand.New(rand.NewSource(seed)))
			checkKeysOnlyMatchesFull(t, s)

			// Data written by an older binary has v1 index values.
			want, err := s.ListRange("", ReadOptions{})
			if err != nil {
				t.Fatal(err)
			}
			downgradeIndex(t, s)
			got, err := s.ListRange("", ReadOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("full listing changed after index downgrade\n got: %+v\nwant: %+v", kvsString(got), kvsString(want))
			}
			checkKeysOnlyMatchesFull(t, s)
		})
	}
}

// TestKeysOnlyListSkipsLog pins that a keys-only listing at HEAD is served
// from the index: it still succeeds with a key's log record gone, where a
// full listing cannot.
func TestKeysOnlyListSkipsLog(t *testing.T) {
	s := openMem(t)
	apply(t, s,
		createEntry(1, "/a/plain", []byte("v1")),
		wal.Entry{Revision: 2, Term: 1, Op: wal.OpTxn, Value: wal.EncodeTxnOps([]wal.TxnSubOp{
			{Op: wal.OpCreate, Key: "/a/txn0", Value: []byte("v2"), CreateRevision: 2, Version: 1},
			{Op: wal.OpCreate, Key: "/a/txn1", Value: []byte("v3"), CreateRevision: 2, Version: 1},
		})},
	)
	for _, k := range [][]byte{logKey(1), logKeyWithSub(2, 0), logKeyWithSub(2, 1)} {
		if err := s.db.Delete(k, pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.ListRange("/a/", ReadOptions{KeysOnly: true})
	if err != nil {
		t.Fatalf("keys-only list: %v", err)
	}
	want := []*KeyValue{
		{Key: "/a/plain", Revision: 1, CreateRevision: 1, Version: 1},
		{Key: "/a/txn0", Revision: 2, CreateRevision: 2, Version: 1},
		{Key: "/a/txn1", Revision: 2, CreateRevision: 2, Version: 1},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("keys-only list\n got: %+v\nwant: %+v", kvsString(got), kvsString(want))
	}
	if _, err := s.ListRange("/a/", ReadOptions{}); err == nil {
		t.Fatal("full list succeeded without log records; the test no longer proves keys-only skips the log")
	}
}

// TestIdxV2ReadableByRevisionOnlyDecoder pins rollback safety: binaries that
// predate v2 index values decode only the leading revision.
func TestIdxV2ReadableByRevisionOnlyDecoder(t *testing.T) {
	v := encodeIdx(42, 7, 3, 5)
	if got := decodeRev(v); got != 42 {
		t.Fatalf("decodeRev(v2) = %d, want 42", got)
	}
	if got := decodeIdx(encodeRev(42)); got != (idxEntry{rev: 42}) {
		t.Fatalf("decodeIdx(v1) = %+v, want rev only", got)
	}
	if got, want := decodeIdx(v), (idxEntry{rev: 42, createRevision: 7, version: 3, sub: 5, v2: true}); got != want {
		t.Fatalf("decodeIdx(v2) = %+v, want %+v", got, want)
	}
}

// TestIdxSubPointerSkipsMetaOps pins that a data sub-op placed after a meta
// sub-op in the same txn is found through its index value: meta sub-ops are
// not written to the log, so data sub-op indexes have gaps.
func TestIdxSubPointerSkipsMetaOps(t *testing.T) {
	s := openMem(t)
	apply(t, s, wal.Entry{Revision: 1, Term: 1, Op: wal.OpTxn, Value: wal.EncodeTxnOps([]wal.TxnSubOp{
		{Op: wal.OpCreate, Key: "/a/0", Value: []byte("v0"), CreateRevision: 1, Version: 1},
		{Op: wal.OpMetaPut, Key: "meta", Value: []byte("m")},
		{Op: wal.OpCreate, Key: "/a/2", Value: []byte("v2"), CreateRevision: 1, Version: 1},
	})})
	iv, closer, err := s.db.Get(idxKey("/a/2"))
	if err != nil {
		t.Fatal(err)
	}
	ie := decodeIdx(iv)
	_ = closer.Close()
	if !ie.v2 || ie.sub != 2 {
		t.Fatalf("idx(/a/2) = %+v, want v2 with sub=2", ie)
	}
	kv, err := s.Get("/a/2")
	if err != nil || kv == nil || string(kv.Value) != "v2" {
		t.Fatalf("Get(/a/2) = %+v, %v; want value v2", kv, err)
	}
	full, err := s.ListRange("/a/", ReadOptions{})
	if err != nil || len(full) != 2 || string(full[1].Value) != "v2" {
		t.Fatalf("ListRange = %+v, %v", kvsString(full), err)
	}
	keys, err := s.ListRange("/a/", ReadOptions{KeysOnly: true})
	want := []*KeyValue{
		{Key: "/a/0", Revision: 1, CreateRevision: 1, Version: 1},
		{Key: "/a/2", Revision: 1, CreateRevision: 1, Version: 1},
	}
	if err != nil || !reflect.DeepEqual(keys, want) {
		t.Fatalf("keys-only ListRange = %+v, %v; want %+v", kvsString(keys), err, kvsString(want))
	}
}
