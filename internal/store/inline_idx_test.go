package store

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/cockroachdb/pebble"

	"github.com/t4db/t4/internal/wal"
)

// TestInlineIndexMatchesLog pins that inline index values change no read: a
// store applying the same history with values inlined, some above the limit,
// lists and gets exactly what one reading every value from the log does, at
// HEAD and at every readable revision.
func TestInlineIndexMatchesLog(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		t.Run(fmt.Sprint("seed=", seed), func(t *testing.T) {
			plain, inline := openMem(t), openMem(t)
			// Values are revisions in decimal: 2 bytes inlines revisions below 100.
			inline.SetInlineValueMax(2)
			buildRandomHistory(t, plain, rand.New(rand.NewSource(seed)))
			buildRandomHistory(t, inline, rand.New(rand.NewSource(seed)))
			for _, prefix := range []string{"/a/", "/b/", ""} {
				for r := int64(0); r <= plain.CurrentRevision(); r++ {
					if r > 0 && r < max(plain.CompactRevision(), 1) {
						continue
					}
					for _, keysOnly := range []bool{false, true} {
						opts := ReadOptions{Revision: r, KeysOnly: keysOnly}
						want, err := plain.ListRange(prefix, opts)
						if err != nil {
							t.Fatal(err)
						}
						got, err := inline.ListRange(prefix, opts)
						if err != nil {
							t.Fatal(err)
						}
						if !reflect.DeepEqual(got, want) {
							t.Fatalf("ListRange(%q, %+v)\n got: %+v\nwant: %+v", prefix, opts, kvsString(got), kvsString(want))
						}
					}
				}
			}
			for _, k := range []string{"/a/1", "/a/2", "/a/3", "/a/4", "/b/1", "/b/2", "/c"} {
				want, err := plain.Get(k)
				if err != nil {
					t.Fatal(err)
				}
				got, err := inline.Get(k)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("Get(%q) = %+v, want %+v", k, got, want)
				}
			}
			checkKeysOnlyMatchesFull(t, inline)
		})
	}
}

// TestInlineListSkipsLog pins that full reads of inline values are served
// from the index: they succeed with the log records gone, while a value above
// the limit still needs its record.
func TestInlineListSkipsLog(t *testing.T) {
	s := openMem(t)
	s.SetInlineValueMax(4)
	apply(t, s,
		wal.Entry{Revision: 1, Term: 1, Op: wal.OpCreate, Key: "/a/plain", Value: []byte("v1"), CreateRevision: 1, Version: 1, Lease: 7},
		wal.Entry{Revision: 2, Term: 1, Op: wal.OpTxn, Value: wal.EncodeTxnOps([]wal.TxnSubOp{
			{Op: wal.OpCreate, Key: "/a/txn0", Value: []byte("v2"), CreateRevision: 2, Version: 1},
			{Op: wal.OpCreate, Key: "/a/txn1", Value: []byte(""), CreateRevision: 2, Version: 1},
		})},
		wal.Entry{Revision: 3, Term: 1, Op: wal.OpUpdate, Key: "/a/plain", Value: []byte("v3"), CreateRevision: 1, PrevRevision: 1, Version: 2, Lease: 7},
	)
	for _, k := range [][]byte{logKey(1), logKeyWithSub(2, 0), logKeyWithSub(2, 1), logKey(3)} {
		if err := s.db.Delete(k, pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}
	want := []*KeyValue{
		{Key: "/a/plain", Value: []byte("v3"), Revision: 3, CreateRevision: 1, PrevRevision: 1, Version: 2, Lease: 7},
		{Key: "/a/txn0", Value: []byte("v2"), Revision: 2, CreateRevision: 2, Version: 1},
		{Key: "/a/txn1", Revision: 2, CreateRevision: 2, Version: 1},
	}
	got, err := s.ListRange("/a/", ReadOptions{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("list\n got: %+v\nwant: %+v", kvsString(got), kvsString(want))
	}
	if kv, err := s.Get("/a/plain"); err != nil || !reflect.DeepEqual(kv, want[0]) {
		t.Fatalf("Get = %+v, %v; want %+v", kv, err, want[0])
	}

	apply(t, s, wal.Entry{Revision: 4, Term: 1, Op: wal.OpCreate, Key: "/a/big", Value: []byte("too big"), CreateRevision: 4, Version: 1})
	if err := s.db.Delete(logKey(4), pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get("/a/big"); err == nil {
		t.Fatal("Get of a value above the limit succeeded without its log record")
	}
}
