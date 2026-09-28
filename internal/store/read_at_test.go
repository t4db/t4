package store

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/t4db/t4/internal/wal"
)

// TestReadAtFromHeadMatchesReplay drives a store through random puts,
// deletes, multi-key transactions and compactions, and checks that reads at
// every readable revision served from HEAD (undoing later changes) agree with
// the log replay they replace.
func TestReadAtFromHeadMatchesReplay(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		t.Run(fmt.Sprint("seed=", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			s := openMem(t)
			live := map[string]*KeyValue{}
			keys := []string{"/a/1", "/a/2", "/a/3", "/b/1", "/b/2", "/c"}
			rev := int64(0)

			sub := func(key string, value []byte, del bool) wal.TxnSubOp {
				old := live[key]
				op := wal.TxnSubOp{Key: key, Value: value}
				switch {
				case del:
					op.Op, op.CreateRevision, op.PrevRevision = wal.OpDelete, old.CreateRevision, old.Revision
					delete(live, key)
				case old == nil:
					op.Op, op.CreateRevision = wal.OpCreate, rev
					live[key] = &KeyValue{Key: key, Revision: rev, CreateRevision: rev}
				default:
					op.Op, op.CreateRevision, op.PrevRevision = wal.OpUpdate, old.CreateRevision, old.Revision
					live[key] = &KeyValue{Key: key, Revision: rev, CreateRevision: old.CreateRevision}
				}
				return op
			}

			for step := 0; step < 300; step++ {
				switch n := rng.Intn(10); {
				case n == 0 && rev > 0:
					// Compact somewhere at or below HEAD.
					target := s.CompactRevision() + rng.Int63n(rev-s.CompactRevision()+1)
					apply(t, s, wal.Entry{Revision: rev, Term: 1, Op: wal.OpCompact, PrevRevision: target})
				case n <= 2:
					// A transaction touching several distinct keys.
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
					apply(t, s, wal.Entry{Revision: rev, Term: 1, Op: op.Op, Key: op.Key, Value: op.Value,
						CreateRevision: op.CreateRevision, PrevRevision: op.PrevRevision})
				}
			}

			for r := max(s.CompactRevision(), 1); r <= s.CurrentRevision(); r++ {
				for _, key := range keys {
					want, err := s.getAtRevision(key, r)
					if err != nil {
						t.Fatal(err)
					}
					got, err := s.getAtFromHead(key, r)
					if err != nil {
						t.Fatalf("getAtFromHead(%q, %d): %v", key, r, err)
					}
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("get %q at %d: from head %+v, replay %+v", key, r, got, want)
					}
				}
				for _, prefix := range []string{"", "/a/", "/b/", "/c"} {
					for _, from := range []string{"", "/a/2", "/b/"} {
						for _, limit := range []int64{0, 1, 2} {
							opts := ReadOptions{Revision: r, FromKey: from, Limit: limit}
							want, err := s.listAtByReplay(prefix, opts, r)
							if err != nil {
								t.Fatal(err)
							}
							got, err := s.listAtFromHead(prefix, opts, r)
							if err != nil {
								t.Fatalf("listAtFromHead(%q, %+v): %v", prefix, opts, err)
							}
							if len(got) != len(want) || (len(want) > 0 && !reflect.DeepEqual(got, want)) {
								t.Fatalf("list %q %+v: from head %v, replay %v", prefix, opts, kvKeys(got), kvKeys(want))
							}
						}
						all, err := s.listAtByReplay(prefix, ReadOptions{Revision: r, FromKey: from}, r)
						if err != nil {
							t.Fatal(err)
						}
						n, err := s.countAtFromHead(prefix, from, r)
						if err != nil {
							t.Fatal(err)
						}
						if n != int64(len(all)) {
							t.Fatalf("count %q from %q at %d: from head %d, replay %d", prefix, from, r, n, len(all))
						}
					}
				}
			}
		})
	}
}

func kvKeys(kvs []*KeyValue) []string {
	out := make([]string, len(kvs))
	for i, kv := range kvs {
		out[i] = fmt.Sprintf("%s@%d", kv.Key, kv.Revision)
	}
	return out
}
