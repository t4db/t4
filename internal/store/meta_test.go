package store

import (
	"context"
	"testing"
	"time"

	"github.com/t4db/t4/internal/wal"
)

func metaPut(seq, rev int64, key, value string) wal.Entry {
	return wal.Entry{ID: seq, Revision: rev, Term: 1, Op: wal.OpMetaPut, Key: key, Value: []byte(value)}
}

func TestApplyMetaOps(t *testing.T) {
	s := openMem(t)
	apply(t, s, wal.Entry{ID: 1, Revision: 1, Term: 1, Op: wal.OpCreate, Key: "k", Value: []byte("v"), CreateRevision: 1})
	apply(t, s,
		metaPut(2, 1, "lease/1", "a"),
		metaPut(3, 1, "lease/2", "b"),
		metaPut(4, 1, "other", "c"),
	)

	if got := s.CurrentRevision(); got != 1 {
		t.Fatalf("CurrentRevision = %d, want 1 (meta ops must not consume revisions)", got)
	}
	if got := s.LastSequence(); got != 4 {
		t.Fatalf("LastSequence = %d, want 4", got)
	}
	if v, ok, err := s.MetaGet("lease/1"); err != nil || !ok || string(v) != "a" {
		t.Fatalf("MetaGet(lease/1) = %q, %v, %v", v, ok, err)
	}
	if kv, err := s.Get("lease/1"); err != nil || kv != nil {
		t.Fatalf("meta key leaked into data keyspace: %+v, %v", kv, err)
	}
	if kvs, err := s.List(""); err != nil || len(kvs) != 1 || kvs[0].Key != "k" {
		t.Fatalf("List = %+v, %v; want only the data key", kvs, err)
	}
	list, err := s.MetaList("lease/")
	if err != nil || len(list) != 2 || list[0].Key != "lease/1" || list[1].Key != "lease/2" {
		t.Fatalf("MetaList(lease/) = %+v, %v", list, err)
	}

	apply(t, s,
		wal.Entry{ID: 5, Revision: 1, Term: 1, Op: wal.OpMetaDelete, Key: "lease/1"},
		wal.Entry{ID: 6, Revision: 1, Term: 1, Op: wal.OpMetaDelete, Key: "lease/2"},
	)
	if _, ok, _ := s.MetaGet("lease/1"); ok {
		t.Fatal("lease/1 still present after delete")
	}
	if has, err := s.HasMeta(); err != nil || !has {
		t.Fatalf("HasMeta = %v, %v; want true (\"other\" remains)", has, err)
	}
	apply(t, s, wal.Entry{ID: 7, Revision: 1, Term: 1, Op: wal.OpMetaDelete, Key: "other"})
	if has, err := s.HasMeta(); err != nil || has {
		t.Fatalf("HasMeta = %v, %v; want false", has, err)
	}
}

func TestApplyTxnWithMetaSubOps(t *testing.T) {
	s := openMem(t)
	apply(t, s, wal.Entry{ID: 1, Revision: 1, Term: 1, Op: wal.OpTxn, Value: wal.EncodeTxnOps([]wal.TxnSubOp{
		{Op: wal.OpMetaPut, Key: "a", Value: []byte("meta")},
		{Op: wal.OpCreate, Key: "a", Value: []byte("data"), CreateRevision: 1, Version: 1},
		{Op: wal.OpCreate, Key: "b", Value: []byte("data-b"), CreateRevision: 1, Version: 1},
	})})

	if got := s.CurrentRevision(); got != 1 {
		t.Fatalf("CurrentRevision = %d, want 1", got)
	}
	if kv, err := s.GetAt("a", 1); err != nil || kv == nil || string(kv.Value) != "data" {
		t.Fatalf("GetAt(a, 1) = %+v, %v", kv, err)
	}
	if kv, err := s.GetAt("b", 1); err != nil || kv == nil || string(kv.Value) != "data-b" {
		t.Fatalf("GetAt(b, 1) = %+v, %v", kv, err)
	}
	if v, ok, err := s.MetaGet("a"); err != nil || !ok || string(v) != "meta" {
		t.Fatalf("MetaGet(a) = %q, %v, %v", v, ok, err)
	}

	// A meta-only txn carries the previous revision and must not advance it.
	apply(t, s, wal.Entry{ID: 2, Revision: 1, Term: 1, Op: wal.OpTxn, Value: wal.EncodeTxnOps([]wal.TxnSubOp{
		{Op: wal.OpMetaDelete, Key: "a"},
	})})
	if got := s.CurrentRevision(); got != 1 {
		t.Fatalf("CurrentRevision after meta-only txn = %d, want 1", got)
	}
	if _, ok, _ := s.MetaGet("a"); ok {
		t.Fatal("meta key a still present")
	}
}

// TestRecoverMetaEntryKeepsDataAtSharedRevision guards the term-conflict
// cleanup in Recover: it removes whatever is logged at e.Revision before
// applying e. A meta entry shares its revision with the preceding data write,
// so running the cleanup for it would delete that write.
func TestRecoverMetaEntryKeepsDataAtSharedRevision(t *testing.T) {
	for _, tc := range []struct {
		name string
		e    wal.Entry
	}{
		{"meta op", metaPut(2, 1, "m", "v")},
		{"meta-only txn", wal.Entry{ID: 2, Revision: 1, Term: 1, Op: wal.OpTxn,
			Value: wal.EncodeTxnOps([]wal.TxnSubOp{{Op: wal.OpMetaPut, Key: "m", Value: []byte("v")}})}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := openMem(t)
			// The cleanup inspects committed state, so the data write must be
			// committed before the meta entry is recovered.
			apply(t, s, wal.Entry{ID: 1, Revision: 1, Term: 1, Op: wal.OpCreate, Key: "k", Value: []byte("v1"), CreateRevision: 1})
			if err := s.Recover([]wal.Entry{tc.e}); err != nil {
				t.Fatalf("Recover: %v", err)
			}
			if kv, err := s.Get("k"); err != nil || kv == nil || string(kv.Value) != "v1" {
				t.Fatalf("Get(k) = %+v, %v; data write at the shared revision was lost", kv, err)
			}
			if _, ok, _ := s.MetaGet("m"); !ok {
				t.Fatal("meta key not recovered")
			}
			if s.CurrentRevision() != 1 || s.LastSequence() != 2 {
				t.Fatalf("rev=%d seq=%d, want rev=1 seq=2", s.CurrentRevision(), s.LastSequence())
			}
		})
	}
}

func TestWaitForSequence(t *testing.T) {
	s := openMem(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- s.WaitForSequence(ctx, 1) }()
	select {
	case err := <-done:
		t.Fatalf("WaitForSequence returned early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	// A meta op advances the sequence but not the revision; the waiter must
	// still wake up.
	apply(t, s, metaPut(1, 0, "m", "v"))
	if err := <-done; err != nil {
		t.Fatalf("WaitForSequence: %v", err)
	}
}
