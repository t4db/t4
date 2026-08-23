package t4_test

import (
	"context"
	"errors"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/t4db/t4"
	"github.com/t4db/t4/internal/wal"
	"github.com/t4db/t4/pkg/object"
)

// openNode starts a Node with an in-memory object store and a temp data dir.
func openNode(t *testing.T) *t4.Node {
	t.Helper()
	n, err := t4.Open(t4.Config{
		DataDir:     t.TempDir(),
		ObjectStore: object.NewMem(),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = n.Close() })
	return n
}

func ctx(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return c
}

// ── Put / Get ─────────────────────────────────────────────────────────────────

func TestNodePutGet(t *testing.T) {
	n := openNode(t)

	rev, err := n.Put(ctx(t), "foo", []byte("bar"), 0)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if rev != 1 {
		t.Errorf("Put rev: want 1 got %d", rev)
	}

	kv, err := n.Get("foo")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if kv == nil {
		t.Fatal("Get returned nil")
	}
	if string(kv.Value) != "bar" {
		t.Errorf("Value: want bar got %q", kv.Value)
	}
	if kv.Revision != 1 {
		t.Errorf("Revision: want 1 got %d", kv.Revision)
	}
	if kv.CreateRevision != 1 {
		t.Errorf("CreateRevision: want 1 got %d", kv.CreateRevision)
	}
}

func TestNodeGetMissing(t *testing.T) {
	n := openNode(t)
	kv, err := n.Get("nope")
	if err != nil {
		t.Fatal(err)
	}
	if kv != nil {
		t.Errorf("expected nil, got %+v", kv)
	}
}

func TestNodePutUpdatesRevision(t *testing.T) {
	n := openNode(t)
	c := ctx(t)
	n.Put(c, "k", []byte("v1"), 0)
	rev2, _ := n.Put(c, "k", []byte("v2"), 0)

	kv, _ := n.Get("k")
	if kv.Revision != rev2 {
		t.Errorf("Revision: want %d got %d", rev2, kv.Revision)
	}
	if kv.CreateRevision != 1 {
		t.Errorf("CreateRevision should stay 1, got %d", kv.CreateRevision)
	}
	if kv.Version != 2 {
		t.Errorf("Version: want 2 got %d", kv.Version)
	}
	if kv.PrevRevision != 1 {
		t.Errorf("PrevRevision: want 1 got %d", kv.PrevRevision)
	}
	if string(kv.Value) != "v2" {
		t.Errorf("Value: want v2 got %q", kv.Value)
	}
}

func TestNodeVersionCreateUpdateDeleteRecreate(t *testing.T) {
	n := openNode(t)
	c := ctx(t)

	if _, err := n.Put(c, "k", []byte("v1"), 0); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	kv, _ := n.Get("k")
	if kv.Version != 1 {
		t.Fatalf("initial Version: want 1 got %d", kv.Version)
	}

	if _, err := n.Put(c, "k", []byte("v2"), 0); err != nil {
		t.Fatalf("Put v2: %v", err)
	}
	kv, _ = n.Get("k")
	if kv.Version != 2 {
		t.Fatalf("updated Version: want 2 got %d", kv.Version)
	}

	if _, err := n.Delete(c, "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := n.Put(c, "k", []byte("v3"), 0); err != nil {
		t.Fatalf("Put v3: %v", err)
	}
	kv, _ = n.Get("k")
	if kv.Version != 1 {
		t.Fatalf("recreated Version: want 1 got %d", kv.Version)
	}
}

// ── Create ────────────────────────────────────────────────────────────────────

func TestNodeCreate(t *testing.T) {
	n := openNode(t)
	c := ctx(t)

	rev, err := n.Create(c, "k", []byte("v"), 0)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if rev != 1 {
		t.Errorf("Create rev: want 1 got %d", rev)
	}
}

func TestNodeCreateExisting(t *testing.T) {
	n := openNode(t)
	c := ctx(t)
	n.Create(c, "k", []byte("v"), 0)

	_, err := n.Create(c, "k", []byte("v2"), 0)
	if !errors.Is(err, t4.ErrKeyExists) {
		t.Errorf("expected ErrKeyExists, got %v", err)
	}

	// Value must not have changed.
	kv, _ := n.Get("k")
	if string(kv.Value) != "v" {
		t.Errorf("value mutated: %q", kv.Value)
	}
}

// ── Update ────────────────────────────────────────────────────────────────────

func TestNodeUpdateCAS(t *testing.T) {
	n := openNode(t)
	c := ctx(t)
	n.Put(c, "k", []byte("v1"), 0) // rev=1

	newRev, oldKV, updated, err := n.Update(c, "k", []byte("v2"), 1, 0)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if !updated {
		t.Error("expected updated=true")
	}
	if newRev != 2 {
		t.Errorf("newRev: want 2 got %d", newRev)
	}
	if string(oldKV.Value) != "v1" {
		t.Errorf("oldKV.Value: want v1 got %q", oldKV.Value)
	}

	kv, _ := n.Get("k")
	if string(kv.Value) != "v2" {
		t.Errorf("updated value: want v2 got %q", kv.Value)
	}
}

func TestNodeUpdateRevisionMismatch(t *testing.T) {
	n := openNode(t)
	c := ctx(t)
	n.Put(c, "k", []byte("v1"), 0) // rev=1

	curRev, oldKV, updated, err := n.Update(c, "k", []byte("v2"), 999, 0)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated {
		t.Error("expected updated=false on revision mismatch")
	}
	if curRev != 1 {
		t.Errorf("curRev: want 1 got %d", curRev)
	}
	if oldKV == nil || string(oldKV.Value) != "v1" {
		t.Errorf("oldKV should be current value")
	}

	// Value must be unchanged.
	kv, _ := n.Get("k")
	if string(kv.Value) != "v1" {
		t.Errorf("value must not change on mismatch")
	}
}

// ── Delete ────────────────────────────────────────────────────────────────────

func TestNodeDelete(t *testing.T) {
	n := openNode(t)
	c := ctx(t)
	n.Put(c, "k", []byte("v"), 0)

	rev, err := n.Delete(c, "k")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if rev != 2 {
		t.Errorf("Delete rev: want 2 got %d", rev)
	}

	kv, _ := n.Get("k")
	if kv != nil {
		t.Errorf("key should be gone, got %+v", kv)
	}
}

func TestNodeDeleteMissing(t *testing.T) {
	n := openNode(t)
	rev, err := n.Delete(ctx(t), "nope")
	if err != nil {
		t.Fatal(err)
	}
	if rev != 0 {
		t.Errorf("Delete missing: want rev=0 got %d", rev)
	}
}

func TestNodeDeleteIfRevision(t *testing.T) {
	n := openNode(t)
	c := ctx(t)
	n.Put(c, "k", []byte("v"), 0) // rev=1

	// Wrong revision → should not delete.
	_, _, deleted, err := n.DeleteIfRevision(c, "k", 99)
	if err != nil || deleted {
		t.Errorf("DeleteIfRevision wrong rev: deleted=%v err=%v", deleted, err)
	}

	// Correct revision → should delete.
	newRev, oldKV, deleted, err := n.DeleteIfRevision(c, "k", 1)
	if err != nil {
		t.Fatalf("DeleteIfRevision: %v", err)
	}
	if !deleted {
		t.Error("expected deleted=true")
	}
	if newRev != 2 {
		t.Errorf("newRev: want 2 got %d", newRev)
	}
	if string(oldKV.Value) != "v" {
		t.Errorf("oldKV.Value: want v got %q", oldKV.Value)
	}
}

// ── List / Count ──────────────────────────────────────────────────────────────

func TestNodeList(t *testing.T) {
	n := openNode(t)
	c := ctx(t)
	n.Put(c, "/a/1", []byte("1"), 0)
	n.Put(c, "/a/2", []byte("2"), 0)
	n.Put(c, "/b/1", []byte("3"), 0)

	kvs, err := n.List("/a/")
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 2 {
		t.Fatalf("List /a/: want 2 got %d", len(kvs))
	}
}

func TestNodeCount(t *testing.T) {
	n := openNode(t)
	c := ctx(t)
	n.Put(c, "/a/1", nil, 0)
	n.Put(c, "/a/2", nil, 0)
	n.Put(c, "/b/1", nil, 0)
	n.Delete(c, "/a/1")

	cnt, err := n.Count("/a/")
	if err != nil {
		t.Fatal(err)
	}
	if cnt != 1 {
		t.Errorf("Count /a/: want 1 got %d", cnt)
	}
}

// ── Watch ─────────────────────────────────────────────────────────────────────

func TestNodeWatch(t *testing.T) {
	n := openNode(t)
	c := ctx(t)

	ch, err := n.Watch(c, "/w/", 0)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		n.Put(c, "/w/a", []byte("1"), 0)
		n.Put(c, "/w/b", []byte("2"), 0)
		n.Delete(c, "/w/a")
	}()

	wantKeys := []string{"/w/a", "/w/b", "/w/a"}
	wantTypes := []t4.EventType{t4.EventPut, t4.EventPut, t4.EventDelete}

	for i := 0; i < 3; i++ {
		select {
		case ev := <-ch:
			if ev.KV.Key != wantKeys[i] {
				t.Errorf("event %d key: want %q got %q", i, wantKeys[i], ev.KV.Key)
			}
			if ev.Type != wantTypes[i] {
				t.Errorf("event %d type: want %v got %v", i, wantTypes[i], ev.Type)
			}
		case <-c.Done():
			t.Fatalf("timeout waiting for event %d", i)
		}
	}
}

func TestNodeWatchAfterCloseReturnsErrClosed(t *testing.T) {
	n := openNode(t)
	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	ch, err := n.Watch(ctx(t), "/w/", 0)
	if !errors.Is(err, t4.ErrClosed) {
		t.Fatalf("Watch after Close = ch %v err %v, want ErrClosed", ch, err)
	}
}

func TestNodeWatchPrevKV(t *testing.T) {
	n := openNode(t)
	c := ctx(t)
	n.Put(c, "k", []byte("old"), 0)

	ch, _ := n.Watch(c, "k", n.CurrentRevision()+1, t4.WithPrevKV())

	go func() { n.Put(c, "k", []byte("new"), 0) }()

	ev := <-ch
	if ev.PrevKV == nil {
		t.Error("expected PrevKV on update")
	} else if string(ev.PrevKV.Value) != "old" {
		t.Errorf("PrevKV.Value: want old got %q", ev.PrevKV.Value)
	}
}

// ── Compact ───────────────────────────────────────────────────────────────────

func TestNodeCompact(t *testing.T) {
	n := openNode(t)
	c := ctx(t)

	n.Put(c, "k", []byte("v1"), 0) // rev 1
	n.Put(c, "k", []byte("v2"), 0) // rev 2
	n.Put(c, "k", []byte("v3"), 0) // rev 3

	if err := n.Compact(c, 2); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if n.CompactRevision() != 2 {
		t.Errorf("CompactRevision: want 2 got %d", n.CompactRevision())
	}
	if n.CurrentRevision() != 3 {
		t.Errorf("CurrentRevision after compact: want 3 got %d", n.CurrentRevision())
	}
	rev, err := n.Put(c, "k", []byte("v4"), 0)
	if err != nil {
		t.Fatalf("Put after compact: %v", err)
	}
	if rev != 4 {
		t.Errorf("Put after compact revision: want 4 got %d", rev)
	}

	// Current value still accessible.
	kv, _ := n.Get("k")
	if string(kv.Value) != "v4" {
		t.Errorf("current value: want v4 got %q", kv.Value)
	}
}

func TestNodeAutoCompactTimeWindow(t *testing.T) {
	n, err := t4.Open(t4.Config{
		DataDir:                   t.TempDir(),
		ObjectStore:               object.NewMem(),
		AutoCompactRetention:      20 * time.Millisecond,
		AutoCompactInterval:       5 * time.Millisecond,
		AutoCompactSampleInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = n.Close() })

	c := ctx(t)
	if _, err := n.Put(c, "k", []byte("v1"), 0); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	time.Sleep(10 * time.Millisecond)
	if _, err := n.Put(c, "k", []byte("v2"), 0); err != nil {
		t.Fatalf("Put v2: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for n.CompactRevision() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	compactRev := n.CompactRevision()
	if compactRev == 0 {
		t.Fatal("auto compaction did not advance compact revision")
	}

	kv, err := n.Get("k")
	if err != nil {
		t.Fatalf("Get after auto compact: %v", err)
	}
	if kv == nil || string(kv.Value) != "v2" {
		t.Fatalf("current value after auto compact: got %+v", kv)
	}

	wctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	// The compact revision itself stays watchable (etcd contract).
	if _, err := n.Watch(wctx, "k", compactRev); err != nil {
		t.Fatalf("Watch at compact revision: got %v, want nil", err)
	}
	if compactRev > 1 {
		if _, err := n.Watch(wctx, "k", compactRev-1); !errors.Is(err, t4.ErrCompacted) {
			t.Fatalf("Watch below compact revision: got %v, want ErrCompacted", err)
		}
	}
}

func TestNodeAutoCompactRevisionWindow(t *testing.T) {
	n, err := t4.Open(t4.Config{
		DataDir:                      t.TempDir(),
		ObjectStore:                  object.NewMem(),
		AutoCompactMode:              t4.AutoCompactRevision,
		AutoCompactRevisionRetention: 1,
		AutoCompactInterval:          5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = n.Close() })

	c := ctx(t)
	for _, value := range []string{"v1", "v2", "v3"} {
		if _, err := n.Put(c, "k", []byte(value), 0); err != nil {
			t.Fatalf("Put %s: %v", value, err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for n.CompactRevision() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if compactRev := n.CompactRevision(); compactRev < 2 {
		t.Fatalf("auto compaction did not reach revision window target, got compactRev=%d", compactRev)
	}

	kv, err := n.Get("k")
	if err != nil {
		t.Fatalf("Get after auto compact: %v", err)
	}
	if kv == nil || string(kv.Value) != "v3" {
		t.Fatalf("current value after auto compact: got %+v", kv)
	}
}

func TestNodeCompactDoesNotReuseWALSequenceAfterRestart(t *testing.T) {
	dir := t.TempDir()
	c := ctx(t)
	n, err := t4.Open(t4.Config{
		DataDir: dir,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := n.Put(c, "k", []byte("v1"), 0); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := n.Compact(c, 1); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if n.CurrentRevision() != 1 {
		t.Fatalf("CurrentRevision after compact: want 1 got %d", n.CurrentRevision())
	}
	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	n, err = t4.Open(t4.Config{
		DataDir: dir,
	})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	t.Cleanup(func() { _ = n.Close() })
	rev, err := n.Put(c, "k", []byte("v2"), 0)
	if err != nil {
		t.Fatalf("Put after reopen: %v", err)
	}
	if rev != 2 {
		t.Fatalf("Put after reopen revision: want 2 got %d", rev)
	}
}

func TestNodeFlushAfterCompactDoesNotReuseWALSequence(t *testing.T) {
	dir := t.TempDir()
	c := ctx(t)
	n, err := t4.Open(t4.Config{
		DataDir: dir,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = n.Close() })

	if _, err := n.Put(c, "k", []byte("v1"), 0); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := n.Compact(c, 1); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if err := n.Flush(); err != nil {
		t.Fatalf("Flush after first compact: %v", err)
	}
	if err := n.Compact(c, 1); err != nil {
		t.Fatalf("second Compact: %v", err)
	}
	if err := n.Flush(); err != nil {
		t.Fatalf("Flush after second compact: %v", err)
	}

	maxSeq, err := wal.MaxSequence(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatalf("MaxSequence: %v", err)
	}
	if maxSeq != 3 {
		t.Fatalf("MaxSequence after repeated compact/flush: want 3 got %d", maxSeq)
	}
	if n.CurrentRevision() != 1 {
		t.Fatalf("CurrentRevision after repeated compact/flush: want 1 got %d", n.CurrentRevision())
	}
}

func TestWatchCompactionBoundary(t *testing.T) {
	n := openNode(t)
	c := ctx(t)

	n.Put(c, "k", []byte("v1"), 0) // rev 1
	n.Put(c, "k", []byte("v2"), 0) // rev 2
	n.Put(c, "k", []byte("v3"), 0) // rev 3

	// compact(3): deletes stale entries in [0,3), keeps rev 3 intact.
	if err := n.Compact(c, 3); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// startRev=1 is inside the compacted range → ErrCompacted.
	_, err := n.Watch(c, "k", 1)
	if !errors.Is(err, t4.ErrCompacted) {
		t.Errorf("Watch from deeply compacted rev: want ErrCompacted, got %v", err)
	}

	// startRev=3 is the compact boundary itself; etcd requires it to stay
	// watchable, and the event at rev 3 must still be replayed.
	ch3, err := n.Watch(c, "k", 3)
	if err != nil {
		t.Fatalf("Watch from compact watermark (startRev=compactRev): want nil, got %v", err)
	}
	select {
	case ev, ok := <-ch3:
		if !ok {
			t.Fatalf("watch channel closed before replaying rev 3")
		}
		if ev.KV == nil || ev.KV.Revision != 3 {
			t.Errorf("replayed event at compact watermark: want rev 3, got %+v", ev.KV)
		}
	case <-time.After(2 * time.Second):
		t.Errorf("timeout waiting for replay of rev 3")
	}

	// startRev=4 (above compact boundary) is always fine.
	ch, err := n.Watch(c, "k", 4)
	if err != nil {
		t.Errorf("Watch above compact boundary: unexpected error %v", err)
	} else {
		_ = ch
	}
}

// ── Revision monotonicity ─────────────────────────────────────────────────────

func TestRevisionMonotonicity(t *testing.T) {
	n := openNode(t)
	c := ctx(t)

	var prev int64
	for i := 0; i < 20; i++ {
		rev, err := n.Put(c, "k", []byte("v"), 0)
		if err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		if rev <= prev {
			t.Errorf("revision went backwards: %d -> %d", prev, rev)
		}
		prev = rev
	}
}

// ── Restart / recovery ────────────────────────────────────────────────────────

func TestNodeRestart(t *testing.T) {
	dir := t.TempDir()
	obj := object.NewMem()
	cfg := t4.Config{DataDir: dir, ObjectStore: obj}

	// First open: write some data.
	n, err := t4.Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	c := ctx(t)
	n.Put(c, "persistent", []byte("yes"), 0)
	n.Put(c, "also", []byte("here"), 0)
	rev := n.CurrentRevision()
	n.Close()

	// Second open: state must survive.
	n2, err := t4.Open(cfg)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer func() { _ = n2.Close() }()

	if n2.CurrentRevision() != rev {
		t.Errorf("CurrentRevision after restart: want %d got %d", rev, n2.CurrentRevision())
	}
	kv, err := n2.Get("persistent")
	if err != nil || kv == nil {
		t.Fatalf("Get after restart: err=%v kv=%v", err, kv)
	}
	if string(kv.Value) != "yes" {
		t.Errorf("value after restart: want yes got %q", kv.Value)
	}
}

func TestNodeVersionSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	obj := object.NewMem()
	cfg := t4.Config{DataDir: dir, ObjectStore: obj}

	n, err := t4.Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	c := ctx(t)
	if _, err := n.Put(c, "versioned", []byte("v1"), 0); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	if _, err := n.Put(c, "versioned", []byte("v2"), 0); err != nil {
		t.Fatalf("Put v2: %v", err)
	}
	if err := n.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	n2, err := t4.Open(cfg)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer func() { _ = n2.Close() }()
	kv, err := n2.Get("versioned")
	if err != nil || kv == nil {
		t.Fatalf("Get after restart: err=%v kv=%v", err, kv)
	}
	if kv.Version != 2 {
		t.Fatalf("Version after restart: want 2 got %d", kv.Version)
	}
}

// ── Txn ───────────────────────────────────────────────────────────────────────

func TestTxnMultiKeyAtomicPut(t *testing.T) {
	n := openNode(t)

	resp, err := n.Txn(ctx(t), t4.TxnRequest{
		Success: []t4.TxnOp{
			{Type: t4.TxnPut, Key: "a", Value: []byte("1")},
			{Type: t4.TxnPut, Key: "b", Value: []byte("2")},
			{Type: t4.TxnPut, Key: "c", Value: []byte("3")},
		},
	})
	if err != nil {
		t.Fatalf("Txn: %v", err)
	}
	if !resp.Succeeded {
		t.Error("Txn: want Succeeded=true")
	}
	rev := resp.Revision

	// All three keys must share the same revision.
	for key, want := range map[string]string{"a": "1", "b": "2", "c": "3"} {
		kv, err := n.Get(key)
		if err != nil || kv == nil {
			t.Fatalf("Get(%q): err=%v kv=%v", key, err, kv)
		}
		if string(kv.Value) != want {
			t.Errorf("Get(%q): want %q got %q", key, want, kv.Value)
		}
		if kv.Revision != rev {
			t.Errorf("Get(%q).Revision: want %d got %d", key, rev, kv.Revision)
		}
		if kv.CreateRevision != rev {
			t.Errorf("Get(%q).CreateRevision: want %d got %d", key, rev, kv.CreateRevision)
		}
	}
}

func TestTxnConditionSucceeded(t *testing.T) {
	n := openNode(t)

	// Pre-create key a.
	aRev, err := n.Put(ctx(t), "a", []byte("old"), 0)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Txn: if a.ModRevision == aRev, update a and create b.
	resp, err := n.Txn(ctx(t), t4.TxnRequest{
		Conditions: []t4.TxnCondition{
			{Key: "a", Target: t4.TxnCondMod, Result: t4.TxnCondEqual, ModRevision: aRev},
		},
		Success: []t4.TxnOp{
			{Type: t4.TxnPut, Key: "a", Value: []byte("new")},
			{Type: t4.TxnPut, Key: "b", Value: []byte("created")},
		},
		Failure: []t4.TxnOp{
			{Type: t4.TxnPut, Key: "b", Value: []byte("fallback")},
		},
	})
	if err != nil {
		t.Fatalf("Txn: %v", err)
	}
	if !resp.Succeeded {
		t.Error("Txn: want Succeeded=true")
	}

	aKV, _ := n.Get("a")
	bKV, _ := n.Get("b")
	if aKV == nil || string(aKV.Value) != "new" {
		t.Errorf("a: want new got %v", aKV)
	}
	if bKV == nil || string(bKV.Value) != "created" {
		t.Errorf("b: want created got %v", bKV)
	}
	if aKV != nil && bKV != nil && aKV.Revision != bKV.Revision {
		t.Errorf("a and b must share revision: a=%d b=%d", aKV.Revision, bKV.Revision)
	}
}

func TestTxnVersionCondition(t *testing.T) {
	n := openNode(t)
	c := ctx(t)

	if _, err := n.Put(c, "a", []byte("v1"), 0); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	if _, err := n.Put(c, "a", []byte("v2"), 0); err != nil {
		t.Fatalf("Put v2: %v", err)
	}

	resp, err := n.Txn(c, t4.TxnRequest{
		Conditions: []t4.TxnCondition{
			{Key: "a", Target: t4.TxnCondVersion, Result: t4.TxnCondEqual, Version: 2},
		},
		Success: []t4.TxnOp{{Type: t4.TxnPut, Key: "result", Value: []byte("success")}},
		Failure: []t4.TxnOp{{Type: t4.TxnPut, Key: "result", Value: []byte("failure")}},
	})
	if err != nil {
		t.Fatalf("Txn: %v", err)
	}
	if !resp.Succeeded {
		t.Fatal("Txn: want Succeeded=true")
	}
	kv, _ := n.Get("result")
	if kv == nil || string(kv.Value) != "success" {
		t.Fatalf("result: want success got %v", kv)
	}
}

func TestTxnConditionFailed(t *testing.T) {
	n := openNode(t)

	_, _ = n.Put(ctx(t), "a", []byte("v1"), 0)
	// Give a a second revision so it no longer matches ModRevision==0.
	_, _ = n.Put(ctx(t), "a", []byte("v2"), 0)

	resp, err := n.Txn(ctx(t), t4.TxnRequest{
		Conditions: []t4.TxnCondition{
			{Key: "a", Target: t4.TxnCondMod, Result: t4.TxnCondEqual, ModRevision: 0},
		},
		Success: []t4.TxnOp{{Type: t4.TxnPut, Key: "result", Value: []byte("success")}},
		Failure: []t4.TxnOp{{Type: t4.TxnPut, Key: "result", Value: []byte("failure")}},
	})
	if err != nil {
		t.Fatalf("Txn: %v", err)
	}
	if resp.Succeeded {
		t.Error("Txn: want Succeeded=false")
	}

	kv, _ := n.Get("result")
	if kv == nil || string(kv.Value) != "failure" {
		t.Errorf("result: want failure got %v", kv)
	}
}

func TestTxnDeleteMissingKey(t *testing.T) {
	n := openNode(t)

	// Delete of a non-existent key is a no-op; the txn should succeed and not
	// write anything.
	resp, err := n.Txn(ctx(t), t4.TxnRequest{
		Success: []t4.TxnOp{
			{Type: t4.TxnDelete, Key: "ghost"},
		},
	})
	if err != nil {
		t.Fatalf("Txn: %v", err)
	}
	if !resp.Succeeded {
		t.Error("want Succeeded=true")
	}
	if resp.Revision != 0 {
		// No write should have occurred; revision stays at initial 0.
		t.Errorf("Revision: want 0 (no-op) got %d", resp.Revision)
	}
}

func TestTxnDeleteAndDeletedKeys(t *testing.T) {
	n := openNode(t)

	_, _ = n.Put(ctx(t), "x", []byte("val"), 0)

	resp, err := n.Txn(ctx(t), t4.TxnRequest{
		Success: []t4.TxnOp{
			{Type: t4.TxnDelete, Key: "x"},
			{Type: t4.TxnDelete, Key: "y"}, // does not exist
		},
	})
	if err != nil {
		t.Fatalf("Txn: %v", err)
	}
	if _, ok := resp.DeletedKeys["x"]; !ok {
		t.Error("DeletedKeys: want x present")
	}
	if _, ok := resp.DeletedKeys["y"]; ok {
		t.Error("DeletedKeys: want y absent (never existed)")
	}

	// x must be gone, y was never there.
	if kv, _ := n.Get("x"); kv != nil {
		t.Error("x should be deleted")
	}
}

func TestTxnSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	store := object.NewMem()
	cfg := t4.Config{DataDir: dir, ObjectStore: store}

	n, err := t4.Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	resp, err := n.Txn(ctx(t), t4.TxnRequest{
		Success: []t4.TxnOp{
			{Type: t4.TxnPut, Key: "p", Value: []byte("alpha")},
			{Type: t4.TxnPut, Key: "q", Value: []byte("beta")},
		},
	})
	if err != nil {
		t.Fatalf("Txn: %v", err)
	}
	txnRev := resp.Revision
	_ = n.Close()

	// Reopen and verify both keys survived at the same revision.
	n2, err := t4.Open(cfg)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer func() { _ = n2.Close() }()

	for key, want := range map[string]string{"p": "alpha", "q": "beta"} {
		kv, err := n2.Get(key)
		if err != nil || kv == nil {
			t.Fatalf("Get(%q) after restart: err=%v kv=%v", key, err, kv)
		}
		if string(kv.Value) != want {
			t.Errorf("Get(%q) value: want %q got %q", key, want, kv.Value)
		}
		if kv.Revision != txnRev {
			t.Errorf("Get(%q) revision: want %d got %d", key, txnRev, kv.Revision)
		}
	}
}

func TestTxnWatchEvents(t *testing.T) {
	n := openNode(t)
	c := ctx(t)

	ch, err := n.Watch(c, "", 0) // watch all keys
	if err != nil {
		t.Fatal(err)
	}

	// Execute the txn after the watcher is registered.
	resp, err := n.Txn(c, t4.TxnRequest{
		Success: []t4.TxnOp{
			{Type: t4.TxnPut, Key: "x", Value: []byte("1")},
			{Type: t4.TxnPut, Key: "y", Value: []byte("2")},
		},
	})
	if err != nil {
		t.Fatalf("Txn: %v", err)
	}
	txnRev := resp.Revision

	// Collect exactly 2 events (one per key).
	got := map[string]int64{}
	for i := 0; i < 2; i++ {
		select {
		case ev := <-ch:
			if ev.Type != t4.EventPut {
				t.Errorf("event %d: want EventPut, got %v", i, ev.Type)
			}
			got[ev.KV.Key] = ev.KV.Revision
		case <-c.Done():
			t.Fatalf("timeout waiting for txn watch event %d", i)
		}
	}
	for _, key := range []string{"x", "y"} {
		if rev, ok := got[key]; !ok {
			t.Errorf("missing Watch event for key %q", key)
		} else if rev != txnRev {
			t.Errorf("key %q: Watch event revision %d, want %d", key, rev, txnRev)
		}
	}
}

func TestTxnCompactSubKeys(t *testing.T) {
	n := openNode(t)
	c := ctx(t)

	// Write a single-key entry at rev 1, then a 2-key txn at rev 2.
	_, _ = n.Put(c, "before", []byte("v"), 0)
	txnResp, err := n.Txn(c, t4.TxnRequest{
		Success: []t4.TxnOp{
			{Type: t4.TxnPut, Key: "p", Value: []byte("1")},
			{Type: t4.TxnPut, Key: "q", Value: []byte("2")},
		},
	})
	if err != nil {
		t.Fatalf("Txn: %v", err)
	}

	// Compact up to and including the txn revision.
	if err := n.Compact(c, txnResp.Revision); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// Keys written in the txn must still be readable after compaction.
	for key, want := range map[string]string{"p": "1", "q": "2"} {
		kv, err := n.Get(key)
		if err != nil || kv == nil {
			t.Fatalf("Get(%q) after compact: err=%v kv=%v", key, err, kv)
		}
		if string(kv.Value) != want {
			t.Errorf("Get(%q): want %q got %q", key, want, kv.Value)
		}
	}
}

func TestTxnConcurrentCAS(t *testing.T) {
	n := openNode(t)

	seedRev, err := n.Put(context.Background(), "counter", []byte("0"), 0)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	const workers = 20
	successes := make([]bool, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := n.Txn(context.Background(), t4.TxnRequest{
				Conditions: []t4.TxnCondition{
					{Key: "counter", Target: t4.TxnCondMod, Result: t4.TxnCondEqual, ModRevision: seedRev},
				},
				Success: []t4.TxnOp{
					{Type: t4.TxnPut, Key: "counter", Value: []byte(strconv.Itoa(i + 1))},
				},
			})
			if err == nil && resp.Succeeded {
				successes[i] = true
			}
		}(i)
	}
	wg.Wait()

	wins := 0
	for _, ok := range successes {
		if ok {
			wins++
		}
	}
	if wins != 1 {
		t.Errorf("concurrent CAS: want exactly 1 winner, got %d", wins)
	}
}

func TestTxnDuplicateKey(t *testing.T) {
	n := openNode(t)

	_, err := n.Txn(ctx(t), t4.TxnRequest{
		Success: []t4.TxnOp{
			{Type: t4.TxnPut, Key: "a", Value: []byte("1")},
			{Type: t4.TxnPut, Key: "b", Value: []byte("2")},
			{Type: t4.TxnPut, Key: "a", Value: []byte("3")}, // duplicate
		},
	})
	if err == nil {
		t.Error("Txn with duplicate key: want error, got nil")
	}

	// No write should have occurred.
	kv, _ := n.Get("a")
	if kv != nil {
		t.Errorf("key a should not exist after rejected txn, got %v", kv)
	}
}
