package strata_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/strata-db/strata"
	"github.com/strata-db/strata/pkg/object"
)

// openNode starts a Node with an in-memory object store and a temp data dir.
func openNode(t *testing.T) *strata.Node {
	t.Helper()
	n, err := strata.Open(strata.Config{
		DataDir:     t.TempDir(),
		ObjectStore: object.NewMem(),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { n.Close() })
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
	if kv.PrevRevision != 1 {
		t.Errorf("PrevRevision: want 1 got %d", kv.PrevRevision)
	}
	if string(kv.Value) != "v2" {
		t.Errorf("Value: want v2 got %q", kv.Value)
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
	if !errors.Is(err, strata.ErrKeyExists) {
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
	wantTypes := []strata.EventType{strata.EventPut, strata.EventPut, strata.EventDelete}

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

func TestNodeWatchPrevKV(t *testing.T) {
	n := openNode(t)
	c := ctx(t)
	n.Put(c, "k", []byte("old"), 0)

	ch, _ := n.Watch(c, "k", n.CurrentRevision()+1)

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

	// Current value still accessible.
	kv, _ := n.Get("k")
	if string(kv.Value) != "v3" {
		t.Errorf("current value: want v3 got %q", kv.Value)
	}
}

func TestWatchCompactedRevisionReturnsError(t *testing.T) {
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
	if !errors.Is(err, strata.ErrCompacted) {
		t.Errorf("Watch from deeply compacted rev: want ErrCompacted, got %v", err)
	}

	// startRev=3 is the compact boundary itself; etcd semantics require ErrCompacted.
	_, err = n.Watch(c, "k", 3)
	if !errors.Is(err, strata.ErrCompacted) {
		t.Errorf("Watch from compact watermark (startRev=compactRev): want ErrCompacted, got %v", err)
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
	cfg := strata.Config{DataDir: dir, ObjectStore: obj}

	// First open: write some data.
	n, err := strata.Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	c := ctx(t)
	n.Put(c, "persistent", []byte("yes"), 0)
	n.Put(c, "also", []byte("here"), 0)
	rev := n.CurrentRevision()
	n.Close()

	// Second open: state must survive.
	n2, err := strata.Open(cfg)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer n2.Close()

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
