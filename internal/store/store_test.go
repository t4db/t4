package store

import (
	"context"
	"testing"
	"time"

	"github.com/t4db/t4/internal/wal"
)

// openMem opens an in-memory store and fails the test on error.
func openMem(t *testing.T) *Store {
	t.Helper()
	s, err := OpenMem()
	if err != nil {
		t.Fatalf("OpenMem: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func apply(t *testing.T, s *Store, entries ...wal.Entry) {
	t.Helper()
	if err := s.Apply(entries); err != nil {
		t.Fatalf("Apply: %v", err)
	}
}

func createEntry(rev int64, key string, value []byte) wal.Entry {
	return wal.Entry{
		Revision:       rev,
		Term:           1,
		Op:             wal.OpCreate,
		Key:            key,
		Value:          value,
		CreateRevision: rev,
	}
}

func updateEntry(rev int64, key string, value []byte, createRev, prevRev int64) wal.Entry {
	return wal.Entry{
		Revision:       rev,
		Term:           1,
		Op:             wal.OpUpdate,
		Key:            key,
		Value:          value,
		CreateRevision: createRev,
		PrevRevision:   prevRev,
	}
}

func deleteEntry(rev int64, key string, createRev, prevRev int64) wal.Entry {
	return wal.Entry{
		Revision:       rev,
		Term:           1,
		Op:             wal.OpDelete,
		Key:            key,
		CreateRevision: createRev,
		PrevRevision:   prevRev,
	}
}

// ── CurrentRevision ──────────────────────────────────────────────────────────

func TestCurrentRevision(t *testing.T) {
	s := openMem(t)
	if s.CurrentRevision() != 0 {
		t.Errorf("want 0, got %d", s.CurrentRevision())
	}
	apply(t, s, createEntry(1, "k", []byte("v")))
	if s.CurrentRevision() != 1 {
		t.Errorf("want 1, got %d", s.CurrentRevision())
	}
	apply(t, s, updateEntry(2, "k", []byte("v2"), 1, 1))
	if s.CurrentRevision() != 2 {
		t.Errorf("want 2, got %d", s.CurrentRevision())
	}
}

// ── Get ──────────────────────────────────────────────────────────────────────

func TestGetNotFound(t *testing.T) {
	s := openMem(t)
	kv, err := s.Get("missing")
	if err != nil {
		t.Fatal(err)
	}
	if kv != nil {
		t.Errorf("want nil, got %+v", kv)
	}
}

func TestGetCreate(t *testing.T) {
	s := openMem(t)
	apply(t, s, createEntry(1, "foo", []byte("bar")))

	kv, err := s.Get("foo")
	if err != nil {
		t.Fatal(err)
	}
	if kv == nil {
		t.Fatal("expected kv, got nil")
	}
	if kv.Key != "foo" {
		t.Errorf("Key: want %q got %q", "foo", kv.Key)
	}
	if string(kv.Value) != "bar" {
		t.Errorf("Value: want %q got %q", "bar", kv.Value)
	}
	if kv.Revision != 1 {
		t.Errorf("Revision: want 1 got %d", kv.Revision)
	}
	if kv.CreateRevision != 1 {
		t.Errorf("CreateRevision: want 1 got %d", kv.CreateRevision)
	}
}

func TestGetAfterUpdate(t *testing.T) {
	s := openMem(t)
	apply(t, s,
		createEntry(1, "k", []byte("v1")),
		updateEntry(2, "k", []byte("v2"), 1, 1),
	)

	kv, err := s.Get("k")
	if err != nil {
		t.Fatal(err)
	}
	if string(kv.Value) != "v2" {
		t.Errorf("Value: want v2 got %q", kv.Value)
	}
	if kv.Revision != 2 {
		t.Errorf("Revision: want 2 got %d", kv.Revision)
	}
	if kv.CreateRevision != 1 {
		t.Errorf("CreateRevision: want 1 got %d", kv.CreateRevision)
	}
	if kv.PrevRevision != 1 {
		t.Errorf("PrevRevision: want 1 got %d", kv.PrevRevision)
	}
}

func TestGetAfterDelete(t *testing.T) {
	s := openMem(t)
	apply(t, s,
		createEntry(1, "k", []byte("v")),
		deleteEntry(2, "k", 1, 1),
	)
	kv, err := s.Get("k")
	if err != nil {
		t.Fatal(err)
	}
	if kv != nil {
		t.Errorf("expected nil after delete, got %+v", kv)
	}
}

func TestExists(t *testing.T) {
	s := openMem(t)
	apply(t, s, createEntry(1, "k", []byte("v")))

	ok, err := s.Exists("k")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected key to exist")
	}

	ok, err = s.Exists("missing")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("missing key exists")
	}

	apply(t, s, deleteEntry(2, "k", 1, 1))
	ok, err = s.Exists("k")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("deleted key exists")
	}
}

// ── List ─────────────────────────────────────────────────────────────────────

func TestList(t *testing.T) {
	s := openMem(t)
	apply(t, s,
		createEntry(1, "/a/1", []byte("1")),
		createEntry(2, "/a/2", []byte("2")),
		createEntry(3, "/b/1", []byte("3")),
	)

	kvs, err := s.List("/a/")
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 2 {
		t.Fatalf("List /a/: want 2 got %d", len(kvs))
	}
	if kvs[0].Key != "/a/1" || kvs[1].Key != "/a/2" {
		t.Errorf("unexpected keys: %q %q", kvs[0].Key, kvs[1].Key)
	}
}

func TestListEmpty(t *testing.T) {
	s := openMem(t)
	kvs, err := s.List("/nope/")
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 0 {
		t.Errorf("expected empty, got %d", len(kvs))
	}
}

func TestListAfterDelete(t *testing.T) {
	s := openMem(t)
	apply(t, s,
		createEntry(1, "/a/1", []byte("1")),
		createEntry(2, "/a/2", []byte("2")),
		deleteEntry(3, "/a/1", 1, 1),
	)
	kvs, err := s.List("/a/")
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 1 || kvs[0].Key != "/a/2" {
		t.Errorf("unexpected kvs after delete: %+v", kvs)
	}
}

// ── Count ────────────────────────────────────────────────────────────────────

func TestCount(t *testing.T) {
	s := openMem(t)
	apply(t, s,
		createEntry(1, "/x/1", nil),
		createEntry(2, "/x/2", nil),
		createEntry(3, "/y/1", nil),
		deleteEntry(4, "/x/1", 1, 1),
	)
	n, err := s.Count("/x/")
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("Count /x/: want 1 got %d", n)
	}
}

// ── Compact ───────────────────────────────────────────────────────────────────

func TestCompact(t *testing.T) {
	s := openMem(t)
	apply(t, s,
		createEntry(1, "k", []byte("v1")),
		updateEntry(2, "k", []byte("v2"), 1, 1),
		updateEntry(3, "k", []byte("v3"), 1, 2),
	)

	// Compact at rev 2 — the log entry at rev 1 should be gone.
	compactEntry := wal.Entry{
		Revision:     4,
		Term:         1,
		Op:           wal.OpCompact,
		PrevRevision: 2, // compact target
	}
	apply(t, s, compactEntry)

	if s.CompactRevision() != 2 {
		t.Errorf("CompactRevision: want 2 got %d", s.CompactRevision())
	}
	if s.CurrentRevision() != 4 {
		t.Errorf("CurrentRevision after compact: want 4 got %d", s.CurrentRevision())
	}

	// The current value should still be accessible.
	kv, err := s.Get("k")
	if err != nil {
		t.Fatal(err)
	}
	if string(kv.Value) != "v3" {
		t.Errorf("value after compact: want v3 got %q", kv.Value)
	}
}

// ── Recover ───────────────────────────────────────────────────────────────────

func TestRecover(t *testing.T) {
	s := openMem(t)
	entries := []wal.Entry{
		createEntry(1, "a", []byte("1")),
		createEntry(2, "b", []byte("2")),
		updateEntry(3, "a", []byte("3"), 1, 1),
	}
	if err := s.Recover(entries); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if s.CurrentRevision() != 3 {
		t.Errorf("CurrentRevision: want 3 got %d", s.CurrentRevision())
	}

	kv, _ := s.Get("a")
	if string(kv.Value) != "3" {
		t.Errorf("value of a: want 3 got %q", kv.Value)
	}
}

// ── Watch ─────────────────────────────────────────────────────────────────────

func TestWatch(t *testing.T) {
	s := openMem(t)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	ch, err := s.Watch(ctx, "/w/", 0, false)
	if err != nil {
		t.Fatal(err)
	}

	// Apply events in a goroutine after the watcher is registered.
	go func() {
		apply(t, s,
			createEntry(1, "/w/x", []byte("1")),
			createEntry(2, "/w/y", []byte("2")),
			createEntry(3, "/other/z", []byte("3")), // should be filtered out
			deleteEntry(4, "/w/x", 1, 1),
		)
	}()

	wantKeys := []string{"/w/x", "/w/y", "/w/x"}
	wantTypes := []EventType{EventPut, EventPut, EventDelete}

	for i := 0; i < 3; i++ {
		select {
		case ev := <-ch:
			if ev.KV.Key != wantKeys[i] {
				t.Errorf("event %d: key want %q got %q", i, wantKeys[i], ev.KV.Key)
			}
			if ev.Type != wantTypes[i] {
				t.Errorf("event %d: type want %v got %v", i, wantTypes[i], ev.Type)
			}
		case <-ctx.Done():
			t.Fatalf("timeout waiting for watch event %d", i)
		}
	}
}

func TestWatchPrevKV(t *testing.T) {
	s := openMem(t)
	// Apply a create before the watcher starts — event will include PrevKV for update.
	apply(t, s, createEntry(1, "k", []byte("old")))

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	ch, _ := s.Watch(ctx, "k", 1, true)

	go func() {
		apply(t, s, updateEntry(2, "k", []byte("new"), 1, 1))
	}()

	ev := <-ch
	if ev.PrevKV == nil {
		t.Error("expected PrevKV for update event")
	} else if string(ev.PrevKV.Value) != "old" {
		t.Errorf("PrevKV.Value: want old got %q", ev.PrevKV.Value)
	}
}

func TestWatchCancelStopsChannel(t *testing.T) {
	s := openMem(t)
	ctx, cancel := context.WithCancel(context.Background())
	ch, _ := s.Watch(ctx, "", 0, false)
	cancel()

	// Channel must close shortly after cancel.
	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected channel to close, got value")
		}
	case <-timer.C:
		t.Error("channel did not close after context cancel")
	}
}

// ── WaitForRevision ───────────────────────────────────────────────────────────

func TestWaitForRevision(t *testing.T) {
	s := openMem(t)

	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	go func() {
		done <- s.WaitForRevision(ctx, 3)
	}()

	apply(t, s, createEntry(1, "a", nil), createEntry(2, "b", nil))
	// Not yet at rev 3.
	select {
	case err := <-done:
		t.Fatalf("WaitForRevision returned early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	apply(t, s, createEntry(3, "c", nil))
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("WaitForRevision: %v", err)
		}
	case <-ctx.Done():
		t.Error("timeout")
	}
}

// TestWaitForRevisionConcurrent stresses many waiters racing against an Apply
// that satisfies their target revision. With the lost-wakeup bug, a waiter
// could read the new notify channel after broadcast already fired and block
// indefinitely on it. The race window is narrow so this test is probabilistic;
// it serves as a smoke check that the wait path stays sane under contention.
func TestWaitForRevisionConcurrent(t *testing.T) {
	const iters = 500
	const waiters = 32

	for i := 0; i < iters; i++ {
		s := openMem(t)
		targetRev := int64(1)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

		errs := make(chan error, waiters)
		for w := 0; w < waiters; w++ {
			go func() { errs <- s.WaitForRevision(ctx, targetRev) }()
		}

		apply(t, s, createEntry(targetRev, "k", nil))

		for w := 0; w < waiters; w++ {
			select {
			case err := <-errs:
				if err != nil {
					cancel()
					t.Fatalf("iter %d waiter %d: %v", i, w, err)
				}
			case <-ctx.Done():
				cancel()
				t.Fatalf("iter %d: lost wakeup — %d/%d waiters returned",
					i, w, waiters)
			}
		}
		cancel()
	}
}
