package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/t4db/t4/internal/metrics"
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

func TestRevisionSamples(t *testing.T) {
	s := openMem(t)
	base := time.Unix(100, 0)

	if err := s.RevisionSample(10, base); err != nil {
		t.Fatalf("RevisionSample rev=10: %v", err)
	}
	if err := s.RevisionSample(20, base.Add(time.Hour)); err != nil {
		t.Fatalf("RevisionSample rev=20: %v", err)
	}

	rev, ts, ok, err := s.RevisionSampleAtOrBefore(base.Add(30 * time.Minute))
	if err != nil {
		t.Fatalf("RevisionSampleAtOrBefore: %v", err)
	}
	if !ok || rev != 10 || !ts.Equal(base) {
		t.Fatalf("sample before cutoff: rev=%d ts=%s ok=%v", rev, ts, ok)
	}

	rev, ts, ok, err = s.LatestRevisionSample()
	if err != nil {
		t.Fatalf("LatestRevisionSample: %v", err)
	}
	if !ok || rev != 20 || !ts.Equal(base.Add(time.Hour)) {
		t.Fatalf("latest sample: rev=%d ts=%s ok=%v", rev, ts, ok)
	}

	if err := s.DeleteRevisionSamplesBefore(base.Add(time.Hour)); err != nil {
		t.Fatalf("DeleteRevisionSamplesBefore: %v", err)
	}
	rev, ts, ok, err = s.RevisionSampleAtOrBefore(base.Add(30 * time.Minute))
	if err != nil {
		t.Fatalf("RevisionSampleAtOrBefore after delete: %v", err)
	}
	if ok {
		t.Fatalf("old sample survived delete: rev=%d ts=%s", rev, ts)
	}
}

func TestClosedStoreRejectsReadyWaitAndWatch(t *testing.T) {
	s, err := OpenMem()
	if err != nil {
		t.Fatalf("OpenMem: %v", err)
	}
	apply(t, s, createEntry(1, "k", []byte("v")))
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := s.WaitForRevision(context.Background(), 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("WaitForRevision after close = %v, want ErrClosed", err)
	}
	if _, err := s.Watch(context.Background(), "", 0, false); !errors.Is(err, ErrClosed) {
		t.Fatalf("Watch after close = %v, want ErrClosed", err)
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

func TestGetAtRevision(t *testing.T) {
	s := openMem(t)
	apply(t, s,
		createEntry(1, "k", []byte("v1")),
		updateEntry(3, "k", []byte("v3"), 1, 1),
		deleteEntry(5, "k", 1, 3),
	)

	kv, err := s.GetAt("k", 2)
	if err != nil {
		t.Fatal(err)
	}
	if kv == nil || string(kv.Value) != "v1" || kv.Revision != 1 {
		t.Fatalf("GetAt rev=2: got %+v", kv)
	}

	kv, err = s.GetAt("k", 4)
	if err != nil {
		t.Fatal(err)
	}
	if kv == nil || string(kv.Value) != "v3" || kv.Revision != 3 {
		t.Fatalf("GetAt rev=4: got %+v", kv)
	}

	kv, err = s.GetAt("k", 5)
	if err != nil {
		t.Fatal(err)
	}
	if kv != nil {
		t.Fatalf("GetAt rev=5: want nil after delete, got %+v", kv)
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

func TestListLimitCountAtRevision(t *testing.T) {
	s := openMem(t)
	apply(t, s,
		createEntry(1, "/a/1", []byte("1")),
		createEntry(2, "/a/2", []byte("2")),
		updateEntry(3, "/a/1", []byte("1b"), 1, 1),
		createEntry(4, "/a/3", []byte("3")),
		deleteEntry(5, "/a/2", 2, 2),
	)

	kvs, err := s.ListRange("/a/", ReadOptions{Revision: 3, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 2 || kvs[0].Key != "/a/1" || string(kvs[0].Value) != "1b" || kvs[1].Key != "/a/2" {
		t.Fatalf("ListLimitAt rev=3: got %+v", kvs)
	}

	n, err := s.CountRange("/a/", ReadOptions{Revision: 3})
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("CountAt rev=3: want 2 got %d", n)
	}

	kvs, err = s.ListRange("/a/", ReadOptions{Revision: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 2 || kvs[0].Key != "/a/1" || kvs[1].Key != "/a/3" {
		t.Fatalf("ListAt rev=5: got %+v", kvs)
	}
}

func TestListRangeAppliesFromKeyBeforeLimit(t *testing.T) {
	s := openMem(t)
	apply(t, s,
		createEntry(1, "/page/1", []byte("1")),
		createEntry(2, "/page/2", []byte("2")),
		createEntry(3, "/page/3", []byte("3")),
		createEntry(4, "/page/4", []byte("4")),
	)

	kvs, err := s.ListRange("/page/", ReadOptions{
		Revision: 4,
		FromKey:  "/page/3",
		Limit:    2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 2 || kvs[0].Key != "/page/3" || kvs[1].Key != "/page/4" {
		t.Fatalf("ListRange fromKey+limit: got %+v", kvs)
	}

	n, err := s.CountRange("/page/", ReadOptions{Revision: 4, FromKey: "/page/3"})
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("CountRange fromKey: want 2 got %d", n)
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
	if s.CurrentRevision() != 3 {
		t.Errorf("CurrentRevision after compact: want 3 got %d", s.CurrentRevision())
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

func TestCompactPreservesVersionVisibleAfterCompactRevision(t *testing.T) {
	s := openMem(t)
	apply(t, s,
		createEntry(1, "k", []byte("v1")),
		updateEntry(5, "k", []byte("v5"), 1, 1),
	)
	apply(t, s, wal.Entry{
		Revision:     6,
		Term:         1,
		Op:           wal.OpCompact,
		PrevRevision: 3,
	})

	kv, err := s.GetAt("k", 4)
	if err != nil {
		t.Fatal(err)
	}
	if kv == nil || string(kv.Value) != "v1" || kv.Revision != 1 {
		t.Fatalf("GetAt rev=4 after compact: got %+v", kv)
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

func TestWatchMetrics(t *testing.T) {
	metrics.Register(prometheus.NewRegistry())
	s := openMem(t)

	ctx, cancel := context.WithCancel(context.Background())
	ch1, err := s.Watch(ctx, "/metrics/", 0, false)
	if err != nil {
		t.Fatal(err)
	}
	ch2, err := s.Watch(ctx, "/metrics/", 0, false)
	if err != nil {
		t.Fatal(err)
	}

	if got := testutil.ToFloat64(metrics.WatchActive); got != 2 {
		t.Fatalf("active watches: want 2 got %v", got)
	}
	if got := testutil.ToFloat64(metrics.WatchActivePrefixes); got != 1 {
		t.Fatalf("active watch prefixes: want 1 got %v", got)
	}

	apply(t, s,
		createEntry(1, "/metrics/a", []byte("1")),
		createEntry(2, "/other/a", []byte("2")),
	)

	for i, ch := range []<-chan Event{ch1, ch2} {
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatalf("watcher %d did not receive matching event", i)
		}
	}

	waitForMetric(t, func() float64 {
		return testutil.ToFloat64(metrics.WatchScanEntriesTotal.WithLabelValues("scanned"))
	}, func(v float64) bool { return v > 0 }, "scanned watch entries")
	waitForMetric(t, func() float64 {
		return testutil.ToFloat64(metrics.WatchScanEntriesTotal.WithLabelValues("matched"))
	}, func(v float64) bool { return v > 0 }, "matched watch entries")

	cancel()
	waitForMetric(t, func() float64 {
		return testutil.ToFloat64(metrics.WatchActive)
	}, func(v float64) bool { return v == 0 }, "active watches after cancel")
	waitForMetric(t, func() float64 {
		return testutil.ToFloat64(metrics.WatchActivePrefixes)
	}, func(v float64) bool { return v == 0 }, "active watch prefixes after cancel")
}

func waitForMetric(t *testing.T, get func() float64, ok func(float64) bool, name string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		v := get()
		if ok(v) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s, last value %v", name, get())
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
