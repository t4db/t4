package wal

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

// fakeStore is an uploader backed by a map, which removes the local file on
// success as the real one does. While down is set every upload fails.
type fakeStore struct {
	mu       sync.Mutex
	down     bool
	block    chan struct{} // when non-nil, uploads wait on it
	attempts int
	tried    map[string]bool // keys an upload was attempted for
	objects  map[string]bool
	// conflicts holds keys already taken by different entries.
	conflicts map[string]bool
}

func newFakeStore() *fakeStore {
	return &fakeStore{tried: make(map[string]bool), objects: make(map[string]bool), conflicts: make(map[string]bool)}
}

func (f *fakeStore) upload(ctx context.Context, path, key string) error {
	f.mu.Lock()
	f.attempts++
	f.tried[key] = true
	down, block := f.down, f.block
	f.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if down {
		return errors.New("object store unreachable")
	}
	f.mu.Lock()
	conflict := f.conflicts[key]
	f.mu.Unlock()
	if conflict {
		return ErrSegmentConflict
	}
	if _, err := os.Stat(path); err != nil {
		return err
	}
	f.mu.Lock()
	f.objects[key] = true
	f.mu.Unlock()
	return os.Remove(path)
}

func (f *fakeStore) set(down bool) {
	f.mu.Lock()
	f.down = down
	f.mu.Unlock()
}

func (f *fakeStore) counts() (attempts, objects int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts, len(f.objects)
}

func shortBackoff(t *testing.T, minD, maxD time.Duration) {
	t.Helper()
	oldMin, oldMax := uploadRetryMin, uploadRetryMax
	uploadRetryMin, uploadRetryMax = minD, maxD
	t.Cleanup(func() { uploadRetryMin, uploadRetryMax = oldMin, oldMax })
}

func startWAL(t *testing.T, dir string, up Uploader) *WAL {
	t.Helper()
	w, err := Open(dir, 1, 1, WithUploader(up), WithSegmentMaxAge(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	w.Start(ctx)
	return w
}

// sealSegments writes n one-entry segments.
func sealSegments(t *testing.T, w *WAL, first int64, n int) {
	t.Helper()
	for i := range int64(n) {
		seq := first + i
		if err := w.Append(&Entry{Revision: seq, Term: 1, Op: OpCreate, Key: "k", Value: []byte("v")}); err != nil {
			t.Fatal(err)
		}
		if err := w.SealAndFlush(seq + 1); err != nil {
			t.Fatal(err)
		}
	}
}

func pendingCount(w *WAL) int {
	w.pendingMu.Lock()
	defer w.pendingMu.Unlock()
	return len(w.pending)
}

// TestUploadBacksOffDuringOutage pins that a backlog of failing segments is
// retried as one attempt per backoff period, not one per segment, and that
// all of it is uploaded once object storage is back.
func TestUploadBacksOffDuringOutage(t *testing.T) {
	shortBackoff(t, 20*time.Millisecond, 80*time.Millisecond)
	store := newFakeStore()
	store.set(true)
	w := startWAL(t, t.TempDir(), store.upload)

	const segments = 100
	sealSegments(t, w, 1, segments)

	time.Sleep(500 * time.Millisecond)

	// A pass stops at its first failure, so while the store is down only the
	// oldest segment is tried, once per backoff period. Before, every segment
	// was tried and retried on its own.
	store.mu.Lock()
	tried, attempts := len(store.tried), store.attempts
	store.mu.Unlock()
	if tried != 1 {
		t.Errorf("uploads tried for %d of %d segments while the store was down, want only the oldest", tried, segments)
	}
	if attempts > segments/2 {
		t.Errorf("%d upload attempts for %d failing segments, want one per backoff period", attempts, segments)
	}

	store.set(false)
	deadline := time.Now().Add(5 * time.Second)
	for pendingCount(w) > 0 {
		if time.Now().After(deadline) {
			_, uploaded := store.counts()
			t.Fatalf("%d segments still pending after recovery, %d uploaded", pendingCount(w), uploaded)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, uploaded := store.counts(); uploaded != segments {
		t.Errorf("uploaded %d segments, want %d", uploaded, segments)
	}
}

// TestSealDoesNotBlockOnUploadBacklog pins that sealing never waits for
// uploads. SealAndFlush runs under the write fence during a checkpoint, so
// blocking it on a hung object store stalled every write.
func TestSealDoesNotBlockOnUploadBacklog(t *testing.T) {
	store := newFakeStore()
	store.block = make(chan struct{}) // every upload hangs
	defer close(store.block)
	w := startWAL(t, t.TempDir(), store.upload)

	done := make(chan struct{})
	go func() {
		defer close(done)
		sealSegments(t, w, 1, 200) // more than the old 64-slot queue
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("sealing blocked behind a hung upload")
	}
	if n := pendingCount(w); n != 200 {
		t.Errorf("%d segments pending, want all 200", n)
	}
}

// TestLeftoverSegmentsUploadedOnStart pins that segments left in the WAL
// directory by an earlier run, never uploaded, are uploaded once the WAL
// starts again.
func TestLeftoverSegmentsUploadedOnStart(t *testing.T) {
	dir := t.TempDir()
	store := newFakeStore()
	store.set(true)
	w := startWAL(t, dir, store.upload)
	sealSegments(t, w, 1, 5)
	_ = w.Close() // the outage outlasts the run: nothing is uploaded

	store.set(false)
	w2 := startWAL(t, dir, store.upload)
	defer func() { _ = w2.Close() }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, uploaded := store.counts(); uploaded == 5 {
			break
		}
		if time.Now().After(deadline) {
			_, uploaded := store.counts()
			t.Fatalf("uploaded %d of 5 leftover segments after restart", uploaded)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestConflictingLeftoverDoesNotBlockSyncWrites reproduces a promotion: the
// new leader's WAL directory holds a segment it wrote as a follower, whose
// key the previous leader already published with different entries. That
// conflict is permanent, but it must not block writes once the leader
// switches to synchronous uploads (replication below its ACK target).
func TestConflictingLeftoverDoesNotBlockSyncWrites(t *testing.T) {
	dir := t.TempDir()
	follower, err := Open(dir, 1, 1) // no uploader: segments stay local
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	follower.Start(ctx)
	for seq := int64(1); seq <= 3; seq++ {
		if err := follower.Append(&Entry{Revision: seq, Term: 1, Op: OpCreate, Key: "k", Value: []byte("v")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := follower.Close(); err != nil {
		t.Fatal(err)
	}

	store := newFakeStore()
	store.conflicts[ObjectKey(1, 1)] = true
	w := startWAL(t, dir, store.upload)
	t.Cleanup(func() { _ = w.Close() })
	w.SetSyncUpload(true)

	for seq := int64(4); seq <= 6; seq++ {
		err := w.AppendBatch(context.Background(), []*Entry{{Revision: seq, Term: 2, Op: OpCreate, Key: "k", Value: []byte("v")}})
		if err != nil {
			t.Fatalf("write %d after promotion: %v", seq, err)
		}
	}
	// The conflicting leftover is dropped, not retried forever.
	deadline := time.Now().Add(5 * time.Second)
	for {
		w.pendingMu.Lock()
		n := len(w.leftover)
		w.pendingMu.Unlock()
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("conflicting leftover segment still queued")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
