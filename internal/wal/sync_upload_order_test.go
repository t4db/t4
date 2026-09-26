package wal

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeObjectStore is an idempotent Uploader that records uploaded object
// keys in order and can be told to fail uploads of chosen keys.
type fakeObjectStore struct {
	mu       sync.Mutex
	uploaded []string
	attempts map[string]int
	failFor  func(key string, attempt int) bool
}

func (f *fakeObjectStore) upload(_ context.Context, _, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.attempts == nil {
		f.attempts = make(map[string]int)
	}
	f.attempts[key]++
	if f.failFor != nil && f.failFor(key, f.attempts[key]) {
		return errors.New("injected upload failure")
	}
	f.uploaded = append(f.uploaded, key)
	return nil
}

func (f *fakeObjectStore) has(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, k := range f.uploaded {
		if k == key {
			return true
		}
	}
	return false
}

func (f *fakeObjectStore) attemptsFor(key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts[key]
}

// sealFirstSegment writes seq 1, seals it, and waits until its asynchronous
// upload has been attempted (and, with failFor, failed) once.
func sealFirstSegment(t *testing.T, w *WAL, store *fakeObjectStore) string {
	t.Helper()
	ctx := context.Background()
	if err := w.AppendBatch(ctx, makeEntries(1, 1, 1)); err != nil {
		t.Fatal(err)
	}
	if err := w.SealAndFlush(2); err != nil {
		t.Fatal(err)
	}
	first := ObjectKey(1, 1)
	deadline := time.Now().Add(5 * time.Second)
	for store.attemptsFor(first) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("first segment upload never attempted")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return first
}

// TestSyncUploadIncludesEarlierSegments: a batch acknowledged in
// synchronous-upload mode must not be in object storage while an earlier
// segment is not, or recovery from object storage hits a gap in the WAL.
func TestSyncUploadIncludesEarlierSegments(t *testing.T) {
	store := &fakeObjectStore{}
	// The asynchronous upload of the first segment fails once (it would be
	// retried only seconds later).
	store.failFor = func(key string, attempt int) bool { return key == ObjectKey(1, 1) && attempt == 1 }
	w, err := Open(t.TempDir(), 1, 1, WithUploader(store.upload))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)
	t.Cleanup(func() { _ = w.Close() })

	first := sealFirstSegment(t, w, store)
	w.SetSyncUpload(true)
	if err := w.AppendBatch(ctx, makeEntries(1, 2, 1)); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if !store.has(first) || !store.has(ObjectKey(1, 2)) {
		t.Fatalf("after a synchronous batch, object storage has %v; want the earlier segment too", store.uploaded)
	}
}

// TestSyncUploadRejectsBatchWhileEarlierSegmentFails: if an earlier segment
// cannot be uploaded, the batch is not acknowledged and not replayed.
func TestSyncUploadRejectsBatchWhileEarlierSegmentFails(t *testing.T) {
	store := &fakeObjectStore{}
	store.failFor = func(key string, _ int) bool { return key == ObjectKey(1, 1) }
	dir := t.TempDir()
	w, err := Open(dir, 1, 1, WithUploader(store.upload))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)

	sealFirstSegment(t, w, store)
	w.SetSyncUpload(true)
	if err := w.AppendBatch(ctx, makeEntries(1, 2, 1)); err == nil {
		t.Fatal("AppendBatch succeeded while an earlier segment could not be uploaded")
	}
	if store.has(ObjectKey(1, 2)) {
		t.Fatal("rejected batch reached object storage")
	}
	cancel()
	_ = w.Close()

	reopened, err := Open(dir, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	recovered := &recordingRecoveryStore{}
	if err := reopened.ReplayLocal(recovered, 0); err != nil {
		t.Fatal(err)
	}
	for _, e := range recovered.entries {
		if e.Revision == 2 {
			t.Fatal("rejected batch is replayed from the local WAL")
		}
	}
}
