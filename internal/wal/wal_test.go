package wal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func makeEntries(term uint64, startRev, count int64) []*Entry {
	entries := make([]*Entry, count)
	for i := int64(0); i < count; i++ {
		rev := startRev + i
		entries[i] = &Entry{
			Revision: rev,
			Term:     term,
			Op:       OpCreate,
			Key:      fmt.Sprintf("key-%d", rev),
			Value:    []byte(fmt.Sprintf("val-%d", rev)),
		}
	}
	return entries
}

func TestWALAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	const term uint64 = 1

	w, err := Open(dir, term, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)

	entries := makeEntries(term, 1, 10)
	for _, e := range entries {
		if err := w.Append(e); err != nil {
			t.Fatalf("Append rev=%d: %v", e.Revision, err)
		}
	}

	// Close without rotating to leave an unsealed (open) segment.
	cancel()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Replay via LocalSegments.
	paths, err := LocalSegments(dir)
	if err != nil {
		t.Fatalf("LocalSegments: %v", err)
	}
	if len(paths) == 0 {
		t.Fatal("no segments found after close")
	}

	var replayed []*Entry
	for _, path := range paths {
		sr, closer, err := OpenSegmentFile(path)
		if err != nil {
			t.Fatalf("OpenSegmentFile %q: %v", path, err)
		}
		es, err := sr.ReadAll()
		closer()
		if err != nil {
			t.Fatalf("ReadAll %q: %v", path, err)
		}
		replayed = append(replayed, es...)
	}

	if len(replayed) != len(entries) {
		t.Fatalf("replayed %d entries, want %d", len(replayed), len(entries))
	}
	for i, e := range entries {
		assertEntryEqual(t, e, replayed[i])
	}
}

func TestWALSizeRotation(t *testing.T) {
	dir := t.TempDir()

	// Set a tiny segment size so rotation happens quickly.
	w, err := Open(dir, 1, 1, WithSegmentMaxSize(512))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)

	// Write enough entries to exceed 512 bytes several times.
	for i := int64(1); i <= 20; i++ {
		e := &Entry{
			Revision: i, Term: 1, Op: OpCreate,
			Key:   fmt.Sprintf("key-%04d", i),
			Value: make([]byte, 64),
		}
		if err := w.Append(e); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	cancel()
	w.Close()

	paths, _ := LocalSegments(dir)
	if len(paths) < 2 {
		t.Errorf("expected multiple segments after size rotation, got %d", len(paths))
	}
}

func TestWALAgeRotation(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, 1, 1,
		WithSegmentMaxAge(50*time.Millisecond),
		WithSegmentMaxSize(50<<20),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)

	// Write one entry, then wait for age rotation.
	if err := w.Append(&Entry{Revision: 1, Term: 1, Op: OpCreate, Key: "k", Value: []byte("v")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	// Write a second entry into a new segment.
	if err := w.Append(&Entry{Revision: 2, Term: 1, Op: OpCreate, Key: "k2", Value: []byte("v2")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	cancel()
	w.Close()

	paths, _ := LocalSegments(dir)
	if len(paths) < 2 {
		t.Errorf("expected ≥2 segments after age rotation, got %d", len(paths))
	}
}

func TestWALSealAndFlush(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, 1, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)

	_ = w.Append(&Entry{Revision: 1, Term: 1, Op: OpCreate, Key: "k"})

	if err := w.SealAndFlush(2); err != nil {
		t.Fatalf("SealAndFlush: %v", err)
	}
	// A second flush with nothing new should be a no-op.
	if err := w.SealAndFlush(2); err != nil {
		t.Fatalf("SealAndFlush (empty): %v", err)
	}

	cancel()
	w.Close()

	paths, _ := LocalSegments(dir)
	if len(paths) == 0 {
		t.Error("expected at least one sealed segment")
	}
}

func TestMaxSequenceScansPastBadSegment(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, SegmentName(1, 1)), []byte("bad segment"), 0o600); err != nil {
		t.Fatalf("write bad segment: %v", err)
	}
	sw, err := OpenSegmentWriter(dir, 1, 5)
	if err != nil {
		t.Fatalf("OpenSegmentWriter: %v", err)
	}
	if err := sw.Append(&Entry{ID: 7, Revision: 3, Term: 1, Op: OpCreate, Key: "k", Value: []byte("v")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := sw.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	maxSeq, err := MaxSequence(dir)
	if err == nil {
		t.Fatal("MaxSequence error: got nil, want bad segment error")
	}
	if maxSeq != 7 {
		t.Fatalf("MaxSequence: want 7 got %d", maxSeq)
	}
}

func TestObjectKey(t *testing.T) {
	k := ObjectKey(1, 42)
	want := "wal/0000000001/00000000000000000042"
	if k != want {
		t.Errorf("ObjectKey: want %q got %q", want, k)
	}
}

type recordingRecoveryStore struct {
	entries []Entry
}

func (r *recordingRecoveryStore) Recover(entries []Entry) error {
	r.entries = append(r.entries, entries...)
	return nil
}

func TestWALSyncUploadFailureDoesNotReplayFailedBatch(t *testing.T) {
	dir := t.TempDir()
	uploadErr := errors.New("injected sync upload failure")
	uploader := func(_ context.Context, _, _ string) error { return uploadErr }

	w, err := Open(dir, 1, 1, WithUploader(uploader), WithSyncUpload())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)

	err = w.AppendBatch(ctx, makeEntries(1, 1, 1))
	if !errors.Is(err, uploadErr) {
		t.Fatalf("AppendBatch: want upload error, got %v", err)
	}
	cancel()
	if err := w.Close(); err != nil {
		t.Fatalf("Close after failed sync upload: %v", err)
	}

	reopened, err := Open(dir, 1, 1)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	recovered := &recordingRecoveryStore{}
	if err := reopened.ReplayLocal(recovered, 0); err != nil {
		t.Fatalf("ReplayLocal: %v", err)
	}
	if got := len(recovered.entries); got != 0 {
		t.Fatalf("ReplayLocal recovered %d entries from failed sync upload, want 0", got)
	}
}

// A batch rolled back after a failed sync upload also takes back the header
// format it raised: the segment keeps only data entries, which earlier
// releases can read.
func TestWALSyncUploadRollbackRestoresFormat(t *testing.T) {
	dir := t.TempDir()
	uploadErr := errors.New("injected sync upload failure")
	uploader := func(_ context.Context, _, _ string) error { return uploadErr }

	w, err := Open(dir, 1, 1, WithUploader(uploader))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)
	defer func() { _ = w.Close() }()

	if err := w.AppendBatch(ctx, makeEntries(1, 1, 1)); err != nil {
		t.Fatalf("AppendBatch data: %v", err)
	}
	w.SetSyncUpload(true)
	meta := &Entry{ID: 2, Revision: 1, Term: 1, Op: OpMetaPut, Key: "m", Value: []byte("v")}
	if err := w.AppendBatch(ctx, []*Entry{meta}); !errors.Is(err, uploadErr) {
		t.Fatalf("AppendBatch meta: want upload error, got %v", err)
	}

	hdr := make([]byte, segHeaderLen)
	f, err := os.Open(filepath.Join(dir, SegmentName(1, 1)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.ReadAt(hdr, 0); err != nil {
		t.Fatal(err)
	}
	if got, want := string(hdr[:4]), segMagic(formatBase); got != want {
		t.Fatalf("segment header after rollback = %q, want %q", got, want)
	}
}
