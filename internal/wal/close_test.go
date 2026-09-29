package wal

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"
)

// TestWALCloseUploadFailureReturnsError verifies that Close propagates a
// final-segment upload error to the caller instead of silently returning nil.
func TestWALCloseUploadFailureReturnsError(t *testing.T) {
	dir := t.TempDir()

	uploadErr := errors.New("injected upload failure")
	uploader := func(_ context.Context, _, _ string) error { return uploadErr }

	w, err := Open(dir, 1, 1, WithUploader(uploader))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)

	if err := w.Append(&Entry{Revision: 1, Term: 1, Op: OpCreate, Key: "k", Value: []byte("v")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	cancel()

	if err := w.Close(); !errors.Is(err, uploadErr) {
		t.Errorf("Close: want upload error, got %v", err)
	}
}

// TestWALCloseNoUploadErrorOnSuccess verifies that Close returns nil when the
// final-segment upload succeeds.
func TestWALCloseNoUploadErrorOnSuccess(t *testing.T) {
	dir := t.TempDir()

	uploader := func(_ context.Context, _, _ string) error { return nil }

	w, err := Open(dir, 1, 1, WithUploader(uploader))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)

	if err := w.Append(&Entry{Revision: 1, Term: 1, Op: OpCreate, Key: "k", Value: []byte("v")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	cancel()

	if err := w.Close(); err != nil {
		t.Errorf("Close with successful upload: want nil, got %v", err)
	}
}

// TestWALCloseUploadContextHasDeadline verifies that the upload triggered by
// Close uses a context with a deadline so a hung object store cannot block
// shutdown indefinitely.
func TestWALCloseUploadContextHasDeadline(t *testing.T) {
	dir := t.TempDir()

	var uploadCtx context.Context
	uploader := func(ctx context.Context, _, _ string) error {
		uploadCtx = ctx
		return nil
	}

	w, err := Open(dir, 1, 1, WithUploader(uploader))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)

	if err := w.Append(&Entry{Revision: 1, Term: 1, Op: OpCreate, Key: "k", Value: []byte("v")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	cancel()
	w.Close()

	if uploadCtx == nil {
		t.Fatal("uploader was never called")
	}
	if _, ok := uploadCtx.Deadline(); !ok {
		t.Error("upload context has no deadline — shutdown can block indefinitely on storage stall")
	}
}

// TestWALCloseNoUploadWhenEmpty verifies that Close does not call the uploader
// when the active segment has no entries (nothing to persist).
func TestWALCloseNoUploadWhenEmpty(t *testing.T) {
	dir := t.TempDir()

	uploadCalled := false
	uploader := func(_ context.Context, _, _ string) error {
		uploadCalled = true
		return nil
	}

	w, err := Open(dir, 1, 1, WithUploader(uploader))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	cancel()

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if uploadCalled {
		t.Error("uploader should not be called for an empty segment")
	}
}

// TestWALSegmentNamedAfterFirstEntry verifies that every segment is named
// after the sequence of its first entry, whatever startRev Open was given and
// across rotations. A segment named in advance can carry a stale name: after
// a disk loss, Open ran before remote replay advanced the sequence, and the
// segment reused the object key of one already in object storage.
func TestWALSegmentNamedAfterFirstEntry(t *testing.T) {
	dir := t.TempDir()
	// startRev 1 is stale: the first entry written is sequence 5.
	w, err := Open(dir, 1, 1, WithSegmentMaxSize(1))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if paths, _ := LocalSegments(dir); len(paths) != 0 {
		t.Fatalf("Open created segments %v before any append", paths)
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)
	defer cancel()

	// Each append exceeds the tiny max size and rotates the segment.
	for rev := int64(5); rev <= 7; rev++ {
		if err := w.Append(&Entry{Revision: rev, Term: 1, Op: OpCreate, Key: "key", Value: make([]byte, 64)}); err != nil {
			t.Fatalf("Append %d: %v", rev, err)
		}
	}
	w.Close()

	paths, err := LocalSegments(dir)
	if err != nil {
		t.Fatalf("LocalSegments: %v", err)
	}
	var names []string
	for _, p := range paths {
		names = append(names, filepath.Base(p))
	}
	want := []string{SegmentName(1, 5), SegmentName(1, 6), SegmentName(1, 7)}
	if !slices.Equal(names, want) {
		t.Fatalf("segments = %v, want %v", names, want)
	}
}
