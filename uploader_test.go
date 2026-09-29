package t4

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/t4db/t4/internal/wal"
	"github.com/t4db/t4/pkg/object"
)

// writeSegment creates a local file standing in for a sealed WAL segment.
func writeSegment(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write segment: %v", err)
	}
	return path
}

func readObject(t *testing.T, store object.Store, key string) string {
	t.Helper()
	rc, err := store.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("get %q: %v", key, err)
	}
	defer func() {
		_ = rc.Close()
	}()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read %q: %v", key, err)
	}
	return string(b)
}

// plainStore hides the ConditionalStore methods of the store it wraps, so the
// uploader must fall back to an unconditional Put.
type plainStore struct{ inner object.Store }

func (s plainStore) Put(ctx context.Context, key string, r io.Reader) error {
	return s.inner.Put(ctx, key, r)
}
func (s plainStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.inner.Get(ctx, key)
}
func (s plainStore) Delete(ctx context.Context, key string) error {
	return s.inner.Delete(ctx, key)
}
func (s plainStore) DeleteMany(ctx context.Context, keys []string) error {
	return s.inner.DeleteMany(ctx, keys)
}
func (s plainStore) List(ctx context.Context, prefix string) ([]string, error) {
	return s.inner.List(ctx, prefix)
}

func TestUploaderPublishesSegment(t *testing.T) {
	dir := t.TempDir()
	path := writeSegment(t, dir, "seg-1", "entries")
	store := object.NewMem()

	if err := makeUploader(store, NoopLogger)(context.Background(), path, "wal/1/1"); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if got := readObject(t, store, "wal/1/1"); got != "entries" {
		t.Fatalf("object content = %q, want %q", got, "entries")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("local segment still present after upload: %v", err)
	}
}

// A segment key that is already published must not be overwritten. This is the
// leader-handover race: the outgoing leader's uploadLoop publishes the full
// segment while the incoming leader uploads its own shorter copy of the same
// key. The shorter copy's entries are all published, so the conflict succeeds.
func TestUploaderKeepsExistingSegmentOnConflict(t *testing.T) {
	dir := t.TempDir()
	store := object.NewMem()
	ctx := context.Background()

	if err := store.Put(ctx, "wal/1/1", strings.NewReader("entry1entry2entry3")); err != nil {
		t.Fatalf("seed object: %v", err)
	}

	path := writeSegment(t, dir, "seg-1", "entry1entry2")
	if err := makeUploader(store, NoopLogger)(ctx, path, "wal/1/1"); err != nil {
		t.Fatalf("conflict with a superset of the local segment must succeed, got: %v", err)
	}

	if got := readObject(t, store, "wal/1/1"); got != "entry1entry2entry3" {
		t.Fatalf("existing object was overwritten: got %q", got)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("local segment still present after conflict: %v", err)
	}
}

// A conflict with an object that lacks some of the local entries must fail:
// reporting success would acknowledge writes that never reached object
// storage. This is the key reuse that lost a write after a disk-loss restart.
func TestUploaderRejectsConflictWithDifferentEntries(t *testing.T) {
	for name, local := range map[string]string{
		"different entries":  "entry1entry9",
		"longer than object": "entry1entry2entry3entry4",
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			store := object.NewMem()
			ctx := context.Background()

			if err := store.Put(ctx, "wal/1/1", strings.NewReader("entry1entry2entry3")); err != nil {
				t.Fatalf("seed object: %v", err)
			}
			path := writeSegment(t, dir, "seg-1", local)
			err := makeUploader(store, NoopLogger)(ctx, path, "wal/1/1")
			if !errors.Is(err, wal.ErrSegmentConflict) {
				t.Fatalf("upload err = %v, want ErrSegmentConflict", err)
			}
			if got := readObject(t, store, "wal/1/1"); got != "entry1entry2entry3" {
				t.Fatalf("existing object was overwritten: got %q", got)
			}
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("local segment must be kept after a rejected conflict: %v", err)
			}
		})
	}
}

// uploadLocalWALSegments runs while becoming leader, where a follower's copy
// of a segment key can legitimately differ from the published one. It keeps
// the published object and moves on rather than failing the election.
func TestUploadLocalWALSegmentsSkipsConflictingSegment(t *testing.T) {
	dir := t.TempDir()
	store := object.NewMem()
	ctx := context.Background()

	conflicting := wal.ObjectKey(1, 1)
	fresh := wal.ObjectKey(1, 5)
	if err := store.PutIfAbsent(ctx, conflicting, strings.NewReader("published")); err != nil {
		t.Fatalf("seed object: %v", err)
	}
	writeSegment(t, dir, wal.SegmentName(1, 1), "follower-copy")
	writeSegment(t, dir, wal.SegmentName(1, 5), "later")

	// Hide the conflicting key from List so the upload reaches PutIfAbsent,
	// as it does when the outgoing leader publishes between List and Put.
	if err := uploadLocalWALSegments(ctx, dir, hideKeys{store, conflicting}, NoopLogger); err != nil {
		t.Fatalf("uploadLocalWALSegments: %v", err)
	}
	if got := readObject(t, store, conflicting); got != "published" {
		t.Fatalf("published object was overwritten: got %q", got)
	}
	if got := readObject(t, store, fresh); got != "later" {
		t.Fatalf("later segment not uploaded: got %q", got)
	}
}

// hideKeys omits one key from List results.
type hideKeys struct {
	*object.Mem
	hidden string
}

func (s hideKeys) List(ctx context.Context, prefix string) ([]string, error) {
	keys, err := s.Mem.List(ctx, prefix)
	out := keys[:0]
	for _, k := range keys {
		if k != s.hidden {
			out = append(out, k)
		}
	}
	return out, err
}

// Re-uploading the same segment (retry after an ambiguous timeout) is a no-op
// rather than an error.
func TestUploaderRetryIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	store := object.NewMem()
	up := makeUploader(store, NoopLogger)
	ctx := context.Background()

	first := writeSegment(t, dir, "seg-1", "entries")
	if err := up(ctx, first, "wal/1/1"); err != nil {
		t.Fatalf("first upload: %v", err)
	}
	// The retry re-opens a local file that the uploader had already removed,
	// so recreate it the way a restart would.
	retry := writeSegment(t, dir, "seg-1", "entries")
	if err := up(ctx, retry, "wal/1/1"); err != nil {
		t.Fatalf("retry upload: %v", err)
	}
	if got := readObject(t, store, "wal/1/1"); got != "entries" {
		t.Fatalf("object content = %q after retry", got)
	}
}

func TestUploaderFallsBackToPlainPut(t *testing.T) {
	dir := t.TempDir()
	path := writeSegment(t, dir, "seg-1", "entries")
	store := plainStore{inner: object.NewMem()}

	if err := makeUploader(store, NoopLogger)(context.Background(), path, "wal/1/1"); err != nil {
		t.Fatalf("upload via non-conditional store: %v", err)
	}
	if got := readObject(t, store, "wal/1/1"); got != "entries" {
		t.Fatalf("object content = %q, want %q", got, "entries")
	}
}
