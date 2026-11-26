package wal

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func preallocEntries(term uint64, n int, valueLen int) []*Entry {
	es := make([]*Entry, n)
	for i := range es {
		es[i] = &Entry{Revision: int64(10 + i), Term: term, Op: OpCreate,
			Key: "k" + strings.Repeat("x", i%7), Value: bytes.Repeat([]byte{byte(i)}, valueLen)}
	}
	return es
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}

// A preallocated segment that was never sealed, as after a crash, has a
// zero-filled tail past its last entry. Readers must stop there, and DataLen
// must find where it starts.
func TestPreallocatedSegmentUnsealed(t *testing.T) {
	dir := t.TempDir()
	sw, err := OpenSegmentWriter(dir, 3, 10)
	if err != nil {
		t.Fatal(err)
	}
	sw.Preallocate()
	entries := preallocEntries(3, 300, 700) // ~220 KiB: several growth steps
	for i, e := range entries {
		if i%3 == 0 {
			err = sw.Append(e)
		} else {
			err = sw.AppendNoSync(e)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := sw.Sync(); err != nil {
		t.Fatal(err)
	}
	if got := fileSize(t, sw.Path()); got <= sw.Size() {
		t.Fatalf("file is %d bytes for %d bytes of entries: nothing was preallocated", got, sw.Size())
	}
	// Crash: the file is left as it is.
	if err := sw.Close(); err != nil {
		t.Fatal(err)
	}

	sr, closer, err := OpenSegmentFile(sw.Path())
	if err != nil {
		t.Fatal(err)
	}
	got, err := sr.ReadAll()
	closer()
	if err != nil {
		t.Fatalf("ReadAll over the zero-filled tail: %v", err)
	}
	if len(got) != len(entries) {
		t.Fatalf("read %d entries, want %d", len(got), len(entries))
	}
	for i := range entries {
		assertEntryEqual(t, entries[i], got[i])
	}

	f, err := os.Open(sw.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	n, err := DataLen(f, fileSize(t, sw.Path()))
	if err != nil {
		t.Fatal(err)
	}
	if n != sw.Size() {
		t.Fatalf("DataLen = %d, want the entries' end %d", n, sw.Size())
	}

	if seq, err := MaxSequence(dir); err != nil || seq != entries[len(entries)-1].Sequence() {
		t.Fatalf("MaxSequence = %d, %v; want %d", seq, err, entries[len(entries)-1].Sequence())
	}
}

// Sealing trims the preallocated tail, so a sealed segment is exactly its
// entries, as before preallocation.
func TestPreallocatedSegmentSealTrims(t *testing.T) {
	dir := t.TempDir()
	sw, err := OpenSegmentWriter(dir, 3, 10)
	if err != nil {
		t.Fatal(err)
	}
	sw.Preallocate()
	for _, e := range preallocEntries(3, 20, 100) {
		if err := sw.Append(e); err != nil {
			t.Fatal(err)
		}
	}
	size := sw.Size()
	if err := sw.Seal(); err != nil {
		t.Fatal(err)
	}
	if got := fileSize(t, sw.Path()); got != size {
		t.Fatalf("sealed file is %d bytes, want %d", got, size)
	}
}

// A rollback (a failed sync upload) returns to the mark, and appends after it
// grow the file again.
func TestPreallocatedSegmentRollback(t *testing.T) {
	dir := t.TempDir()
	sw, err := OpenSegmentWriter(dir, 3, 10)
	if err != nil {
		t.Fatal(err)
	}
	sw.Preallocate()
	entries := preallocEntries(3, 40, 3000)
	for _, e := range entries[:10] {
		if err := sw.Append(e); err != nil {
			t.Fatal(err)
		}
	}
	m := sw.mark()
	for _, e := range entries[10:20] {
		if err := sw.Append(e); err != nil {
			t.Fatal(err)
		}
	}
	if err := sw.rollback(m); err != nil {
		t.Fatal(err)
	}
	for _, e := range entries[20:] {
		if err := sw.Append(e); err != nil {
			t.Fatal(err)
		}
	}
	if err := sw.Seal(); err != nil {
		t.Fatal(err)
	}
	sr, closer, err := OpenSegmentFile(sw.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer closer()
	got, err := sr.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	want := append(append([]*Entry{}, entries[:10]...), entries[20:]...)
	if len(got) != len(want) {
		t.Fatalf("read %d entries, want %d", len(got), len(want))
	}
	for i := range want {
		assertEntryEqual(t, want[i], got[i])
	}
}

// DataLen of a segment without a preallocated tail is its size, whatever its
// last frame looks like: a torn frame is left for the reader to handle, as it
// was before preallocation.
func TestDataLenWithoutTail(t *testing.T) {
	dir := t.TempDir()
	sw, err := OpenSegmentWriter(dir, 3, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range preallocEntries(3, 5, 50) {
		if err := sw.Append(e); err != nil {
			t.Fatal(err)
		}
	}
	if err := sw.Seal(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(sw.Path())
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range [][]byte{b, b[:len(b)-7]} {
		n, err := DataLen(bytes.NewReader(data), int64(len(data)))
		if err != nil || n != int64(len(data)) {
			t.Fatalf("DataLen = %d, %v; want %d", n, err, len(data))
		}
	}
}

// The WAL preallocates segments it uploads in the background, and not those
// it uploads after every batch, which are sealed right away.
func TestWALPreallocatesOnlyAsyncSegments(t *testing.T) {
	for _, syncUpload := range []bool{false, true} {
		dir := t.TempDir()
		var uploaded []string
		up := func(_ context.Context, localPath, _ string) error {
			uploaded = append(uploaded, localPath)
			return nil
		}
		opts := []Option{WithUploader(up)}
		if syncUpload {
			opts = append(opts, WithSyncUpload())
		}
		w, err := Open(dir, 3, 1, opts...)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.AppendBatch(context.Background(), preallocEntries(3, 3, 100)); err != nil {
			t.Fatal(err)
		}
		paths, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
		switch {
		case syncUpload && len(uploaded) != 1:
			t.Fatalf("sync upload: %d uploads, want 1", len(uploaded))
		case !syncUpload:
			if len(paths) != 1 {
				t.Fatalf("async: %d local segments, want 1", len(paths))
			}
			if size := fileSize(t, paths[0]); size <= w.active.Size() {
				t.Fatalf("async segment is %d bytes for %d of entries: not preallocated", size, w.active.Size())
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
