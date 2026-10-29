package object

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

// The uploader takes a way to send a chunk and nothing else, so these tests
// drive the chunking directly: no server, no emulator, and every boundary
// reachable.

type sentChunk struct {
	Offset int64
	Data   []byte
	Total  int64
}

// recorder collects the chunks an upload produces. failAt, when non-zero,
// makes the n-th send (1-based) fail.
type recorder struct {
	chunks []sentChunk
	failAt int
	err    error
}

func (rec *recorder) send(_ context.Context, chunk []byte, offset, total int64) error {
	rec.chunks = append(rec.chunks, sentChunk{
		Offset: offset,
		Data:   append([]byte(nil), chunk...), // the uploader reuses its buffers
		Total:  total,
	})
	if rec.failAt != 0 && len(rec.chunks) == rec.failAt {
		return rec.err
	}
	return nil
}

// An upload has to reassemble into exactly the bytes it was given, whatever
// the size, and only the last chunk may name the total. Every size from empty
// to several chunks is covered, so no boundary is left to chance.
func TestResumableUploadChunkingAtEverySize(t *testing.T) {
	const chunk = 4
	for size := 0; size <= 3*chunk+2; size++ {
		t.Run(fmt.Sprintf("%d bytes", size), func(t *testing.T) {
			want := make([]byte, size)
			for i := range want {
				want[i] = byte('a' + i%26)
			}
			rec := &recorder{}
			up := &resumableUploader{chunkSize: chunk, send: rec.send}
			if err := up.upload(context.Background(), bytes.NewReader(want)); err != nil {
				t.Fatalf("upload: %v", err)
			}

			if len(rec.chunks) == 0 {
				t.Fatal("an object is created by at least one request, even when empty")
			}

			// The chunks must tile the object: contiguous, in order, and
			// reassembling into the original bytes.
			var got []byte
			var expectedOffset int64
			for i, c := range rec.chunks {
				if c.Offset != expectedOffset {
					t.Errorf("chunk %d starts at %d, want %d — a gap or overlap", i, c.Offset, expectedOffset)
				}
				expectedOffset += int64(len(c.Data))
				got = append(got, c.Data...)
			}
			if !bytes.Equal(got, want) {
				t.Errorf("reassembled %d bytes, want %d", len(got), len(want))
			}

			// Only the final chunk knows the total, and it must be right:
			// naming it early would finalize the object short.
			for i, c := range rec.chunks[:len(rec.chunks)-1] {
				if c.Total != -1 {
					t.Errorf("chunk %d names total %d; only the last chunk may", i, c.Total)
				}
				if int64(len(c.Data)) != chunk {
					t.Errorf("chunk %d carries %d bytes; every chunk but the last is full", i, len(c.Data))
				}
			}
			last := rec.chunks[len(rec.chunks)-1]
			if last.Total != int64(size) {
				t.Errorf("final chunk names total %d, want %d", last.Total, size)
			}
		})
	}
}

// A reader that does not report its length takes the same path, since the
// uploader never asks for one.
func TestResumableUploadDoesNotNeedAKnownLength(t *testing.T) {
	const chunk = 8
	body := strings.Repeat("x", 20)
	rec := &recorder{}
	up := &resumableUploader{chunkSize: chunk, send: rec.send}

	// A plain io.Reader: no Len, no Seek, no Stat.
	if err := up.upload(context.Background(), io.MultiReader(strings.NewReader(body))); err != nil {
		t.Fatalf("upload: %v", err)
	}
	if n := len(rec.chunks); n != 3 {
		t.Fatalf("sent %d chunks, want 3 for %d bytes in %d-byte pieces", n, len(body), chunk)
	}
	if last := rec.chunks[2]; last.Total != int64(len(body)) {
		t.Errorf("final total %d, want %d", last.Total, len(body))
	}
}

// A failure part-way through must surface and stop, not be retried here or
// swallowed: the caller decides what a failed upload means.
func TestResumableUploadStopsAtTheFirstSendFailure(t *testing.T) {
	const chunk = 4
	sendErr := errors.New("network went away")
	for _, failAt := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("fails on chunk %d", failAt), func(t *testing.T) {
			rec := &recorder{failAt: failAt, err: sendErr}
			up := &resumableUploader{chunkSize: chunk, send: rec.send}

			err := up.upload(context.Background(), bytes.NewReader(make([]byte, 3*chunk)))
			if !errors.Is(err, sendErr) {
				t.Fatalf("err=%v, want the send error", err)
			}
			if len(rec.chunks) != failAt {
				t.Errorf("sent %d chunks; it should stop at the one that failed (%d)", len(rec.chunks), failAt)
			}
		})
	}
}

// A read failure is not the end of the stream and must not be mistaken for it,
// or a truncated object would be finalized as if complete.
func TestResumableUploadPropagatesAReadFailure(t *testing.T) {
	readErr := errors.New("disk gave up")
	rec := &recorder{}
	up := &resumableUploader{chunkSize: 4, send: rec.send}

	r := io.MultiReader(strings.NewReader("abcd"), errReader{readErr})
	err := up.upload(context.Background(), r)
	if !errors.Is(err, readErr) {
		t.Fatalf("err=%v, want the read error", err)
	}
	for _, c := range rec.chunks {
		if c.Total >= 0 {
			t.Error("no chunk may name a total once the stream has failed: that would finalize a truncated object")
		}
	}
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

func TestResumableUploadRejectsANonPositiveChunkSize(t *testing.T) {
	for _, size := range []int64{0, -1} {
		up := &resumableUploader{chunkSize: size, send: (&recorder{}).send}
		if err := up.upload(context.Background(), strings.NewReader("x")); err == nil {
			t.Errorf("chunk size %d: want an error rather than an endless loop", size)
		}
	}
}

func TestContentRange(t *testing.T) {
	for _, tc := range []struct {
		name   string
		offset int64
		n      int
		total  int64
		want   string
	}{
		{"empty object", 0, 0, 0, "bytes */0"},
		{"first chunk, total unknown", 0, 4, -1, "bytes 0-3/*"},
		{"later chunk, total unknown", 8, 4, -1, "bytes 8-11/*"},
		{"final chunk", 8, 2, 10, "bytes 8-9/10"},
		{"whole object in one chunk", 0, 3, 3, "bytes 0-2/3"},
		{"single byte", 5, 1, 6, "bytes 5-5/6"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := contentRange(tc.offset, tc.n, tc.total); got != tc.want {
				t.Errorf("contentRange(%d, %d, %d) = %q, want %q", tc.offset, tc.n, tc.total, got, tc.want)
			}
		})
	}
}

func TestReadChunk(t *testing.T) {
	t.Run("a short read is the end of the stream, not an error", func(t *testing.T) {
		buf := make([]byte, 8)
		got, err := readChunk(strings.NewReader("abc"), buf)
		if err != nil {
			t.Fatalf("err=%v, want nil for a short read", err)
		}
		if string(got) != "abc" {
			t.Errorf("got %q, want %q", got, "abc")
		}
	})
	t.Run("an exhausted stream reads empty", func(t *testing.T) {
		buf := make([]byte, 8)
		got, err := readChunk(strings.NewReader(""), buf)
		if err != nil {
			t.Fatalf("err=%v, want nil at the end of the stream", err)
		}
		if len(got) != 0 {
			t.Errorf("got %d bytes, want none", len(got))
		}
	})
	t.Run("a real failure is returned", func(t *testing.T) {
		boom := errors.New("boom")
		buf := make([]byte, 8)
		if _, err := readChunk(errReader{boom}, buf); !errors.Is(err, boom) {
			t.Errorf("err=%v, want the reader's error", err)
		}
	})
}
