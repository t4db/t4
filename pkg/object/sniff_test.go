package object

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

// The WAL uploader passes a section of the segment file (its entries, not a
// preallocated tail); S3 needs its size to avoid a multipart upload.
func TestSniffReaderSize(t *testing.T) {
	sec := io.NewSectionReader(strings.NewReader("0123456789"), 0, 6)
	if got := sniffReaderSize(sec); got != 6 {
		t.Fatalf("fresh section: %d, want 6", got)
	}
	if _, err := sec.Read(make([]byte, 2)); err != nil {
		t.Fatal(err)
	}
	if got := sniffReaderSize(sec); got != 4 {
		t.Fatalf("section after reading 2: %d, want 4", got)
	}
	if got := sniffReaderSize(bytes.NewReader([]byte("abc"))); got != 3 {
		t.Fatalf("bytes.Reader: %d, want 3", got)
	}
	if got := sniffReaderSize(io.MultiReader(strings.NewReader("x"))); got != -1 {
		t.Fatalf("unsized reader: %d, want -1", got)
	}
}
