package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Segment file format:
//
//	[8:  magic    "STRATA\x01\n"]
//	[8:  term     uint64 BE]
//	[8:  firstRev int64  BE]      ← first revision in this segment (0 if unknown at open time)
//	[entry frames ... ]
const (
	segMagic     = "STRATA\x01\n"
	segHeaderLen = 24
)

// SegmentWriter appends entries to a local WAL segment file.
type SegmentWriter struct {
	f          *os.File
	path       string
	term       uint64
	firstRev   int64
	size       int64
	entryCount int
}

// OpenSegmentWriter creates (or truncates) a new segment file and writes the header.
func OpenSegmentWriter(dir string, term uint64, firstRev int64) (*SegmentWriter, error) {
	name := SegmentName(term, firstRev)
	path := filepath.Join(dir, name)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("wal: create segment %q: %w", path, err)
	}
	sw := &SegmentWriter{f: f, path: path, term: term, firstRev: firstRev}
	if err := sw.writeHeader(); err != nil {
		f.Close()
		os.Remove(path)
		return nil, err
	}
	return sw, nil
}

func (sw *SegmentWriter) writeHeader() error {
	hdr := make([]byte, segHeaderLen)
	copy(hdr[0:8], segMagic)
	binary.BigEndian.PutUint64(hdr[8:16], sw.term)
	binary.BigEndian.PutUint64(hdr[16:24], uint64(sw.firstRev))
	if _, err := sw.f.Write(hdr); err != nil {
		return fmt.Errorf("wal: write segment header: %w", err)
	}
	sw.size = segHeaderLen
	return nil
}

// Append writes e to the segment and fsyncs.
func (sw *SegmentWriter) Append(e *Entry) error {
	var buf bytes.Buffer
	if err := AppendEntry(&buf, e); err != nil {
		return err
	}
	b := buf.Bytes()
	if _, err := sw.f.Write(b); err != nil {
		return fmt.Errorf("wal: write entry rev=%d: %w", e.Revision, err)
	}
	if err := sw.f.Sync(); err != nil {
		return fmt.Errorf("wal: fsync rev=%d: %w", e.Revision, err)
	}
	sw.size += int64(len(b))
	sw.entryCount++
	return nil
}

// Close closes the underlying file without sealing.
// Call this only when the segment will not be uploaded (e.g. on error).
func (sw *SegmentWriter) Close() error { return sw.f.Close() }

// Seal closes the file. The segment is now safe to upload.
func (sw *SegmentWriter) Seal() error {
	return sw.f.Close()
}

// Path returns the local file path.
func (sw *SegmentWriter) Path() string { return sw.path }

// Size returns the approximate byte size written so far.
func (sw *SegmentWriter) Size() int64 { return sw.size }

// EntryCount returns the number of entries appended.
func (sw *SegmentWriter) EntryCount() int { return sw.entryCount }

// Term returns the term this segment belongs to.
func (sw *SegmentWriter) Term() uint64 { return sw.term }

// FirstRev returns the first revision in this segment.
func (sw *SegmentWriter) FirstRev() int64 { return sw.firstRev }

// SegmentName returns the canonical file name for a segment.
// Zero-padding ensures lexicographic order == chronological order.
func SegmentName(term uint64, firstRev int64) string {
	return fmt.Sprintf("%010d-%020d.wal", term, firstRev)
}

// ── SegmentReader ──────────────────────────────────────────────────────────────

// SegmentReader reads WAL entries from a segment.
type SegmentReader struct {
	r        io.Reader
	Term     uint64
	FirstRev int64
}

// OpenSegmentFile opens a local file for reading.
func OpenSegmentFile(path string) (*SegmentReader, func(), error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("wal: open segment %q: %w", path, err)
	}
	sr, err := newSegmentReader(f)
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	return sr, func() { f.Close() }, nil
}

// NewSegmentReader wraps an arbitrary reader (e.g. bytes downloaded from S3).
func NewSegmentReader(r io.Reader) (*SegmentReader, error) {
	return newSegmentReader(r)
}

func newSegmentReader(r io.Reader) (*SegmentReader, error) {
	hdr := make([]byte, segHeaderLen)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, fmt.Errorf("wal: read segment header: %w", err)
	}
	if string(hdr[0:8]) != segMagic {
		return nil, fmt.Errorf("wal: bad segment magic %q", hdr[0:8])
	}
	return &SegmentReader{
		r:        r,
		Term:     binary.BigEndian.Uint64(hdr[8:16]),
		FirstRev: int64(binary.BigEndian.Uint64(hdr[16:24])),
	}, nil
}

// Next reads the next entry. Returns nil, io.EOF when the segment is exhausted.
func (sr *SegmentReader) Next() (*Entry, error) {
	return ReadEntry(sr.r)
}

// ReadAll reads all valid entries from the segment.
func (sr *SegmentReader) ReadAll() ([]*Entry, error) {
	var entries []*Entry
	for {
		e, err := sr.Next()
		if err == io.EOF {
			return entries, nil
		}
		if err != nil {
			return entries, err
		}
		entries = append(entries, e)
	}
}
