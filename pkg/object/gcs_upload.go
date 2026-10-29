package object

import (
	"context"
	"errors"
	"fmt"
	"io"
)

// Resumable upload, kept apart from the store that uses it.
//
// This is the only part of the GCS backend with non-obvious logic, so it is
// isolated behind one injected function: the chunking decides what to send and
// never talks to a server. That keeps the boundaries — an empty object, a size
// that is an exact multiple of the chunk, the ragged last chunk — testable
// exhaustively without HTTP, and keeps a failure part-way through a transfer
// testable without a flaky server.

// chunkSender sends one piece of a resumable upload. total is the object's
// full length on the final chunk, and -1 while it is still unknown.
type chunkSender func(ctx context.Context, chunk []byte, offset, total int64) error

// resumableUploader splits a stream into chunks and hands each to send.
type resumableUploader struct {
	chunkSize int64
	send      chunkSender
}

// upload streams r to send in chunkSize pieces.
//
// GCS needs the object's total length on the final chunk, which an io.Reader
// does not offer, so a chunk is held back: it is only sent once the following
// read has shown whether anything comes after it. An empty stream is one
// zero-length chunk, because the object still has to be created.
func (u *resumableUploader) upload(ctx context.Context, r io.Reader) error {
	if u.chunkSize <= 0 {
		return errors.New("object/gcs: resumable upload: chunk size must be positive")
	}

	bufA := make([]byte, u.chunkSize)
	bufB := make([]byte, u.chunkSize)

	pending, err := readChunk(r, bufA)
	if err != nil {
		return err
	}
	var offset int64
	for {
		next, err := readChunk(r, bufB)
		if err != nil {
			return err
		}
		if len(next) == 0 {
			// Nothing follows, so pending is the last chunk and the total
			// length is offset plus what it holds.
			return u.send(ctx, pending, offset, offset+int64(len(pending)))
		}
		if err := u.send(ctx, pending, offset, -1); err != nil {
			return err
		}
		offset += int64(len(pending))
		pending, bufA, bufB = next, bufB, bufA
	}
}

// readChunk fills buf as far as the reader allows, returning a short or empty
// slice at the end of the stream. A truncated read is the end of the stream,
// not a failure: io.ReadFull reports it as ErrUnexpectedEOF.
func readChunk(r io.Reader, buf []byte) ([]byte, error) {
	n, err := io.ReadFull(r, buf)
	switch {
	case err == nil, errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return buf[:n], nil
	default:
		return nil, fmt.Errorf("object/gcs: resumable upload: read: %w", err)
	}
}

// contentRange formats the header one chunk carries.
//
// An empty object names no bytes, only its length: "bytes */0". Otherwise the
// range is inclusive of both ends, so a chunk of n bytes at offset ends at
// offset+n-1, and the total is "*" until the final chunk knows it.
func contentRange(offset int64, n int, total int64) string {
	if n == 0 {
		return "bytes */0"
	}
	last := offset + int64(n) - 1
	if total < 0 {
		return fmt.Sprintf("bytes %d-%d/*", offset, last)
	}
	return fmt.Sprintf("bytes %d-%d/%d", offset, last, total)
}
