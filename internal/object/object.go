// Package object provides a small interface for object storage operations
// used by Strata (WAL archive, checkpoints, manifest).
package object

import (
	"context"
	"errors"
	"io"
)

// ErrNotFound is returned by Get when the object does not exist.
var ErrNotFound = errors.New("object: not found")

// Store is a minimal object storage interface.
type Store interface {
	// Put writes data to key. The operation is atomic from the reader's
	// perspective (last-writer-wins semantics are sufficient).
	Put(ctx context.Context, key string, r io.Reader) error

	// Get returns a reader for the object at key.
	// Returns ErrNotFound if the object does not exist.
	Get(ctx context.Context, key string) (io.ReadCloser, error)

	// Delete removes the object at key. Not an error if it doesn't exist.
	Delete(ctx context.Context, key string) error

	// List returns keys that share the given prefix, in lexicographic order.
	List(ctx context.Context, prefix string) ([]string, error)
}
