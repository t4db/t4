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

// ErrPreconditionFailed is returned by ConditionalPut when the ETag
// condition was not met (someone else modified the object concurrently).
var ErrPreconditionFailed = errors.New("object: precondition failed")

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

// GetWithETag wraps the result of a conditional-aware Get, pairing the body
// with the ETag of the version that was read.
type GetWithETag struct {
	Body io.ReadCloser
	ETag string // opaque version token; "" if the store doesn't support ETags
}

// ConditionalStore extends Store with atomic compare-and-swap writes used by
// the leader-election lock to prevent split-brain races.
type ConditionalStore interface {
	Store
	// GetETag returns the body and current ETag for key.
	// Returns ErrNotFound if the object does not exist.
	GetETag(ctx context.Context, key string) (*GetWithETag, error)

	// PutIfAbsent writes data to key only if the key does not yet exist.
	// Returns ErrPreconditionFailed if the key already exists.
	PutIfAbsent(ctx context.Context, key string, r io.Reader) error

	// PutIfMatch writes data to key only if its current ETag equals matchETag.
	// Returns ErrPreconditionFailed if the ETag has changed (someone else
	// modified the object concurrently).
	PutIfMatch(ctx context.Context, key string, r io.Reader, matchETag string) error
}

// VersionedStore extends Store with version-pinned reads. Implementations
// backed by S3 (or any store with object versioning) can satisfy this
// interface. It is used for point-in-time restore via RestorePoint.
type VersionedStore interface {
	Store
	// GetVersioned returns a reader for a specific stored version of key.
	// Returns ErrNotFound if the version does not exist.
	GetVersioned(ctx context.Context, key, versionID string) (io.ReadCloser, error)
}
