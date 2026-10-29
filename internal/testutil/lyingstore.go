package testutil

import (
	"context"
	"io"

	"github.com/t4db/t4/pkg/object"
)

// LyingStore is an object store that accepts conditional writes and does not
// enforce them, the way Google Cloud Storage behaves through its S3-compatible
// XML API: If-None-Match and If-Match are taken and ignored, and the write
// succeeds unconditionally.
//
// object.Mem enforces its preconditions faithfully, so no test built on it can
// reach the code that exists for a store which does not. This is that store.
type LyingStore struct {
	object.ConditionalStore
	// IgnoreIfNoneMatch makes PutIfAbsent overwrite an existing key instead of
	// refusing it.
	IgnoreIfNoneMatch bool
	// IgnoreIfMatch makes PutIfMatch write whatever ETag it is given.
	IgnoreIfMatch bool
}

// NewLyingStore wraps inner so that both preconditions are ignored.
func NewLyingStore(inner object.ConditionalStore) *LyingStore {
	return &LyingStore{ConditionalStore: inner, IgnoreIfNoneMatch: true, IgnoreIfMatch: true}
}

func (s *LyingStore) PutIfAbsent(ctx context.Context, key string, r io.Reader) error {
	if s.IgnoreIfNoneMatch {
		return s.ConditionalStore.Put(ctx, key, r)
	}
	return s.ConditionalStore.PutIfAbsent(ctx, key, r)
}

func (s *LyingStore) PutIfMatch(ctx context.Context, key string, r io.Reader, matchETag string) error {
	if s.IgnoreIfMatch {
		return s.ConditionalStore.Put(ctx, key, r)
	}
	return s.ConditionalStore.PutIfMatch(ctx, key, r, matchETag)
}

// FrozenETagStore is an object store whose ETag never changes, so an ETag
// cannot identify a version. Such a store may refuse a mismatched If-Match and
// still be unusable for compare-and-swap, because every writer presents the
// same ETag.
type FrozenETagStore struct {
	object.ConditionalStore
}

func (s *FrozenETagStore) GetETag(ctx context.Context, key string) (*object.GetWithETag, error) {
	got, err := s.ConditionalStore.GetETag(ctx, key)
	if err != nil {
		return nil, err
	}
	got.ETag = "\"00000000000000000000000000000000\""
	return got, nil
}

func (s *FrozenETagStore) PutIfMatch(ctx context.Context, key string, r io.Reader, matchETag string) error {
	// Accepts the frozen ETag it hands out, so the probe gets past the step
	// that checks whether If-Match is rejected outright.
	return s.ConditionalStore.Put(ctx, key, r)
}

// RejectingStore is an object store that refuses conditional writes with a
// plain error rather than a precondition failure, the way older radosgw
// releases do. T4 can see this one; it is here so the probe is shown to tell
// it apart from a store that lies.
type RejectingStore struct {
	object.ConditionalStore
	// Err is returned by both conditional writes.
	Err error
	// RejectIfMatchOnly leaves PutIfAbsent working, which is the shape of the
	// store #256 was written for.
	RejectIfMatchOnly bool
}

func (s *RejectingStore) PutIfAbsent(ctx context.Context, key string, r io.Reader) error {
	if s.RejectIfMatchOnly {
		return s.ConditionalStore.PutIfAbsent(ctx, key, r)
	}
	return s.Err
}

func (s *RejectingStore) PutIfMatch(context.Context, string, io.Reader, string) error {
	return s.Err
}
