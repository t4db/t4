package object

import (
	"context"
	"io"
	"time"

	"github.com/t4db/t4/internal/metrics"
)

// Observer records the outcome and latency of each object store operation.
type Observer interface {
	ObserveObjectStoreOp(op string, d time.Duration, err error)
}

// instrumentedStore wraps a Store and reports every operation to an
// Observer: for a node, t4_object_store_ops_total{op, result} and
// t4_object_store_duration_seconds{op}.
type instrumentedStore struct {
	inner Store
	o     Observer
}

// instrumentedConditionalStore additionally implements ConditionalStore.
type instrumentedConditionalStore struct {
	instrumentedStore
	inner ConditionalStore
}

type instrumentedVersionedStore struct {
	instrumentedStore
	inner VersionedStore
}

type instrumentedConditionalVersionedStore struct {
	instrumentedStore
	conditional ConditionalStore
	versioned   VersionedStore
}

// NewInstrumentedStore wraps s with metrics instrumentation recorded on the
// t4 metrics of the nodes open on prometheus.DefaultRegisterer, while there
// are any. If s also implements optional Store extensions, the returned value
// implements them too, so callers that type-assert to ConditionalStore or
// VersionedStore continue to work.
func NewInstrumentedStore(s Store) Store {
	return NewObservedStore(s, defaultObserver{})
}

// NewObservedStore wraps s so that every operation is reported to o. Like
// NewInstrumentedStore, the result implements the optional Store extensions
// s implements.
func NewObservedStore(s Store, o Observer) Store {
	base := instrumentedStore{inner: s, o: o}
	cs, hasConditional := s.(ConditionalStore)
	vs, hasVersioned := s.(VersionedStore)
	switch {
	case hasConditional && hasVersioned:
		return &instrumentedConditionalVersionedStore{
			instrumentedStore: base,
			conditional:       cs,
			versioned:         vs,
		}
	case hasConditional:
		return &instrumentedConditionalStore{
			instrumentedStore: base,
			inner:             cs,
		}
	case hasVersioned:
		return &instrumentedVersionedStore{
			instrumentedStore: base,
			inner:             vs,
		}
	default:
		return &base
	}
}

// defaultObserver records on the metrics of the nodes open on
// prometheus.DefaultRegisterer.
type defaultObserver struct{}

func (defaultObserver) ObserveObjectStoreOp(op string, d time.Duration, err error) {
	if m := metrics.Lookup(nil); m != nil {
		m.ObserveObjectStoreOp(op, d, err)
	}
}

func (s *instrumentedStore) record(op string, start time.Time, err error) {
	s.o.ObserveObjectStoreOp(op, time.Since(start), err)
}

func (s *instrumentedStore) Put(ctx context.Context, key string, r io.Reader) error {
	start := time.Now()
	err := s.inner.Put(ctx, key, r)
	s.record("put", start, err)
	return err
}

func (s *instrumentedStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	start := time.Now()
	rc, err := s.inner.Get(ctx, key)
	s.record("get", start, err)
	return rc, err
}

func (s *instrumentedStore) Delete(ctx context.Context, key string) error {
	start := time.Now()
	err := s.inner.Delete(ctx, key)
	s.record("delete", start, err)
	return err
}

func (s *instrumentedStore) DeleteMany(ctx context.Context, keys []string) error {
	start := time.Now()
	err := s.inner.DeleteMany(ctx, keys)
	s.record("delete_many", start, err)
	return err
}

func (s *instrumentedStore) List(ctx context.Context, prefix string) ([]string, error) {
	start := time.Now()
	keys, err := s.inner.List(ctx, prefix)
	s.record("list", start, err)
	return keys, err
}

func (s *instrumentedConditionalStore) GetETag(ctx context.Context, key string) (*GetWithETag, error) {
	start := time.Now()
	res, err := s.inner.GetETag(ctx, key)
	s.record("get_etag", start, err)
	return res, err
}

func (s *instrumentedConditionalStore) PutIfAbsent(ctx context.Context, key string, r io.Reader) error {
	start := time.Now()
	err := s.inner.PutIfAbsent(ctx, key, r)
	s.record("put_if_absent", start, err)
	return err
}

func (s *instrumentedConditionalStore) PutIfMatch(ctx context.Context, key string, r io.Reader, matchETag string) error {
	start := time.Now()
	err := s.inner.PutIfMatch(ctx, key, r, matchETag)
	s.record("put_if_match", start, err)
	return err
}

func (s *instrumentedVersionedStore) GetVersioned(ctx context.Context, key, versionID string) (io.ReadCloser, error) {
	start := time.Now()
	rc, err := s.inner.GetVersioned(ctx, key, versionID)
	s.record("get_versioned", start, err)
	return rc, err
}

func (s *instrumentedConditionalVersionedStore) GetETag(ctx context.Context, key string) (*GetWithETag, error) {
	start := time.Now()
	res, err := s.conditional.GetETag(ctx, key)
	s.record("get_etag", start, err)
	return res, err
}

func (s *instrumentedConditionalVersionedStore) PutIfAbsent(ctx context.Context, key string, r io.Reader) error {
	start := time.Now()
	err := s.conditional.PutIfAbsent(ctx, key, r)
	s.record("put_if_absent", start, err)
	return err
}

func (s *instrumentedConditionalVersionedStore) PutIfMatch(ctx context.Context, key string, r io.Reader, matchETag string) error {
	start := time.Now()
	err := s.conditional.PutIfMatch(ctx, key, r, matchETag)
	s.record("put_if_match", start, err)
	return err
}

func (s *instrumentedConditionalVersionedStore) GetVersioned(ctx context.Context, key, versionID string) (io.ReadCloser, error) {
	start := time.Now()
	rc, err := s.versioned.GetVersioned(ctx, key, versionID)
	s.record("get_versioned", start, err)
	return rc, err
}
