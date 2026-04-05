package object

import (
	"context"
	"io"
	"time"

	"github.com/strata-db/strata/internal/metrics"
)

// instrumentedStore wraps a Store and records Prometheus metrics for every
// operation: strata_object_store_ops_total{op, result} and
// strata_object_store_duration_seconds{op}.
type instrumentedStore struct{ inner Store }

// instrumentedConditionalStore additionally implements ConditionalStore.
type instrumentedConditionalStore struct {
	instrumentedStore
	inner ConditionalStore
}

// NewInstrumentedStore wraps s with metrics instrumentation. If s also
// implements ConditionalStore the returned value implements it too, so
// callers that type-assert to ConditionalStore continue to work.
func NewInstrumentedStore(s Store) Store {
	if cs, ok := s.(ConditionalStore); ok {
		return &instrumentedConditionalStore{
			instrumentedStore: instrumentedStore{inner: s},
			inner:             cs,
		}
	}
	return &instrumentedStore{inner: s}
}

func record(op string, start time.Time, err error) {
	result := "success"
	if err != nil {
		result = "error"
	}
	metrics.ObjectStoreOpsTotal.WithLabelValues(op, result).Inc()
	metrics.ObjectStoreDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())
}

func (s *instrumentedStore) Put(ctx context.Context, key string, r io.Reader) error {
	start := time.Now()
	err := s.inner.Put(ctx, key, r)
	record("put", start, err)
	return err
}

func (s *instrumentedStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	start := time.Now()
	rc, err := s.inner.Get(ctx, key)
	record("get", start, err)
	return rc, err
}

func (s *instrumentedStore) Delete(ctx context.Context, key string) error {
	start := time.Now()
	err := s.inner.Delete(ctx, key)
	record("delete", start, err)
	return err
}

func (s *instrumentedStore) List(ctx context.Context, prefix string) ([]string, error) {
	start := time.Now()
	keys, err := s.inner.List(ctx, prefix)
	record("list", start, err)
	return keys, err
}

func (s *instrumentedConditionalStore) GetETag(ctx context.Context, key string) (*GetWithETag, error) {
	start := time.Now()
	res, err := s.inner.GetETag(ctx, key)
	record("get_etag", start, err)
	return res, err
}

func (s *instrumentedConditionalStore) PutIfAbsent(ctx context.Context, key string, r io.Reader) error {
	start := time.Now()
	err := s.inner.PutIfAbsent(ctx, key, r)
	record("put_if_absent", start, err)
	return err
}

func (s *instrumentedConditionalStore) PutIfMatch(ctx context.Context, key string, r io.Reader, matchETag string) error {
	start := time.Now()
	err := s.inner.PutIfMatch(ctx, key, r, matchETag)
	record("put_if_match", start, err)
	return err
}
