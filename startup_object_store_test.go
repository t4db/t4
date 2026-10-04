package t4_test

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/t4db/t4"
	"github.com/t4db/t4/pkg/object"
)

// outageStore fails every call while down is set, as an unreachable object
// store does.
type outageStore struct {
	*object.Mem
	down atomic.Bool
}

var errOutage = errors.New("object store unreachable")

func (s *outageStore) check() error {
	if s.down.Load() {
		return errOutage
	}
	return nil
}

func (s *outageStore) Put(ctx context.Context, key string, r io.Reader) error {
	if err := s.check(); err != nil {
		return err
	}
	return s.Mem.Put(ctx, key, r)
}

func (s *outageStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := s.check(); err != nil {
		return nil, err
	}
	return s.Mem.Get(ctx, key)
}

func (s *outageStore) List(ctx context.Context, prefix string) ([]string, error) {
	if err := s.check(); err != nil {
		return nil, err
	}
	return s.Mem.List(ctx, prefix)
}

func (s *outageStore) GetETag(ctx context.Context, key string) (*object.GetWithETag, error) {
	if err := s.check(); err != nil {
		return nil, err
	}
	return s.Mem.GetETag(ctx, key)
}

func (s *outageStore) PutIfAbsent(ctx context.Context, key string, r io.Reader) error {
	if err := s.check(); err != nil {
		return err
	}
	return s.Mem.PutIfAbsent(ctx, key, r)
}

func (s *outageStore) PutIfMatch(ctx context.Context, key string, r io.Reader, etag string) error {
	if err := s.check(); err != nil {
		return err
	}
	return s.Mem.PutIfMatch(ctx, key, r, etag)
}

func (s *outageStore) Delete(ctx context.Context, key string) error {
	if err := s.check(); err != nil {
		return err
	}
	return s.Mem.Delete(ctx, key)
}

// TestSingleNodeRestartsDuringObjectStoreOutage: a single node with local data
// starts from it when the object store is unreachable, instead of failing,
// and uploads once the store is back.
func TestSingleNodeRestartsDuringObjectStoreOutage(t *testing.T) {
	store := &outageStore{Mem: object.NewMem()}
	dir := filepath.Join(t.TempDir(), "data")
	async := false
	cfg := t4.Config{DataDir: dir, ObjectStore: store, WALSyncUpload: &async, CheckpointInterval: time.Hour}
	ctx := context.Background()

	n, err := t4.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n.Put(ctx, "/before", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}

	store.down.Store(true)
	start := time.Now()
	n, err = t4.Open(cfg)
	if err != nil {
		t.Fatalf("restart during the outage: %v", err)
	}
	defer func() { _ = n.Close() }()
	if d := time.Since(start); d > 30*time.Second {
		t.Errorf("restart during the outage took %v", d)
	}
	if kv, err := n.Get("/before"); err != nil || kv == nil || string(kv.Value) != "v" {
		t.Fatalf("read local data after restart: kv=%v err=%v", kv, err)
	}
	if _, err := n.Put(ctx, "/during", []byte("v"), 0); err != nil {
		t.Fatalf("write during the outage (async upload): %v", err)
	}
}

// TestFreshNodeStillNeedsObjectStore: without local data there is nothing to
// start from, so an unreachable object store still fails Open.
func TestFreshNodeStillNeedsObjectStore(t *testing.T) {
	store := &outageStore{Mem: object.NewMem()}
	store.down.Store(true)
	n, err := t4.Open(t4.Config{DataDir: t.TempDir(), ObjectStore: store})
	if err == nil {
		_ = n.Close()
		t.Fatal("fresh node started with an unreachable object store")
	}
	if !errors.Is(err, errOutage) {
		t.Errorf("err=%v, want the object store error", err)
	}
}
