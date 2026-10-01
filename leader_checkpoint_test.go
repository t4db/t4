package t4

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/t4db/t4/pkg/object"
)

// blockingStore delays object-store Puts like a slow remote object store and
// signals when the first upload lands, so a test can observe the system while
// a checkpoint upload is still in flight.
type blockingStore struct {
	object.Store
	delay    time.Duration
	firstPut chan struct{}
	once     sync.Once
	mu       sync.Mutex
	putKeys  []string
	trace    []string
	t0       time.Time
}

func (s *blockingStore) Put(ctx context.Context, key string, r io.Reader) error {
	start := time.Since(s.t0)
	s.once.Do(func() { close(s.firstPut) })
	s.mu.Lock()
	s.putKeys = append(s.putKeys, key)
	s.mu.Unlock()
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	err := s.Store.Put(ctx, key, r)
	s.mu.Lock()
	s.trace = append(s.trace, fmt.Sprintf("put %-60q start=%dms end=%dms", key, start.Milliseconds(), time.Since(s.t0).Milliseconds()))
	s.mu.Unlock()
	return err
}

// TestCheckpointUploadDoesNotBlockWrites pins the fixed write-fence scope: the
// store copy, WAL seal and Pebble flush happen under fenceMu, but the
// object-store upload runs with the fence released. A write admitted while the
// upload is still running must complete quickly (previously it waited for
// every checkpoint PUT: the ~500ms-per-15min write p99 tail).
func TestCheckpointUploadDoesNotBlockWrites(t *testing.T) {
	store := &blockingStore{
		Store:    object.NewMem(),
		delay:    300 * time.Millisecond,
		firstPut: make(chan struct{}),
		t0:       time.Now(),
	}
	// Async WAL upload, as in production (default is sync for safety, which
	// would make every write pay an object-store PUT and swamp the signal).
	walSyncUpload := false
	n, err := Open(Config{
		DataDir:       filepath.Join(t.TempDir(), "db"),
		ObjectStore:   store,
		WALSyncUpload: &walSyncUpload,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = n.Close() }()

	ctx := context.Background()
	if _, err := n.Put(ctx, "k1", []byte("v1"), 0); err != nil {
		t.Fatalf("Put k1: %v", err)
	}

	cpDone := make(chan struct{})
	go func() {
		n.maybeCheckpoint(ctx)
		close(cpDone)
	}()

	// Wait until the checkpoint reaches its first object-store PUT, then write:
	// with the fence released before the upload, this Put only waits on local
	// WAL + Pebble.
	select {
	case <-store.firstPut:
	case <-time.After(5 * time.Second):
		t.Fatal("checkpoint did not start uploading within 5s")
	}
	start := time.Now()
	if _, err := n.Put(ctx, "k2", []byte("v2"), 0); err != nil {
		t.Fatalf("Put k2 during checkpoint upload: %v", err)
	}
	if d := time.Since(start); d > 250*time.Millisecond {
		store.mu.Lock()
		t.Errorf("Put blocked by checkpoint upload: took %v", d)
		for _, line := range store.trace {
			t.Log(line)
		}
		store.mu.Unlock()
	}

	<-cpDone
	store.mu.Lock()
	puts := append([]string(nil), store.putKeys...)
	store.mu.Unlock()
	var manifest bool
	for _, k := range puts {
		if filepath.Base(k) == "latest" {
			manifest = true
		}
	}
	if !manifest {
		t.Errorf("checkpoint upload never wrote the manifest; puts: %v", puts)
	}
}
