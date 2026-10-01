package t4

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/t4db/t4/internal/checkpoint"
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

// manifestStore records every manifest/latest write and can fail checkpoint
// index uploads on demand. Index uploads for older sequences are delayed
// longer, so concurrent checkpoints, if any were allowed, would write
// manifest/latest out of order.
type manifestStore struct {
	object.Store
	failIndex atomic.Bool
	mu        sync.Mutex
	seqs      []int64
}

func (s *manifestStore) Put(ctx context.Context, key string, r io.Reader) error {
	if key == checkpoint.ManifestKey {
		b, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		var m checkpoint.Manifest
		if err := json.Unmarshal(b, &m); err != nil {
			return err
		}
		s.mu.Lock()
		s.seqs = append(s.seqs, m.LastSequence)
		s.mu.Unlock()
		return s.Store.Put(ctx, key, bytes.NewReader(b))
	}
	if filepath.Base(key) == "manifest.json" {
		if s.failIndex.Load() {
			return errors.New("injected checkpoint index failure")
		}
		b, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		var idx checkpoint.CheckpointIndex
		if err := json.Unmarshal(b, &idx); err != nil {
			return err
		}
		time.Sleep(time.Duration(max(0, 200-20*idx.LastSequence)) * time.Millisecond)
		return s.Store.Put(ctx, key, bytes.NewReader(b))
	}
	return s.Store.Put(ctx, key, r)
}

func (s *manifestStore) manifestSeqs() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int64(nil), s.seqs...)
}

func openCheckpointTestNode(t *testing.T, store object.Store) *Node {
	t.Helper()
	walSyncUpload := false
	n, err := Open(Config{
		DataDir:            filepath.Join(t.TempDir(), "db"),
		ObjectStore:        store,
		WALSyncUpload:      &walSyncUpload,
		CheckpointInterval: time.Hour, // keep the ticker out of the way
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = n.Close() })
	return n
}

// TestCheckpointRequestsKeepManifestMonotonic pins that checkpoints are
// serialized through checkpointLoop now that the write fence no longer covers
// the upload: concurrent forced checkpoints interleaved with writes must never
// move manifest/latest backwards.
func TestCheckpointRequestsKeepManifestMonotonic(t *testing.T) {
	store := &manifestStore{Store: object.NewMem()}
	n := openCheckpointTestNode(t, store)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := n.Put(ctx, fmt.Sprintf("k%d", i), []byte("v"), 0); err != nil {
				t.Errorf("Put: %v", err)
				return
			}
			n.requestCheckpoint(ctx, true)
		}()
	}
	wg.Wait()

	seqs := store.manifestSeqs()
	if len(seqs) == 0 {
		t.Fatal("no manifest written")
	}
	for i := 1; i < len(seqs); i++ {
		if seqs[i] < seqs[i-1] {
			t.Fatalf("manifest/latest went backwards: %v", seqs)
		}
	}
	if last := seqs[len(seqs)-1]; last != n.db.Load().LastSequence() {
		t.Errorf("final manifest seq=%d, want last sequence %d", last, n.db.Load().LastSequence())
	}
}

// TestFailedCheckpointIsRetried pins that entriesSinceCheckpoint is only
// discounted after a checkpoint is durably written: a failed upload must leave
// the counter non-zero so the next maybeCheckpoint retries.
func TestFailedCheckpointIsRetried(t *testing.T) {
	store := &manifestStore{Store: object.NewMem()}
	n := openCheckpointTestNode(t, store)
	ctx := context.Background()

	if _, err := n.Put(ctx, "k", []byte("v"), 0); err != nil {
		t.Fatalf("Put: %v", err)
	}
	store.failIndex.Store(true)
	n.maybeCheckpoint(ctx)
	if got := atomic.LoadInt64(&n.entriesSinceCheckpoint); got == 0 {
		t.Fatal("entriesSinceCheckpoint reset by a failed checkpoint; it would never be retried")
	}
	if seqs := store.manifestSeqs(); len(seqs) != 0 {
		t.Fatalf("failed checkpoint wrote manifest: %v", seqs)
	}

	store.failIndex.Store(false)
	n.maybeCheckpoint(ctx)
	if len(store.manifestSeqs()) == 0 {
		t.Fatal("retry did not write a checkpoint")
	}
	if got := atomic.LoadInt64(&n.entriesSinceCheckpoint); got != 0 {
		t.Errorf("entriesSinceCheckpoint=%d after successful checkpoint, want 0", got)
	}
}
