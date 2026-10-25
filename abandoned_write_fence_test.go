package t4

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/t4db/t4/pkg/object"
)

// TestFenceWaitsForAbandonedWrites checks that holding fenceMu exclusively
// (here through Flush, as checkpoints, shutdown and genesis do) waits for a
// write whose caller gave up: that caller released its share of fenceMu, but
// its write is still queued and is still committed.
func TestFenceWaitsForAbandonedWrites(t *testing.T) {
	n, err := Open(Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = n.Close() }()
	ctx := context.Background()
	if _, err := n.Put(ctx, "/seed", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	counted := atomic.LoadInt64(&n.entriesSinceCheckpoint)

	fw := newFakeWAL(n)
	blockC := make(chan struct{})
	fw.setBlockChan(blockC)
	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := n.Put(short, "/abandoned", []byte("v"), 0); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Put against a blocked WAL: %v, want the caller's deadline", err)
	}

	flushed := make(chan error, 1)
	go func() { flushed <- n.Flush() }()
	select {
	case err := <-flushed:
		t.Fatalf("Flush returned (%v) while an abandoned write was still queued", err)
	case <-time.After(200 * time.Millisecond):
	}
	fw.setBlockChan(nil)
	close(blockC)
	if err := <-flushed; err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if kv, err := n.Get("/abandoned"); err != nil || kv == nil {
		t.Fatalf("abandoned write after Flush: %+v, %v; want it committed before Flush", kv, err)
	}
	if got := atomic.LoadInt64(&n.entriesSinceCheckpoint) - counted; got != 1 {
		t.Fatalf("entries counted toward the next checkpoint: %d, want the abandoned write", got)
	}
}

// TestCanceledCallerQueuesNothing checks that a write whose caller has
// already given up is not queued: queued, it would hold a revision and be
// committed, though its caller is told it failed.
func TestCanceledCallerQueuesNothing(t *testing.T) {
	n, err := Open(Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = n.Close() }()
	ctx := context.Background()
	before, err := n.Put(ctx, "/before", []byte("v"), 0)
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := n.Put(canceled, "/canceled", []byte("v"), 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("Put with a canceled context: %v, want context.Canceled", err)
	}
	if _, err := n.Txn(canceled, TxnRequest{Success: []TxnOp{{Type: TxnPut, Key: "/canceled", Value: []byte("v")}}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Txn with a canceled context: %v, want context.Canceled", err)
	}
	if err := n.Compact(canceled, before); !errors.Is(err, context.Canceled) {
		t.Fatalf("Compact with a canceled context: %v, want context.Canceled", err)
	}
	after, err := n.Put(ctx, "/after", []byte("v"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if after != before+1 {
		t.Fatalf("revision after canceled writes = %d, want %d", after, before+1)
	}
	if kv, err := n.Get("/canceled"); err != nil || kv != nil {
		t.Fatalf("canceled write: %+v, %v; want none", kv, err)
	}
}

// TestShutdownBoundsWaitForQueuedWrites checks that graceful shutdown does not
// wait forever for a write stuck in the commit loop: it gives up after
// shutdownWait, and Close then stops the loop and returns.
func TestShutdownBoundsWaitForQueuedWrites(t *testing.T) {
	addr := freeAddrLocal(t)
	n, err := Open(Config{
		NodeID:            "n1",
		DataDir:           filepath.Join(t.TempDir(), "db"),
		ObjectStore:       object.NewMem(),
		PeerListenAddr:    addr,
		AdvertisePeerAddr: addr,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !n.IsLeader() {
		t.Fatal("node did not become leader")
	}
	n.shutdownWait = 200 * time.Millisecond

	fw := newFakeWAL(n)
	fw.setBlockChan(make(chan struct{})) // never unblocked: only the loop's context ends the append
	short, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := n.Put(short, "/stuck", []byte("v"), 0); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Put against a blocked WAL: %v, want the caller's deadline", err)
	}

	closed := make(chan error, 1)
	go func() { closed <- n.Close() }()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close hung on a write stuck in the commit loop")
	}
}
