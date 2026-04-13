package t4

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/t4db/t4/internal/wal"
	"github.com/t4db/t4/pkg/object"
)

var errInjected = errors.New("injected fault")

// TestConcurrentCompactPutRevisionUniqueness is a regression test for the
// Compact/Put revision collision race.
//
// Before the group-commit refactor, Compact() and Put() both read n.nextRev
// without holding n.mu, so concurrent calls could get the same revision.
// The peer server's maxSent dedup filter then silently dropped the Put entry
// on followers, causing missing keys.
//
// The fix: all write paths increment n.nextRev under n.mu before sending to
// the commit loop. This test verifies that concurrent Compact+Put operations
// always produce strictly unique, monotonically increasing revisions.
func TestConcurrentCompactPutRevisionUniqueness(t *testing.T) {
	n, err := Open(Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	ctx := context.Background()
	const writers = 8
	const writesPerWorker = 50

	// Buffer must hold all revisions: writers Put goroutines + writers compact-seed
	// goroutines each emit writesPerWorker revisions.
	revC := make(chan int64, 2*writers*writesPerWorker)
	var wg sync.WaitGroup

	// Concurrent Put goroutines.
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < writesPerWorker; i++ {
				rev, err := n.Put(ctx, "/race/put", []byte("v"), 0)
				if err != nil {
					t.Errorf("Put worker %d: %v", w, err)
					return
				}
				revC <- rev
			}
		}(w)
	}

	// Concurrent Compact goroutines interleaved with the Puts.
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < writesPerWorker; i++ {
				rev, err := n.Put(ctx, "/race/compact-seed", []byte("v"), 0)
				if err != nil {
					t.Errorf("compact-seed worker %d: %v", w, err)
					return
				}
				if err := n.Compact(ctx, rev-1); err != nil {
					t.Errorf("Compact worker %d: %v", w, err)
					return
				}
				revC <- rev
			}
		}(w)
	}

	wg.Wait()
	close(revC)

	seen := make(map[int64]bool, cap(revC))
	for rev := range revC {
		if seen[rev] {
			t.Errorf("duplicate revision %d: Compact and Put raced for the same revision", rev)
		}
		seen[rev] = true
	}
}

// fakeWAL wraps a real walWriter and can be configured to fail or block.
type fakeWAL struct {
	real    walWriter
	failNow bool          // AppendBatch returns errInjected when true
	blockC  chan struct{} // AppendBatch blocks until this is closed (nil = no block)
}

func (f *fakeWAL) Append(e *wal.Entry) error        { return f.real.Append(e) }
func (f *fakeWAL) SealAndFlush(nextRev int64) error { return f.real.SealAndFlush(nextRev) }
func (f *fakeWAL) Close() error                     { return f.real.Close() }

func (f *fakeWAL) AppendBatch(ctx context.Context, entries []*wal.Entry) error {
	if f.blockC != nil {
		select {
		case <-f.blockC:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if f.failNow {
		return errInjected
	}
	return f.real.AppendBatch(ctx, entries)
}

func newFakeWAL(n *Node) *fakeWAL {
	real := n.wal
	fw := &fakeWAL{real: real}
	n.wal = fw
	return fw
}

// TestCommitLoopWALErrorFences verifies that after a WAL/commit error the node
// refuses all further writes (self-fence) rather than continuing on a
// potentially corrupt segment.
func TestCommitLoopWALErrorFences(t *testing.T) {
	n, err := Open(Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	ctx := context.Background()

	if _, err := n.Put(ctx, "/fault/k", []byte("v1"), 0); err != nil {
		t.Fatalf("pre-fault Put: %v", err)
	}

	fw := newFakeWAL(n)
	fw.failNow = true

	if _, err := n.Put(ctx, "/fault/k", []byte("v2"), 0); err == nil {
		t.Fatal("expected error from injected WAL failure, got nil")
	}

	// Restore the WAL so that the fakeWAL itself is no longer broken.
	// The node must refuse this write because it fenced itself, not because
	// the WAL is still injecting errors.
	fw.failNow = false

	// Node must now be fenced — this write should also fail.
	if _, err := n.Put(ctx, "/fault/k", []byte("v3"), 0); err == nil {
		t.Fatal("node accepted write after commit error: want self-fence")
	}
}

// TestCommitLoopDeathUnblocksWrites verifies that if the commitLoop is stuck
// (e.g. blocked in AppendBatch indefinitely), in-flight writes return an error
// promptly rather than blocking forever once the context is cancelled.
func TestCommitLoopDeathUnblocksWrites(t *testing.T) {
	n, err := Open(Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	if _, err := n.Put(ctx, "/fault/k", []byte("v1"), 0); err != nil {
		t.Fatalf("pre-fault Put: %v", err)
	}

	// Replace WAL with one that blocks forever in AppendBatch.
	fw := newFakeWAL(n)
	fw.blockC = make(chan struct{})

	// Defers run LIFO: blockC is closed first (unblocking the commit loop),
	// then n.Close() can wait for it to exit cleanly.
	defer n.Close()
	defer close(fw.blockC)

	// Issue a write in the background — it will be stuck in the commit loop.
	writeErr := make(chan error, 1)
	writeCtx, writeCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer writeCancel()
	go func() {
		_, err := n.Put(writeCtx, "/fault/k", []byte("v2"), 0)
		writeErr <- err
	}()

	// The write context expires; the caller should get an error promptly —
	// not hang past the deadline.
	select {
	case err := <-writeErr:
		if err == nil {
			t.Fatal("write succeeded despite blocked commit loop")
		}
		if errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("write returned DeadlineExceeded: caller was not unblocked promptly")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("write goroutine never returned: stuck waiting on done channel")
	}
}

func TestClearPendingBatchRemovesOnlyMatchingRevisions(t *testing.T) {
	n := &Node{
		pending: map[string]pendingKV{
			"/same": {rev: 2, kv: nil},
			"/gone": {rev: 3, kv: nil},
		},
	}

	batch := []*writeReq{
		{entry: wal.Entry{Key: "/same", Revision: 1}},
		{entry: wal.Entry{Key: "/gone", Revision: 3}},
		{entry: wal.Entry{Revision: 4}}, // compact/no-key path
	}

	n.clearPendingBatch(batch)

	if _, ok := n.pending["/gone"]; ok {
		t.Fatal("matching pending entry was not cleared")
	}
	if got, ok := n.pending["/same"]; !ok || got.rev != 2 {
		t.Fatalf("newer pending entry was incorrectly removed: %+v", got)
	}
}

func waitForLeaderNodeLocal(t *testing.T, nodes []*Node, timeout time.Duration) *Node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n != nil && n.IsLeader() {
				return n
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no leader elected within %v", timeout)
	return nil
}

func freeAddrLocal(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func TestFollowerDoesNotExposeUncommittedEntryAfterLeaderWALError(t *testing.T) {
	store := object.NewMem()

	openNode := func(id string) *Node {
		t.Helper()
		addr := freeAddrLocal(t)
		n, err := Open(Config{
			DataDir:            t.TempDir(),
			ObjectStore:        store,
			NodeID:             id,
			PeerListenAddr:     addr,
			AdvertisePeerAddr:  addr,
			FollowerMaxRetries: 2,
			PeerBufferSize:     1000,
			CheckpointInterval: 300 * time.Millisecond,
			SegmentMaxAge:      200 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("open %s: %v", id, err)
		}
		return n
	}

	leaderOrFollowerA := openNode("node-a")
	defer leaderOrFollowerA.Close()
	leaderOrFollowerB := openNode("node-b")
	defer leaderOrFollowerB.Close()

	nodes := []*Node{leaderOrFollowerA, leaderOrFollowerB}
	leader := waitForLeaderNodeLocal(t, nodes, 10*time.Second)
	var follower *Node
	for _, n := range nodes {
		if n != leader {
			follower = n
			break
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	seedRev, err := leader.Put(ctx, "/seed", []byte("ok"), 0)
	if err != nil {
		t.Fatalf("seed Put: %v", err)
	}
	if err := follower.WaitForRevision(ctx, seedRev); err != nil {
		t.Fatalf("follower seed WaitForRevision: %v", err)
	}

	fw := newFakeWAL(leader)
	fw.failNow = true
	if _, err := leader.Put(ctx, "/ghost", []byte("should-not-commit"), 0); err == nil {
		t.Fatal("expected injected WAL failure, got nil")
	}
	fw.failNow = false

	// The follower may already have received the staged entry over the stream,
	// but without a commit marker it must never expose it locally.
	time.Sleep(300 * time.Millisecond)
	if kv, err := follower.Get("/ghost"); err != nil {
		t.Fatalf("follower Get /ghost before takeover: %v", err)
	} else if kv != nil {
		t.Fatalf("follower exposed uncommitted key before takeover: %+v", kv)
	}

	if err := leader.Close(); err != nil {
		t.Fatalf("close fenced leader: %v", err)
	}

	newLeader := waitForLeaderNodeLocal(t, []*Node{follower}, 30*time.Second)
	if newLeader != follower {
		t.Fatalf("expected follower to take over, got %p", newLeader)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		kv, err := newLeader.Get("/ghost")
		if err == nil && kv == nil {
			return
		}
		if err != nil {
			t.Fatalf("new leader Get /ghost: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	kv, _ := newLeader.Get("/ghost")
	t.Fatalf("new leader exposed uncommitted key after takeover: %v", fmt.Sprintf("%+v", kv))
}
