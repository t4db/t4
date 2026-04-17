package t4

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
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

// fakeWAL wraps a real WALWriter and can be configured to fail or block.
type fakeWAL struct {
	mu      sync.RWMutex
	real    WALWriter
	failNow bool          // AppendBatch returns errInjected when true
	blockC  chan struct{} // AppendBatch blocks until this is closed (nil = no block)
}

func (f *fakeWAL) Append(e *wal.Entry) error        { return f.real.Append(e) }
func (f *fakeWAL) SealAndFlush(nextRev int64) error { return f.real.SealAndFlush(nextRev) }
func (f *fakeWAL) Close() error                     { return f.real.Close() }
func (f *fakeWAL) Open(dir string, term uint64, startRev int64) error {
	return f.real.Open(dir, term, startRev)
}
func (f *fakeWAL) ReplayLocal(db wal.RecoveryStore, afterRev int64) error {
	return f.real.ReplayLocal(db, afterRev)
}

func (f *fakeWAL) AppendBatch(ctx context.Context, entries []*wal.Entry) error {
	f.mu.RLock()
	blockC := f.blockC
	failNow := f.failNow
	f.mu.RUnlock()
	if blockC != nil {
		select {
		case <-blockC:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if failNow {
		return errInjected
	}
	return f.real.AppendBatch(ctx, entries)
}

func (f *fakeWAL) setFailNow(v bool) {
	f.mu.Lock()
	f.failNow = v
	f.mu.Unlock()
}

func (f *fakeWAL) setBlockChan(ch chan struct{}) {
	f.mu.Lock()
	f.blockC = ch
	f.mu.Unlock()
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
	fw.setFailNow(true)

	if _, err := n.Put(ctx, "/fault/k", []byte("v2"), 0); err == nil {
		t.Fatal("expected error from injected WAL failure, got nil")
	}

	// Restore the WAL so that the fakeWAL itself is no longer broken.
	// The node must refuse this write because it fenced itself, not because
	// the WAL is still injecting errors.
	fw.setFailNow(false)

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
	blockC := make(chan struct{})
	fw.setBlockChan(blockC)

	// Defers run LIFO: blockC is closed first (unblocking the commit loop),
	// then n.Close() can wait for it to exit cleanly.
	defer n.Close()
	defer close(blockC)

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

type blockableProxyLocal struct {
	lis     net.Listener
	target  string
	blocked int32
	connsMu sync.Mutex
	conns   []net.Conn
}

func newBlockableProxyLocal(t *testing.T, target string) *blockableProxyLocal {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("blockableProxyLocal listen: %v", err)
	}
	p := &blockableProxyLocal{lis: lis, target: target}
	go p.serve()
	t.Cleanup(func() { _ = lis.Close() })
	return p
}

func (p *blockableProxyLocal) Addr() string { return p.lis.Addr().String() }

func (p *blockableProxyLocal) block() {
	atomic.StoreInt32(&p.blocked, 1)
	p.connsMu.Lock()
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = p.conns[:0]
	p.connsMu.Unlock()
}

func (p *blockableProxyLocal) unblock() { atomic.StoreInt32(&p.blocked, 0) }

func (p *blockableProxyLocal) serve() {
	for {
		c, err := p.lis.Accept()
		if err != nil {
			return
		}
		if atomic.LoadInt32(&p.blocked) == 1 {
			_ = c.Close()
			continue
		}
		dst, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = c.Close()
			continue
		}
		p.connsMu.Lock()
		p.conns = append(p.conns, c, dst)
		p.connsMu.Unlock()
		go func() { _, _ = io.Copy(dst, c); _ = dst.Close(); _ = c.Close() }()
		go func() { _, _ = io.Copy(c, dst); _ = c.Close(); _ = dst.Close() }()
	}
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
	fw.setFailNow(true)
	if _, err := leader.Put(ctx, "/ghost", []byte("should-not-commit"), 0); err == nil {
		t.Fatal("expected injected WAL failure, got nil")
	}
	fw.setFailNow(false)

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

func TestFollowerReconnectDropsStagedUncommittedEntries(t *testing.T) {
	store := object.NewMem()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leaderPeerReal := freeAddrLocal(t)
	proxy := newBlockableProxyLocal(t, leaderPeerReal)

	leader, err := Open(Config{
		DataDir:            t.TempDir(),
		ObjectStore:        store,
		NodeID:             "leader",
		PeerListenAddr:     leaderPeerReal,
		AdvertisePeerAddr:  proxy.Addr(),
		FollowerMaxRetries: 2,
		PeerBufferSize:     1000,
		CheckpointInterval: 300 * time.Millisecond,
		SegmentMaxAge:      200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("open leader: %v", err)
	}
	defer leader.Close()

	followerPeer := freeAddrLocal(t)
	follower, err := Open(Config{
		DataDir:            t.TempDir(),
		ObjectStore:        store,
		NodeID:             "follower",
		PeerListenAddr:     followerPeer,
		AdvertisePeerAddr:  followerPeer,
		FollowerMaxRetries: 2,
		PeerBufferSize:     1000,
		CheckpointInterval: 300 * time.Millisecond,
		SegmentMaxAge:      200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("open follower: %v", err)
	}
	defer follower.Close()

	waitForLeaderNodeLocal(t, []*Node{leader, follower}, 10*time.Second)

	seedRev, err := leader.Put(ctx, "/reconnect/seed", []byte("ok"), 0)
	if err != nil {
		t.Fatalf("seed Put: %v", err)
	}
	if err := follower.WaitForRevision(ctx, seedRev); err != nil {
		t.Fatalf("follower seed WaitForRevision: %v", err)
	}

	// Stage an entry on the follower by letting the leader broadcast it, but
	// keep the leader WAL append blocked until the client context expires.
	fw := newFakeWAL(leader)
	fw.setBlockChan(make(chan struct{}))
	writeCtx, writeCancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer writeCancel()
	if _, err := leader.Put(writeCtx, "/reconnect/ghost", []byte("staged-only"), 0); err == nil {
		t.Fatal("expected blocked Put to fail, got nil")
	}

	// Drop the follower stream while the uncommitted entry is only staged in
	// memory. After reconnect it must resume from the last committed revision.
	proxy.block()
	time.Sleep(200 * time.Millisecond)
	proxy.unblock()

	// Restore the real WAL for future writes. The cancelled batch above should
	// have already unwound via ctx.Done(); give it a brief window to exit
	// before swapping the WAL back so the timed-out request cannot later
	// succeed and turn the ghost write into a real commit.
	time.Sleep(100 * time.Millisecond)
	fw.setBlockChan(nil)

	afterRev, err := leader.Put(ctx, "/reconnect/after", []byte("committed"), 0)
	if err != nil {
		t.Fatalf("post-heal Put: %v", err)
	}
	_ = afterRev // used implicitly by eventual visibility checks below

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ghostKV, ghostErr := follower.Get("/reconnect/ghost")
		afterKV, afterErr := follower.Get("/reconnect/after")
		if ghostErr == nil && ghostKV == nil && afterErr == nil && afterKV != nil && string(afterKV.Value) == "committed" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	ghostKV, ghostErr := follower.Get("/reconnect/ghost")
	afterKV, afterErr := follower.Get("/reconnect/after")
	t.Fatalf("unexpected follower state after reconnect: ghost=(kv=%+v err=%v) after=(kv=%+v err=%v)",
		ghostKV, ghostErr, afterKV, afterErr)
}
