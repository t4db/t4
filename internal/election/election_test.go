package election

import (
	"context"
	"testing"
	"time"

	"github.com/t4db/t4/pkg/object"
)

func newLock(t *testing.T, nodeID, addr string) *Lock {
	t.Helper()
	return NewLock(object.NewMem(), nodeID, addr)
}

func newLockShared(store *object.Mem, nodeID, addr string) *Lock {
	return NewLock(store, nodeID, addr)
}

func TestTryAcquireEmpty(t *testing.T) {
	l := newLock(t, "node-1", "localhost:2380")
	rec, won, err := l.TryAcquire(context.Background(), 0, Fence{})
	if err != nil {
		t.Fatalf("TryAcquire: %v", err)
	}
	if !won {
		t.Error("expected to win election on empty store")
	}
	if rec.NodeID != "node-1" {
		t.Errorf("NodeID: want node-1 got %q", rec.NodeID)
	}
	if rec.Term != 1 {
		t.Errorf("Term: want 1 got %d", rec.Term)
	}
}

func TestTryAcquireFloorTerm(t *testing.T) {
	l := newLock(t, "node-1", "localhost:2380")
	rec, won, err := l.TryAcquire(context.Background(), 5, Fence{Rev: 5})
	if err != nil || !won {
		t.Fatalf("TryAcquire: won=%v err=%v", won, err)
	}
	if rec.Term != 6 {
		t.Errorf("Term: want 6 (floorTerm+1) got %d", rec.Term)
	}
}

func TestTwoNodeElection(t *testing.T) {
	store := object.NewMem()
	l1 := newLockShared(store, "node-1", "localhost:2380")
	l2 := newLockShared(store, "node-2", "localhost:2381")

	_, won1, err := l1.TryAcquire(context.Background(), 0, Fence{})
	if err != nil || !won1 {
		t.Fatalf("node-1 should win: won=%v err=%v", won1, err)
	}

	// node-2 tries while node-1 holds the lock → should lose.
	existing, won2, err := l2.TryAcquire(context.Background(), 0, Fence{})
	if err != nil {
		t.Fatalf("node-2 TryAcquire: %v", err)
	}
	if won2 {
		t.Error("node-2 should not win while node-1 holds the lock")
	}
	if existing == nil || existing.NodeID != "node-1" {
		t.Errorf("existing lock should belong to node-1, got %+v", existing)
	}
}

func TestTakeOverAfterLeaderDead(t *testing.T) {
	store := object.NewMem()
	l1 := newLockShared(store, "node-1", "localhost:2380")
	l2 := newLockShared(store, "node-2", "localhost:2381")

	// node-1 acquires.
	rec1, won1, err := l1.TryAcquire(context.Background(), 0, Fence{})
	if err != nil || !won1 {
		t.Fatalf("node-1 should win: %v", err)
	}

	// node-2 cannot acquire normally (node-1 still holds it, no TTL).
	_, won2, err := l2.TryAcquire(context.Background(), 0, Fence{})
	if err != nil || won2 {
		t.Fatalf("node-2 should lose TryAcquire: won=%v err=%v", won2, err)
	}

	// Simulate: node-2 detects leader dead via stream → calls TakeOver.
	rec2, won2, err := l2.TakeOver(context.Background(), rec1.Term, Fence{Rev: rec1.CommittedRev}, nil)
	if err != nil {
		t.Fatalf("TakeOver: %v", err)
	}
	if !won2 {
		t.Error("node-2 should win TakeOver after leader is dead")
	}
	if rec2.NodeID != "node-2" {
		t.Errorf("expected node-2 to hold the lock, got %q", rec2.NodeID)
	}
	if rec2.Term <= rec1.Term {
		t.Errorf("new term %d should be > old term %d", rec2.Term, rec1.Term)
	}
}

func TestTakeOverRace(t *testing.T) {
	// Two followers simultaneously attempt TakeOver — exactly one should win.
	store := object.NewMem()
	l1 := newLockShared(store, "leader", "localhost:2380")
	_, won, _ := l1.TryAcquire(context.Background(), 0, Fence{})
	if !won {
		t.Fatal("leader should win initial election")
	}

	f1 := newLockShared(store, "follower-1", "localhost:2381")
	f2 := newLockShared(store, "follower-2", "localhost:2382")

	type result struct {
		rec *LockRecord
		won bool
	}
	ch := make(chan result, 2)
	go func() {
		rec, won, _ := f1.TakeOver(context.Background(), 1, Fence{}, nil)
		ch <- result{rec, won}
	}()
	go func() {
		rec, won, _ := f2.TakeOver(context.Background(), 1, Fence{}, nil)
		ch <- result{rec, won}
	}()

	r1 := <-ch
	r2 := <-ch
	wins := 0
	if r1.won {
		wins++
	}
	if r2.won {
		wins++
	}
	if wins != 1 {
		t.Errorf("expected exactly 1 winner, got %d (r1.won=%v r2.won=%v)", wins, r1.won, r2.won)
	}
}

func TestRelease(t *testing.T) {
	store := object.NewMem()
	l1 := newLockShared(store, "node-1", "addr1")
	_, _, _ = l1.TryAcquire(context.Background(), 0, Fence{})

	if err := l1.Release(context.Background()); err != nil {
		t.Fatalf("Release: %v", err)
	}

	// After release, the lock object should be gone.
	rec, err := l1.Read(context.Background())
	if err != nil {
		t.Fatalf("Read after release: %v", err)
	}
	if rec != nil {
		t.Errorf("expected nil record after release, got %+v", rec)
	}
}

func TestTermMonotonicity(t *testing.T) {
	store := object.NewMem()
	var lastTerm uint64
	var committedRev int64
	for i := 0; i < 5; i++ {
		// Simulate a takeover on each iteration.
		l := newLockShared(store, "node-1", "addr")
		time.Sleep(time.Millisecond) // ensure distinct wall-clock instants
		rec, won, err := l.TakeOver(context.Background(), lastTerm, Fence{Rev: committedRev}, nil)
		if err != nil || !won {
			t.Fatalf("iter %d: TakeOver won=%v err=%v", i, won, err)
		}
		if rec.Term <= lastTerm {
			t.Errorf("term not monotonic: %d -> %d", lastTerm, rec.Term)
		}
		lastTerm = rec.Term
		committedRev = rec.CommittedRev
	}
}

func TestLeaderWatchDetectsSupersession(t *testing.T) {
	store := object.NewMem()
	l1 := newLockShared(store, "node-1", "addr1")
	l2 := newLockShared(store, "node-2", "addr2")

	rec1, _, _ := l1.TryAcquire(context.Background(), 0, Fence{})

	// node-2 takes over.
	rec2, won, _ := l2.TakeOver(context.Background(), rec1.Term, Fence{Rev: rec1.CommittedRev}, nil)
	if !won {
		t.Fatal("node-2 should win TakeOver")
	}

	// node-1's watch reads the lock and should see a different term/nodeID.
	current, err := l1.Read(context.Background())
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if current.Term == rec1.Term && current.NodeID == "node-1" {
		t.Error("expected lock to reflect new leader, but still shows node-1")
	}
	if current.NodeID != "node-2" || current.Term != rec2.Term {
		t.Errorf("expected node-2 term=%d, got %+v", rec2.Term, current)
	}
}

// The fence orders positions by the term of the last entry, then sequence: a
// candidate holding a deposed leader's uncommitted suffix is behind the next
// leader's entries however long that suffix is.
func TestBlocksByTermThenSequence(t *testing.T) {
	rec := &LockRecord{CommittedRev: 50, CommittedSeq: 100, CommittedTerm: 2}
	for _, c := range []struct {
		name  string
		cand  Fence
		block bool
	}{
		{"at the fence", Fence{Seq: 100, Term: 2}, false},
		{"past the fence", Fence{Seq: 120, Term: 2}, false},
		{"behind in sequence", Fence{Seq: 99, Term: 2}, true},
		{"older term, longer log", Fence{Seq: 150, Term: 1}, true},
		{"newer term", Fence{Seq: 1, Term: 3}, false},
	} {
		if got := rec.Blocks(c.cand); got != c.block {
			t.Errorf("%s: Blocks(%+v) = %v, want %v", c.name, c.cand, got, c.block)
		}
	}
}

// A lock record of an earlier release carries only CommittedRev.
func TestBlocksLegacyRecord(t *testing.T) {
	rec := &LockRecord{CommittedRev: 5}
	if rec.Blocks(Fence{Rev: 5}) || !rec.Blocks(Fence{Rev: 4, Seq: 100, Term: 9}) {
		t.Fatal("a record without positions should fence by revision alone")
	}
}

// Renew records the fence and the object storage flag; a new term starts
// without the flag; Relinquish keeps the later of the recorded and given
// positions.
func TestLockRecordsFenceAndFlag(t *testing.T) {
	store := object.NewMem()
	ctx := context.Background()
	l := newLockShared(store, "node-1", "addr1")
	rec, _, err := l.TryAcquire(ctx, 0, Fence{Rev: 3, Seq: 4, Term: 1})
	if err != nil {
		t.Fatal(err)
	}
	if rec.ObjectStoreComplete || rec.Fence() != (Fence{Rev: 3, Seq: 4, Term: 1}) {
		t.Fatalf("acquired record = %+v", rec)
	}
	_, etag, err := l.ReadETag(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Renew(ctx, rec.Term, "addr1", etag, Fence{Rev: 5, Seq: 7, Term: 2}, true, time.Now(), FastTTL); err != nil {
		t.Fatal(err)
	}
	got, err := l.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !got.ObjectStoreComplete || got.Fence() != (Fence{Rev: 5, Seq: 7, Term: 2}) {
		t.Fatalf("renewed record = %+v", got)
	}

	if err := l.Relinquish(ctx, rec.Term, Fence{Rev: 4, Seq: 9, Term: 1}); err != nil {
		t.Fatal(err)
	}
	if got, err = l.Read(ctx); err != nil {
		t.Fatal(err)
	}
	if got.Fence() != (Fence{Rev: 5, Seq: 7, Term: 2}) {
		t.Fatalf("relinquish lowered the fence: %+v", got.Fence())
	}
}
