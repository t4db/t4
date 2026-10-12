package t4

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestCompactRefusesWhatEtcdRefuses checks that a compaction etcd refuses is
// refused, and changes nothing: one to a revision not reached yet, which would
// leave later writes below the compaction revision, and one at or below the
// revision the store is already compacted to.
func TestCompactRefusesWhatEtcdRefuses(t *testing.T) {
	n, err := Open(Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	ctx := context.Background()
	var rev int64
	for range 5 {
		if rev, err = n.Put(ctx, "k", []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
	}

	if err := n.Compact(ctx, rev+3); !errors.Is(err, ErrFutureRevision) {
		t.Fatalf("Compact(%d) at revision %d = %v, want ErrFutureRevision", rev+3, rev, err)
	}
	if got := n.CompactRevision(); got != 0 {
		t.Fatalf("CompactRevision after a refused compaction = %d, want 0", got)
	}

	if err := n.Compact(ctx, rev-1); err != nil {
		t.Fatalf("Compact(%d): %v", rev-1, err)
	}
	for _, r := range []int64{rev - 1, rev - 2} {
		if err := n.Compact(ctx, r); !errors.Is(err, ErrCompacted) {
			t.Fatalf("Compact(%d) after compacting to %d = %v, want ErrCompacted", r, rev-1, err)
		}
	}
	if got := n.CompactRevision(); got != rev-1 {
		t.Fatalf("CompactRevision = %d, want %d", got, rev-1)
	}
	if err := n.Compact(ctx, rev); err != nil {
		t.Fatalf("Compact(%d) at the current revision: %v", rev, err)
	}
}

// TestCompactThroughFollowerKeepsErrors checks that a compaction a follower
// forwards to the leader is refused with the same errors.
func TestCompactThroughFollowerKeepsErrors(t *testing.T) {
	cluster := newFailoverCluster(t, 2)
	leader := cluster.leader(t, 10*time.Second)
	follower := cluster.survivors(leader)[0]
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rev, err := leader.Put(ctx, "k", []byte("v"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := follower.Compact(ctx, rev+3); !errors.Is(err, ErrFutureRevision) {
		t.Fatalf("Compact(%d) through the follower = %v, want ErrFutureRevision", rev+3, err)
	}
	if err := follower.Compact(ctx, rev); err != nil {
		t.Fatalf("Compact(%d) through the follower: %v", rev, err)
	}
	if err := follower.Compact(ctx, rev); !errors.Is(err, ErrCompacted) {
		t.Fatalf("Compact(%d) again through the follower = %v, want ErrCompacted", rev, err)
	}
}
