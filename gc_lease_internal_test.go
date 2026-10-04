package t4

import (
	"context"
	"testing"
	"time"
)

// TestGCContextFollowsLease pins that a leader runs object-store GC only
// while its lease shows no other node can have taken over, and only until
// the lease ends: a superseded leader would delete checkpoints and SSTs the
// new leader's checkpoints rely on. A single node has no rival.
func TestGCContextFollowsLease(t *testing.T) {
	ctx := context.Background()

	single := &Node{}
	single.storeRole(roleSingle)
	if _, cancel, ok := single.gcContext(ctx); !ok {
		t.Error("single node may not GC")
	} else {
		cancel()
	}

	leader := &Node{}
	leader.storeRole(roleLeader)
	if _, _, ok := leader.gcContext(ctx); ok {
		t.Error("leader without a lease may GC")
	}

	leader.extendLease(time.Now().Add(-time.Hour), false)
	if _, _, ok := leader.gcContext(ctx); ok {
		t.Error("leader with an expired lease may GC")
	}

	start := time.Now()
	leader.extendLease(start, false)
	gcCtx, cancel, ok := leader.gcContext(ctx)
	if !ok {
		t.Fatal("leader with a fresh lease may not GC")
	}
	defer cancel()
	deadline, has := gcCtx.Deadline()
	if want := leader.leaseDeadline(start); !has || deadline.After(want) {
		t.Errorf("GC deadline %v, want at most the lease deadline %v", deadline, want)
	}
}
