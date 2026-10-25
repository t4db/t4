package t4

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestInPlaceRestoreKeepsWaiters checks that a read waiting on a follower for
// a revision is not failed when the follower restores a checkpoint in place:
// the restore closes the old store to release its waiters, and the wait must
// go on against the restored store rather than report the node closed.
func TestInPlaceRestoreKeepsWaiters(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cluster := newFailoverCluster(t, 2)
	leader := cluster.leader(t, 10*time.Second)
	follower := cluster.survivors(leader)[0]

	// Cut the follower off; the leader writes and checkpoints past it.
	cluster.proxies[cluster.indexOf(leader)].block()
	deadline := time.Now().Add(10 * time.Second)
	for !leader.replicationDegraded() {
		if time.Now().After(deadline) {
			t.Fatal("leader did not notice its follower was gone")
		}
		time.Sleep(10 * time.Millisecond)
	}
	for i := range 20 {
		if _, err := leader.Put(ctx, fmt.Sprintf("/k/%03d", i), []byte("v"), 0); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := leader.Flush(); err != nil {
		t.Fatal(err)
	}
	leader.maybeCheckpoint(leader.bgCtx)
	m, err := leader.cp.ReadManifest(ctx, cluster.shared)
	if err != nil || m == nil {
		t.Fatalf("manifest: %v %v", m, err)
	}
	if m.Revision <= follower.db.Load().CurrentRevision() {
		t.Fatalf("setup: follower at rev %d is not behind the checkpoint at rev %d", follower.db.Load().CurrentRevision(), m.Revision)
	}

	waited := make(chan error, 1)
	go func() { waited <- follower.WaitForRevision(ctx, m.Revision) }()
	select {
	case err := <-waited:
		t.Fatalf("WaitForRevision returned (%v) before the follower caught up", err)
	case <-time.After(100 * time.Millisecond):
	}

	if err := follower.resyncFromCheckpoint(follower.bgCtx); err != nil {
		t.Fatalf("resync: %v", err)
	}
	select {
	case err := <-waited:
		if err != nil {
			t.Fatalf("WaitForRevision across an in-place restore: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("WaitForRevision did not return after the restore reached its revision")
	}
}
