package t4

import (
	"context"
	"testing"
	"time"
)

// TestLeaderStepsDownWithoutObjectStorage covers a leader that can no longer
// reach object storage while its followers can. It cannot renew its lock, so
// its lease ends and it stops serving. Its followers still hear it and would
// never take over, leaving the cluster without a leader for as long as the
// outage lasts. The leader must step down instead, so that a follower takes
// over.
func TestLeaderStepsDownWithoutObjectStorage(t *testing.T) {
	cluster := newFailoverCluster(t, 2)
	leader := cluster.leader(t, 10*time.Second)
	follower := cluster.survivors(leader)[0]
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if _, err := leader.Put(ctx, "/before", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}

	cut := time.Now()
	cluster.stores[cluster.indexOf(leader)].block()

	select {
	case <-leader.Done():
		t.Logf("leader stopped after %v: %v", time.Since(cut), leader.Err())
	case <-ctx.Done():
		t.Fatal("the leader cut off from object storage never stepped down")
	}
	newLeader := waitForLeaderNodeLocal(t, []*Node{follower}, 60*time.Second)
	t.Logf("follower leads after %v", time.Since(cut))
	if _, err := newLeader.Put(ctx, "/after", []byte("v"), 0); err != nil {
		t.Fatalf("write after failover: %v", err)
	}
	if kv, err := newLeader.Get("/before"); err != nil || kv == nil {
		t.Fatalf("new leader lost /before: %v, %v", kv, err)
	}
}
