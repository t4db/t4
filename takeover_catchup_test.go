package t4

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/t4db/t4/internal/election"
)

// TestTakeoverCatchesUpFromObjectStorage covers a leader that dies after
// committing a write its followers never received: with no follower
// connected, the leader made the write durable in object storage and
// recorded it as the election fence (the lock's committed revision). The
// followers are behind that fence, so none may take over as it is. Following
// the dead leader to catch up cannot succeed; they must catch up from object
// storage and then take over, or the cluster stays without a leader.
func TestTakeoverCatchesUpFromObjectStorage(t *testing.T) {
	// "followers behind" also needs every earlier WAL segment in object
	// storage: the followers miss the warm-up write too, and it was sealed
	// and queued for upload before the write that went to object storage.
	for _, tc := range []struct {
		name          string
		followersHave bool
	}{
		{"followers current", true},
		{"followers behind", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Rarely the cut-off leader is deposed before the write commits
			// (its liveness touch waits behind the write, which waits on a
			// follower whose broken stream is not yet detected). Nothing is
			// committed then, so the scenario was not reached: retry it.
			for attempt := 1; ; attempt++ {
				if testTakeoverCatchUp(t, tc.followersHave) {
					return
				}
				if attempt == 3 {
					t.Fatal("leader was deposed before the write committed in every attempt")
				}
				t.Logf("attempt %d: leader deposed before the write committed; retrying", attempt)
			}
		})
	}
}

// testTakeoverCatchUp runs the scenario on a fresh cluster. It returns false
// if the leader lost leadership before the write under test committed.
func testTakeoverCatchUp(t *testing.T, followersHaveWarmup bool) bool {
	cluster := newFailoverCluster(t, 3)
	leader := cluster.leader(t, 10*time.Second)
	leaderIdx := cluster.indexOf(leader)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	warm, err := leader.Put(ctx, "/warmup", []byte("v"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if followersHaveWarmup {
		for _, n := range cluster.survivors(leader) {
			if err := n.WaitForRevision(ctx, warm); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Cut the followers off and wait until the leader has noticed: with too
	// few followers to acknowledge, it makes each write durable in object
	// storage before acknowledging it. (A write acknowledged while the
	// followers were disconnecting lives only in the leader's local WAL; then
	// the survivors must wait for that leader to return, which is a
	// different case.)
	cluster.proxies[leaderIdx].block()
	deadline := time.Now().Add(10 * time.Second)
	for !leader.replicationDegraded() {
		if time.Now().After(deadline) {
			t.Fatal("leader did not notice its followers were gone")
		}
		time.Sleep(10 * time.Millisecond)
	}
	rev, err := leader.Put(ctx, "/committed", []byte("v"), 0)
	if errors.Is(err, ErrClosed) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}

	// Wait until the lock records the write as committed: the followers are
	// now behind the election fence.
	lock := election.NewLock(cluster.shared, "observer", "")
	deadline = time.Now().Add(10 * time.Second)
	for {
		rec, err := lock.Read(ctx)
		if err == nil && rec != nil && rec.CommittedRev >= rev {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lock never recorded committed revision %d (%+v, %v)", rev, rec, err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Crash the leader: no graceful shutdown, no object storage access.
	cluster.stores[leaderIdx].block()

	newLeader := waitForLeaderNodeLocal(t, cluster.survivors(leader), 30*time.Second)
	if kv, err := newLeader.Get("/committed"); err != nil || kv == nil {
		t.Fatalf("new leader lost the committed write: %+v, %v", kv, err)
	}
	return true
}
