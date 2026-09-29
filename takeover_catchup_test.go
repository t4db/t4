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
			// follower whose broken stream is not yet detected), or the
			// followers are not behind the fence. The scenario was not
			// reached then: retry it.
			for attempt := 1; ; attempt++ {
				if testTakeoverCatchUp(t, tc.followersHave) {
					return
				}
				if attempt == 3 {
					t.Fatal("scenario not reached in any attempt")
				}
				t.Logf("attempt %d: scenario not reached; retrying", attempt)
			}
		})
	}
}

// testTakeoverCatchUp runs the scenario on a fresh cluster. It returns false
// if the scenario was not reached: the leader lost leadership before the write
// under test committed, or the followers were not behind the fence.
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

	// Wait until the followers, all lacking the write, are behind the
	// election fence, judged by what they received from a leader. The fence
	// need not move for this write if an earlier one already blocks them.
	lock := election.NewLock(cluster.shared, "observer", "")
	survivors := cluster.survivors(leader)
	fenced := func(rec *election.LockRecord) bool {
		for _, n := range survivors {
			if !rec.Blocks(n.leaderKnownFence()) {
				return false
			}
		}
		return true
	}
	deadline = time.Now().Add(10 * time.Second)
	for {
		rec, err := lock.Read(ctx)
		if err == nil && rec != nil && fenced(rec) {
			break
		}
		if time.Now().After(deadline) {
			t.Logf("followers lacking revision %d not behind the fence (%+v, %v)", rev, rec, err)
			return false
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Crash the leader: no graceful shutdown, no object storage access.
	cluster.stores[leaderIdx].block()

	// A leader none of whose followers connected renews in slow mode, and
	// followers it never heard from wait out that lease (3 × the default
	// 20 s LeaderWatchInterval) before they may take over.
	newLeader := waitForLeaderNodeLocal(t, cluster.survivors(leader), 70*time.Second)
	if kv, err := newLeader.Get("/committed"); err != nil || kv == nil {
		t.Fatalf("new leader lost the committed write: %+v, %v", kv, err)
	}
	return true
}

// TestTakeoverFencesMetaWrites covers a leader that dies after acknowledging
// a meta write its followers never received. A meta write does not advance
// the revision, so a revision fence cannot tell a follower that lacks it
// from one that has it: the fence has to cover sequences too, or a follower
// takes over without catching up and the acknowledged write is lost.
func TestTakeoverFencesMetaWrites(t *testing.T) {
	for attempt := 1; ; attempt++ {
		if testTakeoverFencesMetaWrite(t) {
			return
		}
		if attempt == 3 {
			t.Fatal("leader was deposed before the meta write committed in every attempt")
		}
		t.Logf("attempt %d: leader deposed before the meta write committed; retrying", attempt)
	}
}

func testTakeoverFencesMetaWrite(t *testing.T) bool {
	cluster := newFailoverCluster(t, 3)
	leader := cluster.leader(t, 10*time.Second)
	leaderIdx := cluster.indexOf(leader)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if on, err := leader.MetaEnabled(); err != nil || !on {
		t.Fatalf("meta keyspace not enabled: %v, %v", on, err)
	}

	// Followers hold every data write, so the revision fence cannot block
	// them: only the meta write below sets them apart.
	warm, err := leader.Put(ctx, "/warmup", []byte("v"), 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range cluster.survivors(leader) {
		if err := n.WaitForRevision(ctx, warm); err != nil {
			t.Fatal(err)
		}
	}

	cluster.proxies[leaderIdx].block()
	deadline := time.Now().Add(10 * time.Second)
	for !leader.replicationDegraded() {
		if time.Now().After(deadline) {
			t.Fatal("leader did not notice its followers were gone")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Acknowledged only once durable in object storage and fenced.
	err = leader.MetaPut(ctx, "fenced", []byte("v"))
	if errors.Is(err, ErrClosed) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}

	// The fence is written before the write is acknowledged, and every
	// survivor lacks the write: the lock must block each of them.
	rec, err := election.NewLock(cluster.shared, "observer", "").Read(ctx)
	if err != nil || rec == nil {
		t.Fatalf("read lock: %+v, %v", rec, err)
	}
	for _, n := range cluster.survivors(leader) {
		if f := n.leaderKnownFence(); !rec.Blocks(f) {
			t.Fatalf("lock fence %+v does not block %s at %+v, which lacks an acknowledged meta write", rec.Fence(), n.cfg.NodeID, f)
		}
	}

	// Crash the leader: no graceful shutdown, no object storage access.
	cluster.stores[leaderIdx].block()

	newLeader := waitForLeaderNodeLocal(t, cluster.survivors(leader), 30*time.Second)
	if _, ok, err := newLeader.MetaGet("fenced"); err != nil || !ok {
		t.Fatalf("new leader lost the acknowledged meta write: ok=%v, %v", ok, err)
	}
	return true
}
