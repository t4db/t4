package t4

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/t4db/t4/internal/election"
)

// TestTakeoverDefersToBetterNominee: once the leader is gone, candidates
// nominate themselves and the best-placed one goes first. A nominee that
// ranks first but never takes over holds the others back for one stagger, not
// forever; then the best of the rest takes over.
func TestTakeoverDefersToBetterNominee(t *testing.T) {
	cluster := newFailoverCluster(t, 3)
	leader := cluster.leader(t, 10*time.Second)
	leaderIdx := cluster.indexOf(leader)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	rev, err := leader.Put(ctx, "/k", []byte("v"), 0)
	if err != nil {
		t.Fatal(err)
	}
	survivors := cluster.survivors(leader)
	for _, n := range survivors {
		if err := n.WaitForRevision(ctx, rev); err != nil {
			t.Fatal(err)
		}
	}
	// Let a renewal list both followers in the lock.
	lock := election.NewLock(cluster.shared, "observer", "")
	for {
		rec, err := lock.Read(ctx)
		if err == nil && rec != nil && len(rec.Followers) == 2 {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("lock never listed both followers: %+v, %v", rec, err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Crash the leader, and plant a nomination that outranks everyone but
	// will never take over. Survivors nominate once they may take over, a
	// known-follower delay after they last heard the leader; the ghost is
	// stamped then, so that it does not open the election window earlier.
	cluster.proxies[leaderIdx].block()
	cluster.stores[leaderIdx].block()
	ghost := election.NewLock(cluster.shared, "ghost", "")
	ghostAt := time.Now().Add(knownTakeoverDelay).UnixNano()
	if _, ok, err := ghost.Nominate(ctx, election.Nomination{NodeID: "ghost", Seq: 1 << 40, AtNano: ghostAt}, nil); err != nil || !ok {
		t.Fatalf("plant ghost nomination: %v, %v", ok, err)
	}

	// Watch the election: note when both survivors have nominated, and who
	// takes over when.
	var bothAt, tookAt time.Time
	var ranked []election.Nomination
	var winner string
	for winner == "" {
		if ctx.Err() != nil {
			t.Fatal("no takeover")
		}
		rec, err := lock.Read(ctx)
		if err != nil || rec == nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if rec.NodeID != leader.cfg.NodeID {
			winner, tookAt = rec.NodeID, time.Now()
			break
		}
		if bothAt.IsZero() && len(rec.Nominations) == 3 {
			bothAt = time.Now()
			ranked = append([]election.Nomination(nil), rec.Nominations...)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if bothAt.IsZero() {
		t.Fatalf("%s took over before both survivors nominated", winner)
	}
	if waited := tookAt.Sub(bothAt); waited < takeoverRank.Stagger-time.Second {
		t.Fatalf("%s took over %v after the election completed; the ghost ranked first and should have held it back for %v",
			winner, waited, takeoverRank.Stagger)
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].Seq != ranked[j].Seq {
			return ranked[i].Seq > ranked[j].Seq
		}
		return ranked[i].NodeID < ranked[j].NodeID
	})
	if want := ranked[1].NodeID; winner != want {
		t.Fatalf("winner %s, want %s, the best nominee after the ghost (%+v)", winner, want, ranked)
	}
	newLeader := waitForLeaderNodeLocal(t, survivors, 30*time.Second)
	if kv, err := newLeader.Get("/k"); err != nil || kv == nil {
		t.Fatalf("new leader lost the write: %+v, %v", kv, err)
	}
}
