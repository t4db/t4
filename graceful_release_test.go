package t4

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/t4db/t4/internal/election"
	"github.com/t4db/t4/internal/peer"
	"github.com/t4db/t4/pkg/object"
)

// TestGracefulShutdownReleasesLock: a leader that shuts down gracefully has
// stopped serving, so its lock must not make anyone wait out the liveness
// TTL, including followers that never received its shutdown broadcast.
func TestGracefulShutdownReleasesLock(t *testing.T) {
	cluster := newFailoverCluster(t, 3)
	leader := cluster.leader(t, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rev, err := leader.Put(ctx, "/k", []byte("v"), 0)
	if err != nil {
		t.Fatal(err)
	}
	candidate := cluster.survivors(leader)[0]
	if err := candidate.WaitForRevision(ctx, rev); err != nil {
		t.Fatal(err)
	}

	// The followers miss the shutdown broadcast.
	cluster.proxies[cluster.indexOf(leader)].block()
	if err := leader.Close(); err != nil {
		t.Fatal(err)
	}
	lock := election.NewLock(cluster.shared, candidate.cfg.NodeID, candidate.cfg.AdvertisePeerAddr)
	rec, err := lock.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rec == nil || rec.LastSeenNano != 0 {
		t.Fatalf("lock after graceful shutdown = %+v, want it released (LastSeenNano 0)", rec)
	}
	if rec.CommittedRev < rev {
		t.Fatalf("released lock lost the election fence: CommittedRev=%d, want >= %d", rec.CommittedRev, rev)
	}
	// A follower that missed the broadcast takes the non-graceful path.
	if _, promoted := candidate.attemptPromotion(candidate.bgCtx, lock, false); !promoted {
		t.Fatal("follower could not take over at once from a leader that shut down gracefully")
	}
}

// TestGracefulShutdownKeepsNewerLeadersLock: a leader that has already been
// replaced must not write its own record over the new leader's lock when it
// shuts down; followers would chase its dead address and the new leader's
// next conditional renewal would fail.
func TestGracefulShutdownKeepsNewerLeadersLock(t *testing.T) {
	cluster := newFailoverCluster(t, 3)
	leader := cluster.leader(t, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rev, err := leader.Put(ctx, "/k", []byte("v"), 0)
	if err != nil {
		t.Fatal(err)
	}
	candidate := cluster.survivors(leader)[0]
	if err := candidate.WaitForRevision(ctx, rev); err != nil {
		t.Fatal(err)
	}

	// A node outside the test cluster takes over, so that the survivors,
	// which react to the shutdown broadcast, cannot win the lock back and
	// hide an overwrite.
	lock := election.NewLock(cluster.shared, "newer-leader", "127.0.0.1:1")
	newRec, won, err := lock.TakeOver(ctx, candidate.currentTerm(), candidate.db.Load().CurrentRevision(), nil)
	if err != nil || !won {
		t.Fatalf("precondition: takeover won=%v err=%v", won, err)
	}
	if err := leader.Close(); err != nil {
		t.Fatal(err)
	}
	rec, err := lock.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rec == nil || rec.NodeID != newRec.NodeID || rec.Term != newRec.Term {
		t.Fatalf("replaced leader's shutdown overwrote the lock: have %+v, want owner %s term %d", rec, newRec.NodeID, newRec.Term)
	}
}

// TestFollowerTakesOverPromptlyFromReleasedLock: a follower that missed a
// graceful shutdown broadcast learns from the released lock that the leader
// is gone, instead of retrying the dead address FollowerMaxRetries times.
func TestFollowerTakesOverPromptlyFromReleasedLock(t *testing.T) {
	shared := object.NewMem()
	nodes := make([]*Node, 3)
	proxies := make([]*blockableProxyLocal, 3)
	for i := range nodes {
		listen := freeAddrLocal(t)
		proxies[i] = newBlockableProxyLocal(t, listen)
		n, err := Open(Config{
			DataDir:           t.TempDir(),
			ObjectStore:       shared,
			NodeID:            fmt.Sprintf("release-node-%d", i),
			PeerListenAddr:    listen,
			AdvertisePeerAddr: proxies[i].Addr(),
			// FollowerMaxRetries left at its default of 5.
		})
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = n
		t.Cleanup(func() { _ = n.Close() })
	}
	leader := waitForLeaderNodeLocal(t, nodes, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rev, err := leader.Put(ctx, "/k", []byte("v"), 0)
	if err != nil {
		t.Fatal(err)
	}
	var survivors []*Node
	leaderIdx := 0
	for i, n := range nodes {
		if n == leader {
			leaderIdx = i
			continue
		}
		if err := n.WaitForRevision(ctx, rev); err != nil {
			t.Fatal(err)
		}
		survivors = append(survivors, n)
	}
	proxies[leaderIdx].block() // the followers miss the shutdown broadcast

	start := time.Now()
	if err := leader.Close(); err != nil {
		t.Fatal(err)
	}
	waitForLeaderNodeLocal(t, survivors, 30*time.Second)
	elapsed := time.Since(start)
	t.Logf("new leader after %v", elapsed.Round(time.Millisecond))
	if limit := 2*peer.FollowerRetryInterval + time.Second; elapsed > limit {
		t.Fatalf("takeover from a released lock took %v, want under %v", elapsed.Round(time.Millisecond), limit)
	}
}
