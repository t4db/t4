package t4

import (
	"context"
	"testing"
	"time"

	"github.com/t4db/t4/internal/election"
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
	newRec, won, err := lock.TakeOver(ctx, candidate.currentTerm(), candidate.db.Load().CurrentRevision())
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
