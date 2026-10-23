package t4

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// TestLeaderLinearizableReadCoversFollowers checks that a linearizable read
// on the leader is never behind one a follower already served. A follower
// applies a batch once it has acknowledged it, which can be before the
// leader, waiting for that acknowledgement, applies it.
func TestLeaderLinearizableReadCoversFollowers(t *testing.T) {
	cluster := newFailoverCluster(t, 3)
	leader := cluster.leader(t, 15*time.Second)
	followers := cluster.survivors(leader)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wctx, wcancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; wctx.Err() == nil; i++ {
			_, _ = leader.Put(wctx, fmt.Sprintf("/k%d", i%10), []byte("v"), 0)
		}
	}()
	defer func() { wcancel(); <-done }()

	// Up to 2000 rounds, within ctx's budget where fsync is slow.
	stop := time.Now().Add(20 * time.Second)
	for i := 0; i < 2000 && time.Now().Before(stop); i++ {
		seen, err := followers[i%len(followers)].LinearizableRevision(ctx)
		if err != nil {
			t.Fatalf("follower LinearizableRevision: %v", err)
		}
		got, err := leader.LinearizableRevision(ctx)
		if err != nil {
			t.Fatalf("leader LinearizableRevision: %v", err)
		}
		if got < seen {
			t.Fatalf("leader read at revision %d after a follower read at %d", got, seen)
		}
	}
}

// TestLeaderNoopTxnCoversFollowers checks that a txn writing nothing,
// answered by the leader, is never ordered before a read a follower already
// served: the leader must not answer at a revision it has applied while
// followers serve later ones.
func TestLeaderNoopTxnCoversFollowers(t *testing.T) {
	cluster := newFailoverCluster(t, 3)
	leader := cluster.leader(t, 15*time.Second)
	followers := cluster.survivors(leader)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wctx, wcancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; wctx.Err() == nil; i++ {
			_, _ = leader.Put(wctx, fmt.Sprintf("/k%d", i%10), []byte("v"), 0)
		}
	}()
	defer func() { wcancel(); <-done }()

	noop := TxnRequest{
		Conditions: []TxnCondition{{Key: "/absent", Target: TxnCondVersion, Result: TxnCondGreater, Version: 0}},
	}
	// Up to 2000 rounds, within ctx's budget where fsync is slow.
	stop := time.Now().Add(20 * time.Second)
	for i := 0; i < 2000 && time.Now().Before(stop); i++ {
		seen, err := followers[i%len(followers)].LinearizableRevision(ctx)
		if err != nil {
			t.Fatalf("follower LinearizableRevision: %v", err)
		}
		resp, err := leader.Txn(ctx, noop)
		if err != nil {
			t.Fatalf("leader Txn: %v", err)
		}
		if resp.Revision < seen {
			t.Fatalf("leader answered a no-op txn at revision %d after a follower read at %d", resp.Revision, seen)
		}
	}
}

// TestLeaderCompactCoversFollowers checks that compacting to a revision a
// follower already served is never refused as a future revision: the
// revision exists although the leader, waiting for that follower's
// acknowledgement, may not have applied it yet.
func TestLeaderCompactCoversFollowers(t *testing.T) {
	cluster := newFailoverCluster(t, 3)
	leader := cluster.leader(t, 15*time.Second)
	followers := cluster.survivors(leader)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wctx, wcancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; wctx.Err() == nil; i++ {
			_, _ = leader.Put(wctx, fmt.Sprintf("/k%d", i%10), []byte("v"), 0)
		}
	}()
	defer func() { wcancel(); <-done }()

	// Up to 2000 rounds, within ctx's budget where fsync is slow.
	stop := time.Now().Add(20 * time.Second)
	for i := 0; i < 2000 && time.Now().Before(stop); i++ {
		seen, err := followers[i%len(followers)].LinearizableRevision(ctx)
		if err != nil {
			t.Fatalf("follower LinearizableRevision: %v", err)
		}
		if err := leader.Compact(ctx, seen); errors.Is(err, ErrFutureRevision) {
			t.Fatalf("compacting to revision %d, which a follower served, was refused as a future revision", seen)
		} else if err != nil && !errors.Is(err, ErrCompacted) {
			t.Fatalf("Compact(%d): %v", seen, err)
		}
	}
}
