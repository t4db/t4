package t4

import (
	"context"
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

	for i := range 2000 {
		seen, err := linearizableRevision(ctx, followers[i%len(followers)])
		if err != nil {
			t.Fatalf("follower LinearizableRevision: %v", err)
		}
		got, err := linearizableRevision(ctx, leader)
		if err != nil {
			t.Fatalf("leader LinearizableRevision: %v", err)
		}
		if got < seen {
			t.Fatalf("leader read at revision %d after a follower read at %d", got, seen)
		}
	}
}

// linearizableRevision returns a revision a linearizable read on n is served
// at: n's revision once the read has synced.
func linearizableRevision(ctx context.Context, n *Node) (int64, error) {
	if _, err := n.LinearizableGet(ctx, "/absent"); err != nil {
		return 0, err
	}
	return n.CurrentRevision(), nil
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
	for i := range 2000 {
		seen, err := linearizableRevision(ctx, followers[i%len(followers)])
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
