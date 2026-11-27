package t4

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/t4db/t4/internal/testhook"
	"github.com/t4db/t4/pkg/object"
)

// A node that wins the lock keeps it while becomeLeader catches up, however
// long that takes: another follower must not take over and depose it (#426).
func TestSlowPromotionKeepsLock(t *testing.T) {
	shared := object.NewMem()
	nodes := make([]*Node, 3)
	for i := range nodes {
		listen := freeAddrLocal(t)
		n, err := Open(Config{DataDir: t.TempDir(), ObjectStore: shared, NodeID: fmt.Sprintf("promo-%d", i),
			PeerListenAddr: listen, AdvertisePeerAddr: listen})
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = n
		t.Cleanup(func() { _ = n.Close() })
	}
	leader := waitForLeaderNodeLocal(t, nodes, 10*time.Second)

	var once sync.Once
	slow := func() { once.Do(func() { time.Sleep(2 * fastTTL) }) }
	testhook.BeforeLeaderCatchUp.Store(&slow)
	t.Cleanup(func() { testhook.BeforeLeaderCatchUp.Store(nil) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rev, err := leader.Put(ctx, "/before", []byte("v"), 0)
	if err != nil {
		t.Fatal(err)
	}
	var survivors []*Node
	for _, n := range nodes {
		if n != leader {
			// Caught up, so the fence lets either take over.
			if err := n.WaitForRevision(ctx, rev); err != nil {
				t.Fatal(err)
			}
			survivors = append(survivors, n)
		}
	}
	if err := leader.Close(); err != nil {
		t.Fatal(err)
	}
	next := waitForLeaderNodeLocal(t, survivors, 30*time.Second)
	term := next.currentTerm()

	// Without the hold, the other survivor takes over fastTTL into the slow
	// promotion and next steps down. Give it time to.
	time.Sleep(fastTTL)
	for i, n := range survivors {
		if _, err := n.Put(ctx, fmt.Sprintf("/after/%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("put via %s: %v", n.cfg.NodeID, err)
		}
	}
	if !next.IsLeader() || next.currentTerm() != term {
		t.Fatalf("%s lost leadership: leader=%v term %d, was %d", next.cfg.NodeID, next.IsLeader(), next.currentTerm(), term)
	}
}
