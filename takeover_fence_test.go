package t4

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/t4db/t4/internal/wal"
	"github.com/t4db/t4/pkg/object"
)

// TestTakeoverWaitsForNodeWithAcknowledgedWrites: F1 falls behind while the
// leader acknowledges writes on F2's acks and uploads some of them. Then the
// leader dies and F2 loses object storage. F1 can catch up from object storage
// past the lock's fence, but still lacks acknowledged writes that only F2
// holds, so it must not take over. Once F2 reaches object storage again, it
// takes over with every write (docs/design/takeover-fence.md).
func TestTakeoverWaitsForNodeWithAcknowledgedWrites(t *testing.T) {
	shared := object.NewMem()
	type member struct {
		cfg   Config
		node  *Node
		store *gatedStore
		proxy *blockableProxyLocal
	}
	members := make([]*member, 3)
	for i := range members {
		listen := freeAddrLocal(t)
		m := &member{proxy: newBlockableProxyLocal(t, listen), store: newGatedStore(shared)}
		m.cfg = Config{
			DataDir:            t.TempDir(),
			ObjectStore:        m.store,
			NodeID:             fmt.Sprintf("fence-node-%d", i),
			PeerListenAddr:     listen,
			AdvertisePeerAddr:  m.proxy.Addr(),
			FollowerMaxRetries: 2,
			PeerBufferSize:     1000,
			SegmentMaxAge:      time.Hour,
			CheckpointInterval: time.Hour,
			// The shortest slow lease, so that F1, which the leader does
			// not know after restarting, may try to take over in the window
			// below rather than a minute later.
			LeaderWatchInterval: minLeaderWatchInterval,
		}
		n, err := Open(m.cfg)
		if err != nil {
			t.Fatalf("open node %d: %v", i, err)
		}
		m.node = n
		members[i] = m
	}
	t.Cleanup(func() {
		for _, m := range members {
			if m.node != nil {
				_ = m.node.Close()
			}
		}
	})
	nodes := func() []*Node {
		var out []*Node
		for _, m := range members {
			if m.node != nil {
				out = append(out, m.node)
			}
		}
		return out
	}
	leaderNode := waitForLeaderNodeLocal(t, nodes(), 10*time.Second)
	var leader, f1, f2 *member
	for _, m := range members {
		switch {
		case m.node == leaderNode:
			leader = m
		case f1 == nil:
			f1 = m
		default:
			f2 = m
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	put := func(key string) int64 {
		t.Helper()
		rev, err := leader.node.Put(ctx, key, []byte("v"), 0)
		if err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
		return rev
	}

	warm := put("/warm")
	for _, m := range []*member{f1, f2} {
		if err := m.node.WaitForRevision(ctx, warm); err != nil {
			t.Fatal(err)
		}
	}

	// F1 leaves; the leader goes on with F2's acks.
	if err := f1.node.Close(); err != nil {
		t.Fatal(err)
	}
	f1.node = nil
	for i := 1; i <= 5; i++ {
		put(fmt.Sprintf("/uploaded/%d", i))
	}
	// Upload what the leader has so far, and wait until it is there.
	before, err := shared.List(ctx, "wal/")
	if err != nil {
		t.Fatal(err)
	}
	w, ok := leader.node.wal.(*wal.WAL)
	if !ok {
		t.Fatalf("unexpected WAL type %T", leader.node.wal)
	}
	if err := w.SealAndFlush(leader.node.db.Load().LastSequence() + 1); err != nil {
		t.Fatal(err)
	}
	for {
		keys, err := shared.List(ctx, "wal/")
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) > len(before) {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("upload never reached object storage")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Acknowledged on F2's acks, not uploaded.
	var last int64
	for i := 1; i <= 3; i++ {
		last = put(fmt.Sprintf("/acked/%d", i))
	}
	if err := f2.node.WaitForRevision(ctx, last); err != nil {
		t.Fatal(err)
	}

	// The leader dies; F2 loses object storage; F1 comes back.
	leader.proxy.block()
	leader.store.block()
	f2.store.block()
	f1.node, err = Open(f1.cfg)
	if err != nil {
		t.Fatalf("reopen F1: %v", err)
	}

	// F1 may catch up from object storage to /uploaded/5, past the fence,
	// but lacks /acked/*: it must not take over. The leader's lease ends
	// within seconds, so F1 tries within this window.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if f1.node.IsLeader() {
			kv, _ := f1.node.Get("/acked/3")
			t.Fatalf("F1 took over while lacking acknowledged writes held by F2 (has /acked/3: %v)", kv != nil)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// F2 reaches object storage again and takes over with every write.
	f2.store.unblock()
	newLeader := waitForLeaderNodeLocal(t, []*Node{f1.node, f2.node}, 60*time.Second)
	if newLeader != f2.node {
		t.Fatalf("new leader %s, want F2 (%s)", newLeader.cfg.NodeID, f2.cfg.NodeID)
	}
	if kv, err := newLeader.Get("/acked/3"); err != nil || kv == nil {
		t.Fatalf("new leader lost an acknowledged write: %+v, %v", kv, err)
	}
}
