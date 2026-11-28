package t4

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/t4db/t4/pkg/object"
)

// TestFollowerWithUnfencedTailResyncsAfterTakeover (#441): follower B applies
// an entry its leader A never got fenced (followers apply on the commit
// message, before the ACK wait). A dies, and C, which lacks the entry, takes
// over and writes different entries at the same sequences. B must not keep
// its entry and continue past C's: C refuses B's stream, and B resyncs from a
// checkpoint of C's term.
func TestFollowerWithUnfencedTailResyncsAfterTakeover(t *testing.T) {
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
			DataDir:             t.TempDir(),
			ObjectStore:         m.store,
			NodeID:              fmt.Sprintf("div-node-%d", i),
			PeerListenAddr:      listen,
			AdvertisePeerAddr:   m.proxy.Addr(),
			FollowerMaxRetries:  2,
			PeerBufferSize:      1000,
			SegmentMaxAge:       time.Hour,
			CheckpointInterval:  2 * time.Second,
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
	var all []*Node
	for _, m := range members {
		all = append(all, m.node)
	}
	leaderNode := waitForLeaderNodeLocal(t, all, 10*time.Second)
	var a, b, c *member
	for _, m := range members {
		switch {
		case m.node == leaderNode:
			a = m
		case b == nil:
			b = m
		default:
			c = m
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	warm, err := a.node.Put(ctx, "/warm", []byte("v"), 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range []*member{b, c} {
		if err := m.node.WaitForRevision(ctx, warm); err != nil {
			t.Fatal(err)
		}
	}

	// C leaves, so A must fence its next write; A cannot, with its object
	// storage gone, but B applies the write anyway.
	if err := c.node.Close(); err != nil {
		t.Fatal(err)
	}
	c.node = nil
	a.store.block()
	putCtx, putCancel := context.WithTimeout(ctx, 2*time.Second)
	_, _ = a.node.Put(putCtx, "/tail", []byte("unfenced"), 0)
	putCancel()
	for {
		if kv, _ := b.node.Get("/tail"); kv != nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("B never applied the unfenced entry")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// A dies. B, which holds the entry, cannot take over without object
	// storage; C comes back and does, without it.
	a.proxy.block()
	b.store.block()
	c.node, err = Open(c.cfg)
	if err != nil {
		t.Fatalf("reopen C: %v", err)
	}
	if got := waitForLeaderNodeLocal(t, []*Node{c.node}, 60*time.Second); got != c.node {
		t.Fatal("C did not take over")
	}
	if kv, _ := c.node.Get("/tail"); kv != nil {
		t.Fatal("C has the unfenced entry; the test did not set up divergence")
	}
	var last int64
	for i := 1; i <= 3; i++ {
		if last, err = c.node.Put(ctx, fmt.Sprintf("/new/%d", i), []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
	}

	// B follows C: it must end up with C's history, not its own entry.
	b.store.unblock()
	for {
		kv, _ := b.node.Get("/new/3")
		tail, _ := b.node.Get("/tail")
		if kv != nil && tail == nil {
			break
		}
		if kv != nil && tail != nil {
			t.Fatal("B applied C's entries on top of its own unfenced entry: replicas diverged")
		}
		if ctx.Err() != nil {
			t.Fatalf("B never converged on C's history (has /new/3: %v, /tail: %v)", kv != nil, tail != nil)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := b.node.WaitForRevision(ctx, last); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"/warm", "/new/1", "/new/2"} {
		if kv, _ := b.node.Get(key); kv == nil {
			t.Errorf("B lacks %s after resync", key)
		}
	}

	// A restarts with the entry still in its local WAL. Object-storage
	// replay must not stack C's entries on it; it rejoins and resyncs.
	_ = a.node.Close()
	a.node = nil
	a.store.unblock()
	a.proxy.unblock()
	if a.node, err = Open(a.cfg); err != nil {
		t.Fatalf("reopen A: %v", err)
	}
	for {
		kv, _ := a.node.Get("/new/3")
		tail, _ := a.node.Get("/tail")
		if kv != nil && tail == nil {
			break
		}
		if kv != nil && tail != nil {
			t.Fatal("restarted A applied C's entries on top of its own unfenced entry")
		}
		if ctx.Err() != nil {
			t.Fatalf("restarted A never converged (has /new/3: %v, /tail: %v)", kv != nil, tail != nil)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
