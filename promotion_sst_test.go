package t4

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"

	"github.com/t4db/t4/pkg/object"
)

// TestPromotedFollowerReuploadsGCdSSTs reproduces a restore failure from the
// chaos test: a follower that bootstrapped from a checkpoint holds that
// checkpoint's SSTs and records them as uploaded. The leader compacts them
// away and its checkpoint GC deletes them from the object store. Promoted,
// the follower must not write a checkpoint that references them, or no node
// can restore from it.
func TestPromotedFollowerReuploadsGCdSSTs(t *testing.T) {
	ctx := context.Background()
	store := object.NewMem()
	open := func(id string, opts ...func(*pebble.Options)) *Node {
		t.Helper()
		addr := freeAddrLocal(t)
		async := false
		n, err := Open(Config{
			DataDir:            t.TempDir(),
			ObjectStore:        store,
			NodeID:             id,
			PeerListenAddr:     addr,
			AdvertisePeerAddr:  addr,
			WALSyncUpload:      &async,
			CheckpointInterval: time.Hour, // checkpoints are driven by the test
			PebbleOptions:      opts,
		})
		if err != nil {
			t.Fatalf("Open %s: %v", id, err)
		}
		t.Cleanup(func() { _ = n.Close() })
		return n
	}
	write := func(n *Node, round int) {
		t.Helper()
		for i := range 200 {
			if _, err := n.Put(ctx, fmt.Sprintf("/k/%03d", i), []byte(fmt.Sprintf("v%d-%d", round, i)), 0); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}
	}
	checkpoint := func(n *Node) {
		t.Helper()
		if err := n.Flush(); err != nil {
			t.Fatal(err)
		}
		n.maybeCheckpoint(n.bgCtx)
	}

	// 1. The leader writes and checkpoints.
	a := open("a")
	waitForLeaderNodeLocal(t, []*Node{a}, 10*time.Second)
	write(a, 1)
	checkpoint(a)
	first, err := a.cp.ReadManifest(ctx, store)
	if err != nil || first == nil {
		t.Fatalf("first manifest: %v %v", first, err)
	}
	firstIdx, err := a.cp.ReadCheckpointIndex(ctx, store, first.CheckpointKey)
	if err != nil {
		t.Fatal(err)
	}

	// 2. A follower bootstraps from that checkpoint. Its Pebble does not
	// compact, so it still holds that checkpoint's SSTs when promoted, as a
	// follower promoted soon after bootstrapping does.
	b := open("b", func(o *pebble.Options) { o.DisableAutomaticCompactions = true })
	if b.IsLeader() {
		t.Fatal("b won the election instead of following a")
	}

	// 3. The leader rewrites everything and compacts, so the first
	// checkpoint's SSTs leave its Pebble, then checkpoints until its GC has
	// deleted them from the object store.
	for round := 2; round <= 5; round++ {
		write(a, round)
		if err := a.db.Load().Pebble().Compact([]byte{0}, []byte{0xff, 0xff, 0xff, 0xff}, false); err != nil {
			t.Fatal(err)
		}
		checkpoint(a)
	}
	gone := 0
	for _, key := range firstIdx.SSTFiles {
		if _, err := store.Get(ctx, key); errors.Is(err, object.ErrNotFound) {
			gone++
		}
	}
	if gone == 0 {
		t.Fatal("setup: the leader's GC deleted none of the first checkpoint's SSTs")
	}
	held := 0
	for name, key := range b.sstUploader.Registry() {
		if _, err := store.Get(ctx, key); errors.Is(err, object.ErrNotFound) {
			if _, err := os.Stat(filepath.Join(b.cfg.DataDir, "db", name)); err == nil {
				held++
			}
		}
	}
	if held == 0 {
		t.Fatal("setup: the follower holds none of the deleted SSTs")
	}

	// 4. The leader shuts down gracefully; the follower takes over and
	// checkpoints.
	termA := a.term
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	waitForLeaderNodeLocal(t, []*Node{b}, 20*time.Second)
	deadline := time.Now().Add(20 * time.Second)
	for {
		m, err := b.cp.ReadManifest(ctx, store)
		if err == nil && m != nil && m.Term > termA {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the promoted follower wrote no checkpoint (manifest %v, err %v)", m, err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// 5. A fresh node restores from the object store.
	c := open("c")
	kv, err := c.Get("/k/000")
	if err != nil || kv == nil || string(kv.Value) != "v5-0" {
		t.Fatalf("fresh node read /k/000 = %v, %v; want v5-0", kv, err)
	}
}
