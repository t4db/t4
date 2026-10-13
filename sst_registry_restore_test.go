package t4

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestInPlaceRestoreForgetsReplacedSSTs reproduces a restore failure from the
// chaos test. A node records the SSTs it uploaded by file name, and Pebble
// instances name tables independently, so two diverged copies of one
// database reuse names for different tables. A follower that restores a
// checkpoint in place must forget the tables it had: a checkpoint it writes
// later would otherwise point a name at the old table's content, and Pebble
// refuses to open a restore of it.
func TestInPlaceRestoreForgetsReplacedSSTs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cluster := newFailoverCluster(t, 2)
	leader := cluster.leader(t, 10*time.Second)
	follower := cluster.survivors(leader)[0]

	// 1. Cut the follower off; the leader writes and checkpoints past it.
	cluster.proxies[cluster.indexOf(leader)].block()
	deadline := time.Now().Add(10 * time.Second)
	for !leader.replicationDegraded() {
		if time.Now().After(deadline) {
			t.Fatal("leader did not notice its follower was gone")
		}
		time.Sleep(10 * time.Millisecond)
	}
	for i := range 200 {
		if _, err := leader.Put(ctx, fmt.Sprintf("/k/%03d", i), []byte(fmt.Sprintf("v%d", i)), 0); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := leader.Flush(); err != nil {
		t.Fatal(err)
	}
	leader.maybeCheckpoint(leader.bgCtx)
	m, err := leader.cp.ReadManifest(ctx, cluster.shared)
	if err != nil || m == nil {
		t.Fatalf("manifest: %v %v", m, err)
	}
	if m.LastSequence <= follower.db.Load().LastSequence() {
		t.Fatalf("setup: follower at seq %d is not behind the checkpoint at seq %d", follower.db.Load().LastSequence(), m.LastSequence)
	}
	idx, err := leader.cp.ReadCheckpointIndex(ctx, cluster.shared, m.CheckpointKey)
	if err != nil {
		t.Fatal(err)
	}

	// 2. The follower uploaded a table of its own under the name of one of
	// the checkpoint's tables.
	pebbleDir := filepath.Join(follower.cfg.DataDir, "db")
	var name string
	for _, key := range idx.SSTFiles {
		if _, err := os.Stat(filepath.Join(pebbleDir, filepath.Base(key))); errors.Is(err, os.ErrNotExist) {
			name = filepath.Base(key)
			break
		}
	}
	if name == "" {
		t.Fatal("setup: the follower already has every table of the checkpoint")
	}
	own := filepath.Join(pebbleDir, name)
	if err := os.WriteFile(own, []byte("the follower's own table"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := follower.sstUploader.Reconcile(ctx, []string{name}); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(own); err != nil {
		t.Fatal(err)
	}

	// 3. The follower restores the checkpoint in place.
	if err := follower.resyncFromCheckpoint(follower.bgCtx); err != nil {
		t.Fatalf("resync: %v", err)
	}
	if got := follower.db.Load().LastSequence(); got < m.LastSequence {
		t.Fatalf("follower at seq %d after resync, want %d", got, m.LastSequence)
	}

	// Every table the follower records must be the one on its disk now.
	for name, key := range follower.sstUploader.Registry() {
		data, err := os.ReadFile(filepath.Join(pebbleDir, name))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		if want := "sst/" + hex.EncodeToString(sum[:8]) + "/" + name; key != want {
			t.Errorf("registry maps %s to %s, but the table on disk is %s", name, key, want)
		}
	}
}
