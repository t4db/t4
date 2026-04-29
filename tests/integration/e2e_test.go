package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/t4db/t4"
	"github.com/t4db/t4/internal/checkpoint"
	"github.com/t4db/t4/pkg/object"
)

// snapshotStore wraps MemStore, recording a version ID for every Put so a
// RestorePoint can be constructed from a captured moment in time.
type snapshotStore struct {
	object.Store
	mu          sync.Mutex
	byVer       map[string][]byte
	latestByKey map[string]string
	seq         int
}

func newSnapshotStore() *snapshotStore {
	return &snapshotStore{
		Store:       object.NewMem(),
		byVer:       make(map[string][]byte),
		latestByKey: make(map[string]string),
	}
}

func (s *snapshotStore) Put(ctx context.Context, key string, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if err := s.Store.Put(ctx, key, bytes.NewReader(data)); err != nil {
		return err
	}
	s.mu.Lock()
	s.seq++
	vid := fmt.Sprintf("ver%d", s.seq)
	s.byVer[vid] = data
	s.latestByKey[key] = vid
	s.mu.Unlock()
	return nil
}

func (s *snapshotStore) GetVersioned(_ context.Context, _, versionID string) (io.ReadCloser, error) {
	s.mu.Lock()
	data, ok := s.byVer[versionID]
	s.mu.Unlock()
	if !ok {
		return nil, object.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

// versionOf returns the current version ID for key, or "" if not yet written.
func (s *snapshotStore) versionOf(key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.latestByKey[key]
}

// freeAddrImpl allocates a random TCP port and releases it.
func freeAddrImpl(t testing.TB) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freeAddr: %v", err)
	}
	addr := lis.Addr().String()
	lis.Close()
	return addr
}

// waitForLeaderNode polls until one of the nodes reports IsLeader.
func waitForLeaderNode(t *testing.T, nodes []*t4.Node, timeout time.Duration) *t4.Node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.IsLeader() {
				return n
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no leader elected within timeout")
	return nil
}

// ── Offline (no S3) ───────────────────────────────────────────────────────────

// TestE2EOffline verifies basic CRUD on a single node with no object store.
func TestE2EOffline(t *testing.T) {
	n, err := t4.Open(t4.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { n.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Put + Get.
	rev, err := n.Put(ctx, "/e2e/k", []byte("hello"), 0)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	kv, err := n.Get("/e2e/k")
	if err != nil || kv == nil || string(kv.Value) != "hello" {
		t.Fatalf("Get: err=%v kv=%v", err, kv)
	}

	// Prefix list.
	for i := 0; i < 5; i++ {
		n.Put(ctx, fmt.Sprintf("/e2e/list/%d", i), []byte("v"), 0)
	}
	kvs, err := n.List("/e2e/list/")
	if err != nil || len(kvs) != 5 {
		t.Fatalf("List: err=%v len=%d", err, len(kvs))
	}

	// Create (create-if-not-exists).
	_, err = n.Create(ctx, "/e2e/new", []byte("created"), 0)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, err = n.Create(ctx, "/e2e/new", []byte("dup"), 0)
	if err != t4.ErrKeyExists {
		t.Errorf("second Create: want ErrKeyExists, got %v", err)
	}

	// CAS update.
	_, _, updated, err := n.Update(ctx, "/e2e/k", []byte("updated"), rev, 0)
	if err != nil || !updated {
		t.Fatalf("Update: err=%v updated=%v", err, updated)
	}

	// Delete.
	delRev, err := n.Delete(ctx, "/e2e/k")
	if err != nil || delRev == 0 {
		t.Fatalf("Delete: err=%v rev=%d", err, delRev)
	}
	kv, _ = n.Get("/e2e/k")
	if kv != nil {
		t.Error("key should be gone after Delete")
	}

	// Watch.
	watchCtx, wcancel := context.WithTimeout(ctx, 5*time.Second)
	defer wcancel()
	ch, err := n.Watch(watchCtx, "/e2e/watch/", 0)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	go func() { n.Put(watchCtx, "/e2e/watch/key", []byte("event"), 0) }()
	select {
	case ev := <-ch:
		if ev.KV.Key != "/e2e/watch/key" {
			t.Errorf("watch event key: want /e2e/watch/key got %q", ev.KV.Key)
		}
	case <-watchCtx.Done():
		t.Error("timeout waiting for watch event")
	}
}

// ── Single node + fake S3 ────────────────────────────────────────────────────

// TestE2ESingleNodeS3 verifies that data written before a restart is recovered
// from the in-memory object store (simulating S3 durability).
func TestE2ESingleNodeS3(t *testing.T) {
	store := object.NewMem()
	dir := t.TempDir()

	// First run: write data, then close.
	func() {
		n, err := t4.Open(t4.Config{
			DataDir:            dir,
			ObjectStore:        store,
			CheckpointInterval: 24 * time.Hour, // disable auto-checkpoint
		})
		if err != nil {
			t.Fatalf("first open: %v", err)
		}
		ctx := context.Background()
		for i := 0; i < 20; i++ {
			if _, err := n.Put(ctx, fmt.Sprintf("/persist/%d", i), []byte(fmt.Sprintf("v%d", i)), 0); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}
		n.Close()
	}()

	// Second run: reopen and verify all keys survived.
	n, err := t4.Open(t4.Config{DataDir: dir, ObjectStore: store})
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	defer n.Close()

	kvs, err := n.List("/persist/")
	if err != nil {
		t.Fatalf("List after restart: %v", err)
	}
	if len(kvs) != 20 {
		t.Errorf("after restart: want 20 keys got %d", len(kvs))
	}
	for i := 0; i < 20; i++ {
		kv, err := n.Get(fmt.Sprintf("/persist/%d", i))
		if err != nil || kv == nil {
			t.Errorf("key %d missing after restart: err=%v", i, err)
		} else if string(kv.Value) != fmt.Sprintf("v%d", i) {
			t.Errorf("key %d value: want v%d got %q", i, i, kv.Value)
		}
	}
}

// ── 3-node cluster + fake S3 ─────────────────────────────────────────────────

// TestE2EThreeNode verifies:
//   - Leader election across 3 nodes sharing a MemStore.
//   - Write replication to followers via WaitForRevision.
//   - Write forwarding from follower to leader.
//   - Leader failover: close the leader, a follower takes over.
func TestE2EThreeNode(t *testing.T) {
	store := object.NewMem()
	const count = 3

	nodes := make([]*t4.Node, count)

	for i := 0; i < count; i++ {
		peerAddr := freeAddrImpl(t)
		node, err := t4.Open(t4.Config{
			DataDir:            t.TempDir(),
			ObjectStore:        store,
			NodeID:             fmt.Sprintf("node-%d", i),
			PeerListenAddr:     peerAddr,
			AdvertisePeerAddr:  peerAddr,
			FollowerMaxRetries: 2,
			PeerBufferSize:     1000,
		})
		if err != nil {
			t.Fatalf("node %d: %v", i, err)
		}
		t.Cleanup(func() { _ = node.Close() })
		nodes[i] = node
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// ── elect a leader ────────────────────────────────────────────────────────
	leader := waitForLeaderNode(t, nodes, 10*time.Second)
	leaderIdx := -1
	for i, nd := range nodes {
		if nd == leader {
			leaderIdx = i
			break
		}
	}
	t.Logf("leader: node-%d", leaderIdx)

	// ── replication ───────────────────────────────────────────────────────────
	writtenRev, err := leader.Put(ctx, "/cluster/replicated", []byte("yes"), 0)
	if err != nil {
		t.Fatalf("leader Put: %v", err)
	}

	for i, node := range nodes {
		if node == leader {
			continue
		}
		if err := node.WaitForRevision(ctx, writtenRev); err != nil {
			t.Fatalf("node-%d WaitForRevision(%d): %v", i, writtenRev, err)
		}
		kv, err := node.Get("/cluster/replicated")
		if err != nil || kv == nil || string(kv.Value) != "yes" {
			t.Errorf("node-%d replication: err=%v kv=%v", i, err, kv)
		}
	}

	// ── write forwarding ──────────────────────────────────────────────────────
	var follower *t4.Node
	followerIdx := -1
	for i, node := range nodes {
		if node != leader {
			follower = node
			followerIdx = i
			break
		}
	}
	t.Logf("follower: node-%d", followerIdx)

	fwdRev, err := follower.Put(ctx, "/cluster/forwarded", []byte("from-follower"), 0)
	if err != nil {
		t.Fatalf("forwarded Put via follower: %v", err)
	}

	if err := leader.WaitForRevision(ctx, fwdRev); err != nil {
		t.Fatalf("leader WaitForRevision(%d): %v", fwdRev, err)
	}
	kv, err := leader.Get("/cluster/forwarded")
	if err != nil || kv == nil || string(kv.Value) != "from-follower" {
		t.Errorf("forwarded key on leader: err=%v kv=%v", err, kv)
	}

	// ── leader failover ───────────────────────────────────────────────────────
	t.Logf("closing leader node-%d to trigger failover", leaderIdx)
	leader.Close()

	survivors := make([]*t4.Node, 0, count-1)
	for _, nd := range nodes {
		if nd != leader {
			survivors = append(survivors, nd)
		}
	}

	newLeader := waitForLeaderNode(t, survivors, 30*time.Second)
	if newLeader == leader {
		t.Fatal("old leader should not win re-election")
	}
	newLeaderIdx := -1
	for i, nd := range nodes {
		if nd == newLeader {
			newLeaderIdx = i
		}
	}
	t.Logf("new leader: node-%d", newLeaderIdx)

	// Write to new leader and verify it persists.
	_, err = newLeader.Put(ctx, "/cluster/after-failover", []byte("ok"), 0)
	if err != nil {
		t.Fatalf("write after failover: %v", err)
	}
	kv, err = newLeader.Get("/cluster/after-failover")
	if err != nil || kv == nil || string(kv.Value) != "ok" {
		t.Errorf("read after failover: err=%v kv=%v", err, kv)
	}
}

// ── RestorePoint ──────────────────────────────────────────────────────────────

// TestRestorePoint verifies that a node bootstrapped with a RestorePoint
// contains exactly the data present at the snapshot moment — no more, no less.
func TestRestorePoint(t *testing.T) {
	store := newSnapshotStore()
	dirA := t.TempDir()
	ctx := context.Background()

	// Node A: write 5 keys, wait for checkpoint and WAL segment upload.
	nodeA, err := t4.Open(t4.Config{
		DataDir:            dirA,
		ObjectStore:        store,
		SegmentMaxAge:      20 * time.Millisecond,
		CheckpointInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("open node A: %v", err)
	}

	for i := 0; i < 5; i++ {
		if _, err := nodeA.Create(ctx, fmt.Sprintf("/key/%d", i), []byte("before"), 0); err != nil {
			t.Fatalf("Create key %d: %v", i, err)
		}
	}

	// Wait for at least one checkpoint and WAL upload cycle.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		m, _ := checkpoint.New(nil).ReadManifest(ctx, store)
		if m != nil && store.versionOf(m.CheckpointKey) != "" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Capture the restore point: checkpoint archive + all WAL segments so far.
	manifest, err := checkpoint.New(nil).ReadManifest(ctx, store)
	if err != nil || manifest == nil {
		t.Fatalf("ReadManifest: err=%v manifest=%v", err, manifest)
	}
	walKeys, err := store.List(ctx, "wal/")
	if err != nil {
		t.Fatalf("List wal: %v", err)
	}
	walSegs := make([]t4.PinnedObject, 0, len(walKeys))
	for _, k := range walKeys {
		if ver := store.versionOf(k); ver != "" {
			walSegs = append(walSegs, t4.PinnedObject{Key: k, VersionID: ver})
		}
	}
	rp := &t4.RestorePoint{
		Store:             store,
		CheckpointArchive: t4.PinnedObject{Key: manifest.CheckpointKey, VersionID: store.versionOf(manifest.CheckpointKey)},
		WALSegments:       walSegs,
	}

	// Node A: write 5 more keys after the snapshot — these must NOT appear in node B.
	for i := 5; i < 10; i++ {
		if _, err := nodeA.Create(ctx, fmt.Sprintf("/key/%d", i), []byte("after"), 0); err != nil {
			t.Fatalf("Create key %d: %v", i, err)
		}
	}
	nodeA.Close()

	// Node B: boot from the RestorePoint into a fresh directory.
	// It uses a separate ObjectStore for its own future writes so it doesn't
	// interfere with node A's prefix.
	dirB := t.TempDir()
	nodeB, err := t4.Open(t4.Config{
		DataDir:      dirB,
		ObjectStore:  object.NewMem(),
		RestorePoint: rp,
	})
	if err != nil {
		t.Fatalf("open node B: %v", err)
	}
	defer nodeB.Close()

	// Keys 0-4 must be present.
	for i := 0; i < 5; i++ {
		kv, err := nodeB.Get(fmt.Sprintf("/key/%d", i))
		if err != nil || kv == nil {
			t.Errorf("key %d: expected present, got err=%v kv=%v", i, err, kv)
		}
	}
	// Keys 5-9 were written after the snapshot and must be absent.
	for i := 5; i < 10; i++ {
		kv, err := nodeB.Get(fmt.Sprintf("/key/%d", i))
		if err != nil {
			t.Errorf("key %d: unexpected error: %v", i, err)
		}
		if kv != nil {
			t.Errorf("key %d: expected absent, got %v", i, kv)
		}
	}
}
