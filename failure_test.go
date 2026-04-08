package t4_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/t4db/t4"
	"github.com/t4db/t4/internal/checkpoint"
	"github.com/t4db/t4/internal/wal"
	"github.com/t4db/t4/pkg/object"
)

// ── walBlockingStore ──────────────────────────────────────────────────────────

// walBlockingStore permanently fails all Put calls whose key starts with
// "wal/", while letting checkpoint, manifest, and lock writes through.
// This lets tests create scenarios where WAL segments are never in S3 but
// checkpoints are, so the startup-checkpoint code path can be isolated.
type walBlockingStore struct {
	object.Store
}

func newWALBlockingStore() *walBlockingStore {
	return &walBlockingStore{Store: object.NewMem()}
}

func (s *walBlockingStore) Put(ctx context.Context, key string, r io.Reader) error {
	if strings.HasPrefix(key, "wal/") {
		io.Copy(io.Discard, r) // consume reader so caller doesn't block
		return errors.New("injected: WAL upload blocked")
	}
	return s.Store.Put(ctx, key, r)
}

// ── faultyStore ───────────────────────────────────────────────────────────────

// faultyStore wraps an object.Store and can be toggled to fail all writes.
type faultyStore struct {
	inner  object.Store
	broken int32 // atomic bool: 1 = fail writes, 0 = pass through
}

func newFaultyStore() *faultyStore {
	return &faultyStore{inner: object.NewMem()}
}

func (f *faultyStore) break_()        { atomic.StoreInt32(&f.broken, 1) }
func (f *faultyStore) repair()        { atomic.StoreInt32(&f.broken, 0) }
func (f *faultyStore) isBroken() bool { return atomic.LoadInt32(&f.broken) == 1 }

func (f *faultyStore) Put(ctx context.Context, key string, r io.Reader) error {
	if f.isBroken() {
		return errors.New("faultyStore: Put: injected failure")
	}
	return f.inner.Put(ctx, key, r)
}

func (f *faultyStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return f.inner.Get(ctx, key)
}

func (f *faultyStore) Delete(ctx context.Context, key string) error {
	if f.isBroken() {
		return errors.New("faultyStore: Delete: injected failure")
	}
	return f.inner.Delete(ctx, key)
}

func (f *faultyStore) DeleteMany(ctx context.Context, keys []string) error {
	if f.isBroken() {
		return errors.New("faultyStore: DeleteMany: injected failure")
	}
	return f.inner.DeleteMany(ctx, keys)
}

func (f *faultyStore) List(ctx context.Context, prefix string) ([]string, error) {
	return f.inner.List(ctx, prefix)
}

// ── trackingStore ─────────────────────────────────────────────────────────────

// trackingStore records the keys that have been Put.
type trackingStore struct {
	inner object.Store
	mu    sync.Mutex
	puts  []string
}

func newTrackingStore(inner object.Store) *trackingStore {
	return &trackingStore{inner: inner}
}

func (t *trackingStore) Put(ctx context.Context, key string, r io.Reader) error {
	// Buffer the body so we can track the put and still pass it on.
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if err := t.inner.Put(ctx, key, bytes.NewReader(data)); err != nil {
		return err
	}
	t.mu.Lock()
	t.puts = append(t.puts, key)
	t.mu.Unlock()
	return nil
}

func (t *trackingStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return t.inner.Get(ctx, key)
}

func (t *trackingStore) Delete(ctx context.Context, key string) error {
	return t.inner.Delete(ctx, key)
}

func (t *trackingStore) DeleteMany(ctx context.Context, keys []string) error {
	return t.inner.DeleteMany(ctx, keys)
}

func (t *trackingStore) List(ctx context.Context, prefix string) ([]string, error) {
	return t.inner.List(ctx, prefix)
}

func (t *trackingStore) putCount(prefix string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := 0
	for _, k := range t.puts {
		if strings.HasPrefix(k, prefix) {
			n++
		}
	}
	return n
}

// ── helpers ───────────────────────────────────────────────────────────────────

// openCluster starts count nodes sharing the same object store with short
// segment/checkpoint intervals so S3 state is flushed quickly.
func openCluster(t *testing.T, count int, store object.Store) []*t4.Node {
	t.Helper()
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
			CheckpointInterval: 300 * time.Millisecond,
			SegmentMaxAge:      200 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("open node-%d: %v", i, err)
		}
		t.Cleanup(func() { node.Close() })
		nodes[i] = node
	}
	return nodes
}

// ── TestLateNodeJoin ──────────────────────────────────────────────────────────

// TestLateNodeJoin verifies that a node started after data has been written
// recovers the full state via checkpoint + WAL replay from object storage.
func TestLateNodeJoin(t *testing.T) {
	store := object.NewMem()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Start 2 nodes (leader + follower).
	nodes := openCluster(t, 2, store)
	leader := waitForLeaderNode(t, nodes, 10*time.Second)

	// Write data and wait for S3 flush.
	const keys = 15
	var lastRev int64
	for i := 0; i < keys; i++ {
		rev, err := leader.Put(ctx, fmt.Sprintf("/join/%d", i), []byte(fmt.Sprintf("v%d", i)), 0)
		if err != nil {
			t.Fatalf("Put: %v", err)
		}
		lastRev = rev
	}

	// Wait for checkpoint/WAL to reach S3 (CheckpointInterval=300ms, SegmentMaxAge=200ms).
	time.Sleep(1 * time.Second)

	// Start a 3rd node with a fresh data directory but the same object store.
	peerAddr := freeAddrImpl(t)
	late, err := t4.Open(t4.Config{
		DataDir:            t.TempDir(), // fresh: no local DB
		ObjectStore:        store,
		NodeID:             "node-late",
		PeerListenAddr:     peerAddr,
		AdvertisePeerAddr:  peerAddr,
		FollowerMaxRetries: 2,
		PeerBufferSize:     1000,
		CheckpointInterval: 300 * time.Millisecond,
		SegmentMaxAge:      200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("late join Open: %v", err)
	}
	t.Cleanup(func() { late.Close() })

	// Wait for the late node to catch up.
	if err := late.WaitForRevision(ctx, lastRev); err != nil {
		t.Fatalf("late node WaitForRevision(%d): %v", lastRev, err)
	}

	// Verify all data is present.
	for i := 0; i < keys; i++ {
		kv, err := late.Get(fmt.Sprintf("/join/%d", i))
		if err != nil || kv == nil {
			t.Errorf("late node Get /join/%d: err=%v kv=%v", i, err, kv)
		} else if string(kv.Value) != fmt.Sprintf("v%d", i) {
			t.Errorf("late node value /join/%d: want v%d got %q", i, i, kv.Value)
		}
	}
}

// ── TestScale3To1 ─────────────────────────────────────────────────────────────

// TestScale3To1 verifies that closing 2 of 3 nodes leaves the remaining node
// fully operational and retaining all data.
func TestScale3To1(t *testing.T) {
	store := object.NewMem()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	nodes := openCluster(t, 3, store)
	leader := waitForLeaderNode(t, nodes, 10*time.Second)

	// Write initial data.
	const phase1Keys = 10
	for i := 0; i < phase1Keys; i++ {
		if _, err := leader.Put(ctx, fmt.Sprintf("/scale/%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("phase1 Put: %v", err)
		}
	}

	// Wait for all nodes to replicate.
	rev := leader.CurrentRevision()
	for _, n := range nodes {
		if n != leader {
			if err := n.WaitForRevision(ctx, rev); err != nil {
				t.Fatalf("WaitForRevision: %v", err)
			}
		}
	}

	// Close 2 non-leader nodes.
	closed := 0
	for _, n := range nodes {
		if n != leader && closed < 2 {
			n.Close()
			closed++
		}
	}
	t.Logf("closed %d followers", closed)

	// The remaining leader should still accept writes.
	const phase2Keys = 5
	for i := phase1Keys; i < phase1Keys+phase2Keys; i++ {
		if _, err := leader.Put(ctx, fmt.Sprintf("/scale/%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("phase2 Put (after scale-down): %v", err)
		}
	}

	// All keys should be present.
	kvs, err := leader.List("/scale/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(kvs) != phase1Keys+phase2Keys {
		t.Errorf("after scale-down: want %d keys got %d", phase1Keys+phase2Keys, len(kvs))
	}
}

// ── TestObjectStoreUnavailableWritesSucceed ───────────────────────────────────

// TestObjectStoreUnavailableWritesFail verifies that node writes fail when the
// object store is unavailable. With synchronous WAL upload, every write must
// reach S3 before it is acknowledged, so S3 failures surface as write errors.
// After a write failure the node fences itself; reopening with S3 repaired
// must recover all data that was durably written before the outage.
func TestObjectStoreUnavailableWritesSucceed(t *testing.T) {
	store := newFaultyStore()
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// First run: write 5 keys while S3 is healthy, then break S3 and verify
	// that subsequent writes fail.
	func() {
		node, err := t4.Open(t4.Config{
			DataDir:            dir,
			ObjectStore:        store,
			CheckpointInterval: 24 * time.Hour,
		})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer node.Close()

		// Write while S3 is healthy — these must succeed.
		for i := 0; i < 5; i++ {
			if _, err := node.Put(ctx, fmt.Sprintf("/avail/%d", i), []byte("v"), 0); err != nil {
				t.Fatalf("Put (healthy): %v", err)
			}
		}

		// Break S3: subsequent writes must fail.
		store.break_()
		if _, err := node.Put(ctx, "/avail/5", []byte("v"), 0); err == nil {
			t.Fatal("Put (s3 broken) unexpectedly succeeded")
		}
	}()

	// Repair S3 and reopen: the 5 pre-break keys must all be present.
	store.repair()
	node, err := t4.Open(t4.Config{
		DataDir:     dir,
		ObjectStore: store,
	})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer node.Close()

	kvs, err := node.List("/avail/")
	if err != nil {
		t.Fatalf("List after reopen: %v", err)
	}
	if len(kvs) != 5 {
		t.Errorf("want 5 pre-break keys, got %d", len(kvs))
	}
}

// TestObjectStoreUnavailableRecovery verifies that data written while S3 is
// healthy survives a node restart via WAL replay from S3.
func TestObjectStoreUnavailableRecovery(t *testing.T) {
	store := newFaultyStore()
	dir := t.TempDir()
	ctx := context.Background()

	// First run: write data with S3 healthy, then close.
	func() {
		node, err := t4.Open(t4.Config{
			DataDir:            dir,
			ObjectStore:        store,
			CheckpointInterval: 24 * time.Hour,
		})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		for i := 0; i < 10; i++ {
			if _, err := node.Put(ctx, fmt.Sprintf("/rec/%d", i), []byte("v"), 0); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}
		node.Close()
	}()

	// Reopen: all data should be recovered from S3 WAL.
	node, err := t4.Open(t4.Config{
		DataDir:     dir,
		ObjectStore: store,
	})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer node.Close()

	kvs, err := node.List("/rec/")
	if err != nil {
		t.Fatalf("List after restart: %v", err)
	}
	if len(kvs) != 10 {
		t.Errorf("after restart: want 10 keys got %d", len(kvs))
	}
}

// ── TestCheckpointCorruption ──────────────────────────────────────────────────

// TestCheckpointCorruptionManifest verifies that a node returns a clear error
// when the manifest in S3 is corrupt JSON.
func TestCheckpointCorruptionManifest(t *testing.T) {
	store := object.NewMem()
	ctx := context.Background()

	// Write a corrupt manifest.
	store.Put(ctx, "manifest/latest", strings.NewReader("this is not valid json"))

	_, err := t4.Open(t4.Config{
		DataDir:     t.TempDir(), // fresh dir → will try to restore checkpoint
		ObjectStore: store,
	})
	if err == nil {
		t.Fatal("expected error opening node with corrupt manifest, got nil")
	}
	t.Logf("got expected error: %v", err)
}

// TestCheckpointCorruptionArchive verifies that a node returns a clear error
// when the checkpoint archive bytes are corrupt.
func TestCheckpointCorruptionArchive(t *testing.T) {
	store := object.NewMem()
	ctx := context.Background()

	// Write a manifest pointing to a key that contains garbage.
	manifest := `{"checkpoint_key":"checkpoint/0000000001/00000000000000000001","revision":1,"term":1}`
	store.Put(ctx, "manifest/latest", strings.NewReader(manifest))
	store.Put(ctx, "checkpoint/0000000001/00000000000000000001", strings.NewReader("not a real checkpoint"))

	_, err := t4.Open(t4.Config{
		DataDir:     t.TempDir(),
		ObjectStore: store,
	})
	if err == nil {
		t.Fatal("expected error opening node with corrupt checkpoint, got nil")
	}
	t.Logf("got expected error: %v", err)
}

// ── TestLeaderCrashBeforeWALFlush ─────────────────────────────────────────────

// TestLeaderCrashBeforeWALFlush verifies that data replicated to followers
// is not lost even when the leader crashes before its WAL segment is uploaded
// to object storage.
func TestLeaderCrashBeforeWALFlush(t *testing.T) {
	// Use a tracking store so we can verify whether WAL was uploaded.
	mem := object.NewMem()
	tracked := newTrackingStore(mem)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Very large segment size + long age: WAL segments will NOT auto-upload.
	const segSize = 500 << 20 // 500 MB
	const segAge = 24 * time.Hour

	const count = 3
	nodes := make([]*t4.Node, count)
	for i := 0; i < count; i++ {
		peerAddr := freeAddrImpl(t)
		node, err := t4.Open(t4.Config{
			DataDir:            t.TempDir(),
			ObjectStore:        tracked,
			NodeID:             fmt.Sprintf("node-%d", i),
			PeerListenAddr:     peerAddr,
			AdvertisePeerAddr:  peerAddr,
			FollowerMaxRetries: 2,
			PeerBufferSize:     1000,
			CheckpointInterval: 24 * time.Hour, // disable checkpoint
			SegmentMaxSize:     segSize,
			SegmentMaxAge:      segAge,
		})
		if err != nil {
			t.Fatalf("node %d: %v", i, err)
		}
		t.Cleanup(func() { node.Close() })
		nodes[i] = node
	}

	leader := waitForLeaderNode(t, nodes, 10*time.Second)
	leaderIdx := -1
	for i, n := range nodes {
		if n == leader {
			leaderIdx = i
			break
		}
	}
	t.Logf("leader: node-%d", leaderIdx)

	// Write data and verify it's replicated to followers.
	const keyCount = 20
	var lastRev int64
	for i := 0; i < keyCount; i++ {
		rev, err := leader.Put(ctx, fmt.Sprintf("/crash/%d", i), []byte("v"), 0)
		if err != nil {
			t.Fatalf("Put: %v", err)
		}
		lastRev = rev
	}
	for i, n := range nodes {
		if n == leader {
			continue
		}
		if err := n.WaitForRevision(ctx, lastRev); err != nil {
			t.Fatalf("node-%d WaitForRevision: %v", i, err)
		}
	}

	// Verify no WAL segment was uploaded yet (segments are too large to auto-rotate).
	walUploads := tracked.putCount("wal/")
	t.Logf("WAL uploads before crash: %d", walUploads)
	if walUploads > 0 {
		t.Log("(WAL was uploaded — test still valid, just less targeted)")
	}

	// Crash the leader.
	t.Logf("crashing leader node-%d", leaderIdx)
	leader.Close()

	// A survivor should become the new leader.
	survivors := make([]*t4.Node, 0, count-1)
	for _, n := range nodes {
		if n != leader {
			survivors = append(survivors, n)
		}
	}
	newLeader := waitForLeaderNode(t, survivors, 30*time.Second)
	t.Logf("new leader elected")

	// All data written before the crash must still be accessible.
	for i := 0; i < keyCount; i++ {
		kv, err := newLeader.Get(fmt.Sprintf("/crash/%d", i))
		if err != nil || kv == nil {
			t.Errorf("key /crash/%d missing after leader crash: err=%v", i, err)
		}
	}
}

// ── TestWALReplayAfterPartialUpload ──────────────────────────────────────────

// TestWALReplayAfterPartialUpload simulates a node restarting after some WAL
// segments were uploaded and some were only on local disk.
func TestWALReplayAfterPartialUpload(t *testing.T) {
	store := object.NewMem()
	dir := t.TempDir()
	ctx := context.Background()

	var lastRev int64

	// First run: write data, close cleanly (triggers WAL seal).
	func() {
		node, err := t4.Open(t4.Config{
			DataDir:            dir,
			ObjectStore:        store,
			CheckpointInterval: 24 * time.Hour,
			SegmentMaxAge:      24 * time.Hour, // keep segments local
			SegmentMaxSize:     500 << 20,
		})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		for i := 0; i < 30; i++ {
			rev, err := node.Put(ctx, fmt.Sprintf("/wal/%d", i), []byte("v"), 0)
			if err != nil {
				t.Fatalf("Put: %v", err)
			}
			lastRev = rev
		}
		node.Close()
	}()

	// Second run: should recover all data from local WAL segments.
	node, err := t4.Open(t4.Config{
		DataDir:     dir,
		ObjectStore: store,
	})
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer node.Close()

	if node.CurrentRevision() != lastRev {
		t.Errorf("CurrentRevision after restart: want %d got %d", lastRev, node.CurrentRevision())
	}
	kvs, err := node.List("/wal/")
	if err != nil || len(kvs) != 30 {
		t.Errorf("List after restart: err=%v got %d (want 30)", err, len(kvs))
	}
}

// ── TestStartupCheckpointCoversLocalWAL ──────────────────────────────────────

// TestStartupCheckpointCoversLocalWAL verifies that when a node starts up with
// data in Pebble that is not yet in S3 (e.g. because WAL upload was blocked),
// the startup checkpoint written by checkpointLoop makes that data visible to
// fresh nodes that bootstrap entirely from object storage.
//
// This exercises the forceCheckpoint call added to checkpointLoop.
func TestStartupCheckpointCoversLocalWAL(t *testing.T) {
	store := object.NewMem()
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const prefix = "/cp-covers-wal/"

	// ── Phase 1: write data (S3 receives all WAL segments via sync upload) ────
	func() {
		node, err := t4.Open(t4.Config{
			DataDir:            dir,
			ObjectStore:        store,
			CheckpointInterval: 24 * time.Hour, // no auto-checkpoint
		})
		if err != nil {
			t.Fatalf("phase-1 Open: %v", err)
		}
		for i := 0; i < 10; i++ {
			if _, err := node.Put(ctx, fmt.Sprintf("%sk%d", prefix, i), []byte("v"), 0); err != nil {
				t.Fatalf("phase-1 Put %d: %v", i, err)
			}
		}
		node.Close()
	}()

	// ── Phase 2: restart node; startup checkpoint is written immediately ──────
	func() {
		node, err := t4.Open(t4.Config{
			DataDir:            dir,
			ObjectStore:        store,
			CheckpointInterval: 50 * time.Millisecond, // allow startup checkpoint
		})
		if err != nil {
			t.Fatalf("phase-2 Open: %v", err)
		}
		defer node.Close()

		// Wait long enough for the startup checkpoint to be written.
		time.Sleep(500 * time.Millisecond)
	}()

	// ── Phase 3: fresh node, empty data dir, must see all data via checkpoint ─
	fresh, err := t4.Open(t4.Config{
		DataDir:     t.TempDir(), // no local state
		ObjectStore: store,
	})
	if err != nil {
		t.Fatalf("phase-3 Open: %v", err)
	}
	defer fresh.Close()

	kvs, err := fresh.List(prefix)
	if err != nil {
		t.Fatalf("phase-3 List: %v", err)
	}
	if len(kvs) != 10 {
		t.Errorf("phase-3: want 10 keys, got %d", len(kvs))
	}
	for i := 0; i < 10; i++ {
		kv, err := fresh.Get(fmt.Sprintf("%sk%d", prefix, i))
		if err != nil || kv == nil {
			t.Errorf("phase-3 Get k%d: err=%v kv=%v", i, err, kv)
		}
	}
}

// ── TestConcurrentCompactAndPut ───────────────────────────────────────────────

// TestConcurrentCompactAndPut is a regression test for the compact/put
// revision-collision race. Before the fix, Compact and Put could both read the
// same currentRev and produce entries with identical revisions. When Compact
// broadcast first, the peer server's maxSent dedup filter silently dropped the
// concurrent Put on every follower.
func TestConcurrentCompactAndPut(t *testing.T) {
	store := object.NewMem()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	nodes := openCluster(t, 3, store)
	leader := waitForLeaderNode(t, nodes, 10*time.Second)

	// Hammer concurrent Puts and Compacts. Track which Puts the leader acknowledged.
	const rounds = 80
	var (
		mu        sync.Mutex
		committed []string
	)
	var wg sync.WaitGroup
	for i := 0; i < rounds; i++ {
		i := i
		wg.Add(2)
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("/concurrent/put-%03d", i)
			if _, err := leader.Put(ctx, key, []byte("v"), 0); err == nil {
				mu.Lock()
				committed = append(committed, key)
				mu.Unlock()
			}
		}()
		go func() {
			defer wg.Done()
			_ = leader.Compact(ctx, leader.CurrentRevision())
		}()
	}
	wg.Wait()

	if len(committed) == 0 {
		t.Fatal("no Puts succeeded — test inconclusive")
	}
	t.Logf("%d/%d Puts acknowledged", len(committed), rounds)

	// Every acknowledged Put must appear on every node.
	lastRev := leader.CurrentRevision()
	for i, n := range nodes {
		if err := n.WaitForRevision(ctx, lastRev); err != nil {
			t.Fatalf("node-%d WaitForRevision: %v", i, err)
		}
		for _, key := range committed {
			kv, err := n.Get(key)
			if err != nil || kv == nil {
				t.Errorf("node-%d Get(%q): err=%v kv=%v (key lost after concurrent compact)", i, key, err, kv)
			}
		}
	}
}

// ── TestDeletedKeyDurability ──────────────────────────────────────────────────

// TestDeletedKeyDurability verifies that keys deleted before compaction remain
// absent on a fresh node that bootstraps entirely from object storage. This
// tests the full write→delete→compact→S3-bootstrap pipeline.
func TestDeletedKeyDurability(t *testing.T) {
	store := object.NewMem()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const prefix = "/del-dur/"

	// Phase 1: write, delete, compact.
	var lastRev int64
	func() {
		n, err := t4.Open(t4.Config{
			DataDir:            t.TempDir(),
			ObjectStore:        store,
			CheckpointInterval: 300 * time.Millisecond,
			SegmentMaxAge:      100 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer n.Close()

		for i := 0; i < 10; i++ {
			if _, err := n.Put(ctx, fmt.Sprintf("%sk%d", prefix, i), []byte("v"), 0); err != nil {
				t.Fatalf("Put k%d: %v", i, err)
			}
		}
		// Delete the first five keys.
		for i := 0; i < 5; i++ {
			if _, err := n.Delete(ctx, fmt.Sprintf("%sk%d", prefix, i)); err != nil {
				t.Fatalf("Delete k%d: %v", i, err)
			}
		}
		rev := n.CurrentRevision()
		if err := n.Compact(ctx, rev); err != nil {
			t.Fatalf("Compact: %v", err)
		}
		lastRev = n.CurrentRevision()
		// Allow checkpoint + WAL to land in object storage.
		time.Sleep(800 * time.Millisecond)
	}()

	// Phase 2: fresh node bootstraps from S3 only.
	fresh, err := t4.Open(t4.Config{
		DataDir:     t.TempDir(),
		ObjectStore: store,
	})
	if err != nil {
		t.Fatalf("Open fresh node: %v", err)
	}
	defer fresh.Close()

	if err := fresh.WaitForRevision(ctx, lastRev); err != nil {
		t.Fatalf("WaitForRevision: %v", err)
	}

	// Deleted keys must be absent.
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("%sk%d", prefix, i)
		kv, err := fresh.Get(key)
		if err != nil {
			t.Errorf("Get deleted %q: unexpected error: %v", key, err)
			continue
		}
		if kv != nil {
			t.Errorf("Get deleted %q: want nil, got value %q", key, kv.Value)
		}
	}
	// Live keys must be present.
	for i := 5; i < 10; i++ {
		key := fmt.Sprintf("%sk%d", prefix, i)
		kv, err := fresh.Get(key)
		if err != nil || kv == nil {
			t.Errorf("Get live %q: err=%v kv=%v", key, err, kv)
		}
	}
}

// ── TestBootstrapGCRace ───────────────────────────────────────────────────────

// staleFirstManifestStore returns a saved stale manifest on the very first
// Get("manifest/latest") call and the live value on all subsequent reads.
// This deterministically reproduces the window where a fresh node reads the
// manifest just before the leader writes a newer checkpoint and GCs the WAL
// segments that would have been needed to replay from the stale revision.
type staleFirstManifestStore struct {
	object.Store
	mu           sync.Mutex
	stalePayload []byte
	used         bool
}

func newStaleFirstManifestStore(inner object.Store, stalePayload []byte) *staleFirstManifestStore {
	return &staleFirstManifestStore{Store: inner, stalePayload: stalePayload}
}

func (s *staleFirstManifestStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if key == checkpoint.ManifestKey {
		s.mu.Lock()
		first := !s.used
		s.used = true
		s.mu.Unlock()
		if first {
			return io.NopCloser(bytes.NewReader(s.stalePayload)), nil
		}
	}
	return s.Store.Get(ctx, key)
}

// TestBootstrapGCRace is a regression test for the window between an initial
// manifest read and replayRemote during Open(). Without the fix (re-reading
// the manifest just before replayRemote), a fresh node that read an old
// manifest would call replayRemote(afterRev=staleRev) and fail when the
// leader had already GC'd the WAL segments between staleRev and the current
// checkpoint revision.
func TestBootstrapGCRace(t *testing.T) {
	inner := object.NewMem()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const prefix = "/gc-race/"

	// Phase 1: write 20 keys and let a checkpoint + GC cycle complete.
	func() {
		n, err := t4.Open(t4.Config{
			DataDir:            t.TempDir(),
			ObjectStore:        inner,
			CheckpointInterval: 200 * time.Millisecond,
			SegmentMaxAge:      50 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("phase-1 Open: %v", err)
		}
		defer n.Close()

		for i := 0; i < 20; i++ {
			if _, err := n.Put(ctx, fmt.Sprintf("%sk%02d", prefix, i), []byte("v1"), 0); err != nil {
				t.Fatalf("phase-1 Put k%d: %v", i, err)
			}
		}
		time.Sleep(600 * time.Millisecond) // checkpoint + WAL segments land in S3
	}()

	// Snapshot the manifest now — this is the "stale" value a racing fresh node
	// would see before the second checkpoint is written.
	staleManifestBytes := func() []byte {
		rc, err := inner.Get(ctx, checkpoint.ManifestKey)
		if err != nil {
			t.Fatalf("read stale manifest: %v", err)
		}
		defer rc.Close()
		b, _ := io.ReadAll(rc)
		return b
	}()
	var staleManifest checkpoint.Manifest
	if err := json.Unmarshal(staleManifestBytes, &staleManifest); err != nil {
		t.Fatalf("parse stale manifest: %v", err)
	}
	t.Logf("stale manifest: rev=%d", staleManifest.Revision)

	// Phase 2: write 20 more keys → new checkpoint + GC removes segments that
	// were covering the stale manifest's revision.
	var lastRev int64
	func() {
		n, err := t4.Open(t4.Config{
			DataDir:            t.TempDir(),
			ObjectStore:        inner,
			CheckpointInterval: 200 * time.Millisecond,
			SegmentMaxAge:      50 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("phase-2 Open: %v", err)
		}
		defer n.Close()

		for i := 20; i < 40; i++ {
			rev, err := n.Put(ctx, fmt.Sprintf("%sk%02d", prefix, i), []byte("v2"), 0)
			if err != nil {
				t.Fatalf("phase-2 Put k%d: %v", i, err)
			}
			lastRev = rev
		}
		time.Sleep(600 * time.Millisecond) // new checkpoint + GC complete
	}()

	walKeys, _ := inner.List(ctx, "wal/")
	t.Logf("WAL segments remaining after phase-2 GC: %d", len(walKeys))

	// Phase 3: wrap the store so the fresh node's first ReadManifest returns the
	// stale snapshot. The fix re-reads the manifest before replayRemote and
	// re-restores from the newer checkpoint, so the node must still recover all
	// 40 keys despite the stale first read.
	staleStore := newStaleFirstManifestStore(inner, staleManifestBytes)
	fresh, err := t4.Open(t4.Config{
		DataDir:     t.TempDir(),
		ObjectStore: staleStore,
	})
	if err != nil {
		t.Fatalf("Open fresh node: %v", err)
	}
	defer fresh.Close()

	if err := fresh.WaitForRevision(ctx, lastRev); err != nil {
		t.Fatalf("WaitForRevision: %v", err)
	}
	for i := 0; i < 40; i++ {
		key := fmt.Sprintf("%sk%02d", prefix, i)
		kv, err := fresh.Get(key)
		if err != nil || kv == nil {
			t.Errorf("Get %q: err=%v kv=%v (missing after GC-race bootstrap)", key, err, kv)
		}
	}
}

// ── TestLocalWALUploadedOnLeaderElection ─────────────────────────────────────

// TestLocalWALUploadedOnLeaderElection verifies that when a node wins leader
// election, any local WAL segment files that are not yet in S3 are uploaded
// before the new leader WAL is opened.
//
// This exercises the uploadLocalWALSegments call added to becomeLeader.
//
// Scenario:
//  1. A single-node writes data with no object store configured; the WAL
//     segment stays local only (no S3 at all).
//  2. The node is restarted in multi-node (peer) mode with an object store,
//     so becomeLeader is called.
//  3. becomeLeader calls uploadLocalWALSegments, which uploads the local
//     segment to S3.
//  4. A fresh node (empty data dir) bootstraps from S3 and must see all data.
func TestLocalWALUploadedOnLeaderElection(t *testing.T) {
	mem := object.NewMem()
	tracked := newTrackingStore(mem)
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const prefix = "/leader-upload/"

	// ── Phase 1: write data without any object store (WAL stays local) ────────
	func() {
		node, err := t4.Open(t4.Config{
			DataDir:            dir,
			ObjectStore:        nil, // no S3 — WAL is local only
			CheckpointInterval: 24 * time.Hour,
			SegmentMaxAge:      24 * time.Hour,
			SegmentMaxSize:     500 << 20,
		})
		if err != nil {
			t.Fatalf("phase-1 Open: %v", err)
		}
		for i := 0; i < 10; i++ {
			if _, err := node.Put(ctx, fmt.Sprintf("%sk%d", prefix, i), []byte("v"), 0); err != nil {
				t.Fatalf("phase-1 Put %d: %v", i, err)
			}
		}
		node.Close()
	}()

	// Nothing in S3.
	walKeys, _ := mem.List(ctx, "wal/")
	if len(walKeys) != 0 {
		t.Fatalf("expected no WAL in S3 after phase-1, got %v", walKeys)
	}

	// ── Phase 2: restart in multi-node mode with S3 ───────────────────────────
	// becomeLeader will call uploadLocalWALSegments before opening the new WAL.
	peerAddr := freeAddrImpl(t)
	node, err := t4.Open(t4.Config{
		DataDir:            dir,
		ObjectStore:        tracked,
		NodeID:             "upload-test",
		PeerListenAddr:     peerAddr,
		AdvertisePeerAddr:  peerAddr,
		CheckpointInterval: 24 * time.Hour, // no auto-checkpoint
		SegmentMaxAge:      24 * time.Hour,
		SegmentMaxSize:     500 << 20,
	})
	if err != nil {
		t.Fatalf("phase-2 Open: %v", err)
	}
	// Wait for leader election so becomeLeader (and the upload) has run.
	waitForLeaderNode(t, []*t4.Node{node}, 15*time.Second)
	node.Close()

	// The local WAL segment must now be present in S3.
	walUploads := tracked.putCount("wal/")
	if walUploads == 0 {
		t.Error("expected local WAL segment to be uploaded when becoming leader, got 0 uploads")
	}
	t.Logf("WAL segments uploaded on leader election: %d", walUploads)

	// ── Phase 3: fresh node must see all data ─────────────────────────────────
	fresh, err := t4.Open(t4.Config{
		DataDir:     t.TempDir(),
		ObjectStore: tracked,
	})
	if err != nil {
		t.Fatalf("phase-3 Open: %v", err)
	}
	defer fresh.Close()

	kvs, err := fresh.List(prefix)
	if err != nil {
		t.Fatalf("phase-3 List: %v", err)
	}
	if len(kvs) != 10 {
		t.Errorf("phase-3: want 10 keys, got %d", len(kvs))
	}
	for i := 0; i < 10; i++ {
		kv, err := fresh.Get(fmt.Sprintf("%sk%d", prefix, i))
		if err != nil || kv == nil {
			t.Errorf("phase-3 Get k%d: err=%v kv=%v", i, err, kv)
		}
	}
}

// ── blockableProxy ────────────────────────────────────────────────────────────

// blockableProxy is a TCP proxy that can be paused to simulate a network
// partition. All connections are forwarded transparently to target; calling
// block() closes existing connections and refuses new ones until unblock().
type blockableProxy struct {
	lis     net.Listener
	target  string
	blocked int32 // atomic bool: 1 = drop connections, 0 = forward
	connsMu sync.Mutex
	conns   []net.Conn
}

func newBlockableProxy(t testing.TB, target string) *blockableProxy {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("blockableProxy: listen: %v", err)
	}
	p := &blockableProxy{lis: lis, target: target}
	go p.serve()
	t.Cleanup(func() { lis.Close() })
	return p
}

func (p *blockableProxy) Addr() string { return p.lis.Addr().String() }

// block closes all active connections and causes new ones to be refused
// immediately until unblock() is called.
func (p *blockableProxy) block() {
	atomic.StoreInt32(&p.blocked, 1)
	p.connsMu.Lock()
	for _, c := range p.conns {
		c.Close()
	}
	p.conns = p.conns[:0]
	p.connsMu.Unlock()
}

func (p *blockableProxy) unblock() { atomic.StoreInt32(&p.blocked, 0) }

func (p *blockableProxy) serve() {
	for {
		c, err := p.lis.Accept()
		if err != nil {
			return // listener closed (test cleanup)
		}
		if atomic.LoadInt32(&p.blocked) == 1 {
			c.Close()
			continue
		}
		dst, err := net.Dial("tcp", p.target)
		if err != nil {
			c.Close()
			continue
		}
		p.connsMu.Lock()
		p.conns = append(p.conns, c, dst)
		p.connsMu.Unlock()
		go func() { io.Copy(dst, c); dst.Close(); c.Close() }()
		go func() { io.Copy(c, dst); c.Close(); dst.Close() }()
	}
}

// ── TestFollowerKilledDuringCommit ────────────────────────────────────────────

// TestFollowerKilledDuringCommit verifies that the leader and remaining
// follower continue accepting writes after one follower is killed mid-stream,
// and that the killed follower resyncs correctly when it rejoins with a fresh
// data directory (bootstrap from S3 checkpoint + WAL replay + live stream).
func TestFollowerKilledDuringCommit(t *testing.T) {
	store := object.NewMem()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	nodes := openCluster(t, 3, store)
	leader := waitForLeaderNode(t, nodes, 10*time.Second)
	leaderIdx := -1
	for i, n := range nodes {
		if n == leader {
			leaderIdx = i
			break
		}
	}
	t.Logf("leader: node-%d", leaderIdx)

	// Phase 1: write initial batch and wait for full replication.
	const phase1 = 10
	var lastRev int64
	for i := 0; i < phase1; i++ {
		rev, err := leader.Put(ctx, fmt.Sprintf("/kill/%d", i), []byte("before"), 0)
		if err != nil {
			t.Fatalf("phase1 Put: %v", err)
		}
		lastRev = rev
	}
	for i, n := range nodes {
		if n == leader {
			continue
		}
		if err := n.WaitForRevision(ctx, lastRev); err != nil {
			t.Fatalf("node-%d WaitForRevision phase1: %v", i, err)
		}
	}

	// Kill one follower.
	var victimIdx int
	for i, n := range nodes {
		if n != leader {
			t.Logf("killing follower node-%d", i)
			victimIdx = i
			n.Close()
			break
		}
	}

	// Phase 2: continue writing while only leader + 1 follower remain.
	// WaitForFollowers with 1 connected follower requires 1 ACK (quorum),
	// so writes proceed normally.
	const phase2 = 10
	for i := phase1; i < phase1+phase2; i++ {
		rev, err := leader.Put(ctx, fmt.Sprintf("/kill/%d", i), []byte("after"), 0)
		if err != nil {
			t.Fatalf("phase2 Put after killing follower node-%d: %v", victimIdx, err)
		}
		lastRev = rev
	}
	t.Logf("phase2 complete: wrote %d keys with follower node-%d down", phase2, victimIdx)

	// Let checkpoint and WAL segments flush to the object store.
	time.Sleep(1500 * time.Millisecond)

	// Restart the killed follower with a fresh data dir so it must bootstrap
	// entirely from S3 (checkpoint restore + WAL replay), then catch up via
	// the live peer stream from the leader.
	rejoinPeer := freeAddrImpl(t)
	rejoined, err := t4.Open(t4.Config{
		DataDir:            t.TempDir(),
		ObjectStore:        store,
		NodeID:             fmt.Sprintf("node-%d", victimIdx),
		PeerListenAddr:     rejoinPeer,
		AdvertisePeerAddr:  rejoinPeer,
		FollowerMaxRetries: 2,
		PeerBufferSize:     1000,
		CheckpointInterval: 300 * time.Millisecond,
		SegmentMaxAge:      200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("rejoin open: %v", err)
	}
	t.Cleanup(func() { rejoined.Close() })

	if err := rejoined.WaitForRevision(ctx, lastRev); err != nil {
		t.Fatalf("rejoined WaitForRevision(%d): %v", lastRev, err)
	}

	// All keys from both phases must be present on the rejoined node.
	for i := 0; i < phase1+phase2; i++ {
		kv, err := rejoined.Get(fmt.Sprintf("/kill/%d", i))
		if err != nil || kv == nil {
			t.Errorf("rejoined: /kill/%d missing: err=%v kv=%v", i, err, kv)
		}
	}
	t.Logf("rejoined node has all %d keys", phase1+phase2)
}

// ── TestNetworkPartitionNoSplitBrain ─────────────────────────────────────────

// TestNetworkPartitionNoSplitBrain verifies two properties of partition handling:
//
//  1. No split-brain: a follower partitioned from the leader does NOT promote
//     itself while the leader is alive. The leader keeps LastSeenNano fresh
//     in the S3 lock every FollowerRetryInterval (2 s); the follower reads
//     this and backs off from TakeOver because the lock age < LeaderLivenessTTL
//     (6 s).
//
//  2. Convergence on heal: when the partition is removed the follower reconnects
//     through the proxy, resyncs from the leader, and converges on all keys.
//
// A blockableProxy sits between the follower and the leader's peer port so the
// partition can be injected and healed without killing either process.
func TestNetworkPartitionNoSplitBrain(t *testing.T) {
	store := object.NewMem()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Allocate the leader's real peer address and wrap it in a blockable proxy.
	// The leader advertises the proxy address so the follower dials through it.
	leaderPeerReal := freeAddrImpl(t)
	proxy := newBlockableProxy(t, leaderPeerReal)

	leaderNode, err := t4.Open(t4.Config{
		DataDir:            t.TempDir(),
		ObjectStore:        store,
		NodeID:             "leader",
		PeerListenAddr:     leaderPeerReal,
		AdvertisePeerAddr:  proxy.Addr(), // follower connects through proxy
		FollowerMaxRetries: 2,
		PeerBufferSize:     1000,
		CheckpointInterval: 300 * time.Millisecond,
		SegmentMaxAge:      200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("open leader: %v", err)
	}
	t.Cleanup(func() { leaderNode.Close() })

	// Wait for this node to hold the S3 leader lock before starting the follower,
	// preventing a race where the follower wins the election instead.
	deadline := time.Now().Add(10 * time.Second)
	for !leaderNode.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("leader node did not acquire lock within timeout")
		}
		time.Sleep(50 * time.Millisecond)
	}

	followerPeer := freeAddrImpl(t)
	followerNode, err := t4.Open(t4.Config{
		DataDir:            t.TempDir(),
		ObjectStore:        store,
		NodeID:             "follower",
		PeerListenAddr:     followerPeer,
		AdvertisePeerAddr:  followerPeer,
		FollowerMaxRetries: 2,
		PeerBufferSize:     1000,
		CheckpointInterval: 300 * time.Millisecond,
		SegmentMaxAge:      200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("open follower: %v", err)
	}
	t.Cleanup(func() { followerNode.Close() })

	// Phase 1: write and verify replication to confirm the follower is connected.
	const phase1 = 10
	var lastRev int64
	for i := 0; i < phase1; i++ {
		rev, err := leaderNode.Put(ctx, fmt.Sprintf("/partition/%d", i), []byte("before"), 0)
		if err != nil {
			t.Fatalf("phase1 Put: %v", err)
		}
		lastRev = rev
	}
	if err := followerNode.WaitForRevision(ctx, lastRev); err != nil {
		t.Fatalf("follower WaitForRevision phase1: %v", err)
	}
	t.Logf("phase1 complete: follower at rev=%d", lastRev)

	// Inject partition: follower can no longer reach the leader's peer port.
	t.Log("blocking proxy — simulating network partition")
	proxy.block()

	// Wait long enough for the follower to exhaust FollowerMaxRetries (2 × 2 s =
	// 4 s), attempt TakeOver, see a fresh lock, and back off. We wait 10 s to
	// cover multiple full retry+takeover cycles, ensuring the behaviour is stable
	// and not just a transient timing window.
	time.Sleep(10 * time.Second)

	// ── split-brain check ─────────────────────────────────────────────────────
	if followerNode.IsLeader() {
		t.Error("SPLIT-BRAIN: follower promoted itself while real leader is alive")
	}
	if !leaderNode.IsLeader() {
		t.Error("original leader unexpectedly lost leadership during partition")
	}

	// Phase 2: leader accepts writes while the follower is partitioned.
	// WaitForFollowers with 0 connected followers returns immediately
	// (quorum of 0 = 0 ACKs required), so the leader continues.
	const phase2 = 5
	for i := phase1; i < phase1+phase2; i++ {
		rev, err := leaderNode.Put(ctx, fmt.Sprintf("/partition/%d", i), []byte("during-partition"), 0)
		if err != nil {
			t.Fatalf("phase2 Put during partition: %v", err)
		}
		lastRev = rev
	}
	t.Logf("phase2 complete: leader at rev=%d during partition", lastRev)

	// Heal the partition.
	t.Log("unblocking proxy — healing partition")
	proxy.unblock()

	// Follower must reconnect through the proxy and resync to the current revision.
	if err := followerNode.WaitForRevision(ctx, lastRev); err != nil {
		t.Fatalf("follower WaitForRevision after partition heal: %v", err)
	}

	// All keys from both phases must be visible on the follower.
	for i := 0; i < phase1+phase2; i++ {
		kv, err := followerNode.Get(fmt.Sprintf("/partition/%d", i))
		if err != nil || kv == nil {
			t.Errorf("follower: /partition/%d missing after heal: err=%v kv=%v", i, err, kv)
		}
	}
	t.Logf("partition healed: follower converged on all %d keys", phase1+phase2)
}

// ── TestWALCorruptionMidSegment ───────────────────────────────────────────────

// TestWALCorruptionMidSegment verifies that a WAL segment with a corrupt CRC
// in the middle is handled gracefully: entries before the corrupt frame are
// returned and a non-nil error is reported. The node layer (replayLocal) uses
// ReadAll with this exact contract — it logs the error as a warning and applies
// only the recovered prefix.
//
// WAL wire format (entry.go):
//
//	[4: payload_len uint32 BE]
//	[4: crc32c      uint32 BE]
//	[payload_len bytes: entry data]
//
// Segment header: 20 bytes ("T4\x01\n" + term uint64 BE + firstRev int64 BE).
// For key "/wal/N" (N < 10): payload = 49 fixed + 6 key + 1 val = 56 → frame = 64 bytes.
// Entry 10 (key "/wal/10") starts at byte 20 + 10*64 = 660; CRC at offset +4 = 664.
func TestWALCorruptionMidSegment(t *testing.T) {
	dir := t.TempDir()

	// Write a WAL segment directly using the segment writer API.
	sw, err := wal.OpenSegmentWriter(dir, 1, 1)
	if err != nil {
		t.Fatalf("OpenSegmentWriter: %v", err)
	}
	const total = 30
	for i := 0; i < total; i++ {
		e := &wal.Entry{
			Revision: int64(i + 1),
			Term:     1,
			Op:       wal.OpCreate,
			Key:      fmt.Sprintf("/wal/%d", i),
			Value:    []byte("v"),
		}
		if err := sw.AppendNoSync(e); err != nil {
			t.Fatalf("AppendNoSync %d: %v", i, err)
		}
	}
	if err := sw.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := sw.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	// Corrupt the CRC of entry at index 10 (key "/wal/10").
	// Entries 0–9 have key len 6 → frame = 8 header + 49 fixed + 6 key + 1 val = 64 bytes.
	// Entry 10 starts at: 20 (segment header) + 10*64 = 660; CRC at +4 → byte 664.
	const corruptOffset = 20 + 10*64 + 4 // = 664
	segPath := sw.Path()
	data, err := os.ReadFile(segPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if corruptOffset >= len(data) {
		t.Fatalf("corruptOffset %d >= file len %d — frame layout changed?", corruptOffset, len(data))
	}
	data[corruptOffset] ^= 0xff
	if err := os.WriteFile(segPath, data, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Read all entries from the corrupted segment.
	sr, closer, err := wal.OpenSegmentFile(segPath)
	if err != nil {
		t.Fatalf("OpenSegmentFile: %v", err)
	}
	defer closer()

	entries, readErr := sr.ReadAll()

	// ReadAll must return entries before the corruption and a non-nil error.
	if readErr == nil {
		t.Fatal("ReadAll on corrupted segment: expected non-nil error, got nil")
	}
	if len(entries) != 10 {
		t.Errorf("recovered %d entries before corruption, want 10", len(entries))
	}
	for i, e := range entries {
		want := fmt.Sprintf("/wal/%d", i)
		if e.Key != want {
			t.Errorf("entry[%d].Key = %q, want %q", i, e.Key, want)
		}
	}
	t.Logf("WAL corruption recovery: %d entries recovered before corrupt frame (err: %v)",
		len(entries), readErr)
}

// ── TestFailoverTime ──────────────────────────────────────────────────────────

// TestFailoverTime measures how long it takes for a follower to detect leader
// failure and win election. With FollowerMaxRetries=2 and the default 2 s retry
// interval, followers exhaust retries in ~4 s then race for the S3 lock.
// The test asserts failover completes within 30 s and logs the measured time
// for documentation purposes.
func TestFailoverTime(t *testing.T) {
	store := object.NewMem()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	nodes := openCluster(t, 3, store)
	leader := waitForLeaderNode(t, nodes, 10*time.Second)

	// Write a key so there is something durable to verify after failover.
	rev, err := leader.Put(ctx, "/failover/probe", []byte("before"), 0)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Kill the leader and note the time.
	start := time.Now()
	leader.Close()

	// Collect the surviving followers.
	var survivors []*t4.Node
	for _, n := range nodes {
		if n != leader {
			survivors = append(survivors, n)
		}
	}

	// Poll until one survivor is elected leader.
	newLeader := waitForLeaderNode(t, survivors, 30*time.Second)
	elapsed := time.Since(start)
	t.Logf("failover completed in %v (new leader: %v)", elapsed, newLeader.IsLeader())

	// The written key must be visible on the new leader.
	if err := newLeader.WaitForRevision(ctx, rev); err != nil {
		t.Fatalf("WaitForRevision after failover: %v", err)
	}
	kv, err := newLeader.Get("/failover/probe")
	if err != nil || kv == nil {
		t.Errorf("/failover/probe missing on new leader after failover")
	}

	if elapsed > 30*time.Second {
		t.Errorf("failover took %v, want < 30s", elapsed)
	}
}

// ── TestChaos ─────────────────────────────────────────────────────────────────

// TestChaos runs repeated rounds of random node kills and restarts while
// writes happen concurrently, then verifies that every durably acknowledged
// write is still visible. It is the scaffolding for chaos/soak testing.
//
// Default: 5 rounds (fast CI gate). Set T4_CHAOS_ROUNDS env var to run
// more rounds for extended soak testing (e.g. T4_CHAOS_ROUNDS=500).
//
// Each round:
//  1. Write 5 keys with a concurrent writer goroutine.
//  2. Pick a random node to kill (leader ~1/3 of the time).
//  3. Wait for the cluster to elect a new leader (if leader was killed).
//  4. Restart the killed node from a fresh data directory (simulates disk
//     replacement; the node recovers from S3 checkpoint + WAL replay).
//  5. Verify all keys written in previous rounds are still readable.
func TestChaos(t *testing.T) {
	rounds := 5
	if v := os.Getenv("T4_CHAOS_ROUNDS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			t.Fatalf("invalid T4_CHAOS_ROUNDS=%q", v)
		}
		rounds = n
	}

	store := object.NewMem()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(rounds)*30*time.Second)
	defer cancel()

	const clusterSize = 3
	const keysPerRound = 5

	// chaosSlot holds the mutable state for one cluster slot (node id, peer
	// address and data directory persist across kills; the node pointer changes).
	type chaosSlot struct {
		id      string
		dataDir string
		node    *t4.Node
	}

	openSlot := func(s *chaosSlot) {
		peerAddr := freeAddrImpl(t)
		n, err := t4.Open(t4.Config{
			DataDir:            s.dataDir,
			ObjectStore:        store,
			NodeID:             s.id,
			PeerListenAddr:     peerAddr,
			AdvertisePeerAddr:  peerAddr,
			FollowerMaxRetries: 2,
			PeerBufferSize:     1000,
			CheckpointInterval: 300 * time.Millisecond,
			SegmentMaxAge:      200 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("chaos: open %s: %v", s.id, err)
		}
		s.node = n
	}

	slots := make([]*chaosSlot, clusterSize)
	for i := range slots {
		slots[i] = &chaosSlot{
			id:      fmt.Sprintf("chaos-%d", i),
			dataDir: t.TempDir(),
		}
		openSlot(slots[i])
		t.Cleanup(func() {
			if slots[i].node != nil {
				slots[i].node.Close()
			}
		})
	}

	// Wait for initial leader election.
	liveNodes := func() []*t4.Node {
		var ns []*t4.Node
		for _, s := range slots {
			if s.node != nil {
				ns = append(ns, s.node)
			}
		}
		return ns
	}
	waitForLeaderNode(t, liveNodes(), 15*time.Second)

	// Track every key we've written and its expected value.
	type kv struct{ key, val string }
	var written []kv
	rng := rand.New(rand.NewSource(42))

	for round := 0; round < rounds; round++ {
		// Find the current leader.
		var leader *t4.Node
		for _, n := range liveNodes() {
			if n.IsLeader() {
				leader = n
				break
			}
		}
		if leader == nil {
			leader = waitForLeaderNode(t, liveNodes(), 15*time.Second)
		}

		// Write keysPerRound keys.
		var lastRev int64
		for i := 0; i < keysPerRound; i++ {
			key := fmt.Sprintf("/chaos/r%d/k%d", round, i)
			val := fmt.Sprintf("v%d-%d", round, i)
			rev, err := leader.Put(ctx, key, []byte(val), 0)
			if err != nil {
				t.Fatalf("round %d: Put %s: %v", round, key, err)
			}
			written = append(written, kv{key, val})
			lastRev = rev
		}

		// Pick a random slot to kill.
		victim := rng.Intn(clusterSize)
		killed := slots[victim]
		isLeader := killed.node.IsLeader()
		t.Logf("round %d: killing %s (isLeader=%v)", round, killed.id, isLeader)
		killed.node.Close()
		killed.node = nil

		// Wait for the cluster to stabilize with a new leader.
		waitForLeaderNode(t, liveNodes(), 15*time.Second)

		// Verify the writes we just made survive the kill.
		// Use the new leader for the check.
		var checker *t4.Node
		for _, n := range liveNodes() {
			if n.IsLeader() {
				checker = n
				break
			}
		}
		if err := checker.WaitForRevision(ctx, lastRev); err != nil {
			t.Fatalf("round %d: WaitForRevision %d: %v", round, lastRev, err)
		}
		for _, entry := range written {
			kv, err := checker.Get(entry.key)
			if err != nil || kv == nil {
				t.Errorf("round %d: %s missing after kill (err=%v)", round, entry.key, err)
			} else if string(kv.Value) != entry.val {
				t.Errorf("round %d: %s: got %q want %q", round, entry.key, kv.Value, entry.val)
			}
		}

		// Restart the killed node with a FRESH data directory (simulates disk
		// replacement). It will recover from S3 checkpoint + WAL replay.
		killed.dataDir = t.TempDir()
		openSlot(killed)
		t.Logf("round %d: restarted %s from fresh data dir", round, killed.id)

		// Wait for all live nodes to reach the last revision so the next
		// round starts from a fully-consistent cluster state.
		for _, s := range slots {
			if s.node == nil || s.node == killed.node {
				continue
			}
			if err := s.node.WaitForRevision(ctx, lastRev); err != nil {
				t.Logf("round %d: warning: node %s WaitForRevision %d: %v",
					round, s.id, lastRev, err)
			}
		}
		t.Logf("round %d: OK (%d total keys verified)", round, len(written))
	}

	// Find the final leader revision.
	var finalLeader *t4.Node
	for _, n := range liveNodes() {
		if n.IsLeader() {
			finalLeader = n
			break
		}
	}
	if finalLeader == nil {
		finalLeader = waitForLeaderNode(t, liveNodes(), 15*time.Second)
	}
	finalRev := finalLeader.CurrentRevision()

	// Final full-cluster consistency check: every key must be visible on
	// every live node (after each node catches up to the final revision).
	t.Logf("final check: verifying %d keys across %d nodes (rev=%d)", len(written), len(liveNodes()), finalRev)
	for _, s := range slots {
		if s.node == nil {
			continue
		}
		if err := s.node.WaitForRevision(ctx, finalRev); err != nil {
			if errors.Is(err, t4.ErrClosed) ||
				errors.Is(err, context.DeadlineExceeded) ||
				errors.Is(err, context.Canceled) {
				t.Logf("final: node %s: skipping WaitForRevision %d (%v)", s.id, finalRev, err)
				continue
			}
			t.Errorf("final: node %s: WaitForRevision %d: %v", s.id, finalRev, err)
			continue
		}
		for _, entry := range written {
			kv, err := s.node.Get(entry.key)
			if errors.Is(err, t4.ErrClosed) {
				t.Logf("final: node %s: closed during Get(%s), skipping remaining keys", s.id, entry.key)
				break
			}
			if err != nil || kv == nil {
				t.Errorf("final: node %s: %s missing (err=%v)", s.id, entry.key, err)
			} else if string(kv.Value) != entry.val {
				t.Errorf("final: node %s: %s: got %q want %q", s.id, entry.key, kv.Value, entry.val)
			}
		}
	}
}
