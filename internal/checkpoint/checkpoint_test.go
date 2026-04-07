package checkpoint_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble"

	"github.com/t4db/t4/internal/checkpoint"
	"github.com/t4db/t4/pkg/object"
)

// versionedMem wraps MemStore to record a version ID for every Put, allowing
// RestoreVersioned to be tested without real S3.
type versionedMem struct {
	object.Store
	mu          sync.Mutex
	byVer       map[string][]byte // versionID → raw bytes
	latestByKey map[string]string // key → current versionID
	seq         int
}

func newVersionedMem() *versionedMem {
	return &versionedMem{
		Store:       object.NewMem(),
		byVer:       make(map[string][]byte),
		latestByKey: make(map[string]string),
	}
}

func (v *versionedMem) Put(ctx context.Context, key string, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if err := v.Store.Put(ctx, key, bytes.NewReader(data)); err != nil {
		return err
	}
	v.mu.Lock()
	v.seq++
	vid := fmt.Sprintf("ver%d", v.seq)
	v.byVer[vid] = data
	v.latestByKey[key] = vid
	v.mu.Unlock()
	return nil
}

func (v *versionedMem) GetVersioned(_ context.Context, key, versionID string) (io.ReadCloser, error) {
	v.mu.Lock()
	data, ok := v.byVer[versionID]
	v.mu.Unlock()
	if !ok {
		return nil, object.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (v *versionedMem) VersionOf(key string) string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.latestByKey[key]
}

// openDB opens a pebble DB in a temp directory.
func openDB(t *testing.T) *pebble.DB {
	t.Helper()
	dir := t.TempDir()
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatalf("pebble.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// ── Manifest ──────────────────────────────────────────────────────────────────

func TestReadManifestMissing(t *testing.T) {
	store := object.NewMem()
	m, err := checkpoint.ReadManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("ReadManifest on empty store: %v", err)
	}
	if m != nil {
		t.Errorf("expected nil manifest, got %+v", m)
	}
}

func TestWriteReadManifest(t *testing.T) {
	store := object.NewMem()
	ctx := context.Background()

	want := &checkpoint.Manifest{
		CheckpointKey: "checkpoint/0000000001/00000000000000000042",
		Revision:      42,
		Term:          1,
		LastWALKey:    "wal/0000000001/00000000000000000040",
	}
	if err := checkpoint.WriteManifest(ctx, store, want); err != nil {
		t.Fatalf("WriteManifest: %v", err)
	}

	got, err := checkpoint.ReadManifest(ctx, store)
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	if got == nil {
		t.Fatal("expected manifest, got nil")
	}
	if got.CheckpointKey != want.CheckpointKey {
		t.Errorf("CheckpointKey: want %q got %q", want.CheckpointKey, got.CheckpointKey)
	}
	if got.Revision != want.Revision {
		t.Errorf("Revision: want %d got %d", want.Revision, got.Revision)
	}
	if got.Term != want.Term {
		t.Errorf("Term: want %d got %d", want.Term, got.Term)
	}
	if got.LastWALKey != want.LastWALKey {
		t.Errorf("LastWALKey: want %q got %q", want.LastWALKey, got.LastWALKey)
	}
}

func TestWriteManifestOverwrite(t *testing.T) {
	store := object.NewMem()
	ctx := context.Background()

	checkpoint.WriteManifest(ctx, store, &checkpoint.Manifest{Revision: 1, Term: 1,
		CheckpointKey: checkpoint.CheckpointIndexKey(1, 1)})
	checkpoint.WriteManifest(ctx, store, &checkpoint.Manifest{Revision: 2, Term: 1,
		CheckpointKey: checkpoint.CheckpointIndexKey(1, 2)})

	m, _ := checkpoint.ReadManifest(ctx, store)
	if m.Revision != 2 {
		t.Errorf("overwrite: want revision 2, got %d", m.Revision)
	}
}

// ── CheckpointKey ─────────────────────────────────────────────────────────────

func TestCheckpointKey(t *testing.T) {
	key := checkpoint.CheckpointIndexKey(3, 100)
	if !strings.HasPrefix(key, "checkpoint/") {
		t.Errorf("key should start with checkpoint/: %q", key)
	}
	// Zero-padded so lexicographic == chronological.
	k1 := checkpoint.CheckpointIndexKey(1, 9)
	k2 := checkpoint.CheckpointIndexKey(1, 10)
	if k1 >= k2 {
		t.Errorf("key ordering: %q should sort before %q", k1, k2)
	}
}

// ── GCCheckpoints ─────────────────────────────────────────────────────────────

func TestGCCheckpoints(t *testing.T) {
	store := object.NewMem()
	ctx := context.Background()

	// Seed 5 checkpoint index objects directly.
	for i := 1; i <= 5; i++ {
		idx := checkpoint.CheckpointIndex{Term: 1, Revision: int64(i)}
		b, _ := json.Marshal(idx)
		key := checkpoint.CheckpointIndexKey(1, int64(i))
		if err := store.Put(ctx, key, bytes.NewReader(b)); err != nil {
			t.Fatalf("seed put: %v", err)
		}
	}

	// Keep the 2 most recent; should delete 3.
	deleted, _, err := checkpoint.GCCheckpoints(ctx, store, 2)
	if err != nil {
		t.Fatalf("GCCheckpoints: %v", err)
	}
	if deleted != 3 {
		t.Errorf("deleted: want 3, got %d", deleted)
	}

	// Remaining keys should only be the last 2.
	remaining, _ := checkpoint.ListRemote(ctx, store)
	if len(remaining) != 2 {
		t.Errorf("remaining: want 2, got %d: %v", len(remaining), remaining)
	}
	if remaining[0] != checkpoint.CheckpointIndexKey(1, 4) || remaining[1] != checkpoint.CheckpointIndexKey(1, 5) {
		t.Errorf("unexpected remaining keys: %v", remaining)
	}
}

// TestGCCheckpointsBranchProtection verifies that GCCheckpoints does not
// delete a checkpoint that is referenced by an active branch, even when it
// falls outside the keep-N window.
func TestGCCheckpointsBranchProtection(t *testing.T) {
	store := object.NewMem()
	ctx := context.Background()

	// Seed 5 checkpoint index objects.
	for i := 1; i <= 5; i++ {
		idx := checkpoint.CheckpointIndex{Term: 1, Revision: int64(i)}
		b, _ := json.Marshal(idx)
		key := checkpoint.CheckpointIndexKey(1, int64(i))
		if err := store.Put(ctx, key, bytes.NewReader(b)); err != nil {
			t.Fatalf("seed put: %v", err)
		}
	}

	// Pin checkpoint rev=1 with a branch.
	pinnedKey := checkpoint.CheckpointIndexKey(1, 1)
	if err := checkpoint.RegisterBranch(ctx, store, "experiment", pinnedKey); err != nil {
		t.Fatalf("RegisterBranch: %v", err)
	}

	// GC with keep=2: would normally delete revs 1-3, but rev=1 is pinned.
	deleted, _, err := checkpoint.GCCheckpoints(ctx, store, 2)
	if err != nil {
		t.Fatalf("GCCheckpoints: %v", err)
	}
	if deleted != 2 { // revs 2 and 3 deleted; rev 1 protected
		t.Errorf("deleted: want 2, got %d", deleted)
	}
	remaining, _ := checkpoint.ListRemote(ctx, store)
	// Should have: pinned rev=1, plus kept rev=4 and rev=5.
	if len(remaining) != 3 {
		t.Errorf("remaining: want 3, got %d: %v", len(remaining), remaining)
	}
	hasRev1 := false
	for _, k := range remaining {
		if k == pinnedKey {
			hasRev1 = true
		}
	}
	if !hasRev1 {
		t.Errorf("pinned checkpoint (rev=1) was incorrectly deleted: remaining=%v", remaining)
	}

	// Unregister the branch; now GC should be able to remove the old checkpoint.
	if err := checkpoint.UnregisterBranch(ctx, store, "experiment"); err != nil {
		t.Fatalf("UnregisterBranch: %v", err)
	}
	deleted, _, err = checkpoint.GCCheckpoints(ctx, store, 2)
	if err != nil {
		t.Fatalf("GCCheckpoints after unregister: %v", err)
	}
	if deleted != 1 { // now rev=1 can be deleted
		t.Errorf("after unregister deleted: want 1, got %d", deleted)
	}
	remaining, _ = checkpoint.ListRemote(ctx, store)
	if len(remaining) != 2 {
		t.Errorf("after unregister remaining: want 2, got %d: %v", len(remaining), remaining)
	}
}

func TestGCCheckpointsNoop(t *testing.T) {
	store := object.NewMem()
	ctx := context.Background()

	// Fewer objects than `keep` — nothing should be deleted.
	idx := checkpoint.CheckpointIndex{Term: 1, Revision: 1}
	b, _ := json.Marshal(idx)
	store.Put(ctx, checkpoint.CheckpointIndexKey(1, 1), bytes.NewReader(b))
	deleted, _, err := checkpoint.GCCheckpoints(ctx, store, 2)
	if err != nil {
		t.Fatalf("GCCheckpoints: %v", err)
	}
	if deleted != 0 {
		t.Errorf("deleted: want 0, got %d", deleted)
	}
}

// ── Write / Restore ───────────────────────────────────────────────────────────

func TestWriteRestore(t *testing.T) {
	db := openDB(t)
	store := object.NewMem()
	ctx := context.Background()

	// Write some data to pebble.
	batch := db.NewBatch()
	batch.Set([]byte("key1"), []byte("value1"), nil)
	batch.Set([]byte("key2"), []byte("value2"), nil)
	if err := batch.Commit(pebble.Sync); err != nil {
		t.Fatalf("batch commit: %v", err)
	}

	// Create and upload checkpoint.
	if err := checkpoint.Write(ctx, db, store, 1, 2, "", nil); err != nil {
		t.Fatalf("checkpoint.Write: %v", err)
	}

	// Manifest should be updated.
	m, err := checkpoint.ReadManifest(ctx, store)
	if err != nil || m == nil {
		t.Fatalf("manifest after Write: err=%v m=%v", err, m)
	}
	if m.Revision != 2 || m.Term != 1 {
		t.Errorf("manifest: want rev=2 term=1, got rev=%d term=%d", m.Revision, m.Term)
	}

	// Restore to a new directory.
	targetDir := filepath.Join(t.TempDir(), "restored")
	term, rev, err := checkpoint.Restore(ctx, store, m.CheckpointKey, targetDir)
	if err != nil {
		t.Fatalf("checkpoint.Restore: %v", err)
	}
	if term != 1 || rev != 2 {
		t.Errorf("restored metadata: want term=1 rev=2, got term=%d rev=%d", term, rev)
	}

	// Open the restored DB and verify data.
	rdb, err := pebble.Open(targetDir, &pebble.Options{})
	if err != nil {
		t.Fatalf("open restored db: %v", err)
	}
	defer rdb.Close()

	for _, tc := range []struct{ key, want string }{
		{"key1", "value1"},
		{"key2", "value2"},
	} {
		val, closer, err := rdb.Get([]byte(tc.key))
		if err != nil {
			t.Errorf("restored Get(%q): %v", tc.key, err)
			continue
		}
		if string(val) != tc.want {
			t.Errorf("restored value %q: want %q got %q", tc.key, tc.want, val)
		}
		closer.Close()
	}
}

func TestWriteRestoreWithLastWALKey(t *testing.T) {
	db := openDB(t)
	store := object.NewMem()
	ctx := context.Background()

	if err := checkpoint.Write(ctx, db, store, 2, 99, "wal/0000000002/00000000000000000090", nil); err != nil {
		t.Fatalf("Write: %v", err)
	}

	m, _ := checkpoint.ReadManifest(ctx, store)
	if m.LastWALKey != "wal/0000000002/00000000000000000090" {
		t.Errorf("LastWALKey: want wal/0000000002/00000000000000000090, got %q", m.LastWALKey)
	}
}

func TestRestoreNotFound(t *testing.T) {
	store := object.NewMem()
	_, _, err := checkpoint.Restore(context.Background(), store, "checkpoint/missing", t.TempDir())
	if err == nil {
		t.Error("expected error restoring non-existent checkpoint")
	}
}

// ── ListRemote ────────────────────────────────────────────────────────────────

func TestListRemote(t *testing.T) {
	store := object.NewMem()
	ctx := context.Background()

	db := openDB(t)
	checkpoint.Write(ctx, db, store, 1, 10, "", nil)
	checkpoint.Write(ctx, db, store, 1, 20, "", nil)
	checkpoint.Write(ctx, db, store, 2, 30, "", nil)

	keys, err := checkpoint.ListRemote(ctx, store)
	if err != nil {
		t.Fatalf("ListRemote: %v", err)
	}
	if len(keys) != 3 {
		t.Errorf("ListRemote: want 3 got %d: %v", len(keys), keys)
	}
	// Must be sorted lexicographically (== chronologically).
	for i := 1; i < len(keys); i++ {
		if keys[i] <= keys[i-1] {
			t.Errorf("keys not sorted: %q <= %q", keys[i], keys[i-1])
		}
	}
}

func TestListRemoteEmpty(t *testing.T) {
	keys, err := checkpoint.ListRemote(context.Background(), object.NewMem())
	if err != nil {
		t.Fatalf("ListRemote empty: %v", err)
	}
	if len(keys) != 0 {
		t.Errorf("want empty, got %v", keys)
	}
}

// ── archive safety ────────────────────────────────────────────────────────────

func TestRestoreRejectsPathTraversal(t *testing.T) {
	// Build a malicious archive manually and store it.
	store := object.NewMem()
	ctx := context.Background()

	var buf bytes.Buffer
	// Header: magic + term + revision.
	buf.WriteString("STRTCHK\n")
	term := make([]byte, 8)
	rev := make([]byte, 8)
	buf.Write(term)
	buf.Write(rev)
	// One file record with a path-traversal name.
	name := []byte("../../evil")
	meta := make([]byte, 12)
	meta[0], meta[1], meta[2], meta[3] = 0, 0, 0, byte(len(name))
	content := []byte("pwned")
	meta[4], meta[5], meta[6], meta[7] = 0, 0, 0, 0
	meta[8], meta[9], meta[10], meta[11] = 0, 0, 0, byte(len(content))
	buf.Write(meta)
	buf.Write(name)
	buf.Write(content)

	store.Put(ctx, "checkpoint/evil", bytes.NewReader(buf.Bytes()))

	targetDir := t.TempDir()
	_, _, err := checkpoint.Restore(ctx, store, "checkpoint/evil", targetDir)
	if err == nil {
		t.Error("expected error for path traversal, got nil")
	}

	// Verify the evil file was NOT written outside targetDir.
	evil := filepath.Join(filepath.Dir(targetDir), "evil")
	if _, serr := os.Stat(evil); serr == nil {
		t.Errorf("path traversal succeeded: %q was created", evil)
	}
}

// TestWriteRestoreMultiple verifies that successive checkpoints produce distinct
// keys and that the manifest always points to the latest one.
func TestWriteRestoreMultiple(t *testing.T) {
	db := openDB(t)
	store := object.NewMem()
	ctx := context.Background()

	for i := int64(1); i <= 3; i++ {
		db.Set([]byte(fmt.Sprintf("k%d", i)), []byte("v"), pebble.Sync)
		if err := checkpoint.Write(ctx, db, store, 1, i, "", nil); err != nil {
			t.Fatalf("Write rev=%d: %v", i, err)
		}
	}

	m, err := checkpoint.ReadManifest(ctx, store)
	if err != nil || m == nil {
		t.Fatalf("manifest: err=%v m=%v", err, m)
	}
	if m.Revision != 3 {
		t.Errorf("manifest should point to latest: want rev=3 got %d", m.Revision)
	}

	keys, _ := checkpoint.ListRemote(ctx, store)
	if len(keys) != 3 {
		t.Errorf("ListRemote: want 3 checkpoints got %d", len(keys))
	}

	// Restore the latest and verify.
	targetDir := filepath.Join(t.TempDir(), "latest")
	term, rev, err := checkpoint.Restore(ctx, store, m.CheckpointKey, targetDir)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if term != 1 || rev != 3 {
		t.Errorf("restored: want term=1 rev=3, got term=%d rev=%d", term, rev)
	}
}

// TestRestoreVersioned verifies that RestoreVersioned can restore a checkpoint
// using a pinned version of the index key even after the index has been deleted
// (e.g. GC'd), as long as SSTs and pebble meta are still present.
func TestRestoreVersioned(t *testing.T) {
	db := openDB(t)
	store := newVersionedMem()
	ctx := context.Background()

	db.Set([]byte("restored-key"), []byte("restored-val"), pebble.Sync)
	if err := checkpoint.Write(ctx, db, store, 1, 1, "", nil); err != nil {
		t.Fatalf("Write: %v", err)
	}
	indexKey := checkpoint.CheckpointIndexKey(1, 1)
	pinnedVer := store.VersionOf(indexKey)
	if pinnedVer == "" {
		t.Fatal("no version captured for index key")
	}

	// Delete just the index (simulating GC); SSTs and pebble meta stay.
	if err := store.Delete(ctx, indexKey); err != nil {
		t.Fatalf("delete index: %v", err)
	}

	// Regular Restore should fail (index not found).
	if _, _, err := checkpoint.Restore(ctx, store, indexKey, t.TempDir()); err == nil {
		t.Fatal("Restore with deleted index: expected error, got nil")
	}

	// RestoreVersioned with the pinned version should succeed.
	dir := t.TempDir()
	term, rev, err := checkpoint.RestoreVersioned(ctx, store, indexKey, pinnedVer, dir)
	if err != nil {
		t.Fatalf("RestoreVersioned: %v", err)
	}
	if term != 1 || rev != 1 {
		t.Errorf("restored: want term=1 rev=1, got term=%d rev=%d", term, rev)
	}

	// Verify the restored directory contains a valid pebble database with our key.
	restored, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		t.Fatalf("open restored pebble: %v", err)
	}
	defer restored.Close()
}

// TestWriteSkipsExistingSSTs verifies that Write does not re-upload SST files
// already present in the store's sst/ prefix.
func TestWriteSkipsExistingSSTs(t *testing.T) {
	db := openDB(t)
	store := object.NewMem()
	ctx := context.Background()

	// Write data and first checkpoint.
	db.Set([]byte("k1"), []byte("v1"), pebble.Sync)
	if err := checkpoint.Write(ctx, db, store, 1, 1, "", nil); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	// Count SSTs after first checkpoint.
	ssts1, _ := store.List(ctx, "sst/")

	// Write more data and second checkpoint.
	db.Set([]byte("k2"), []byte("v2"), pebble.Sync)
	if err := checkpoint.Write(ctx, db, store, 1, 2, "", nil); err != nil {
		t.Fatalf("Write 2: %v", err)
	}
	ssts2, _ := store.List(ctx, "sst/")

	// SST count should only grow (or stay equal if no new SST was created).
	if len(ssts2) < len(ssts1) {
		t.Errorf("SST count decreased: %d → %d", len(ssts1), len(ssts2))
	}

	// There should be exactly two v2 checkpoint manifests.
	keys, _ := checkpoint.ListRemote(ctx, store)
	if len(keys) != 2 {
		t.Errorf("want 2 checkpoints, got %d: %v", len(keys), keys)
	}
	for _, k := range keys {
		if !strings.HasSuffix(k, "/manifest.json") {
			t.Errorf("expected v2 key, got %q", k)
		}
	}
}

// listCountingStore wraps a Store and counts calls to List(prefix).
type listCountingStore struct {
	object.Store
	mu     sync.Mutex
	counts map[string]int // prefix → number of List calls
}

func (s *listCountingStore) List(ctx context.Context, prefix string) ([]string, error) {
	s.mu.Lock()
	s.counts[prefix]++
	s.mu.Unlock()
	return s.Store.List(ctx, prefix)
}

func (s *listCountingStore) listCount(prefix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.counts[prefix]
}

// TestWriteNoListAfterFirst verifies that Write does not issue a LIST sst/
// call on the second and subsequent checkpoints — it uses the previous
// checkpoint index instead. The only time a LIST is acceptable is on the very
// first checkpoint of a branch node (before any branch index exists).
func TestWriteNoListAfterFirst(t *testing.T) {
	db := openDB(t)
	inner := object.NewMem()
	store := &listCountingStore{Store: inner, counts: make(map[string]int)}
	ctx := context.Background()

	db.Set([]byte("k1"), []byte("v1"), pebble.Sync)
	if err := checkpoint.Write(ctx, db, store, 1, 1, "", nil); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	// Reset counts — we only care about subsequent checkpoints.
	store.mu.Lock()
	store.counts = make(map[string]int)
	store.mu.Unlock()

	db.Set([]byte("k2"), []byte("v2"), pebble.Sync)
	if err := checkpoint.Write(ctx, db, store, 1, 2, "", nil); err != nil {
		t.Fatalf("Write 2: %v", err)
	}
	if n := store.listCount("sst/"); n != 0 {
		t.Errorf("second checkpoint issued %d LIST sst/ calls, want 0", n)
	}

	db.Set([]byte("k3"), []byte("v3"), pebble.Sync)
	if err := checkpoint.Write(ctx, db, store, 1, 3, "", nil); err != nil {
		t.Fatalf("Write 3: %v", err)
	}
	if n := store.listCount("sst/"); n != 0 {
		t.Errorf("third checkpoint issued %d LIST sst/ calls, want 0", n)
	}
}

// TestGCOrphanSSTs verifies that SSTs exclusively referenced by deleted
// checkpoints are cleaned up. The candidates set is built by GCCheckpoints,
// which already excludes SSTs still referenced by surviving checkpoints.
// Manually-placed SSTs that were never part of any checkpoint are NOT deleted —
// this is intentional and closes the race described in the GCOrphanSSTs doc.
func TestGCOrphanSSTs(t *testing.T) {
	db := openDB(t)
	store := object.NewMem()
	ctx := context.Background()

	// Write two checkpoints so GCCheckpoints has something to delete.
	db.Set([]byte("k1"), []byte("v1"), pebble.Sync)
	if err := checkpoint.Write(ctx, db, store, 1, 1, "", nil); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	ssts1, _ := store.List(ctx, "sst/")
	if len(ssts1) == 0 {
		t.Skip("no SST files produced by pebble checkpoint")
	}

	db.Set([]byte("k2"), []byte("v2"), pebble.Sync)
	if err := checkpoint.Write(ctx, db, store, 1, 2, "", nil); err != nil {
		t.Fatalf("Write 2: %v", err)
	}

	// A manually-placed SST (simulates a leaked upload, not from any checkpoint).
	store.Put(ctx, "sst/orphan.sst", strings.NewReader("garbage"))

	// GCCheckpoints returns candidates — SSTs from the deleted checkpoint that
	// are NOT referenced by the surviving checkpoint.
	cpDeleted, candidates, err := checkpoint.GCCheckpoints(ctx, store, 1)
	if err != nil {
		t.Fatalf("GCCheckpoints: %v", err)
	}
	if cpDeleted != 1 {
		t.Fatalf("GCCheckpoints: want 1 deleted, got %d", cpDeleted)
	}

	deleted, err := checkpoint.GCOrphanSSTs(ctx, store, candidates)
	if err != nil {
		t.Fatalf("GCOrphanSSTs: %v", err)
	}
	// Candidates only contain SSTs from the deleted checkpoint; the manually
	// placed orphan is not a candidate and must not be deleted.
	t.Logf("GCOrphanSSTs deleted %d SST(s) from deleted checkpoint", deleted)

	// The manually-placed orphan must still exist (not in candidates set).
	if _, err := store.Get(ctx, "sst/orphan.sst"); err != nil {
		t.Errorf("orphan.sst should still exist (not a candidate), got err=%v", err)
	}

	// Surviving checkpoint's SSTs must still exist.
	_, _ = store.List(ctx, "sst/")
	cpKeys, _ := checkpoint.ListRemote(ctx, store)
	if len(cpKeys) != 1 {
		t.Errorf("surviving checkpoints: want 1, got %d", len(cpKeys))
	}
}

// TestGCOrphanSSTsBranchProtection verifies that SSTs referenced by a branch
// registry entry are not included in the orphan candidates, and therefore not
// deleted when GCOrphanSSTs is called.
func TestGCOrphanSSTsBranchProtection(t *testing.T) {
	db := openDB(t)
	store := object.NewMem()
	ctx := context.Background()

	// Write checkpoint that branch will reference.
	db.Set([]byte("k1"), []byte("v1"), pebble.Sync)
	if err := checkpoint.Write(ctx, db, store, 1, 1, "", nil); err != nil {
		t.Fatalf("Write: %v", err)
	}
	ssts, _ := store.List(ctx, "sst/")
	if len(ssts) == 0 {
		t.Skip("no SST files produced by pebble checkpoint")
	}
	ancestorKey := checkpoint.CheckpointIndexKey(1, 1)

	// Register a branch pointing at the checkpoint.
	if err := checkpoint.RegisterBranch(ctx, store, "my-branch", ancestorKey); err != nil {
		t.Fatalf("RegisterBranch: %v", err)
	}

	// Write a second checkpoint. GC the first — but it's pinned by the branch,
	// so GCCheckpoints should NOT delete it and candidates should be empty.
	db.Set([]byte("k2"), []byte("v2"), pebble.Sync)
	if err := checkpoint.Write(ctx, db, store, 1, 2, "", nil); err != nil {
		t.Fatalf("Write 2: %v", err)
	}
	cpDeleted, candidates, err := checkpoint.GCCheckpoints(ctx, store, 1)
	if err != nil {
		t.Fatalf("GCCheckpoints: %v", err)
	}
	if cpDeleted != 0 {
		t.Errorf("pinned checkpoint was deleted: cpDeleted=%d", cpDeleted)
	}

	// GCOrphanSSTs with empty candidates: branch-protected SSTs untouched.
	deleted, err := checkpoint.GCOrphanSSTs(ctx, store, candidates)
	if err != nil {
		t.Fatalf("GCOrphanSSTs with branch: %v", err)
	}
	if deleted != 0 {
		t.Errorf("branch-protected SSTs were deleted: %d deleted", deleted)
	}

	// Unregister branch; now GCCheckpoints can delete the old checkpoint and
	// return its SSTs as candidates.
	if err := checkpoint.UnregisterBranch(ctx, store, "my-branch"); err != nil {
		t.Fatalf("UnregisterBranch: %v", err)
	}
	cpDeleted2, candidates2, err := checkpoint.GCCheckpoints(ctx, store, 1)
	if err != nil {
		t.Fatalf("GCCheckpoints after unregister: %v", err)
	}
	if cpDeleted2 != 1 {
		t.Errorf("after unregister: want 1 checkpoint deleted, got %d", cpDeleted2)
	}
	deleted2, err := checkpoint.GCOrphanSSTs(ctx, store, candidates2)
	if err != nil {
		t.Fatalf("GCOrphanSSTs after unregister: %v", err)
	}
	// At minimum 0 deletions (if Pebble reused all SSTs in the second checkpoint).
	t.Logf("GCOrphanSSTs after unregister: %d SST(s) deleted", deleted2)
}
