package store

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"

	"github.com/t4db/t4/pkg/object"
)

// TestReconcileVerifiedReuploadsDeletedSSTs pins the promotion fix: a node's
// registry can list SSTs the leader's checkpoint GC has deleted since (such
// as those the node restored from a checkpoint). Plain Reconcile trusts the
// registry; ReconcileVerified re-uploads them, so the new leader's first
// checkpoint does not reference missing SSTs.
func TestReconcileVerifiedReuploadsDeletedSSTs(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := object.NewMem()
	path := filepath.Join(dir, "000123.sst")
	if err := os.WriteFile(path, []byte("sst contents"), 0o600); err != nil {
		t.Fatal(err)
	}
	u := NewSSTUploader(store, dir)
	if err := u.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	key := u.Registry()["000123.sst"]
	if key == "" {
		t.Fatal("SST not registered after upload")
	}

	// The leader's GC deletes it as an orphan.
	if err := store.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if err := u.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, key); !errors.Is(err, object.ErrNotFound) {
		t.Fatalf("plain Reconcile changed something: err=%v", err)
	}

	if err := u.ReconcileVerified(ctx); err != nil {
		t.Fatal(err)
	}
	rc, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("SST not re-uploaded: %v", err)
	}
	_ = rc.Close()
}

// failingListStore fails List, as an unreachable store would.
type failingListStore struct{ object.Store }

func (failingListStore) List(context.Context, string) ([]string, error) {
	return nil, errors.New("list failed")
}

// TestReconcileVerifiedForgetsRegistryWhenUnverifiable: if the store cannot
// be listed, no registry entry can be trusted, so all are forgotten and
// checkpoints upload the SSTs they reference themselves.
func TestReconcileVerifiedForgetsRegistryWhenUnverifiable(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "000123.sst"), []byte("sst contents"), 0o600); err != nil {
		t.Fatal(err)
	}
	mem := object.NewMem()
	u := NewSSTUploader(mem, dir)
	if err := u.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	u.store = failingListStore{mem}
	if err := u.ReconcileVerified(ctx); err == nil {
		t.Fatal("ReconcileVerified succeeded although the store could not be listed")
	}
	if n := len(u.Registry()); n != 0 {
		t.Errorf("registry keeps %d entries it could not verify", n)
	}
}

// TestRegistryForgetsDeletedTables pins that tables Pebble deletes leave the
// registry, so the leader's SST sweep does not treat them as live forever.
func TestRegistryForgetsDeletedTables(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "000123.sst")
	if err := os.WriteFile(path, []byte("sst contents"), 0o600); err != nil {
		t.Fatal(err)
	}
	u := NewSSTUploader(object.NewMem(), dir)
	if err := u.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := u.Registry()["000123.sst"]; !ok {
		t.Fatal("SST not registered after upload")
	}

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	u.EventListener().TableDeleted(pebble.TableDeleteInfo{Path: path})
	if key, ok := u.Registry()["000123.sst"]; ok {
		t.Fatalf("deleted table still registered as %q", key)
	}
}

// deletingStore removes the local table while its upload is in flight, as a
// Pebble compaction finishing mid-upload would.
type deletingStore struct {
	object.Store
	path string
}

func (s deletingStore) Put(ctx context.Context, key string, r io.Reader) error {
	if err := s.Store.Put(ctx, key, r); err != nil {
		return err
	}
	return os.Remove(s.path)
}

// TestRegistrySkipsTableDeletedDuringUpload pins that a table Pebble deletes
// while it uploads is not left registered: its TableDeleted event fired
// before the upload registered it.
func TestRegistrySkipsTableDeletedDuringUpload(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "000123.sst")
	if err := os.WriteFile(path, []byte("sst contents"), 0o600); err != nil {
		t.Fatal(err)
	}
	u := NewSSTUploader(deletingStore{Store: object.NewMem(), path: path}, dir)
	if err := u.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if key, ok := u.Registry()["000123.sst"]; ok {
		t.Fatalf("table deleted during upload still registered as %q", key)
	}
}
