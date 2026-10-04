package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

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
