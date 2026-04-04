package checkpoint_test

import (
	"context"
	"testing"

	"github.com/strata-db/strata/internal/checkpoint"
	"github.com/strata-db/strata/pkg/object"
)

func TestRegisterUnregisterBranch(t *testing.T) {
	store := object.NewMem()
	ctx := context.Background()

	if err := checkpoint.RegisterBranch(ctx, store, "b1", "checkpoint/0001/0000000000000000100/manifest.json"); err != nil {
		t.Fatalf("RegisterBranch: %v", err)
	}
	if err := checkpoint.RegisterBranch(ctx, store, "b2", "checkpoint/0001/0000000000000000200/manifest.json"); err != nil {
		t.Fatalf("RegisterBranch b2: %v", err)
	}

	entries, err := checkpoint.ReadBranchEntries(ctx, store)
	if err != nil {
		t.Fatalf("ReadBranchEntries: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("want 2 entries, got %d", len(entries))
	}
	if entries["b1"].AncestorCheckpointKey != "checkpoint/0001/0000000000000000100/manifest.json" {
		t.Errorf("b1 key: got %q", entries["b1"].AncestorCheckpointKey)
	}

	if err := checkpoint.UnregisterBranch(ctx, store, "b1"); err != nil {
		t.Fatalf("UnregisterBranch: %v", err)
	}
	entries, _ = checkpoint.ReadBranchEntries(ctx, store)
	if len(entries) != 1 {
		t.Errorf("after unregister: want 1 entry, got %d", len(entries))
	}
	if _, ok := entries["b1"]; ok {
		t.Error("b1 should be gone after unregister")
	}
}

func TestReadBranchEntriesEmpty(t *testing.T) {
	entries, err := checkpoint.ReadBranchEntries(context.Background(), object.NewMem())
	if err != nil {
		t.Fatalf("ReadBranchEntries empty: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("want empty, got %d entries", len(entries))
	}
}
