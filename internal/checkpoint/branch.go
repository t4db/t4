package checkpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/strata-db/strata/pkg/object"
)

// BranchEntry is stored at "branches/<id>" in the source object store.
// It records the source checkpoint that the branch was created from so that
// GCOrphanSSTs can protect the SST files the branch still needs.
type BranchEntry struct {
	// AncestorCheckpointKey is the v2 CheckpointIndex key in this (source) store
	// that the branch was bootstrapped from.
	// GC reads this index to find which SST files must be retained.
	AncestorCheckpointKey string `json:"ancestor_checkpoint_key"`
}

// RegisterBranch writes a branch registry entry to the source store.
// Call before starting the branch node to protect source SSTs from GC.
func RegisterBranch(ctx context.Context, store object.Store, id, ancestorCheckpointKey string) error {
	entry := BranchEntry{AncestorCheckpointKey: ancestorCheckpointKey}
	b, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if err := store.Put(ctx, branchKey(id), bytes.NewReader(b)); err != nil {
		return fmt.Errorf("checkpoint: register branch %q: %w", id, err)
	}
	return nil
}

// UnregisterBranch removes the branch registry entry from the source store.
// Call when the branch is decommissioned and its source SSTs are no longer needed.
func UnregisterBranch(ctx context.Context, store object.Store, id string) error {
	if err := store.Delete(ctx, branchKey(id)); err != nil {
		return fmt.Errorf("checkpoint: unregister branch %q: %w", id, err)
	}
	return nil
}

// ReadBranchEntries returns all branch registry entries in the store.
func ReadBranchEntries(ctx context.Context, store object.Store) (map[string]BranchEntry, error) {
	keys, err := store.List(ctx, "branches/")
	if err != nil {
		return nil, fmt.Errorf("checkpoint: list branches: %w", err)
	}
	entries := make(map[string]BranchEntry, len(keys))
	for _, k := range keys {
		rc, err := store.Get(ctx, k)
		if err == object.ErrNotFound {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("checkpoint: read branch %q: %w", k, err)
		}
		var e BranchEntry
		decErr := json.NewDecoder(rc).Decode(&e)
		rc.Close()
		if decErr != nil {
			return nil, fmt.Errorf("checkpoint: decode branch %q: %w", k, decErr)
		}
		id := strings.TrimPrefix(k, "branches/")
		entries[id] = e
	}
	return entries, nil
}

func branchKey(id string) string { return "branches/" + id }
