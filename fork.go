package strata

import (
	"context"
	"fmt"

	"github.com/strata-db/strata/internal/checkpoint"
	"github.com/strata-db/strata/pkg/object"
)

// Fork registers a new branch in sourceStore under branchID, pinning the
// latest checkpoint so GC will not delete its SST files. It returns the
// checkpoint key to use as BranchPoint.CheckpointKey when starting the branch
// node.
//
// The branch is forked from the latest committed checkpoint revision in
// sourceStore. If you need a branch at an older revision, pass that
// checkpoint's index key directly to BranchPoint.CheckpointKey and call
// checkpoint.RegisterBranch yourself.
//
// Call Fork before starting the branch node. When the branch is decommissioned,
// call Unfork to allow GC to reclaim the protected SSTs.
func Fork(ctx context.Context, sourceStore object.Store, branchID string) (checkpointKey string, err error) {
	manifest, err := checkpoint.ReadManifest(ctx, sourceStore)
	if err != nil {
		return "", fmt.Errorf("fork: read source manifest: %w", err)
	}
	if manifest == nil {
		return "", fmt.Errorf("fork: no checkpoint found in source store")
	}
	if err := checkpoint.RegisterBranch(ctx, sourceStore, branchID, manifest.CheckpointKey); err != nil {
		return "", fmt.Errorf("fork: register branch: %w", err)
	}
	return manifest.CheckpointKey, nil
}

// Unfork removes the branch registry entry from sourceStore. Call when the
// branch is decommissioned to allow GC to reclaim SSTs that are no longer
// referenced by any live checkpoint.
func Unfork(ctx context.Context, sourceStore object.Store, branchID string) error {
	if err := checkpoint.UnregisterBranch(ctx, sourceStore, branchID); err != nil {
		return fmt.Errorf("unfork: %w", err)
	}
	return nil
}
