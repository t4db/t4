package main

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/t4db/t4/internal/checkpoint"
	"github.com/t4db/t4/internal/wal"
	"github.com/t4db/t4/pkg/object"
)

func gcCmd() *cobra.Command {
	var (
		s3   *s3Flags
		keep int
	)
	cmd := &cobra.Command{
		Use:   "gc",
		Short: "Garbage-collect old checkpoints, orphan SSTs, and stale WAL segments from S3",
		Long: `Remove objects from S3 that are no longer needed for recovery or active branches.

Three passes are performed in order:
  1. Checkpoint GC  — deletes old checkpoint archives, keeping the most recent --keep.
                      Checkpoints pinned by active branch registrations are never deleted.
  2. Orphan SST GC  — deletes SST files that were exclusively referenced by deleted checkpoints.
  3. WAL segment GC — deletes WAL segments whose entire revision range is covered by the
                      latest surviving checkpoint.

Run this periodically (e.g. once a day) to reclaim S3 storage.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			store, err := object.NewS3StoreFromConfig(cmd.Context(), s3.config())
			if err != nil {
				return fmt.Errorf("init S3: %w", err)
			}
			ctx := cmd.Context()

			cp := checkpoint.New(logrus.StandardLogger())

			// Pass 1: checkpoint GC.
			deletedCPs, orphanSSTs, err := cp.GCCheckpoints(ctx, store, keep)
			if err != nil {
				return fmt.Errorf("checkpoint gc: %w", err)
			}
			logrus.Infof("checkpoint gc: deleted %d checkpoint(s), %d orphan SST candidate(s)", deletedCPs, len(orphanSSTs))

			// Pass 2: orphan SST GC.
			deletedSSTs, err := cp.GCOrphanSSTs(ctx, store, orphanSSTs)
			if err != nil {
				return fmt.Errorf("orphan sst gc: %w", err)
			}
			logrus.Infof("orphan sst gc: deleted %d SST file(s)", deletedSSTs)

			// Pass 3: WAL segment GC — use the current manifest revision as the safe horizon.
			manifest, err := cp.ReadManifest(ctx, store)
			if err != nil {
				return fmt.Errorf("read manifest: %w", err)
			}
			var deletedWAL int
			if manifest != nil {
				deletedWAL, err = wal.GCSegments(ctx, store, manifest.Revision, logrus.StandardLogger())
				if err != nil {
					return fmt.Errorf("wal gc: %w", err)
				}
				logrus.Infof("wal gc: deleted %d segment(s) covered by checkpoint rev=%d", deletedWAL, manifest.Revision)
			} else {
				logrus.Info("wal gc: no manifest found, skipping WAL segment GC")
			}

			fmt.Printf("GC complete\n")
			fmt.Printf("  checkpoints deleted: %d\n", deletedCPs)
			fmt.Printf("  orphan SSTs deleted: %d\n", deletedSSTs)
			fmt.Printf("  WAL segments deleted: %d\n", deletedWAL)
			return nil
		},
	}
	s3 = addS3Flags(cmd, true)
	cmd.Flags().IntVar(&keep, "keep", 3, "number of most-recent checkpoints to retain")
	return cmd
}
