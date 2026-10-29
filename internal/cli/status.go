package cli

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/t4db/t4/internal/checkpoint"
)

func statusCmd() *cobra.Command {
	var (
		s3  *objectStoreFlags
		enc *objectStoreEncryptionFlags
	)
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show the S3 storage status of a t4 node",
		Long: `Display a snapshot of the node's S3 storage: the latest checkpoint,
total checkpoint and WAL segment counts, and any registered branch forks.

This command does not require a running node — it reads directly from S3.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			store, err := newObjectStoreForCommand(cmd.Context(), s3, enc)
			if err != nil {
				return err
			}
			ctx := cmd.Context()

			cp := checkpoint.New(logrus.StandardLogger())

			manifest, err := cp.ReadManifest(ctx, store)
			if err != nil {
				return fmt.Errorf("read manifest: %w", err)
			}

			checkpointKeys, err := cp.ListRemote(ctx, store)
			if err != nil {
				return fmt.Errorf("list checkpoints: %w", err)
			}

			walKeys, err := store.List(ctx, "wal/")
			if err != nil {
				return fmt.Errorf("list WAL segments: %w", err)
			}

			branches, err := cp.ReadBranchEntries(ctx, store)
			if err != nil {
				return fmt.Errorf("read branch entries: %w", err)
			}

			fmt.Printf("Object store status  %s\n\n", s3.uri())

			fmt.Printf("Latest checkpoint\n")
			if manifest == nil {
				fmt.Printf("  (none — node has not written a checkpoint yet)\n")
			} else {
				fmt.Printf("  key:       %s\n", manifest.CheckpointKey)
				fmt.Printf("  revision:  %d\n", manifest.Revision)
				fmt.Printf("  term:      %d\n", manifest.Term)
			}

			fmt.Printf("\nStorage objects\n")
			fmt.Printf("  checkpoints: %d\n", len(checkpointKeys))
			fmt.Printf("  WAL segments: %d\n", len(walKeys))

			fmt.Printf("\nBranch forks\n")
			if len(branches) == 0 {
				fmt.Printf("  (none)\n")
			} else {
				for id, entry := range branches {
					fmt.Printf("  %-30s  pinned at: %s\n", id, entry.AncestorCheckpointKey)
				}
			}

			return nil
		},
	}
	s3 = addObjectStoreFlags(cmd, true)
	enc = addObjectStoreEncryptionFlags(cmd)
	return cmd
}
