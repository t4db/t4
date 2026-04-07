package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/t4db/t4/internal/checkpoint"
)

func statusCmd() *cobra.Command {
	var (
		bucket   string
		prefix   string
		endpoint string
	)
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show the S3 storage status of a t4 node",
		Long: `Display a snapshot of the node's S3 storage: the latest checkpoint,
total checkpoint and WAL segment counts, and any registered branch forks.

This command does not require a running node — it reads directly from S3.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			store, err := newS3Store(cmd.Context(), bucket, prefix, endpoint)
			if err != nil {
				return fmt.Errorf("init S3: %w", err)
			}
			ctx := cmd.Context()

			manifest, err := checkpoint.ReadManifest(ctx, store)
			if err != nil {
				return fmt.Errorf("read manifest: %w", err)
			}

			checkpointKeys, err := checkpoint.ListRemote(ctx, store)
			if err != nil {
				return fmt.Errorf("list checkpoints: %w", err)
			}

			walKeys, err := store.List(ctx, "wal/")
			if err != nil {
				return fmt.Errorf("list WAL segments: %w", err)
			}

			branches, err := checkpoint.ReadBranchEntries(ctx, store)
			if err != nil {
				return fmt.Errorf("read branch entries: %w", err)
			}

			fmt.Printf("S3 status  s3://%s/%s\n\n", bucket, prefix)

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
	cmd.Flags().StringVar(&bucket, "s3-bucket", "", "S3 bucket to inspect (required)")
	cmd.Flags().StringVar(&prefix, "s3-prefix", "", "key prefix inside the S3 bucket")
	cmd.Flags().StringVar(&endpoint, "s3-endpoint", "", "custom S3 endpoint URL")
	cmd.MarkFlagRequired("s3-bucket")
	return cmd
}
