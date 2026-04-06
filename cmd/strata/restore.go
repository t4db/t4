package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/strata-db/strata/internal/checkpoint"
)

func restoreCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "restore",
		Short: "Inspect and restore from S3 checkpoints",
	}
	cmd.AddCommand(restoreListCmd())
	cmd.AddCommand(restoreCheckpointCmd())
	return cmd
}

func restoreListCmd() *cobra.Command {
	var (
		bucket   string
		prefix   string
		endpoint string
	)
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List checkpoints available in S3",
		RunE: func(cmd *cobra.Command, _ []string) error {
			store, err := newS3Store(cmd.Context(), bucket, prefix, endpoint)
			if err != nil {
				return fmt.Errorf("init S3: %w", err)
			}
			ctx := cmd.Context()
			keys, err := checkpoint.ListRemote(ctx, store)
			if err != nil {
				return fmt.Errorf("list checkpoints: %w", err)
			}
			if len(keys) == 0 {
				fmt.Println("No checkpoints found.")
				return nil
			}
			manifest, _ := checkpoint.ReadManifest(ctx, store)
			fmt.Printf("%-72s  %10s  %6s\n", "CHECKPOINT", "REVISION", "TERM")
			for _, key := range keys {
				idx, err := checkpoint.ReadCheckpointIndex(ctx, store, key)
				suffix := ""
				if manifest != nil && manifest.CheckpointKey == key {
					suffix = "  (latest)"
				}
				if err != nil {
					fmt.Printf("%-72s  %10s  %6s%s\n", key, "?", "?", suffix)
				} else {
					fmt.Printf("%-72s  %10d  %6d%s\n", key, idx.Revision, idx.Term, suffix)
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&bucket, "s3-bucket", "", "S3 bucket to list (required)")
	cmd.Flags().StringVar(&prefix, "s3-prefix", "", "key prefix inside the S3 bucket")
	cmd.Flags().StringVar(&endpoint, "s3-endpoint", "", "custom S3 endpoint URL")
	cmd.MarkFlagRequired("s3-bucket")
	return cmd
}

func restoreCheckpointCmd() *cobra.Command {
	var (
		bucket        string
		prefix        string
		endpoint      string
		checkpointKey string
		dataDir       string
	)
	cmd := &cobra.Command{
		Use:   "checkpoint",
		Short: "Download a checkpoint to a local data directory",
		Long: `Download an S3 checkpoint to a local data directory so that
'strata run' can start from it on the next boot.

By default the latest checkpoint is used. Pass --checkpoint to restore
from a specific earlier revision (use 'strata restore list' to find keys).

After this command succeeds, start the node with:

  strata run --data-dir <data-dir> [--s3-bucket <bucket>] ...

Configure --s3-bucket to a different prefix than the source so the
restored node writes to its own namespace and does not overwrite the
original cluster's data. To stay at the restored revision without
replaying newer WAL from S3, omit --s3-bucket entirely.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			pebbleDir := filepath.Join(dataDir, "db")
			if _, err := os.Stat(pebbleDir); err == nil {
				return fmt.Errorf("data directory %q already contains a Pebble database; remove it first", pebbleDir)
			}

			store, err := newS3Store(cmd.Context(), bucket, prefix, endpoint)
			if err != nil {
				return fmt.Errorf("init S3: %w", err)
			}
			ctx := cmd.Context()

			key := checkpointKey
			if key == "" {
				manifest, err := checkpoint.ReadManifest(ctx, store)
				if err != nil {
					return fmt.Errorf("read manifest: %w", err)
				}
				if manifest == nil {
					return fmt.Errorf("no checkpoints found in s3://%s/%s", bucket, prefix)
				}
				key = manifest.CheckpointKey
				logrus.Infof("using latest checkpoint: %s (rev=%d)", key, manifest.Revision)
			}

			logrus.Infof("restoring checkpoint %q → %s", key, pebbleDir)
			term, rev, err := checkpoint.Restore(ctx, store, key, pebbleDir)
			if err != nil {
				return fmt.Errorf("restore checkpoint: %w", err)
			}

			fmt.Printf("Restored checkpoint\n")
			fmt.Printf("  key:       %s\n", key)
			fmt.Printf("  revision:  %d\n", rev)
			fmt.Printf("  term:      %d\n", term)
			fmt.Printf("  data-dir:  %s\n", dataDir)
			fmt.Printf("\nStart the restored node:\n")
			fmt.Printf("  strata run --data-dir %s [--s3-bucket <new-bucket>] --listen 0.0.0.0:3379\n", dataDir)
			return nil
		},
	}
	cmd.Flags().StringVar(&bucket, "s3-bucket", "", "S3 bucket containing the checkpoint (required)")
	cmd.Flags().StringVar(&prefix, "s3-prefix", "", "key prefix inside the S3 bucket")
	cmd.Flags().StringVar(&endpoint, "s3-endpoint", "", "custom S3 endpoint URL")
	cmd.Flags().StringVar(&checkpointKey, "checkpoint", "", "checkpoint key to restore (default: latest; use 'strata restore list' to find keys)")
	cmd.Flags().StringVar(&dataDir, "data-dir", "", "local directory to restore into (required; must not already contain a Pebble database)")
	cmd.MarkFlagRequired("s3-bucket")
	cmd.MarkFlagRequired("data-dir")
	return cmd
}
