package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/t4db/t4"
	"github.com/t4db/t4/internal/checkpoint"
)

func branchCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "branch",
		Short: "Manage database branches",
	}
	cmd.AddCommand(branchForkCmd())
	cmd.AddCommand(branchUnforkCmd())
	return cmd
}

func branchForkCmd() *cobra.Command {
	var (
		sourceBucket   string
		sourcePrefix   string
		sourceEndpoint string
		branchID       string
		checkpointKey  string
	)
	cmd := &cobra.Command{
		Use:   "fork",
		Short: "Register a new branch and print the checkpoint key to use with 't4 run --branch-checkpoint'",
		Long: `Register a new branch in the source store and print the checkpoint index key.

By default the branch is forked from the latest committed checkpoint revision.
Use --checkpoint to fork from a specific older revision instead.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			sourceStore, err := newS3Store(cmd.Context(), sourceBucket, sourcePrefix, sourceEndpoint)
			if err != nil {
				return fmt.Errorf("init source S3: %w", err)
			}
			var cpKey string
			if checkpointKey != "" {
				// Fork from a specific checkpoint rather than the latest.
				if err := checkpoint.RegisterBranch(cmd.Context(), sourceStore, branchID, checkpointKey); err != nil {
					return fmt.Errorf("register branch: %w", err)
				}
				cpKey = checkpointKey
			} else {
				cpKey, err = t4.Fork(cmd.Context(), sourceStore, branchID)
				if err != nil {
					return err
				}
			}
			fmt.Println(cpKey)
			return nil
		},
	}
	cmd.Flags().StringVar(&sourceBucket, "source-bucket", "", "S3 bucket of the source node (required)")
	cmd.Flags().StringVar(&sourcePrefix, "source-prefix", "", "S3 key prefix of the source node")
	cmd.Flags().StringVar(&sourceEndpoint, "source-endpoint", "", "custom S3 endpoint for the source store")
	cmd.Flags().StringVar(&branchID, "branch-id", "", "unique identifier for this branch (required)")
	cmd.Flags().StringVar(&checkpointKey, "checkpoint", "", "fork from this specific checkpoint key instead of the latest revision")
	cmd.MarkFlagRequired("source-bucket")
	cmd.MarkFlagRequired("branch-id")
	return cmd
}

func branchUnforkCmd() *cobra.Command {
	var (
		sourceBucket   string
		sourcePrefix   string
		sourceEndpoint string
		branchID       string
	)
	cmd := &cobra.Command{
		Use:   "unfork",
		Short: "Unregister a branch, allowing GC to reclaim its protected SSTs",
		RunE: func(cmd *cobra.Command, _ []string) error {
			sourceStore, err := newS3Store(cmd.Context(), sourceBucket, sourcePrefix, sourceEndpoint)
			if err != nil {
				return fmt.Errorf("init source S3: %w", err)
			}
			return t4.Unfork(cmd.Context(), sourceStore, branchID)
		},
	}
	cmd.Flags().StringVar(&sourceBucket, "source-bucket", "", "S3 bucket of the source node (required)")
	cmd.Flags().StringVar(&sourcePrefix, "source-prefix", "", "S3 key prefix of the source node")
	cmd.Flags().StringVar(&sourceEndpoint, "source-endpoint", "", "custom S3 endpoint for the source store")
	cmd.Flags().StringVar(&branchID, "branch-id", "", "unique identifier for this branch (required)")
	cmd.MarkFlagRequired("source-bucket")
	cmd.MarkFlagRequired("branch-id")
	return cmd
}
