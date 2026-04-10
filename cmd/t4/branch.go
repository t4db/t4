package main

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/t4db/t4"
	"github.com/t4db/t4/internal/checkpoint"
	"github.com/t4db/t4/pkg/object"
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
		src           *s3Flags
		branchID      string
		checkpointKey string
	)
	cmd := &cobra.Command{
		Use:   "fork",
		Short: "Register a new branch and print the checkpoint key to use with 't4 run --branch-checkpoint'",
		Long: `Register a new branch in the source store and print the checkpoint index key.

By default the branch is forked from the latest committed checkpoint revision.
Use --checkpoint to fork from a specific older revision instead.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			sourceStore, err := object.NewS3StoreFromConfig(cmd.Context(), src.config())
			if err != nil {
				return fmt.Errorf("init source S3: %w", err)
			}
			var cpKey string
			if checkpointKey != "" {
				// Fork from a specific checkpoint rather than the latest.
				cp := checkpoint.New(logrus.StandardLogger())
				if err := cp.RegisterBranch(cmd.Context(), sourceStore, branchID, checkpointKey); err != nil {
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
	src = addS3Flags(cmd, true)
	cmd.Flags().StringVar(&branchID, "branch-id", "", "unique identifier for this branch (required) (env: T4_BRANCH_ID)")
	cmd.Flags().StringVar(&checkpointKey, "checkpoint", "", "fork from this specific checkpoint key instead of the latest revision")
	cmd.MarkFlagRequired("branch-id")
	prependPreRunE(cmd, func(cmd *cobra.Command, _ []string) error {
		return applyEnvVars(cmd, map[string]string{"branch-id": "T4_BRANCH_ID"})
	})
	return cmd
}

func branchUnforkCmd() *cobra.Command {
	var (
		src      *s3Flags
		branchID string
	)
	cmd := &cobra.Command{
		Use:   "unfork",
		Short: "Unregister a branch, allowing GC to reclaim its protected SSTs",
		RunE: func(cmd *cobra.Command, _ []string) error {
			sourceStore, err := object.NewS3StoreFromConfig(cmd.Context(), src.config())
			if err != nil {
				return fmt.Errorf("init source S3: %w", err)
			}
			return t4.Unfork(cmd.Context(), sourceStore, branchID)
		},
	}
	src = addS3Flags(cmd, true)
	cmd.Flags().StringVar(&branchID, "branch-id", "", "unique identifier for this branch (required) (env: T4_BRANCH_ID)")
	cmd.MarkFlagRequired("branch-id")
	prependPreRunE(cmd, func(cmd *cobra.Command, _ []string) error {
		return applyEnvVars(cmd, map[string]string{"branch-id": "T4_BRANCH_ID"})
	})
	return cmd
}
