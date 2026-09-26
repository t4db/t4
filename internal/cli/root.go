package cli

import "github.com/spf13/cobra"

func NewRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "t4",
		Short: "S3-durable kine-compatible datastore",
	}
	root.AddCommand(runCmd())
	root.AddCommand(branchCmd())
	root.AddCommand(restoreCmd())
	root.AddCommand(gcCmd())
	root.AddCommand(statusCmd())
	root.AddCommand(inspectCmd())
	root.AddCommand(replicateCmd())
	root.AddCommand(versionCmd())
	return root
}
