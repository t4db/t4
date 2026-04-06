// Command strata runs a Strata node and exposes it as an etcd v3 gRPC endpoint.
package main

import (
	"os"

	"github.com/spf13/cobra"
)

func main() {
	if err := rootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "strata",
		Short: "S3-durable kine-compatible datastore",
	}
	root.AddCommand(runCmd())
	root.AddCommand(branchCmd())
	root.AddCommand(restoreCmd())
	root.AddCommand(gcCmd())
	root.AddCommand(statusCmd())
	return root
}
