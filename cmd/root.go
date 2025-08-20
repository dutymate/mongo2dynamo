package cmd

import (
	"os"

	"github.com/spf13/cobra"

	"mongo2dynamo/cmd/apply"
	"mongo2dynamo/cmd/completion"
	"mongo2dynamo/cmd/plan"
	"mongo2dynamo/cmd/version"
)

var rootCmd = &cobra.Command{
	Use:           "mongo2dynamo",
	Short:         "CLI tool to migrate data from MongoDB to DynamoDB",
	Long:          `A command-line tool to copy all documents from MongoDB into AWS DynamoDB.`,
	SilenceErrors: true,
	SilenceUsage:  true,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		rootCmd.PrintErrf("Error: %v\n", err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.CompletionOptions.DisableDefaultCmd = true

	rootCmd.AddCommand(apply.ApplyCmd)
	rootCmd.AddCommand(plan.PlanCmd)
	rootCmd.AddCommand(version.VersionCmd)
	rootCmd.AddCommand(completion.CompletionCmd)
}
