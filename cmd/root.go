package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"mongo2dynamo/cmd/apply"
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

func bindPersistentFlags() {
	if err := viper.BindPFlag("auto-approve", rootCmd.PersistentFlags().Lookup("auto-approve")); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to bind auto-approve flag: %v\n", err)
		os.Exit(1)
	}
}

func initConfig() {
	viper.AutomaticEnv()
}

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().Bool("auto-approve", false, "Skip confirmation prompt")
	rootCmd.CompletionOptions.DisableDefaultCmd = true
	bindPersistentFlags()

	rootCmd.AddCommand(apply.ApplyCmd)
	rootCmd.AddCommand(plan.PlanCmd)
	rootCmd.AddCommand(version.VersionCmd)
}
