package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var rootCmd = &cobra.Command{
	Use:   "mongo2dynamo",
	Short: "CLI tool to migrate data from MongoDB to DynamoDB",
	Long:  `A command-line tool to copy all documents from MongoDB into AWS DynamoDB.`,
}

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().Bool("auto-approve", false, "Skip confirmation prompt")
	rootCmd.CompletionOptions.DisableDefaultCmd = true
	bindPersistentFlags()
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

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
