package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "mongo2dynamo",
	Short: "CLI tool to migrate data from MongoDB to DynamoDB",
	Long:  `A command-line tool to copy all documents from MongoDB into AWS DynamoDB.`,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
