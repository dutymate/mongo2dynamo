package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	// These variables are set during build time using ldflags.
	version   = "dev"     // Default to "dev" if not set during build.
	gitCommit = "none"    // Default to "none" if not set during build.
	buildDate = "unknown" // Default to "unknown" if not set during build.
)

// versionCmd represents the version command.
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number",
	Long:  `Print the version number of mongo2dynamo`,
	Run: func(_ *cobra.Command, _ []string) {
		fmt.Printf("Version: %s\nGit Commit: %s\nBuild Date: %s\n", version, gitCommit, buildDate)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
