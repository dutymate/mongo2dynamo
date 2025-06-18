package version

import (
	"fmt"

	"mongo2dynamo/pkg/version"

	"github.com/spf13/cobra"
)

// VersionCmd represents the version command.
var VersionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number",
	Long:  `Print the version number of mongo2dynamo`,
	Run:   runVersion,
}

func runVersion(_ *cobra.Command, _ []string) {
	fmt.Print(version.GetVersionInfo())
}
