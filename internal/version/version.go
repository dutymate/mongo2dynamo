package version

import "fmt"

var (
	// These variables are set during build time using ldflags.
	Version   = "dev"     // Default to "dev" if not set during build.
	GitCommit = "none"    // Default to "none" if not set during build.
	BuildDate = "unknown" // Default to "unknown" if not set during build.
)

// GetVersionInfo returns formatted version information.
func GetVersionInfo() string {
	return fmt.Sprintf("Version: %s\nGit Commit: %s\nBuild Date: %s\n", Version, GitCommit, BuildDate)
}
