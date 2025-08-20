package completion

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// GenerateCompletion generates shell completion script for the given shell.
func GenerateCompletion(cmd *cobra.Command, shell string) error {
	switch shell {
	case "bash":
		if err := cmd.Root().GenBashCompletion(os.Stdout); err != nil {
			return fmt.Errorf("failed to generate bash completion: %w", err)
		}
		return nil
	case "zsh":
		if err := cmd.Root().GenZshCompletion(os.Stdout); err != nil {
			return fmt.Errorf("failed to generate zsh completion: %w", err)
		}
		return nil
	case "fish":
		if err := cmd.Root().GenFishCompletion(os.Stdout, true); err != nil {
			return fmt.Errorf("failed to generate fish completion: %w", err)
		}
		return nil
	case "powershell":
		if err := cmd.Root().GenPowerShellCompletion(os.Stdout); err != nil {
			return fmt.Errorf("failed to generate powershell completion: %w", err)
		}
		return nil
	default:
		if err := cmd.Help(); err != nil {
			return fmt.Errorf("failed to display help: %w", err)
		}
		return nil
	}
}

// GetSupportedShells returns the list of supported shell types.
func GetSupportedShells() []string {
	return []string{"bash", "zsh", "fish", "powershell"}
}
