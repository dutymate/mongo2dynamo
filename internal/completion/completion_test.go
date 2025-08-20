package completion

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
)

func TestGenerateCompletion(t *testing.T) {
	// Create a root command for testing.
	rootCmd := &cobra.Command{Use: "test"}
	rootCmd.AddCommand(&cobra.Command{Use: "subcommand"})

	tests := []struct {
		name        string
		shell       string
		expectError bool
	}{
		{
			name:        "bash completion",
			shell:       "bash",
			expectError: false,
		},
		{
			name:        "zsh completion",
			shell:       "zsh",
			expectError: false,
		},
		{
			name:        "fish completion",
			shell:       "fish",
			expectError: false,
		},
		{
			name:        "powershell completion",
			shell:       "powershell",
			expectError: false,
		},
		{
			name:        "invalid shell",
			shell:       "invalid",
			expectError: false, // Help() doesn't return an error.
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := GenerateCompletion(rootCmd, tt.shell)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGetSupportedShells(t *testing.T) {
	shells := GetSupportedShells()
	expectedShells := []string{"bash", "zsh", "fish", "powershell"}

	assert.ElementsMatch(t, expectedShells, shells)
	assert.Len(t, shells, 4)
}
