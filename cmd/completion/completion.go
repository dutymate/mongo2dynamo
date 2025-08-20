package completion

import (
	"mongo2dynamo/internal/completion"

	"github.com/spf13/cobra"
)

var CompletionCmd = &cobra.Command{
	Use:   "completion [bash|zsh|fish|powershell]",
	Short: "Generate shell completion script",
	Long: `Generate shell completion script for mongo2dynamo.

The completion script supports bash, zsh, fish, and powershell.
To load completions in your current shell session:

Bash:
  $ source <(mongo2dynamo completion bash)

Zsh:
  $ source <(mongo2dynamo completion zsh)

Fish:
  $ mongo2dynamo completion fish | source

PowerShell:
  PS> mongo2dynamo completion powershell | Out-String | Invoke-Expression

To load completions for every new session, write to a file and source in your shell's config file e.g. ~/.bashrc or ~/.zshrc.`,
	ValidArgs: completion.GetSupportedShells(),
	Args:      cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
	RunE: func(cmd *cobra.Command, args []string) error {
		shell := args[0]
		return completion.GenerateCompletion(cmd, shell)
	},
}
