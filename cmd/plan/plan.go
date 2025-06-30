package plan

import (
	"fmt"
	"mongo2dynamo/internal/config"
	"mongo2dynamo/internal/flags"
	"mongo2dynamo/internal/migrator"
	"mongo2dynamo/internal/mongo"

	"github.com/spf13/cobra"
)

// PlanCmd represents the plan command.
var PlanCmd = &cobra.Command{
	Use:   "plan",
	Short: "Show migration plan",
	Long: `Show a preview of the migration plan without executing it.
This command will display what would be migrated without making any changes.`,
	RunE: runPlan,
}

func runPlan(cmd *cobra.Command, _ []string) error {
	// Create config.
	cfg := &config.Config{}

	// Get flag values.
	cfg.MongoHost, _ = cmd.Flags().GetString("mongo-host")
	cfg.MongoPort, _ = cmd.Flags().GetString("mongo-port")
	cfg.MongoUser, _ = cmd.Flags().GetString("mongo-user")
	cfg.MongoPassword, _ = cmd.Flags().GetString("mongo-password")
	cfg.MongoDB, _ = cmd.Flags().GetString("mongo-db")
	cfg.MongoCollection, _ = cmd.Flags().GetString("mongo-collection")

	// Set dry run mode.
	cfg.SetDryRun(true)

	// Load configuration from environment variables.
	if err := cfg.Load(); err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	// Create reader using configuration.
	reader, err := mongo.NewDataReader(cmd.Context(), cfg)
	if err != nil {
		return fmt.Errorf("failed to create mongo reader: %w", err)
	}

	// Create and run migration service in dry run mode.
	service := migrator.NewService(reader, nil, true)
	if err := service.Run(cmd.Context()); err != nil {
		return fmt.Errorf("migration service failed: %w", err)
	}

	return nil
}

func init() {
	// Add flags.
	flags.AddMongoFlags(PlanCmd)
}
