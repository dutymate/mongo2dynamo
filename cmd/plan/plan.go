package plan

import (
	"fmt"
	"mongo2dynamo/internal/common"
	"mongo2dynamo/internal/config"
	"mongo2dynamo/internal/extractor"
	"mongo2dynamo/internal/flags"
	"mongo2dynamo/internal/transformer"

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

	// Create mongoExtractor using configuration.
	mongoExtractor, err := extractor.NewMongoExtractor(cmd.Context(), cfg)
	if err != nil {
		return &common.ExtractError{Reason: "failed to create extractor", Err: err}
	}

	// Create docTransformer for MongoDB to DynamoDB document conversion.
	docTransformer := transformer.NewDocTransformer()
	total := 0
	err = mongoExtractor.Extract(cmd.Context(), func(chunk []map[string]interface{}) error {
		// Apply transformation.
		transformed, err := docTransformer.Transform(chunk)
		if err != nil {
			return &common.TransformError{Reason: "failed to transform chunk", Err: err}
		}
		total += len(transformed)
		return nil
	})
	if err != nil {
		return &common.PlanError{Reason: "unexpected error during plan callback", Err: err}
	}
	fmt.Printf("Found %d documents to migrate\n", total)
	return nil
}

func init() {
	// Add flags.
	flags.AddMongoFlags(PlanCmd)
}
