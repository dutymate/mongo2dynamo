package plan

import (
	"fmt"

	"github.com/spf13/cobra"

	"mongo2dynamo/internal/common"
	"mongo2dynamo/internal/config"
	"mongo2dynamo/internal/extractor"
	"mongo2dynamo/internal/flags"
	"mongo2dynamo/internal/transformer"
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

	// Load configuration from environment variables, config file, and defaults first.
	if err := cfg.Load(); err != nil {
		return &common.ConfigError{Op: "load", Reason: "failed to load from environment variables and config file", Err: err}
	}

	// Then override with flag values if they were explicitly set.
	if cmd.Flags().Changed("mongo-host") {
		cfg.MongoHost, _ = cmd.Flags().GetString("mongo-host")
	}
	if cmd.Flags().Changed("mongo-port") {
		cfg.MongoPort, _ = cmd.Flags().GetString("mongo-port")
	}
	if cmd.Flags().Changed("mongo-user") {
		cfg.MongoUser, _ = cmd.Flags().GetString("mongo-user")
	}
	if cmd.Flags().Changed("mongo-password") {
		cfg.MongoPassword, _ = cmd.Flags().GetString("mongo-password")
	}
	if cmd.Flags().Changed("mongo-db") {
		cfg.MongoDB, _ = cmd.Flags().GetString("mongo-db")
	}
	if cmd.Flags().Changed("mongo-collection") {
		cfg.MongoCollection, _ = cmd.Flags().GetString("mongo-collection")
	}

	// Set dry run mode.
	cfg.SetDryRun(true)

	// Validate configuration after all values are set.
	if err := cfg.Validate(); err != nil {
		return &common.ConfigError{Op: "validate", Reason: "invalid configuration", Err: err}
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
	fmt.Printf("Found %d documents to migrate.\n", total)
	return nil
}

func init() {
	// Add flags.
	flags.AddMongoFlags(PlanCmd)
}
