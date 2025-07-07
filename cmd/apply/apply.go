package apply

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"mongo2dynamo/internal/common"
	"mongo2dynamo/internal/config"
	"mongo2dynamo/internal/extractor"
	"mongo2dynamo/internal/flags"
	"mongo2dynamo/internal/loader"
	"mongo2dynamo/internal/transformer"
)

// ApplyCmd represents the apply command.
var ApplyCmd = &cobra.Command{
	Use:   "apply",
	Short: "Apply the migration plan",
	Long: `Apply the migration plan to transfer data from MongoDB to DynamoDB.
This command will execute the actual data transfer process.`,
	RunE: runApply,
}

func runApply(cmd *cobra.Command, _ []string) error {
	// Create config.
	cfg := &config.Config{}

	// Load configuration from environment variables, config file, and defaults first.
	if err := cfg.Load(); err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
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
	if cmd.Flags().Changed("dynamo-endpoint") {
		cfg.DynamoEndpoint, _ = cmd.Flags().GetString("dynamo-endpoint")
	}
	if cmd.Flags().Changed("dynamo-table") {
		cfg.DynamoTable, _ = cmd.Flags().GetString("dynamo-table")
	}
	if cmd.Flags().Changed("aws-region") {
		cfg.AWSRegion, _ = cmd.Flags().GetString("aws-region")
	}
	if cmd.Flags().Changed("auto-approve") {
		cfg.AutoApprove, _ = cmd.Flags().GetBool("auto-approve")
	}

	// Validate configuration after all values are set.
	if err := cfg.Validate(); err != nil {
		if errors.Is(err, context.Canceled) {
			fmt.Println("Migration canceled by user (table name confirmation declined).")
			return nil
		}
		return fmt.Errorf("configuration validation failed: %w", err)
	}

	// Confirm before proceeding.
	if !cfg.AutoApprove && !common.Confirm("Are you sure you want to proceed with the migration? (y/N) ") {
		fmt.Println("Migration canceled by user.")
		return nil
	}

	// Create mongoExtractor using configuration.
	mongoExtractor, err := extractor.NewMongoExtractor(cmd.Context(), cfg)
	if err != nil {
		return fmt.Errorf("failed to create MongoDB extractor: %w", err)
	}

	// Create docTransformer for MongoDB to DynamoDB document conversion.
	docTransformer := transformer.NewDocTransformer()

	// Create dynamoLoader using configuration.
	dynamoLoader, err := loader.NewDynamoLoader(cmd.Context(), cfg)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			fmt.Println("Migration canceled by user (table creation declined).")
			return nil
		}
		return fmt.Errorf("failed to create DynamoDB loader: %w", err)
	}

	migrated := 0
	fmt.Println("Starting data migration from MongoDB to DynamoDB...")
	err = mongoExtractor.Extract(cmd.Context(), func(chunk []map[string]interface{}) error {
		// Apply transformation to each chunk before loading to DynamoDB.
		transformed, err := docTransformer.Transform(chunk)
		if err != nil {
			return fmt.Errorf("failed to transform document chunk: %w", err)
		}
		if err := dynamoLoader.Load(cmd.Context(), transformed); err != nil {
			return fmt.Errorf("failed to load document chunk to DynamoDB: %w", err)
		}
		migrated += len(transformed)
		return nil
	})
	if err != nil {
		return fmt.Errorf("unexpected error during apply callback: %w", err)
	}
	fmt.Printf("Successfully migrated %s documents.\n", common.FormatNumber(migrated))
	return nil
}

func init() {
	// Add flags.
	flags.AddMongoFlags(ApplyCmd)
	flags.AddDynamoFlags(ApplyCmd)
}
