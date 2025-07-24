package apply

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

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
	if cmd.Flags().Changed("max-retries") {
		cfg.MaxRetries, _ = cmd.Flags().GetInt("max-retries")
	}
	if cmd.Flags().Changed("mongo-filter") {
		cfg.MongoFilter, _ = cmd.Flags().GetString("mongo-filter")
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

	total, err := mongoExtractor.Count(cmd.Context())
	if err != nil {
		return fmt.Errorf("failed to count documents for migration: %w", err)
	}

	if total == 0 {
		fmt.Println("No documents to migrate.")
		return nil
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

	// Use a cancellable context to shut down the pipeline on error.
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	// Pipeline channels.
	const pipelineChannelBufferSize = 10
	extractChan := make(chan []map[string]interface{}, pipelineChannelBufferSize)
	transformChan := make(chan []map[string]interface{}, pipelineChannelBufferSize)
	errorChan := make(chan error, 3)

	// Use a WaitGroup to wait for all pipeline stages to finish.
	var wg sync.WaitGroup
	var migratedCount int64

	// Stage 3: Loader.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for transformed := range transformChan {
			if err := dynamoLoader.Load(ctx, transformed); err != nil {
				errorChan <- fmt.Errorf("failed to load document chunk to DynamoDB: %w", err)
				cancel()
				return
			}
			atomic.AddInt64(&migratedCount, int64(len(transformed)))
		}
	}()

	// Stage 2: Transformer.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(transformChan) // Close downstream channel.
		for chunk := range extractChan {
			// Check for cancellation before processing.
			select {
			case <-ctx.Done():
				return
			default:
			}

			transformed, err := docTransformer.Transform(ctx, chunk)
			if err != nil {
				errorChan <- fmt.Errorf("failed to transform document chunk: %w", err)
				cancel()
				return
			}

			select {
			case transformChan <- transformed:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Stage 1: Extractor.
	fmt.Println("Starting data migration from MongoDB to DynamoDB...")
	extractErr := mongoExtractor.Extract(ctx, func(chunk []map[string]interface{}) error {
		select {
		case extractChan <- chunk:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	close(extractChan) // Done extracting, close channel.

	// Wait for the pipeline to finish processing all extracted data.
	wg.Wait()

	// Check for errors.
	close(errorChan)
	var finalErr error
	for err := range errorChan {
		if finalErr == nil {
			finalErr = err
		}
	}

	if finalErr != nil {
		return finalErr
	}

	// If extraction failed with something other than cancellation, report it.
	if extractErr != nil && !errors.Is(extractErr, context.Canceled) {
		return fmt.Errorf("error during extraction: %w", extractErr)
	}

	fmt.Printf("Successfully migrated %s documents.\n", common.FormatNumber(int(migratedCount)))
	return nil
}

func init() {
	// Add flags.
	flags.AddMongoFlags(ApplyCmd)
	flags.AddDynamoFlags(ApplyCmd)
	flags.AddAutoApproveFlag(ApplyCmd)
}
