package plan

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
	if cmd.Flags().Changed("mongo-filter") {
		cfg.MongoFilter, _ = cmd.Flags().GetString("mongo-filter")
	}

	// Set dry run mode.
	cfg.SetDryRun(true)

	// Validate configuration after all values are set.
	if err := cfg.Validate(); err != nil {
		if errors.Is(err, context.Canceled) {
			fmt.Println("Plan canceled by user (table name confirmation declined).")
			return nil
		}
		return fmt.Errorf("configuration validation failed: %w", err)
	}

	// Create mongoExtractor using configuration.
	mongoExtractor, err := extractor.NewMongoExtractor(cmd.Context(), cfg)
	if err != nil {
		return fmt.Errorf("failed to create MongoDB extractor: %w", err)
	}

	// Create docTransformer for MongoDB to DynamoDB document conversion.
	docTransformer := transformer.NewDocTransformer()

	// Use a cancellable context to shut down the pipeline on error.
	ctx, cancel := context.WithCancel(cmd.Context())
	defer cancel()

	// Pipeline channels.
	const pipelineChannelBufferSize = 10
	extractChan := make(chan []map[string]interface{}, pipelineChannelBufferSize)
	errorChan := make(chan error, 2)

	// Use a WaitGroup to wait for all pipeline stages to finish.
	var wg sync.WaitGroup
	var totalCount int64

	// Stage 2: Transformer and counter.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for chunk := range extractChan {
			// Check for cancellation before processing.
			select {
			case <-ctx.Done():
				return
			default:
			}

			transformed, err := docTransformer.Transform(chunk)
			if err != nil {
				errorChan <- fmt.Errorf("failed to transform document chunk: %w", err)
				cancel()
				return
			}
			atomic.AddInt64(&totalCount, int64(len(transformed)))
		}
	}()

	// Stage 1: Extractor.
	fmt.Println("Starting migration plan analysis...")
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

	fmt.Printf("Found %s documents to migrate.\n", common.FormatNumber(int(totalCount)))
	return nil
}

func init() {
	// Add flags.
	flags.AddMongoFlags(PlanCmd)
}
