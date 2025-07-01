package apply

import (
	"errors"
	"fmt"
	"mongo2dynamo/internal/common"
	"mongo2dynamo/internal/config"
	"mongo2dynamo/internal/dynamo"
	"mongo2dynamo/internal/flags"
	"mongo2dynamo/internal/mongo"
	"mongo2dynamo/internal/transformer"

	"github.com/spf13/cobra"
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

	// Get flag values.
	cfg.MongoHost, _ = cmd.Flags().GetString("mongo-host")
	cfg.MongoPort, _ = cmd.Flags().GetString("mongo-port")
	cfg.MongoUser, _ = cmd.Flags().GetString("mongo-user")
	cfg.MongoPassword, _ = cmd.Flags().GetString("mongo-password")
	cfg.MongoDB, _ = cmd.Flags().GetString("mongo-db")
	cfg.MongoCollection, _ = cmd.Flags().GetString("mongo-collection")
	cfg.DynamoEndpoint, _ = cmd.Flags().GetString("dynamo-endpoint")
	cfg.DynamoTable, _ = cmd.Flags().GetString("dynamo-table")
	cfg.AWSRegion, _ = cmd.Flags().GetString("aws-region")
	cfg.AutoApprove, _ = cmd.Flags().GetBool("auto-approve")

	// Load configuration from environment variables.
	if err := cfg.Load(); err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	// Confirm before proceeding.
	if !cfg.AutoApprove {
		if !common.Confirm("Are you sure you want to proceed with the migration? (y/N) ") {
			return nil
		}
	}

	// Create reader and writer using configuration.
	reader, err := mongo.NewDataReader(cmd.Context(), cfg)
	if err != nil {
		return &common.ReaderError{Reason: "failed to create mongo reader", Err: err}
	}
	writer, err := dynamo.NewDataWriter(cmd.Context(), cfg)
	if err != nil {
		return &common.WriterError{Reason: "failed to create dynamo writer", Err: err}
	}

	// Create transformer for MongoDB to DynamoDB document conversion.
	trans := transformer.NewMongoToDynamoTransformer()

	migrated := 0
	err = reader.Read(cmd.Context(), func(chunk []map[string]interface{}) error {
		// Apply transformation to each chunk before writing to DynamoDB.
		transformed, err := trans.Transform(chunk)
		if err != nil {
			return &common.TransformError{Reason: "failed to transform chunk", Err: err}
		}
		if err := writer.Write(cmd.Context(), transformed); err != nil {
			return &common.WriterError{Reason: "failed to write chunk", Err: err}
		}
		migrated += len(transformed)
		return nil
	})
	if err != nil {
		var transformErr *common.TransformError
		var writerErr *common.WriterError
		if errors.As(err, &transformErr) {
			return transformErr
		}
		if errors.As(err, &writerErr) {
			return writerErr
		}
		return &common.ReaderError{Reason: "failed to read from mongo", Err: err}
	}
	fmt.Printf("Successfully migrated %d documents\n", migrated)
	return nil
}

func init() {
	// Add flags.
	flags.AddMongoFlags(ApplyCmd)
	flags.AddDynamoFlags(ApplyCmd)
}
