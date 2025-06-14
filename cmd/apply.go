package cmd

import (
	"fmt"
	"mongo2dynamo/internal/config"
	"mongo2dynamo/internal/dynamo"
	"mongo2dynamo/internal/migrator"
	"mongo2dynamo/internal/mongo"

	"github.com/spf13/cobra"
)

// applyCmd represents the apply command.
var applyCmd = &cobra.Command{
	Use:   "apply",
	Short: "Apply the migration plan",
	Long: `Apply the migration plan to transfer data from MongoDB to DynamoDB.
This command will execute the actual data transfer process.`,
	RunE: func(cmd *cobra.Command, _ []string) error {
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
			if !confirm("Are you sure you want to proceed with the migration? (y/N) ") {
				return nil
			}
		}

		// Connect to MongoDB.
		mongoClient, err := mongo.Connect(cmd.Context(), cfg)
		if err != nil {
			return fmt.Errorf("failed to connect to MongoDB: %w", err)
		}
		defer func() {
			if err := mongoClient.Disconnect(cmd.Context()); err != nil {
				fmt.Printf("Warning: failed to disconnect from MongoDB: %v\n", err)
			}
		}()

		// Connect to DynamoDB.
		dynamoClient, err := dynamo.Connect(cmd.Context(), cfg)
		if err != nil {
			return fmt.Errorf("failed to connect to DynamoDB: %w", err)
		}

		// Create reader and writer.
		reader := mongo.NewReader(mongoClient.Database(cfg.MongoDB).Collection(cfg.MongoCollection))
		writer := dynamo.NewWriter(dynamoClient, cfg.DynamoTable)

		// Create and run migration service.
		service := migrator.NewService(reader, writer, false)
		if err := service.Run(cmd.Context()); err != nil {
			return fmt.Errorf("migration failed: %w", err)
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(applyCmd)

	// Add flags.
	AddMongoFlags(applyCmd)
	AddDynamoFlags(applyCmd)
	applyCmd.Flags().Bool("auto-approve", false, "Automatically approve the migration.")
}
