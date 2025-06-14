package cmd

import (
	"fmt"
	"mongo2dynamo/internal/config"
	"mongo2dynamo/internal/dynamo"
	"mongo2dynamo/internal/migrator"
	"mongo2dynamo/internal/mongo"

	"github.com/spf13/cobra"
)

// applyCmd represents the apply command
var applyCmd = &cobra.Command{
	Use:   "apply",
	Short: "Apply the migration plan",
	Long: `Apply the migration plan to transfer data from MongoDB to DynamoDB.
This command will execute the actual data transfer process.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Create config
		cfg := &config.Config{}

		// Get flag values
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

		// Load configuration from environment variables
		if err := cfg.Load(); err != nil {
			return fmt.Errorf("failed to load configuration: %w", err)
		}

		// Confirm before proceeding
		if !cfg.AutoApprove {
			if !confirm("Are you sure you want to proceed with the migration? (y/N) ") {
				return nil
			}
		}

		// Connect to MongoDB
		mongoClient, err := mongo.Connect(cfg)
		if err != nil {
			return fmt.Errorf("failed to connect to MongoDB: %w", err)
		}
		defer mongoClient.Disconnect(cmd.Context())

		// Connect to DynamoDB
		dynamoClient, err := dynamo.Connect(cfg)
		if err != nil {
			return fmt.Errorf("failed to connect to DynamoDB: %w", err)
		}

		// Create reader and writer
		reader := mongo.NewReader(mongoClient.Database(cfg.MongoDB).Collection(cfg.MongoCollection))
		writer := dynamo.NewWriter(dynamoClient, cfg.DynamoTable)

		// Create and run migration service
		service := migrator.NewService(reader, writer, false)
		if err := service.Run(cmd.Context()); err != nil {
			return fmt.Errorf("migration failed: %w", err)
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(applyCmd)

	// Add flags
	applyCmd.Flags().String("mongo-host", "localhost", "MongoDB host")
	applyCmd.Flags().String("mongo-port", "27017", "MongoDB port")
	applyCmd.Flags().String("mongo-user", "", "MongoDB username")
	applyCmd.Flags().String("mongo-password", "", "MongoDB password")
	applyCmd.Flags().String("mongo-db", "", "MongoDB database name")
	applyCmd.Flags().String("mongo-collection", "", "MongoDB collection name")
	applyCmd.Flags().String("dynamo-endpoint", "http://localhost:8000", "DynamoDB endpoint")
	applyCmd.Flags().String("dynamo-table", "", "DynamoDB table name")
	applyCmd.Flags().String("aws-region", "us-east-1", "AWS region")
	applyCmd.Flags().Bool("auto-approve", false, "Automatically approve the migration")
}
