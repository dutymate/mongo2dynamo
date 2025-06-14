package cmd

import (
	"fmt"
	"mongo2dynamo/internal/config"
	"mongo2dynamo/internal/migrator"
	"mongo2dynamo/internal/mongo"

	"github.com/spf13/cobra"
)

// planCmd represents the plan command
var planCmd = &cobra.Command{
	Use:   "plan",
	Short: "Show migration plan",
	Long: `Show a preview of the migration plan without executing it.
This command will display what would be migrated without making any changes.`,
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

		// Set dry run mode
		cfg.SetDryRun(true)

		// Load configuration from environment variables
		if err := cfg.Load(); err != nil {
			return fmt.Errorf("failed to load configuration: %w", err)
		}

		// Connect to MongoDB
		mongoClient, err := mongo.Connect(cfg)
		if err != nil {
			return fmt.Errorf("failed to connect to MongoDB: %w", err)
		}
		defer mongoClient.Disconnect(cmd.Context())

		// Create reader
		reader := mongo.NewReader(mongoClient.Database(cfg.MongoDB).Collection(cfg.MongoCollection))

		// Create and run migration service in dry run mode
		service := migrator.NewService(reader, nil, true)
		if err := service.Run(cmd.Context()); err != nil {
			return fmt.Errorf("plan generation failed: %w", err)
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(planCmd)

	// Add flags
	planCmd.Flags().String("mongo-host", "localhost", "MongoDB host")
	planCmd.Flags().String("mongo-port", "27017", "MongoDB port")
	planCmd.Flags().String("mongo-user", "", "MongoDB username")
	planCmd.Flags().String("mongo-password", "", "MongoDB password")
	planCmd.Flags().String("mongo-db", "", "MongoDB database name")
	planCmd.Flags().String("mongo-collection", "", "MongoDB collection name")
}
