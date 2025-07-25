package flags

import (
	"github.com/spf13/cobra"
)

// AddMongoFlags adds MongoDB-related flags to the command.
func AddMongoFlags(cmd *cobra.Command) {
	cmd.Flags().String("mongo-host", "localhost", "MongoDB host.")
	cmd.Flags().String("mongo-port", "27017", "MongoDB port.")
	cmd.Flags().String("mongo-user", "", "MongoDB username.")
	cmd.Flags().String("mongo-password", "", "MongoDB password.")
	cmd.Flags().String("mongo-db", "", "MongoDB database name.")
	cmd.Flags().String("mongo-collection", "", "MongoDB collection name.")
	cmd.Flags().String("mongo-filter", "", "MongoDB query filter (JSON string).")
	cmd.Flags().String("mongo-projection", "", "MongoDB projection (JSON string). Only the specified fields will be included in the result documents.")
}

// AddDynamoFlags adds DynamoDB-related flags to the command.
func AddDynamoFlags(cmd *cobra.Command) {
	cmd.Flags().String("dynamo-endpoint", "http://localhost:8000", "DynamoDB endpoint.")
	cmd.Flags().String("dynamo-table", "", "DynamoDB table name.")
	cmd.Flags().String("dynamo-partition-key", "id", "DynamoDB table partition key.")
	cmd.Flags().String("dynamo-partition-key-type", "S", "DynamoDB table partition key type (S, N, B).")
	cmd.Flags().String("dynamo-sort-key", "", "DynamoDB table sort key.")
	cmd.Flags().String("dynamo-sort-key-type", "S", "DynamoDB table sort key type (S, N, B).")
	cmd.Flags().String("aws-region", "us-east-1", "AWS region.")
	cmd.Flags().Int("max-retries", 5, "Maximum number of retries for DynamoDB batch write.")
}

// AddAutoApproveFlag adds the auto-approve flag to the command.
func AddAutoApproveFlag(cmd *cobra.Command) {
	cmd.Flags().Bool("auto-approve", false, "Skip all confirmation prompts.")
}
