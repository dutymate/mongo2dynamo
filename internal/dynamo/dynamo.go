package dynamo

import (
	"context"
	"fmt"
	"time"

	"mongo2dynamo/internal/config"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// Connect establishes a connection to DynamoDB.
func Connect(ctx context.Context, cfg *config.Config) (*dynamodb.Client, error) {
	// Load AWS configuration.
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.AWSRegion),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS configuration: %w", err)
	}

	// Create DynamoDB client with custom endpoint.
	client := dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(cfg.DynamoEndpoint)
	})

	// Verify connection with timeout.
	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err = client.ListTables(timeoutCtx, &dynamodb.ListTablesInput{})
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}

	return client, nil
}
