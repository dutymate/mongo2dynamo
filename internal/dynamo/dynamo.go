package dynamo

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"mongo2dynamo/internal/common"
)

// Connect establishes a connection to DynamoDB.
func Connect(ctx context.Context, cfg common.ConfigProvider) (*dynamodb.Client, error) {
	// Load AWS configuration.
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.GetAWSRegion()),
	)
	if err != nil {
		return nil, &common.DatabaseConnectionError{Database: "DynamoDB", Reason: err.Error(), Err: err}
	}

	// Create DynamoDB client with custom endpoint.
	client := dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(cfg.GetDynamoEndpoint())
	})

	// Verify connection with timeout.
	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err = client.ListTables(timeoutCtx, &dynamodb.ListTablesInput{})
	if err != nil {
		return nil, &common.DatabaseOperationError{Database: "DynamoDB", Op: "list tables", Reason: err.Error(), Err: err}
	}

	return client, nil
}
