package dynamo

import (
	"context"
	appConfig "mongo2dynamo/internal/config"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// Connect establishes a connection to DynamoDB
func Connect(cfg *appConfig.Config) (*dynamodb.Client, error) {
	// Load AWS configuration from shared config files
	awsCfg, err := awsConfig.LoadDefaultConfig(context.Background(),
		awsConfig.WithRegion(cfg.AWSRegion),
	)
	if err != nil {
		return nil, err
	}

	// Create DynamoDB client with custom endpoint
	client := dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(cfg.DynamoEndpoint)
	})

	// Verify connection by listing tables
	_, err = client.ListTables(context.Background(), &dynamodb.ListTablesInput{})
	if err != nil {
		return nil, err
	}

	return client, nil
}
