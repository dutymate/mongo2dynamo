package loader

import (
	"context"
	"fmt"
	"time"

	"mongo2dynamo/internal/common"
	"mongo2dynamo/internal/dynamo"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const batchSize = 25

// DBClient defines the interface for DynamoDB operations used by Loader.
type DBClient interface {
	BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
}

// MarshalFunc defines the interface for marshaling items to DynamoDB format.
type MarshalFunc func(item interface{}) (map[string]types.AttributeValue, error)

// DynamoLoader implements the Loader interface for DynamoDB.
type DynamoLoader struct {
	client  DBClient
	table   string
	marshal MarshalFunc
}

// newDynamoLoader creates a new DynamoDB loader.
func newDynamoLoader(client DBClient, table string) *DynamoLoader {
	return &DynamoLoader{
		client:  client,
		table:   table,
		marshal: attributevalue.MarshalMap,
	}
}

// NewDynamoLoader creates a DynamoLoader for DynamoDB based on the configuration.
func NewDynamoLoader(ctx context.Context, cfg common.ConfigProvider) (*DynamoLoader, error) {
	client, err := dynamo.Connect(ctx, cfg)
	if err != nil {
		return nil, &common.DatabaseConnectionError{Database: "DynamoDB", Reason: err.Error(), Err: err}
	}
	return newDynamoLoader(client, cfg.GetDynamoTable()), nil
}

// marshalItem marshals a single item to DynamoDB format.
func (l *DynamoLoader) marshalItem(item map[string]interface{}) (map[string]types.AttributeValue, error) {
	return l.marshal(item)
}

// Load saves all documents to DynamoDB.
func (l *DynamoLoader) Load(ctx context.Context, data []map[string]interface{}) error {
	var writeRequests []types.WriteRequest

	for _, item := range data {
		av, err := l.marshalItem(item)
		if err != nil {
			return &common.DataValidationError{
				Database: "DynamoDB",
				Op:       "marshal",
				Reason:   err.Error(),
				Err:      err,
			}
		}
		writeRequests = append(writeRequests, types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: av,
			},
		})

		if len(writeRequests) == batchSize {
			err = l.batchWrite(ctx, writeRequests)
			if err != nil {
				return err
			}
			writeRequests = nil
		}
	}

	if len(writeRequests) > 0 {
		err := l.batchWrite(ctx, writeRequests)
		if err != nil {
			return err
		}
	}

	return nil
}

// batchWrite writes a batch of items to DynamoDB.
func (l *DynamoLoader) batchWrite(ctx context.Context, writeRequests []types.WriteRequest) error {
	const maxRetries = 5
	var lastUnprocessed []types.WriteRequest
	for attempt := 0; attempt < maxRetries; attempt++ {
		input := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				l.table: writeRequests,
			},
		}
		output, err := l.client.BatchWriteItem(ctx, input)
		if err != nil {
			return &common.DatabaseOperationError{
				Database: "DynamoDB",
				Op:       "batch write",
				Reason:   err.Error(),
				Err:      err,
			}
		}
		unprocessed := output.UnprocessedItems[l.table]
		if len(unprocessed) == 0 {
			return nil // All items processed successfully.
		}
		// Prepare for retry.
		writeRequests = unprocessed
		lastUnprocessed = unprocessed
		// Exponential backoff.
		backoffDuration := time.Duration(attempt+1) * time.Second
		time.Sleep(backoffDuration)
	}
	// Log unprocessed items after exhausting retries.
	if len(lastUnprocessed) > 0 {
		return &common.DatabaseOperationError{
			Database: "DynamoDB",
			Op:       "batch write (unprocessed items)",
			Reason:   fmt.Sprintf("failed to process all items after %d retries", maxRetries),
			Err:      fmt.Errorf("unprocessed items: %v", lastUnprocessed),
		}
	}

	return nil
}
