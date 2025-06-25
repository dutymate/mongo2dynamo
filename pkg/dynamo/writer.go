package dynamo

import (
	"context"
	"fmt"
	"time"

	"mongo2dynamo/pkg/common"
	"mongo2dynamo/pkg/config"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// DBClient defines the interface for DynamoDB operations used by Writer.
type DBClient interface {
	BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
}

// MarshalFunc defines the interface for marshaling items to DynamoDB format.
type MarshalFunc func(item interface{}) (map[string]types.AttributeValue, error)

// Writer implements the DataWriter interface for DynamoDB.
type Writer struct {
	client  DBClient
	table   string
	marshal MarshalFunc
}

const batchSize = 25

// newWriter creates a new DynamoDB writer.
func newWriter(client DBClient, table string) *Writer {
	return &Writer{
		client:  client,
		table:   table,
		marshal: attributevalue.MarshalMap,
	}
}

// marshalItem marshals a single item to DynamoDB format.
func (w *Writer) marshalItem(item map[string]interface{}) (map[string]types.AttributeValue, error) {
	return w.marshal(item)
}

// Write saves all documents to DynamoDB.
func (w *Writer) Write(ctx context.Context, data []map[string]interface{}) error {
	var writeRequests []types.WriteRequest

	for _, item := range data {
		av, err := w.marshalItem(item)
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
			err = w.batchWrite(ctx, writeRequests)
			if err != nil {
				return err
			}
			writeRequests = nil
		}
	}

	if len(writeRequests) > 0 {
		err := w.batchWrite(ctx, writeRequests)
		if err != nil {
			return err
		}
	}

	return nil
}

// batchWrite writes a batch of items to DynamoDB.
func (w *Writer) batchWrite(ctx context.Context, writeRequests []types.WriteRequest) error {
	const maxRetries = 5
	var unprocessedItems map[string][]types.WriteRequest
	for attempt := range maxRetries {
		input := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				w.table: writeRequests,
			},
		}
		output, err := w.client.BatchWriteItem(ctx, input)
		if err != nil {
			return &common.DatabaseOperationError{
				Database: "DynamoDB",
				Op:       "batch write",
				Reason:   err.Error(),
				Err:      err,
			}
		}
		unprocessedItems = output.UnprocessedItems
		if len(unprocessedItems) == 0 {
			return nil // All items processed successfully.
		}
		// Prepare for retry.
		writeRequests = unprocessedItems[w.table]
		// Exponential backoff.
		backoffDuration := time.Duration(attempt+1) * time.Second
		time.Sleep(backoffDuration)
	}
	// Log unprocessed items after exhausting retries.
	if len(unprocessedItems) > 0 {
		return &common.DatabaseOperationError{
			Database: "DynamoDB",
			Op:       "batch write (unprocessed items)",
			Reason:   fmt.Sprintf("failed to process all items after %d retries", maxRetries),
			Err:      fmt.Errorf("unprocessed items: %v", unprocessedItems),
		}
	}

	return nil
}

// NewDataWriter creates a DataWriter for DynamoDB based on the configuration.
func NewDataWriter(ctx context.Context, cfg *config.Config) (*Writer, error) {
	client, err := Connect(ctx, cfg)
	if err != nil {
		return nil, err // Already wrapped by Connect.
	}
	return newWriter(client, cfg.DynamoTable), nil
}
