package dynamo

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Writer is a DynamoDB data writer.
type Writer struct {
	client *dynamodb.Client
	table  string
}

const batchSize = 25

// NewWriter creates a new DynamoDB writer.
func NewWriter(client *dynamodb.Client, table string) *Writer {
	return &Writer{
		client: client,
		table:  table,
	}
}

// Write writes data to DynamoDB.
func (w *Writer) Write(ctx context.Context, data []map[string]interface{}) error {
	var writeRequests []types.WriteRequest

	for _, item := range data {
		av, err := attributevalue.MarshalMap(item)
		if err != nil {
			return fmt.Errorf("failed to marshal item: %w", err)
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
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			w.table: writeRequests,
		},
	}

	_, err := w.client.BatchWriteItem(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to batch write items: %w", err)
	}

	return nil
}
