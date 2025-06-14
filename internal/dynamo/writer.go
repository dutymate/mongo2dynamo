package dynamo

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type Writer struct {
	client *dynamodb.Client
	table  string
}

func NewWriter(client *dynamodb.Client, table string) *Writer {
	return &Writer{
		client: client,
		table:  table,
	}
}

func (w *Writer) Write(ctx context.Context, data []map[string]interface{}) error {
	for _, item := range data {
		av, err := attributevalue.MarshalMap(item)
		if err != nil {
			return fmt.Errorf("failed to marshal item: %w", err)
		}

		_, err = w.client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(w.table),
			Item:      av,
		})
		if err != nil {
			return fmt.Errorf("failed to put item: %w", err)
		}
	}

	return nil
}
