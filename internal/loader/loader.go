package loader

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"mongo2dynamo/internal/common"
	"mongo2dynamo/internal/dynamo"
)

const (
	batchSize  = 25
	maxRetries = 5
	baseDelay  = 100 * time.Millisecond
	maxDelay   = 30 * time.Second
)

// Global random source for jitter calculation.
var randomSource = rand.New(rand.NewSource(time.Now().UnixNano()))

// DBClient defines the interface for DynamoDB operations used by Loader.
type DBClient interface {
	BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
	DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	CreateTable(ctx context.Context, params *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
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

	loader := newDynamoLoader(client, cfg.GetDynamoTable())

	// Ensure table exists, create if it doesn't.
	if err := loader.ensureTableExists(ctx, cfg); err != nil {
		return nil, err
	}

	return loader, nil
}

// marshalItem marshals a single item to DynamoDB format.
func (l *DynamoLoader) marshalItem(item map[string]interface{}) (map[string]types.AttributeValue, error) {
	return l.marshal(item)
}

// ensureTableExists checks if the table exists and creates it if it doesn't.
func (l *DynamoLoader) ensureTableExists(ctx context.Context, cfg common.ConfigProvider) error {
	// Check if table exists.
	_, err := l.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: &l.table,
	})

	if err == nil {
		// Table exists, nothing to do.
		fmt.Printf("Table '%s' already exists.\n", l.table)
		return nil
	}

	// Check if error is due to table not existing.
	var resourceNotFoundErr *types.ResourceNotFoundException
	if !errors.As(err, &resourceNotFoundErr) {
		// Some other error occurred.
		return &common.DatabaseOperationError{
			Database: "DynamoDB",
			Op:       "describe table",
			Reason:   err.Error(),
			Err:      err,
		}
	}

	// Check if auto-approve is enabled.
	if cfg.GetAutoApprove() {
		fmt.Printf("Auto-creating table '%s'...\n", l.table)
		return l.createTable(ctx)
	}

	// Ask for confirmation.
	if !common.Confirm(fmt.Sprintf("Create DynamoDB table '%s'? (y/N) ", l.table)) {
		return fmt.Errorf("required DynamoDB table '%s' not created: user declined table creation: %w", l.table, context.Canceled)
	}

	return l.createTable(ctx)
}

// createTable creates a new DynamoDB table with a simple schema.
func (l *DynamoLoader) createTable(ctx context.Context) error {
	fmt.Printf("Creating DynamoDB table '%s'...\n", l.table)

	// Create a simple table with 'id' as the primary key.
	input := &dynamodb.CreateTableInput{
		TableName: &l.table,
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       types.KeyTypeHash,
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	}

	_, err := l.client.CreateTable(ctx, input)
	if err != nil {
		return &common.DatabaseOperationError{
			Database: "DynamoDB",
			Op:       "create table",
			Reason:   err.Error(),
			Err:      err,
		}
	}

	fmt.Printf("Waiting for table '%s' to become active...\n", l.table)

	// Wait for table to be created.
	return l.waitForTableActive(ctx)
}

// waitForTableActive waits for the table to become active.
func (l *DynamoLoader) waitForTableActive(ctx context.Context) error {
	const maxWaitTime = 30 * time.Second
	const checkInterval = 2 * time.Second

	timeoutCtx, cancel := context.WithTimeout(ctx, maxWaitTime)
	defer cancel()

	for {
		select {
		case <-timeoutCtx.Done():
			return &common.DatabaseOperationError{
				Database: "DynamoDB",
				Op:       "wait for table active",
				Reason:   "timeout waiting for table to become active",
				Err:      timeoutCtx.Err(),
			}
		default:
			output, err := l.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
				TableName: &l.table,
			})
			if err != nil {
				return &common.DatabaseOperationError{
					Database: "DynamoDB",
					Op:       "describe table",
					Reason:   err.Error(),
					Err:      err,
				}
			}

			if output.Table.TableStatus == types.TableStatusActive {
				fmt.Printf("Table '%s' is now active and ready for use.\n", l.table)
				return nil
			}

			time.Sleep(checkInterval)
		}
	}
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
				return fmt.Errorf("failed to write batch to DynamoDB: %w", err)
			}
			writeRequests = nil
		}
	}

	if len(writeRequests) > 0 {
		err := l.batchWrite(ctx, writeRequests)
		if err != nil {
			return fmt.Errorf("failed to write final batch to DynamoDB: %w", err)
		}
	}

	return nil
}

// calculateBackoffWithJitter calculates exponential backoff with jitter to prevent thundering herd.
func calculateBackoffWithJitter(attempt int) time.Duration {
	// Handle negative attempts.
	if attempt < 0 {
		attempt = 0
	}

	// Exponential backoff calculation.
	exponentialDelay := time.Duration(math.Pow(2, float64(attempt))) * baseDelay

	// Cap at maximum delay before applying jitter.
	if exponentialDelay > maxDelay {
		exponentialDelay = maxDelay
	}

	// Add jitter (0.5 ~ 1.5 range) to prevent synchronized retries.
	jitter := 0.5 + randomSource.Float64()*1.0
	jitteredDelay := min(time.Duration(float64(exponentialDelay)*jitter), maxDelay)

	return jitteredDelay
}

// batchWrite writes a batch of items to DynamoDB.
func (l *DynamoLoader) batchWrite(ctx context.Context, writeRequests []types.WriteRequest) error {
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
		// Exponential backoff with jitter.
		backoffDuration := calculateBackoffWithJitter(attempt)
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
