package loader

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"mongo2dynamo/internal/common"
	"mongo2dynamo/internal/config"
	"mongo2dynamo/internal/dynamo"
	"mongo2dynamo/internal/retry"
)

const (
	MaxWaitTimeForTableActive   = 30 * time.Second
	CheckIntervalForTableActive = 2 * time.Second
	DefaultDynamoBatchSize      = 25
	DefaultLoaderWorkers        = 10
)

// DBClient defines the interface for DynamoDB operations used by Loader.
type DBClient interface {
	CreateTable(ctx context.Context, params *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
	DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
}

// MarshalFunc defines the interface for marshaling items to DynamoDB format.
type MarshalFunc func(item any) (map[string]types.AttributeValue, error)

// DynamoLoader implements the Loader interface for DynamoDB.
type DynamoLoader struct {
	client      DBClient
	table       string
	marshal     MarshalFunc
	retryConfig *retry.Config
}

// newDynamoLoader creates a new DynamoDB loader.
func newDynamoLoader(client DBClient, table string, maxRetries int) *DynamoLoader {
	return &DynamoLoader{
		client:      client,
		table:       table,
		marshal:     attributevalue.MarshalMap,
		retryConfig: retry.NewConfig().WithMaxRetries(maxRetries),
	}
}

// NewDynamoLoader creates a DynamoLoader for DynamoDB based on the configuration.
func NewDynamoLoader(ctx context.Context, cfg *config.Config) (common.Loader, error) {
	client, err := dynamo.Connect(ctx, cfg)
	if err != nil {
		return nil, &common.DatabaseConnectionError{Database: "DynamoDB", Reason: err.Error(), Err: err}
	}

	loader := newDynamoLoader(client, cfg.DynamoTable, cfg.MaxRetries)

	// Ensure table exists, create if it doesn't.
	if err := loader.ensureTableExists(ctx, cfg); err != nil {
		return nil, err
	}

	return loader, nil
}

// marshalItem marshals a single item to DynamoDB format.
func (l *DynamoLoader) marshalItem(item map[string]any) (map[string]types.AttributeValue, error) {
	return l.marshal(item)
}

// ensureTableExists checks if the table exists and creates it if it doesn't.
func (l *DynamoLoader) ensureTableExists(ctx context.Context, cfg *config.Config) error {
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
	if cfg.AutoApprove {
		return l.createTable(ctx, cfg)
	}

	// Ask for confirmation.
	if !common.Confirm(fmt.Sprintf("Create DynamoDB table '%s'? (y/N) ", l.table)) {
		return fmt.Errorf("required DynamoDB table '%s' not created: user declined table creation: %w", l.table, context.Canceled)
	}

	return l.createTable(ctx, cfg)
}

// createTable creates a new DynamoDB table with a user-defined schema.
func (l *DynamoLoader) createTable(ctx context.Context, cfg *config.Config) error {
	fmt.Printf("Creating DynamoDB table '%s'...\n", l.table)

	// Define attribute definitions and key schema based on config.
	attributeDefinitions := []types.AttributeDefinition{
		{
			AttributeName: aws.String(cfg.DynamoPartitionKey),
			AttributeType: types.ScalarAttributeType(cfg.DynamoPartitionKeyType),
		},
	}
	keySchema := []types.KeySchemaElement{
		{
			AttributeName: aws.String(cfg.DynamoPartitionKey),
			KeyType:       types.KeyTypeHash,
		},
	}

	// Add sort key if defined.
	if sortKey := cfg.DynamoSortKey; sortKey != "" {
		attributeDefinitions = append(attributeDefinitions, types.AttributeDefinition{
			AttributeName: aws.String(sortKey),
			AttributeType: types.ScalarAttributeType(cfg.DynamoSortKeyType),
		})
		keySchema = append(keySchema, types.KeySchemaElement{
			AttributeName: aws.String(sortKey),
			KeyType:       types.KeyTypeRange,
		})
	}

	input := &dynamodb.CreateTableInput{
		TableName:            &l.table,
		AttributeDefinitions: attributeDefinitions,
		KeySchema:            keySchema,
		BillingMode:          types.BillingModePayPerRequest,
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
	timeoutCtx, cancel := context.WithTimeout(ctx, MaxWaitTimeForTableActive)
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

			time.Sleep(CheckIntervalForTableActive)
		}
	}
}

// Load saves all documents to DynamoDB using a pool of workers.
func (l *DynamoLoader) Load(ctx context.Context, data []map[string]any) error {
	if len(data) == 0 {
		return nil
	}

	jobChan := make(chan []types.WriteRequest)
	errorChan := make(chan error, DefaultLoaderWorkers)
	var wg sync.WaitGroup

	// Start a pool of workers.
	for i := 0; i < DefaultLoaderWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done(): // Context cancelled from the outside.
					return
				case writeRequests, ok := <-jobChan:
					if !ok { // Channel closed.
						return
					}
					if err := l.batchWrite(ctx, writeRequests); err != nil {
						// Non-blocking send.
						select {
						case errorChan <- err:
						default:
						}
						return // Stop this worker on first error.
					}
				}
			}
		}()
	}

	// Dispatch jobs.
	go func() {
		defer close(jobChan)
		writeRequests := make([]types.WriteRequest, 0, DefaultDynamoBatchSize)
		for _, item := range data {
			// Check for cancellation before dispatching new jobs.
			if ctx.Err() != nil {
				return
			}

			av, err := l.marshalItem(item)
			if err != nil {
				select {
				case errorChan <- &common.DataValidationError{Database: "DynamoDB", Op: "marshal", Reason: err.Error(), Err: err}:
				default:
				}
				return // Stop dispatching.
			}
			writeRequests = append(writeRequests, types.WriteRequest{PutRequest: &types.PutRequest{Item: av}})
			if len(writeRequests) == DefaultDynamoBatchSize {
				select {
				case jobChan <- writeRequests:
				case <-ctx.Done():
					return
				}
				writeRequests = make([]types.WriteRequest, 0, DefaultDynamoBatchSize)
			}
		}
		if len(writeRequests) > 0 {
			select {
			case jobChan <- writeRequests:
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Wait()
	close(errorChan)

	// Return the first error encountered.
	return <-errorChan
}

// batchWrite writes a batch of items to DynamoDB.
func (l *DynamoLoader) batchWrite(ctx context.Context, writeRequests []types.WriteRequest) error {
	var currentRequests = writeRequests

	if err := retry.DoWithConfig(ctx, l.retryConfig, func() error {
		input := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				l.table: currentRequests,
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

		// Update requests for next retry.
		currentRequests = unprocessed
		return &common.DatabaseOperationError{
			Database: "DynamoDB",
			Op:       "batch write (unprocessed items)",
			Reason:   fmt.Sprintf("unprocessed items: %d", len(unprocessed)),
			Err:      fmt.Errorf("unprocessed items: %v", unprocessed),
		}
	}); err != nil {
		return fmt.Errorf("batch write failed: %w", err)
	}
	return nil
}
