package dynamo

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"mongo2dynamo/internal/common"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockDynamoDBClient is a mock implementation of DBClient for testing.
type MockDBClient struct {
	mock.Mock
}

func (m *MockDBClient) BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	args := m.Called(ctx, params)
	err := args.Error(1)
	if err != nil {
		return args.Get(0).(*dynamodb.BatchWriteItemOutput), fmt.Errorf("mock error: %w", err)
	}
	return args.Get(0).(*dynamodb.BatchWriteItemOutput), nil
}

// newTestWriter creates a new DynamoDB writer with custom marshal function for testing.
func newTestWriter(client DBClient, table string, marshal MarshalFunc) *Writer {
	return &Writer{
		client:  client,
		table:   table,
		marshal: marshal,
	}
}

func TestNewWriter(t *testing.T) {
	mockClient := &MockDBClient{}
	table := "test-table"

	writer := newWriter(mockClient, table)

	assert.NotNil(t, writer)
	assert.Equal(t, mockClient, writer.client)
	assert.Equal(t, table, writer.table)
	assert.NotNil(t, writer.marshal)
}

func TestWriter_Write_Success(t *testing.T) {
	mockClient := &MockDBClient{}
	writer := newWriter(mockClient, "test-table")

	data := []map[string]interface{}{
		{"id": "1", "name": "test1"},
		{"id": "2", "name": "test2"},
	}

	// Expect BatchWriteItem to be called once with the correct parameters.
	mockClient.On("BatchWriteItem", mock.Anything, mock.MatchedBy(func(input *dynamodb.BatchWriteItemInput) bool {
		return len(input.RequestItems["test-table"]) == 2
	})).Return(&dynamodb.BatchWriteItemOutput{
		UnprocessedItems: map[string][]types.WriteRequest{},
	}, nil)

	err := writer.Write(context.Background(), data)

	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestWriter_Write_ComplexDataTypes(t *testing.T) {
	mockClient := &MockDBClient{}
	writer := newWriter(mockClient, "test-table")

	data := []map[string]interface{}{
		{
			"id":      "1",
			"name":    "test",
			"age":     25,
			"active":  true,
			"scores":  []int{90, 85, 95},
			"details": map[string]interface{}{"city": "Seoul", "country": "Korea"},
		},
	}

	mockClient.On("BatchWriteItem", mock.Anything, mock.MatchedBy(func(input *dynamodb.BatchWriteItemInput) bool {
		return len(input.RequestItems["test-table"]) == 1
	})).Return(&dynamodb.BatchWriteItemOutput{
		UnprocessedItems: map[string][]types.WriteRequest{},
	}, nil)

	err := writer.Write(context.Background(), data)

	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestWriter_Write_EmptyData(t *testing.T) {
	mockClient := &MockDBClient{}
	writer := newWriter(mockClient, "test-table")

	data := []map[string]interface{}{}

	err := writer.Write(context.Background(), data)

	assert.NoError(t, err)
	mockClient.AssertNotCalled(t, "BatchWriteItem")
}

func TestWriter_Write_ExactBatchSize(t *testing.T) {
	mockClient := &MockDBClient{}
	writer := newWriter(mockClient, "test-table")

	// Data with exactly batchSize (25) items.
	data := make([]map[string]interface{}, 25)
	for i := range 25 {
		data[i] = map[string]interface{}{"id": i, "name": "test"}
	}

	mockClient.On("BatchWriteItem", mock.Anything, mock.MatchedBy(func(input *dynamodb.BatchWriteItemInput) bool {
		return len(input.RequestItems["test-table"]) == 25
	})).Return(&dynamodb.BatchWriteItemOutput{
		UnprocessedItems: map[string][]types.WriteRequest{},
	}, nil)

	err := writer.Write(context.Background(), data)

	assert.NoError(t, err)
	mockClient.AssertNumberOfCalls(t, "BatchWriteItem", 1)
}

func TestWriter_Write_BatchSizeExceeded(t *testing.T) {
	mockClient := &MockDBClient{}
	writer := newWriter(mockClient, "test-table")

	// Create data that exceeds batch size (25 items).
	data := make([]map[string]interface{}, 30)
	for i := range 30 {
		data[i] = map[string]interface{}{"id": i, "name": "test"}
	}

	// Expect BatchWriteItem to be called twice (25 items + 5 items).
	mockClient.On("BatchWriteItem", mock.Anything, mock.MatchedBy(func(input *dynamodb.BatchWriteItemInput) bool {
		return len(input.RequestItems["test-table"]) == 25
	})).Return(&dynamodb.BatchWriteItemOutput{
		UnprocessedItems: map[string][]types.WriteRequest{},
	}, nil)

	mockClient.On("BatchWriteItem", mock.Anything, mock.MatchedBy(func(input *dynamodb.BatchWriteItemInput) bool {
		return len(input.RequestItems["test-table"]) == 5
	})).Return(&dynamodb.BatchWriteItemOutput{
		UnprocessedItems: map[string][]types.WriteRequest{},
	}, nil)

	err := writer.Write(context.Background(), data)

	assert.NoError(t, err)
	mockClient.AssertNumberOfCalls(t, "BatchWriteItem", 2)
}

func TestWriter_Write_UnprocessedItemsRetry(t *testing.T) {
	mockClient := &MockDBClient{}
	writer := newWriter(mockClient, "test-table")

	data := []map[string]interface{}{
		{"id": "1", "name": "test1"},
		{"id": "2", "name": "test2"},
	}

	// First call returns unprocessed items.
	unprocessedItems := []types.WriteRequest{
		{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{}}},
	}
	mockClient.On("BatchWriteItem", mock.Anything, mock.MatchedBy(func(input *dynamodb.BatchWriteItemInput) bool {
		return len(input.RequestItems["test-table"]) == 2
	})).Return(&dynamodb.BatchWriteItemOutput{
		UnprocessedItems: map[string][]types.WriteRequest{
			"test-table": unprocessedItems,
		},
	}, nil)

	// Second call (retry) succeeds.
	mockClient.On("BatchWriteItem", mock.Anything, mock.MatchedBy(func(input *dynamodb.BatchWriteItemInput) bool {
		return len(input.RequestItems["test-table"]) == 1
	})).Return(&dynamodb.BatchWriteItemOutput{
		UnprocessedItems: map[string][]types.WriteRequest{},
	}, nil)

	err := writer.Write(context.Background(), data)

	assert.NoError(t, err)
	mockClient.AssertNumberOfCalls(t, "BatchWriteItem", 2)
}

func TestWriter_Write_ExponentialBackoff(t *testing.T) {
	mockClient := &MockDBClient{}
	writer := newWriter(mockClient, "test-table")

	data := []map[string]interface{}{
		{"id": "1", "name": "test1"},
	}

	// First call returns unprocessed items.
	mockClient.On("BatchWriteItem", mock.Anything, mock.Anything).Return(&dynamodb.BatchWriteItemOutput{
		UnprocessedItems: map[string][]types.WriteRequest{
			"test-table": {
				{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{}}},
			},
		},
	}, nil).Once()

	// Second call succeeds.
	mockClient.On("BatchWriteItem", mock.Anything, mock.Anything).Return(&dynamodb.BatchWriteItemOutput{
		UnprocessedItems: map[string][]types.WriteRequest{},
	}, nil).Once()

	err := writer.Write(context.Background(), data)

	assert.NoError(t, err)
	mockClient.AssertNumberOfCalls(t, "BatchWriteItem", 2)
}

func TestWriter_Write_MarshalError(t *testing.T) {
	mockClient := &MockDBClient{}

	// Mock marshal function that returns an error.
	mockMarshal := func(_ interface{}) (map[string]types.AttributeValue, error) {
		return nil, errors.New("marshal error")
	}

	writer := newTestWriter(mockClient, "test-table", mockMarshal)

	data := []map[string]interface{}{
		{"id": "1", "name": "test1"},
	}

	err := writer.Write(context.Background(), data)

	assert.Error(t, err)
	var validationError *common.DataValidationError
	assert.ErrorAs(t, err, &validationError)
	assert.Equal(t, "DynamoDB", validationError.Database)
	assert.Equal(t, "marshal", validationError.Op)
	assert.Equal(t, "marshal error", validationError.Reason)
	mockClient.AssertNotCalled(t, "BatchWriteItem")
}

func TestWriter_Write_DynamoDBError(t *testing.T) {
	mockClient := &MockDBClient{}
	writer := newWriter(mockClient, "test-table")

	data := []map[string]interface{}{
		{"id": "1", "name": "test1"},
	}

	expectedError := errors.New("DynamoDB connection failed")
	mockClient.On("BatchWriteItem", mock.Anything, mock.Anything).Return(
		&dynamodb.BatchWriteItemOutput{}, expectedError)

	err := writer.Write(context.Background(), data)

	assert.Error(t, err)
	var dbError *common.DatabaseOperationError
	assert.ErrorAs(t, err, &dbError)
	assert.Equal(t, "DynamoDB", dbError.Database)
	assert.Equal(t, "batch write", dbError.Op)
	assert.Contains(t, dbError.Reason, expectedError.Error())
}

func TestWriter_Write_UnprocessedItemsMaxRetriesExceeded(t *testing.T) {
	mockClient := &MockDBClient{}
	writer := newWriter(mockClient, "test-table")

	data := []map[string]interface{}{
		{"id": "1", "name": "test1"},
	}

	unprocessedItems := []types.WriteRequest{
		{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{}}},
	}

	// All retries return unprocessed items.
	for range 5 {
		mockClient.On("BatchWriteItem", mock.Anything, mock.Anything).Return(&dynamodb.BatchWriteItemOutput{
			UnprocessedItems: map[string][]types.WriteRequest{
				"test-table": unprocessedItems,
			},
		}, nil)
	}

	err := writer.Write(context.Background(), data)

	assert.Error(t, err)
	var dbError *common.DatabaseOperationError
	assert.ErrorAs(t, err, &dbError)
	assert.Equal(t, "DynamoDB", dbError.Database)
	assert.Equal(t, "batch write (unprocessed items)", dbError.Op)
	assert.Contains(t, dbError.Reason, "failed to process all items after 5 retries")
	mockClient.AssertNumberOfCalls(t, "BatchWriteItem", 5)
}

func TestWriter_Write_ContextCancellation(t *testing.T) {
	mockClient := &MockDBClient{}
	writer := newWriter(mockClient, "test-table")

	data := []map[string]interface{}{
		{"id": "1", "name": "test1"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	expectedError := context.Canceled
	mockClient.On("BatchWriteItem", mock.Anything, mock.Anything).Return(
		&dynamodb.BatchWriteItemOutput{}, expectedError)

	err := writer.Write(ctx, data)

	assert.Error(t, err)
	var dbError *common.DatabaseOperationError
	assert.ErrorAs(t, err, &dbError)
	assert.Contains(t, dbError.Reason, expectedError.Error())
}
