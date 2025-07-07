package loader

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"mongo2dynamo/internal/common"
)

// MockDynamoDBClient is a mock implementation of DBClient for testing.
type MockDBClient struct {
	mock.Mock
}

func (m *MockDBClient) CreateTable(ctx context.Context, params *dynamodb.CreateTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	args := m.Called(ctx, params)
	err := args.Error(1)
	if err != nil {
		return args.Get(0).(*dynamodb.CreateTableOutput), fmt.Errorf("mock error: %w", err)
	}
	return args.Get(0).(*dynamodb.CreateTableOutput), nil
}

func (m *MockDBClient) DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	args := m.Called(ctx, params)
	err := args.Error(1)
	if err != nil {
		return args.Get(0).(*dynamodb.DescribeTableOutput), fmt.Errorf("mock error: %w", err)
	}
	return args.Get(0).(*dynamodb.DescribeTableOutput), nil
}

func (m *MockDBClient) BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	args := m.Called(ctx, params)
	err := args.Error(1)
	if err != nil {
		return args.Get(0).(*dynamodb.BatchWriteItemOutput), fmt.Errorf("mock error: %w", err)
	}
	return args.Get(0).(*dynamodb.BatchWriteItemOutput), nil
}

// newTestLoader creates a new DynamoDB loader with custom marshal function for testing.
func newTestLoader(client DBClient, table string, marshal MarshalFunc) *DynamoLoader {
	return &DynamoLoader{
		client:  client,
		table:   table,
		marshal: marshal,
	}
}

func TestNewDynamoLoader(t *testing.T) {
	mockClient := &MockDBClient{}
	table := "test-table"

	dynamoLoader := newDynamoLoader(mockClient, table)

	assert.NotNil(t, dynamoLoader)
	assert.Equal(t, mockClient, dynamoLoader.client)
	assert.Equal(t, table, dynamoLoader.table)
	assert.NotNil(t, dynamoLoader.marshal)
}

func TestDynamoLoader_Load_Success(t *testing.T) {
	mockClient := &MockDBClient{}
	dynamoLoader := newDynamoLoader(mockClient, "test-table")

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

	err := dynamoLoader.Load(context.Background(), data)

	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestDynamoLoader_Load_ComplexDataTypes(t *testing.T) {
	mockClient := &MockDBClient{}
	dynamoLoader := newDynamoLoader(mockClient, "test-table")

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

	err := dynamoLoader.Load(context.Background(), data)

	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestDynamoLoader_Load_EmptyData(t *testing.T) {
	mockClient := &MockDBClient{}
	dynamoLoader := newDynamoLoader(mockClient, "test-table")

	data := []map[string]interface{}{}

	err := dynamoLoader.Load(context.Background(), data)

	assert.NoError(t, err)
	mockClient.AssertNotCalled(t, "BatchWriteItem")
}

func TestDynamoLoader_Load_ExactBatchSize(t *testing.T) {
	mockClient := &MockDBClient{}
	dynamoLoader := newDynamoLoader(mockClient, "test-table")

	// Data with exactly batchSize (25) items.
	data := make([]map[string]interface{}, 25)
	for i := 0; i < 25; i++ {
		data[i] = map[string]interface{}{"id": i, "name": "test"}
	}

	mockClient.On("BatchWriteItem", mock.Anything, mock.MatchedBy(func(input *dynamodb.BatchWriteItemInput) bool {
		return len(input.RequestItems["test-table"]) == 25
	})).Return(&dynamodb.BatchWriteItemOutput{
		UnprocessedItems: map[string][]types.WriteRequest{},
	}, nil)

	err := dynamoLoader.Load(context.Background(), data)

	assert.NoError(t, err)
	mockClient.AssertNumberOfCalls(t, "BatchWriteItem", 1)
}

func TestDynamoLoader_Load_BatchSizeExceeded(t *testing.T) {
	mockClient := &MockDBClient{}
	dynamoLoader := newDynamoLoader(mockClient, "test-table")

	// Create data that exceeds batch size (25 items).
	data := make([]map[string]interface{}, 30)
	for i := 0; i < 30; i++ {
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

	err := dynamoLoader.Load(context.Background(), data)

	assert.NoError(t, err)
	mockClient.AssertNumberOfCalls(t, "BatchWriteItem", 2)
}

func TestDynamoLoader_Load_UnprocessedItemsRetry(t *testing.T) {
	mockClient := &MockDBClient{}
	dynamoLoader := newDynamoLoader(mockClient, "test-table")

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

	err := dynamoLoader.Load(context.Background(), data)

	assert.NoError(t, err)
	mockClient.AssertNumberOfCalls(t, "BatchWriteItem", 2)
}

func TestDynamoLoader_Load_ExponentialBackoff(t *testing.T) {
	mockClient := &MockDBClient{}
	dynamoLoader := newDynamoLoader(mockClient, "test-table")

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

	err := dynamoLoader.Load(context.Background(), data)

	assert.NoError(t, err)
	mockClient.AssertNumberOfCalls(t, "BatchWriteItem", 2)
}

func TestDynamoLoader_Load_MarshalError(t *testing.T) {
	mockClient := &MockDBClient{}

	// Mock marshal function that returns an error.
	mockMarshal := func(_ interface{}) (map[string]types.AttributeValue, error) {
		return nil, errors.New("marshal error")
	}

	dynamoLoader := newTestLoader(mockClient, "test-table", mockMarshal)

	data := []map[string]interface{}{
		{"id": "1", "name": "test1"},
	}

	err := dynamoLoader.Load(context.Background(), data)

	assert.Error(t, err)
	var validationError *common.DataValidationError
	assert.ErrorAs(t, err, &validationError)
	assert.Equal(t, "DynamoDB", validationError.Database)
	assert.Equal(t, "marshal", validationError.Op)
	assert.Equal(t, "marshal error", validationError.Reason)
	mockClient.AssertNotCalled(t, "BatchWriteItem")
}

func TestDynamoLoader_Load_DynamoDBError(t *testing.T) {
	mockClient := &MockDBClient{}
	dynamoLoader := newDynamoLoader(mockClient, "test-table")

	data := []map[string]interface{}{
		{"id": "1", "name": "test1"},
	}

	expectedError := errors.New("DynamoDB connection failed")
	mockClient.On("BatchWriteItem", mock.Anything, mock.Anything).Return(
		&dynamodb.BatchWriteItemOutput{}, expectedError)

	err := dynamoLoader.Load(context.Background(), data)

	assert.Error(t, err)
	var dbError *common.DatabaseOperationError
	assert.ErrorAs(t, err, &dbError)
	assert.Equal(t, "DynamoDB", dbError.Database)
	assert.Equal(t, "batch write", dbError.Op)
	assert.Contains(t, dbError.Reason, expectedError.Error())
}

func TestDynamoLoader_Load_UnprocessedItemsMaxRetriesExceeded(t *testing.T) {
	mockClient := &MockDBClient{}
	dynamoLoader := newDynamoLoader(mockClient, "test-table")

	data := []map[string]interface{}{
		{"id": "1", "name": "test1"},
	}

	unprocessedItems := []types.WriteRequest{
		{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{}}},
	}

	// All retries return unprocessed items.
	for i := 0; i < 5; i++ {
		mockClient.On("BatchWriteItem", mock.Anything, mock.Anything).Return(&dynamodb.BatchWriteItemOutput{
			UnprocessedItems: map[string][]types.WriteRequest{
				"test-table": unprocessedItems,
			},
		}, nil)
	}

	err := dynamoLoader.Load(context.Background(), data)

	assert.Error(t, err)
	var dbError *common.DatabaseOperationError
	assert.ErrorAs(t, err, &dbError)
	assert.Equal(t, "DynamoDB", dbError.Database)
	assert.Equal(t, "batch write (unprocessed items)", dbError.Op)
	assert.Contains(t, dbError.Reason, "failed to process all items after 5 retries")
	mockClient.AssertNumberOfCalls(t, "BatchWriteItem", 5)
}

func TestDynamoLoader_Load_ContextCancellation(t *testing.T) {
	mockClient := &MockDBClient{}
	dynamoLoader := newDynamoLoader(mockClient, "test-table")

	data := []map[string]interface{}{
		{"id": "1", "name": "test1"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately.

	expectedError := context.Canceled
	mockClient.On("BatchWriteItem", mock.Anything, mock.Anything).Return(
		&dynamodb.BatchWriteItemOutput{}, expectedError)

	err := dynamoLoader.Load(ctx, data)

	assert.Error(t, err)
	var dbError *common.DatabaseOperationError
	assert.ErrorAs(t, err, &dbError)
	assert.Contains(t, dbError.Reason, expectedError.Error())
}

func TestCalculateBackoffWithJitter(t *testing.T) {
	tests := []struct {
		name     string
		attempt  int
		expected time.Duration
	}{
		{
			name:     "attempt 0",
			attempt:  0,
			expected: 100 * time.Millisecond, // baseDelay.
		},
		{
			name:     "attempt 1",
			attempt:  1,
			expected: 200 * time.Millisecond, // 2^1 * baseDelay.
		},
		{
			name:     "attempt 2",
			attempt:  2,
			expected: 400 * time.Millisecond, // 2^2 * baseDelay.
		},
		{
			name:     "attempt 3",
			attempt:  3,
			expected: 800 * time.Millisecond, // 2^3 * baseDelay.
		},
		{
			name:     "attempt 4",
			attempt:  4,
			expected: 1600 * time.Millisecond, // 2^4 * baseDelay.
		},
		{
			name:     "attempt 5",
			attempt:  5,
			expected: 3200 * time.Millisecond, // 2^5 * baseDelay.
		},
		{
			name:     "attempt 9 (should be capped at maxDelay)",
			attempt:  9,
			expected: 30 * time.Second, // maxDelay.
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateBackoffWithJitter(tt.attempt)

			// Check that result is within expected range (with jitter).
			// Jitter range is 0.5 ~ 1.5, so result should be between 0.5x and 1.5x of expected.
			minExpected := time.Duration(float64(tt.expected) * 0.5)
			maxExpected := time.Duration(float64(tt.expected) * 1.5)

			assert.GreaterOrEqual(t, result, minExpected,
				"Backoff duration should be at least 0.5x of expected")
			assert.LessOrEqual(t, result, maxExpected,
				"Backoff duration should be at most 1.5x of expected")

			// For attempts that should be capped, ensure they don't exceed maxDelay.
			if tt.attempt >= 9 {
				assert.LessOrEqual(t, result, maxDelay,
					"Backoff duration should not exceed maxDelay for high attempts")
			}
		})
	}
}

func TestCalculateBackoffWithJitter_JitterVariation(t *testing.T) {
	// Test that jitter provides variation across multiple calls.
	attempt := 2
	expected := 400 * time.Millisecond // 2^2 * baseDelay.

	results := make([]time.Duration, 100)
	for i := 0; i < 100; i++ {
		results[i] = calculateBackoffWithJitter(attempt)
	}

	// Check that we have some variation (not all results are the same).
	firstResult := results[0]
	allSame := true
	for _, result := range results {
		if result != firstResult {
			allSame = false
			break
		}
	}

	assert.False(t, allSame, "Jitter should provide variation across multiple calls")

	// Check that all results are within expected range.
	minExpected := time.Duration(float64(expected) * 0.5)
	maxExpected := time.Duration(float64(expected) * 1.5)

	for _, result := range results {
		assert.GreaterOrEqual(t, result, minExpected,
			"All backoff durations should be at least 0.5x of expected")
		assert.LessOrEqual(t, result, maxExpected,
			"All backoff durations should be at most 1.5x of expected")
	}
}

func TestCalculateBackoffWithJitter_EdgeCases(t *testing.T) {
	// Test negative attempt (should be treated as 0).
	result := calculateBackoffWithJitter(-1)
	minExpected := time.Duration(float64(baseDelay) * 0.5)
	maxExpected := time.Duration(float64(baseDelay) * 1.5)
	assert.GreaterOrEqual(t, result, minExpected, "Negative attempt should be at least 0.5x baseDelay")
	assert.LessOrEqual(t, result, maxExpected, "Negative attempt should be at most 1.5x baseDelay")

	// Test very large attempt (should be capped).
	result = calculateBackoffWithJitter(100)
	assert.LessOrEqual(t, result, maxDelay, "Very large attempt should be capped at maxDelay")
}
