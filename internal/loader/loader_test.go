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
	"github.com/stretchr/testify/require"

	"mongo2dynamo/internal/common"
	"mongo2dynamo/internal/config"
	"mongo2dynamo/internal/retry"
)

const defaultMaxRetries = 5

// MockDBClient is a mock implementation of the DBClient interface.
type MockDBClient struct {
	mock.Mock
}

// getMockOutput is a helper function to get the output from a mock call.
func getMockOutput[T any](args mock.Arguments, index int) (*T, error) {
	if args.Get(index) == nil {
		if err := args.Error(1); err != nil {
			return nil, fmt.Errorf("mock error: %w", err)
		}
		return nil, nil
	}

	output, ok := args.Get(index).(*T)
	if !ok {
		return nil, fmt.Errorf("misconfigured mock: unexpected return type: got %T", args.Get(index))
	}

	if err := args.Error(1); err != nil {
		return output, fmt.Errorf("mock error: %w", err)
	}

	return output, nil
}

// CreateTable is a mock implementation of the CreateTable method.
func (m *MockDBClient) CreateTable(ctx context.Context, params *dynamodb.CreateTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	args := m.Called(ctx, params)
	return getMockOutput[dynamodb.CreateTableOutput](args, 0)
}

// DescribeTable is a mock implementation of the DescribeTable method.
func (m *MockDBClient) DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	args := m.Called(ctx, params)
	return getMockOutput[dynamodb.DescribeTableOutput](args, 0)
}

// BatchWriteItem is a mock implementation of the BatchWriteItem method.
func (m *MockDBClient) BatchWriteItem(ctx context.Context, params *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	args := m.Called(ctx, params)
	return getMockOutput[dynamodb.BatchWriteItemOutput](args, 0)
}

// newTestLoader creates a new DynamoDB loader with a custom marshal function for testing.
func newTestLoader(client DBClient, table string, marshal MarshalFunc) *DynamoLoader {
	return &DynamoLoader{
		client:      client,
		table:       table,
		marshal:     marshal,
		retryConfig: retry.NewConfig().WithMaxRetries(defaultMaxRetries),
	}
}

func TestNewDynamoLoader(t *testing.T) {
	mockClient := &MockDBClient{}
	table := "test-table"

	dynamoLoader := newDynamoLoader(mockClient, table, defaultMaxRetries)

	assert.NotNil(t, dynamoLoader)
	assert.Equal(t, mockClient, dynamoLoader.client)
	assert.Equal(t, table, dynamoLoader.table)
	assert.NotNil(t, dynamoLoader.marshal)
}

func TestEnsureTableExists_CreatesNewTable(t *testing.T) {
	mockClient := &MockDBClient{}
	loader := newDynamoLoader(mockClient, "test-table", 5)
	cfg := &config.Config{
		AutoApprove: true,
		DynamoTable: "test-table",
	}

	mockClient.On("DescribeTable", mock.Anything, mock.Anything).Return(nil, &types.ResourceNotFoundException{}).Once()
	mockClient.On("CreateTable", mock.Anything, mock.Anything).Return(&dynamodb.CreateTableOutput{}, nil).Once()
	mockClient.On("DescribeTable", mock.Anything, mock.Anything).Return(&dynamodb.DescribeTableOutput{
		Table: &types.TableDescription{TableStatus: types.TableStatusActive},
	}, nil).Once()

	err := loader.ensureTableExists(context.Background(), cfg)
	require.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestEnsureTableExists_TableAlreadyExists(t *testing.T) {
	mockClient := &MockDBClient{}
	loader := newDynamoLoader(mockClient, "test-table", 5)
	cfg := &config.Config{DynamoTable: "test-table"}

	mockClient.On("DescribeTable", mock.Anything, mock.Anything).Return(&dynamodb.DescribeTableOutput{
		Table: &types.TableDescription{TableStatus: types.TableStatusActive},
	}, nil).Once()

	err := loader.ensureTableExists(context.Background(), cfg)
	require.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestDynamoLoader_Load_Success(t *testing.T) {
	mockClient := &MockDBClient{}
	dynamoLoader := newDynamoLoader(mockClient, "test-table", defaultMaxRetries)

	data := []map[string]any{{"id": "1", "name": "test1"}}

	mockClient.On("BatchWriteItem", mock.Anything, mock.Anything).Return(&dynamodb.BatchWriteItemOutput{
		UnprocessedItems: make(map[string][]types.WriteRequest),
	}, nil)

	err := dynamoLoader.Load(context.Background(), data)
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestDynamoLoader_Load_ComplexDataTypes(t *testing.T) {
	mockClient := &MockDBClient{}
	dynamoLoader := newDynamoLoader(mockClient, "test-table", defaultMaxRetries)
	data := []map[string]any{
		{
			"id":        "user123",
			"age":       30,
			"is_active": true,
			"roles":     []string{"admin", "editor"},
			"address":   map[string]string{"city": "New York", "zip": "10001"},
		},
	}

	mockClient.On("BatchWriteItem", mock.Anything, mock.Anything).Return(&dynamodb.BatchWriteItemOutput{
		UnprocessedItems: make(map[string][]types.WriteRequest),
	}, nil)

	err := dynamoLoader.Load(context.Background(), data)
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestDynamoLoader_Load_EmptyData(t *testing.T) {
	mockClient := &MockDBClient{}
	dynamoLoader := newDynamoLoader(mockClient, "test-table", defaultMaxRetries)

	err := dynamoLoader.Load(context.Background(), []map[string]any{})
	assert.NoError(t, err)
	mockClient.AssertNotCalled(t, "BatchWriteItem")
}

func TestDynamoLoader_Load_ExactBatchSize(t *testing.T) {
	mockClient := &MockDBClient{}
	dynamoLoader := newDynamoLoader(mockClient, "test-table", defaultMaxRetries)
	data := make([]map[string]any, 25)
	for i := 0; i < 25; i++ {
		data[i] = map[string]any{"id": i, "name": "test"}
	}

	mockClient.On("BatchWriteItem", mock.Anything, mock.MatchedBy(func(input *dynamodb.BatchWriteItemInput) bool {
		return len(input.RequestItems["test-table"]) == 25
	})).Return(&dynamodb.BatchWriteItemOutput{}, nil).Once()

	err := dynamoLoader.Load(context.Background(), data)
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestDynamoLoader_Load_BatchSizeExceeded(t *testing.T) {
	mockClient := &MockDBClient{}
	dynamoLoader := newDynamoLoader(mockClient, "test-table", defaultMaxRetries)
	data := make([]map[string]any, 30)
	for i := 0; i < 30; i++ {
		data[i] = map[string]any{"id": i, "name": "test"}
	}

	mockClient.On("BatchWriteItem", mock.Anything, mock.MatchedBy(func(input *dynamodb.BatchWriteItemInput) bool {
		return len(input.RequestItems["test-table"]) == 25
	})).Return(&dynamodb.BatchWriteItemOutput{}, nil).Once()
	mockClient.On("BatchWriteItem", mock.Anything, mock.MatchedBy(func(input *dynamodb.BatchWriteItemInput) bool {
		return len(input.RequestItems["test-table"]) == 5
	})).Return(&dynamodb.BatchWriteItemOutput{}, nil).Once()

	err := dynamoLoader.Load(context.Background(), data)
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestDynamoLoader_Load_UnprocessedItemsRetry(t *testing.T) {
	mockClient := &MockDBClient{}
	dynamoLoader := newDynamoLoader(mockClient, "test-table", defaultMaxRetries)
	data := []map[string]any{
		{"id": "1", "name": "test1"},
		{"id": "2", "name": "test2"},
	}

	unprocessed := []types.WriteRequest{{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{
		"id":   &types.AttributeValueMemberS{Value: "2"},
		"name": &types.AttributeValueMemberS{Value: "test2"},
	}}}}

	mockClient.On("BatchWriteItem", mock.Anything, mock.Anything).Return(&dynamodb.BatchWriteItemOutput{
		UnprocessedItems: map[string][]types.WriteRequest{"test-table": unprocessed},
	}, nil).Once()

	mockClient.On("BatchWriteItem", mock.Anything, mock.MatchedBy(func(input *dynamodb.BatchWriteItemInput) bool {
		return len(input.RequestItems["test-table"]) == 1
	})).Return(&dynamodb.BatchWriteItemOutput{}, nil).Once()

	err := dynamoLoader.Load(context.Background(), data)
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
}

func TestDynamoLoader_Load_ExponentialBackoff(t *testing.T) {
	mockClient := &MockDBClient{}
	dynamoLoader := newDynamoLoader(mockClient, "test-table", defaultMaxRetries)

	data := []map[string]any{
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
	mockMarshal := func(_ any) (map[string]types.AttributeValue, error) {
		return nil, errors.New("marshal error")
	}

	dynamoLoader := newTestLoader(mockClient, "test-table", mockMarshal)

	data := []map[string]any{
		{"id": "1", "name": "test1"},
	}

	err := dynamoLoader.Load(context.Background(), data)

	require.Error(t, err)
	var validationError *common.DataValidationError
	require.ErrorAs(t, err, &validationError)
	assert.Equal(t, "DynamoDB", validationError.Database)
	assert.Equal(t, "marshal", validationError.Op)
	mockClient.AssertNotCalled(t, "BatchWriteItem")
}

func TestDynamoLoader_Load_DynamoDBError(t *testing.T) {
	mockClient := &MockDBClient{}
	dynamoLoader := newDynamoLoader(mockClient, "test-table", defaultMaxRetries)
	expectedErr := errors.New("dynamodb error")
	data := []map[string]any{{"id": "1"}}

	mockClient.On("BatchWriteItem", mock.Anything, mock.Anything).Return(nil, expectedErr)

	err := dynamoLoader.Load(context.Background(), data)
	require.Error(t, err)
	var dbError *common.DatabaseOperationError
	require.ErrorAs(t, err, &dbError, "error should be a DatabaseOperationError")
	assert.Contains(t, dbError.Err.Error(), expectedErr.Error())
}

func TestDynamoLoader_Load_UnprocessedItemsMaxRetriesExceeded(t *testing.T) {
	mockClient := &MockDBClient{}
	dynamoLoader := newDynamoLoader(mockClient, "test-table", 1) // Only 1 attempt.
	data := []map[string]any{{"id": "1"}}

	unprocessed := []types.WriteRequest{{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{
		"id": &types.AttributeValueMemberS{Value: "1"},
	}}}}

	mockClient.On("BatchWriteItem", mock.Anything, mock.Anything).Return(&dynamodb.BatchWriteItemOutput{
		UnprocessedItems: map[string][]types.WriteRequest{"test-table": unprocessed},
	}, nil).Times(2) // Initial attempt + 1 retry.

	err := dynamoLoader.Load(context.Background(), data)
	require.Error(t, err)
	var dbError *common.DatabaseOperationError
	require.ErrorAs(t, err, &dbError)
	assert.Contains(t, dbError.Reason, "unprocessed items:")
}

func TestDynamoLoader_Load_ContextCancellation(t *testing.T) {
	mockClient := &MockDBClient{}
	dynamoLoader := newDynamoLoader(mockClient, "test-table", defaultMaxRetries)
	data := []map[string]any{{"id": "1"}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel the context immediately.

	err := dynamoLoader.Load(ctx, data)
	assert.NoError(t, err, "cancelled context should gracefully stop without returning an error from the loader itself")
	mockClient.AssertNotCalled(t, "BatchWriteItem")
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

	backoffConfig := retry.NewBackoffConfig()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := backoffConfig.Calculate(tt.attempt)

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
				assert.LessOrEqual(t, result, retry.DefaultMaxDelay,
					"Backoff duration should not exceed maxDelay for high attempts")
			}
		})
	}
}

func TestCalculateBackoffWithJitter_JitterVariation(t *testing.T) {
	// Test that jitter provides variation across multiple calls.
	backoffConfig := retry.NewBackoffConfig()
	attempt := 2
	expected := 400 * time.Millisecond // 2^2 * baseDelay.

	results := make([]time.Duration, 100)
	for i := 0; i < 100; i++ {
		results[i] = backoffConfig.Calculate(attempt)
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
	// Test very large attempt (should be capped).
	backoffConfig := retry.NewBackoffConfig()
	result := backoffConfig.Calculate(100)
	assert.LessOrEqual(t, result, retry.DefaultMaxDelay, "Very large attempt should be capped at maxDelay")
}
