package extractor

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"

	"mongo2dynamo/internal/common"
)

// MockCollection is a mock implementation of Collection interface.
type MockCollection struct {
	mock.Mock
}

func (m *MockCollection) Find(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (Cursor, error) {
	args := m.Called(ctx, filter, opts)
	if cursor := args.Get(0); cursor != nil {
		err := args.Error(1)
		if err != nil {
			return nil, fmt.Errorf("mock Find error: %w", err)
		}
		return cursor.(Cursor), nil
	}
	err := args.Error(1)
	if err != nil {
		return nil, fmt.Errorf("mock Find error: %w", err)
	}
	return nil, nil
}

// MockCursor is a mock implementation of Cursor interface.
type MockCursor struct {
	mock.Mock
	docs      []map[string]interface{}
	current   int
	decodeErr error
	nextErr   error
}

func (m *MockCursor) Next(_ context.Context) bool {
	if m.nextErr != nil {
		return false
	}
	if m.current < len(m.docs) {
		m.current++
		return true
	}
	return false
}

func (m *MockCursor) Decode(val interface{}) error {
	if m.decodeErr != nil {
		return m.decodeErr
	}
	if m.current > 0 && m.current <= len(m.docs) {
		*(val.(*map[string]interface{})) = m.docs[m.current-1]
		return nil
	}
	return errors.New("no document to decode")
}

func (m *MockCursor) Close(_ context.Context) error {
	args := m.Called(nil)
	if args.Error(0) == nil {
		return nil
	}
	return fmt.Errorf("mock Close error: %w", args.Error(0))
}

func (m *MockCursor) Err() error {
	args := m.Called()
	if args.Error(0) == nil {
		return nil
	}
	return fmt.Errorf("mock Err error: %w", args.Error(0))
}

func TestMongoExtractor_Extract_EmptyCollection(t *testing.T) {
	mockCollection := new(MockCollection)
	mockCursor := &MockCursor{
		docs: []map[string]interface{}{},
	}
	mockCursor.On("Close", mock.Anything).Return(nil)
	mockCursor.On("Err").Return(nil)
	mockCollection.On("Find", mock.Anything, mock.Anything, mock.Anything).Return(mockCursor, nil)

	mongoExtractor := newMongoExtractor(mockCollection, primitive.M{})
	var processedDocs []map[string]interface{}

	err := mongoExtractor.Extract(context.Background(), func(chunk []map[string]interface{}) error {
		processedDocs = append(processedDocs, chunk...)
		return nil
	})

	assert.NoError(t, err)
	assert.Empty(t, processedDocs)
	mockCollection.AssertExpectations(t)
	mockCursor.AssertExpectations(t)
}

func TestExtractor_Extract_SingleChunk(t *testing.T) {
	mockCollection := new(MockCollection)
	testDocs := []map[string]interface{}{
		{"_id": "1", "name": "doc1"},
		{"_id": "2", "name": "doc2"},
	}
	mockCursor := &MockCursor{
		docs: testDocs,
	}
	mockCursor.On("Close", mock.Anything).Return(nil)
	mockCursor.On("Err").Return(nil)
	mockCollection.On("Find", mock.Anything, mock.Anything, mock.Anything).Return(mockCursor, nil)

	mongoExtractor := newMongoExtractor(mockCollection, primitive.M{})
	var processedDocs []map[string]interface{}

	err := mongoExtractor.Extract(context.Background(), func(chunk []map[string]interface{}) error {
		processedDocs = append(processedDocs, chunk...)
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, testDocs, processedDocs)
	mockCollection.AssertExpectations(t)
	mockCursor.AssertExpectations(t)
}

func TestExtractor_Extract_MultipleChunks(t *testing.T) {
	mockCollection := new(MockCollection)
	testDocs := make([]map[string]interface{}, 2500) // More than chunkSize (2000).
	for i := range testDocs {
		testDocs[i] = map[string]interface{}{
			"_id":  i,
			"name": "doc" + strconv.Itoa(i),
		}
	}
	mockCursor := &MockCursor{
		docs: testDocs,
	}
	mockCursor.On("Close", mock.Anything).Return(nil)
	mockCursor.On("Err").Return(nil)
	mockCollection.On("Find", mock.Anything, mock.Anything, mock.Anything).Return(mockCursor, nil)

	mongoExtractor := newMongoExtractor(mockCollection, primitive.M{})
	var processedDocs []map[string]interface{}
	chunkCount := 0

	err := mongoExtractor.Extract(context.Background(), func(chunk []map[string]interface{}) error {
		processedDocs = append(processedDocs, chunk...)
		chunkCount++
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, testDocs, processedDocs)
	assert.Equal(t, 2, chunkCount) // Should be processed in 2 chunks (2000 + 500).
	mockCollection.AssertExpectations(t)
	mockCursor.AssertExpectations(t)
}

func TestExtractor_Extract_FindError(t *testing.T) {
	mockCollection := new(MockCollection)
	expectedErr := errors.New("find error")
	mockCollection.On("Find", mock.Anything, mock.Anything, mock.Anything).Return(nil, expectedErr)

	mongoExtractor := newMongoExtractor(mockCollection, primitive.M{})
	err := mongoExtractor.Extract(context.Background(), func(_ []map[string]interface{}) error {
		return nil
	})

	assert.Error(t, err)
	var dbErr *common.DatabaseOperationError
	assert.ErrorAs(t, err, &dbErr)
	assert.Equal(t, "MongoDB", dbErr.Database)
	assert.Equal(t, "find", dbErr.Op)
	mockCollection.AssertExpectations(t)
}

func TestExtractor_Extract_DecodeError(t *testing.T) {
	mockColl := new(MockCollection)
	mockCursor := &MockCursor{
		docs:      []map[string]interface{}{{"_id": "1"}},
		decodeErr: errors.New("decode error"),
	}
	mockCursor.On("Close", mock.Anything).Return(nil)
	mockColl.On("Find", mock.Anything, mock.Anything, mock.Anything).Return(mockCursor, nil)

	mongoExtractor := newMongoExtractor(mockColl, primitive.M{})

	err := mongoExtractor.Extract(context.Background(), func(_ []map[string]interface{}) error {
		return nil
	})

	assert.Error(t, err)
	var valErr *common.DataValidationError
	assert.ErrorAs(t, err, &valErr)
	assert.Equal(t, "MongoDB", valErr.Database)
	assert.Equal(t, "decode", valErr.Op)
	mockColl.AssertExpectations(t)
	mockCursor.AssertExpectations(t)
}

func TestExtractor_Extract_CallbackError(t *testing.T) {
	mockCollection := new(MockCollection)
	testDocs := []map[string]interface{}{
		{"_id": "1", "name": "doc1"},
		{"_id": "2", "name": "doc2"},
	}
	mockCursor := &MockCursor{
		docs: testDocs,
	}
	mockCursor.On("Close", mock.Anything).Return(nil)
	mockCollection.On("Find", mock.Anything, mock.Anything, mock.Anything).Return(mockCursor, nil)

	mongoExtractor := newMongoExtractor(mockCollection, primitive.M{})
	expectedErr := errors.New("callback error")

	err := mongoExtractor.Extract(context.Background(), func(_ []map[string]interface{}) error {
		return expectedErr
	})

	assert.Error(t, err)
	var chunkErr *common.ChunkCallbackError
	assert.ErrorAs(t, err, &chunkErr)
	assert.Equal(t, "handleChunk failed", chunkErr.Reason)
	assert.Equal(t, expectedErr, chunkErr.Err)
	mockCollection.AssertExpectations(t)
	mockCursor.AssertExpectations(t)
}

func TestExtractor_Extract_CursorError(t *testing.T) {
	mockCollection := new(MockCollection)
	mockCursor := &MockCursor{
		docs: []map[string]interface{}{{"_id": "1"}},
	}
	expectedErr := errors.New("cursor error")
	mockCursor.On("Close", mock.Anything).Return(nil)
	mockCursor.On("Err").Return(expectedErr)
	mockCollection.On("Find", mock.Anything, mock.Anything, mock.Anything).Return(mockCursor, nil)

	mongoExtractor := newMongoExtractor(mockCollection, primitive.M{})
	err := mongoExtractor.Extract(context.Background(), func(_ []map[string]interface{}) error {
		return nil
	})

	assert.Error(t, err)
	var dbErr *common.DatabaseOperationError
	assert.ErrorAs(t, err, &dbErr)
	assert.Equal(t, "MongoDB", dbErr.Database)
	assert.Equal(t, "cursor", dbErr.Op)
	mockCollection.AssertExpectations(t)
	mockCursor.AssertExpectations(t)
}

func TestExtractor_Extract_WithFilter(t *testing.T) {
	mockCollection := new(MockCollection)
	testDocs := []map[string]interface{}{
		{"_id": "1", "name": "doc1", "status": "active"},
		{"_id": "2", "name": "doc2", "status": "active"},
	}
	mockCursor := &MockCursor{
		docs: testDocs,
	}
	mockCursor.On("Close", mock.Anything).Return(nil)
	mockCursor.On("Err").Return(nil)

	// Expect Find to be called with the filter.
	expectedFilter := primitive.M{"status": "active"}
	mockCollection.On("Find", mock.Anything, expectedFilter, mock.Anything).Return(mockCursor, nil)

	mongoExtractor := newMongoExtractor(mockCollection, expectedFilter)
	var processedDocs []map[string]interface{}

	err := mongoExtractor.Extract(context.Background(), func(chunk []map[string]interface{}) error {
		processedDocs = append(processedDocs, chunk...)
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, testDocs, processedDocs)
	mockCollection.AssertExpectations(t)
	mockCursor.AssertExpectations(t)
}

func TestParseMongoFilter_ValidJSON(t *testing.T) {
	filterStr := `{"status": "active", "age": {"$gte": 18}}`
	filter, err := parseMongoFilter(filterStr)

	assert.NoError(t, err)
	assert.NotNil(t, filter)
	assert.Equal(t, "active", filter["status"])

	ageFilter, ok := filter["age"].(primitive.M)
	assert.True(t, ok)
	// JSON unmarshaling converts numbers to float64 by default.
	assert.Equal(t, float64(18), ageFilter["$gte"])
}

func TestParseMongoFilter_InvalidJSON(t *testing.T) {
	filterStr := `{"status": "active", "age": {"$gte": 18}` // Missing closing brace.
	filter, err := parseMongoFilter(filterStr)

	assert.Error(t, err)
	assert.Nil(t, filter)

	// Check that it's a FilterParseError.
	var filterErr *common.FilterParseError
	assert.ErrorAs(t, err, &filterErr)
	assert.Equal(t, "json unmarshal", filterErr.Op)
	assert.Equal(t, "invalid JSON syntax", filterErr.Reason)
	assert.Equal(t, filterStr, filterErr.Filter)
	assert.Contains(t, filterErr.Error(), "MongoDB filter parse error")
}

func TestParseMongoFilter_EmptyString(t *testing.T) {
	// Empty string should be treated as empty filter.
	filter, err := parseMongoFilter("")

	assert.NoError(t, err)
	assert.NotNil(t, filter)
	assert.Empty(t, filter)
}

func TestParseMongoFilter_ComplexFilter(t *testing.T) {
	filterStr := `{"$and": [{"status": "active"}, {"age": {"$gte": 18, "$lte": 65}}]}`
	filter, err := parseMongoFilter(filterStr)

	assert.NoError(t, err)
	assert.NotNil(t, filter)

	andFilter, ok := filter["$and"].(primitive.A)
	assert.True(t, ok)
	assert.Len(t, andFilter, 2)
}

func TestParseMongoFilter_InvalidBSONConversion(t *testing.T) {
	filterStr := `{"invalid_field": {"$invalid_operator": "value"}}`
	filter, err := parseMongoFilter(filterStr)

	assert.NoError(t, err)
	assert.NotNil(t, filter)
	assert.Contains(t, filter, "invalid_field")
}
