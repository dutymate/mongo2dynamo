package mongo

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"mongo2dynamo/internal/common"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.mongodb.org/mongo-driver/mongo/options"
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

func TestReader_Read_EmptyCollection(t *testing.T) {
	mockCollection := new(MockCollection)
	mockCursor := &MockCursor{
		docs: []map[string]interface{}{},
	}
	mockCursor.On("Close", mock.Anything).Return(nil)
	mockCursor.On("Err").Return(nil)
	mockCollection.On("Find", mock.Anything, mock.Anything, mock.Anything).Return(mockCursor, nil)

	reader := newReader(mockCollection)
	var processedDocs []map[string]interface{}

	err := reader.Read(context.Background(), func(chunk []map[string]interface{}) error {
		processedDocs = append(processedDocs, chunk...)
		return nil
	})

	assert.NoError(t, err)
	assert.Empty(t, processedDocs)
	mockCollection.AssertExpectations(t)
	mockCursor.AssertExpectations(t)
}

func TestReader_Read_SingleChunk(t *testing.T) {
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

	reader := newReader(mockCollection)
	var processedDocs []map[string]interface{}

	err := reader.Read(context.Background(), func(chunk []map[string]interface{}) error {
		processedDocs = append(processedDocs, chunk...)
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, testDocs, processedDocs)
	mockCollection.AssertExpectations(t)
	mockCursor.AssertExpectations(t)
}

func TestReader_Read_MultipleChunks(t *testing.T) {
	mockCollection := new(MockCollection)
	testDocs := make([]map[string]interface{}, 1500) // More than chunkSize (1000).
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

	reader := newReader(mockCollection)
	var processedDocs []map[string]interface{}
	chunkCount := 0

	err := reader.Read(context.Background(), func(chunk []map[string]interface{}) error {
		processedDocs = append(processedDocs, chunk...)
		chunkCount++
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, testDocs, processedDocs)
	assert.Equal(t, 2, chunkCount) // Should be processed in 2 chunks.
	mockCollection.AssertExpectations(t)
	mockCursor.AssertExpectations(t)
}

func TestReader_Read_FindError(t *testing.T) {
	mockCollection := new(MockCollection)
	expectedErr := errors.New("find error")
	mockCollection.On("Find", mock.Anything, mock.Anything, mock.Anything).Return(nil, expectedErr)

	reader := newReader(mockCollection)
	err := reader.Read(context.Background(), func(_ []map[string]interface{}) error {
		return nil
	})

	assert.Error(t, err)
	var dbErr *common.DatabaseOperationError
	assert.ErrorAs(t, err, &dbErr)
	assert.Equal(t, "MongoDB", dbErr.Database)
	assert.Equal(t, "find", dbErr.Op)
	mockCollection.AssertExpectations(t)
}

func TestReader_Read_DecodeError(t *testing.T) {
	mockColl := new(MockCollection)
	mockCursor := &MockCursor{
		docs:      []map[string]interface{}{{"_id": "1"}},
		decodeErr: errors.New("decode error"),
	}
	mockCursor.On("Close", mock.Anything).Return(nil)
	mockColl.On("Find", mock.Anything, mock.Anything, mock.Anything).Return(mockCursor, nil)

	reader := newReader(mockColl)

	err := reader.Read(context.Background(), func(_ []map[string]interface{}) error {
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

func TestReader_Read_CallbackError(t *testing.T) {
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

	reader := newReader(mockCollection)
	expectedErr := errors.New("callback error")

	err := reader.Read(context.Background(), func(_ []map[string]interface{}) error {
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

func TestReader_Read_CursorError(t *testing.T) {
	mockCollection := new(MockCollection)
	mockCursor := &MockCursor{
		docs: []map[string]interface{}{{"_id": "1"}},
	}
	expectedErr := errors.New("cursor error")
	mockCursor.On("Close", mock.Anything).Return(nil)
	mockCursor.On("Err").Return(expectedErr)
	mockCollection.On("Find", mock.Anything, mock.Anything, mock.Anything).Return(mockCursor, nil)

	reader := newReader(mockCollection)
	err := reader.Read(context.Background(), func(_ []map[string]interface{}) error {
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
