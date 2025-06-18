package migrator

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockDataReader is a mock implementation of the DataReader interface.
type MockDataReader struct {
	mock.Mock
}

func (m *MockDataReader) Read(ctx context.Context) ([]map[string]interface{}, error) {
	args := m.Called(ctx)
	if err := args.Error(1); err != nil {
		return nil, fmt.Errorf("mock reader error: %w", err)
	}
	return args.Get(0).([]map[string]interface{}), nil
}

// MockDataWriter is a mock implementation of the DataWriter interface.
type MockDataWriter struct {
	mock.Mock
}

func (m *MockDataWriter) Write(ctx context.Context, data []map[string]interface{}) error {
	args := m.Called(ctx, data)
	if err := args.Error(0); err != nil {
		return fmt.Errorf("mock writer error: %w", err)
	}
	return nil
}

func TestService_Run(t *testing.T) {
	tests := []struct {
		name    string
		reader  *MockDataReader
		writer  *MockDataWriter
		dryRun  bool
		wantErr bool
	}{
		{
			name: "Successful migration",
			reader: func() *MockDataReader {
				m := new(MockDataReader)
				m.On("Read", mock.Anything).Return([]map[string]interface{}{
					{"id": "1", "name": "test1"},
					{"id": "2", "name": "test2"},
				}, nil)
				return m
			}(),
			writer: func() *MockDataWriter {
				m := new(MockDataWriter)
				m.On("Write", mock.Anything, mock.Anything).Return(nil)
				return m
			}(),
			dryRun:  false,
			wantErr: false,
		},
		{
			name: "Dry run mode",
			reader: func() *MockDataReader {
				m := new(MockDataReader)
				m.On("Read", mock.Anything).Return([]map[string]interface{}{
					{"id": "1", "name": "test1"},
				}, nil)
				return m
			}(),
			writer:  nil,
			dryRun:  true,
			wantErr: false,
		},
		{
			name: "Data read failure",
			reader: func() *MockDataReader {
				m := new(MockDataReader)
				m.On("Read", mock.Anything).Return([]map[string]interface{}{}, assert.AnError)
				return m
			}(),
			writer:  nil,
			dryRun:  false,
			wantErr: true,
		},
		{
			name: "Data write failure",
			reader: func() *MockDataReader {
				m := new(MockDataReader)
				m.On("Read", mock.Anything).Return([]map[string]interface{}{
					{"id": "1", "name": "test1"},
				}, nil)
				return m
			}(),
			writer: func() *MockDataWriter {
				m := new(MockDataWriter)
				m.On("Write", mock.Anything, mock.Anything).Return(assert.AnError)
				return m
			}(),
			dryRun:  false,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := NewService(tt.reader, tt.writer, tt.dryRun)
			err := service.Run(context.Background())

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			tt.reader.AssertExpectations(t)
			if tt.writer != nil {
				tt.writer.AssertExpectations(t)
			}
		})
	}
}
