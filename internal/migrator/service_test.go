package migrator

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// captureStdout captures stdout output during the execution of the given function and returns the output as a string.
func captureStdout(f func()) string {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	f()

	w.Close()
	os.Stdout = oldStdout
	var buf bytes.Buffer
	_, readErr := buf.ReadFrom(r)
	if readErr != nil {
		panic(fmt.Sprintf("Failed to read from pipe: %v", readErr))
	}
	return buf.String()
}

// MockDataReader is a mock implementation of the DataReader interface.
type MockDataReader struct {
	mock.Mock
	Data [][]map[string]interface{} // Data for each chunk.
	Err  error                      // Error to simulate read failures.
}

func (m *MockDataReader) Read(_ context.Context, handleChunk func([]map[string]interface{}) error) error {
	if m.Err != nil {
		return m.Err
	}
	for _, chunk := range m.Data {
		if err := handleChunk(chunk); err != nil {
			return err
		}
	}
	return nil
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
		name           string
		reader         *MockDataReader
		writer         *MockDataWriter
		dryRun         bool
		wantErr        bool
		expectedOutput []string
	}{
		{
			name: "Successful migration",
			reader: &MockDataReader{
				Data: [][]map[string]interface{}{
					{{"id": "1", "name": "test1"}, {"id": "2", "name": "test2"}},
				},
			},
			writer: func() *MockDataWriter {
				m := new(MockDataWriter)
				m.On("Write", mock.Anything, mock.Anything).Return(nil)
				return m
			}(),
			dryRun:         false,
			wantErr:        false,
			expectedOutput: []string{"Successfully migrated 2 documents"},
		},
		{
			name: "Dry run mode",
			reader: &MockDataReader{
				Data: [][]map[string]interface{}{
					{{"id": "1", "name": "test1"}},
				},
			},
			writer:         nil,
			dryRun:         true,
			wantErr:        false,
			expectedOutput: []string{"Found 1 documents to migrate"},
		},
		{
			name: "Data read failure",
			reader: &MockDataReader{
				Err: assert.AnError,
			},
			writer:         nil,
			dryRun:         false,
			wantErr:        true,
			expectedOutput: []string{},
		},
		{
			name: "Data write failure",
			reader: &MockDataReader{
				Data: [][]map[string]interface{}{
					{{"id": "1", "name": "test1"}},
				},
			},
			writer: func() *MockDataWriter {
				m := new(MockDataWriter)
				m.On("Write", mock.Anything, mock.Anything).Return(assert.AnError)
				return m
			}(),
			dryRun:         false,
			wantErr:        true,
			expectedOutput: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := NewService(tt.reader, tt.writer, tt.dryRun)
			output := captureStdout(func() {
				err := service.Run(context.Background())
				if tt.wantErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
			})

			// Verify expected output messages.
			for _, expectedMsg := range tt.expectedOutput {
				assert.Contains(t, output, expectedMsg, "Output should contain: %s", expectedMsg)
			}

			// Verify that success message is not present in dry run or error cases.
			if tt.dryRun || tt.wantErr {
				assert.NotContains(t, output, "Successfully migrated", "Success message should not appear in dry run or error cases")
			}

			if tt.writer != nil {
				tt.writer.AssertExpectations(t)
			}
		})
	}
}
