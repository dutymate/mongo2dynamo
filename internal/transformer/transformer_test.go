package transformer

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestDocTransformer_Transform(t *testing.T) {
	docTransformer := NewDocTransformer()
	input := []map[string]interface{}{
		{"_id": "abc123", "__v": 1, "_class": "com.example.MyEntity", "name": "test"},
		{"_id": "def456", "__v": 2, "name": "test2", "other": 42},
	}
	expected := []map[string]interface{}{
		{"id": "abc123", "name": "test"},
		{"id": "def456", "name": "test2", "other": 42},
	}

	// Run the test multiple times with shuffled input to check for race conditions and order preservation.
	runs := 10
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < runs; i++ {
		shuffled := make([]map[string]interface{}, len(input))
		copy(shuffled, input)
		// Shuffle the input slice using the local random generator.
		for j := range shuffled {
			k := r.Intn(j + 1)
			shuffled[j], shuffled[k] = shuffled[k], shuffled[j]
		}
		output, err := docTransformer.Transform(context.Background(), shuffled)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// The output must have the same length as the input.
		if len(output) != len(shuffled) {
			t.Errorf("expected output length %d, got %d", len(shuffled), len(output))
		}
		// The order of output must match the input: input _id and output id must be equal at each index.
		for idx, doc := range output {
			inID := shuffled[idx]["_id"]
			outID := doc["id"]
			if inID != outID {
				t.Errorf("at idx %d: expected id %v, got %v", idx, inID, outID)
			}
		}
		// Check that all expected documents are present in the output, regardless of order.
		for _, want := range expected {
			found := false
			for _, got := range output {
				if reflect.DeepEqual(want, got) {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("expected document %+v not found in output", want)
			}
			// Check that no forbidden fields are present in the output.
			for _, doc := range output {
				if _, exists := doc["_id"]; exists {
					t.Errorf("_id should not be present in output: %v", doc)
				}
				if _, exists := doc["_class"]; exists {
					t.Errorf("_class should not be present in output: %v", doc)
				}
				if _, exists := doc["__v"]; exists {
					t.Errorf("__v should not be present in output: %v", doc)
				}
			}
		}
	}
}

func TestDocTransformer_Transform_EmptyInput(t *testing.T) {
	docTransformer := NewDocTransformer()
	input := []map[string]interface{}{}
	output, err := docTransformer.Transform(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(output) != 0 {
		t.Errorf("expected output length 0, got %d", len(output))
	}
}

func TestConvertID(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected interface{}
	}{
		{
			name:     "ObjectID conversion",
			input:    primitive.NewObjectID(),
			expected: "", // Will be set dynamically.
		},
		{
			name:     "string conversion",
			input:    "test123",
			expected: "test123",
		},
		{
			name:     "integer conversion",
			input:    42,
			expected: 42,
		},
		{
			name:     "float conversion",
			input:    3.14,
			expected: 3.14,
		},
		{
			name:     "boolean conversion",
			input:    true,
			expected: true,
		},
		{
			name:     "nil conversion",
			input:    nil,
			expected: nil,
		},
		{
			name: "primitive.M conversion",
			input: primitive.M{
				"user": "some user",
				"ts":   "2023-01-01T00:00:00Z",
			},
			expected: `{"ts":"2023-01-01T00:00:00Z","user":"some user"}`,
		},
		{
			name: "nested primitive.M conversion",
			input: primitive.M{
				"user": primitive.M{
					"id":   "123",
					"name": "John Doe",
				},
				"timestamp": "2023-01-01T00:00:00Z",
			},
			expected: `{"timestamp":"2023-01-01T00:00:00Z","user":{"id":"123","name":"John Doe"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertID(tt.input)

			// Special handling for ObjectID test.
			switch tt.name {
			case "ObjectID conversion":
				// Verify it's a valid hex string.
				resultStr, ok := result.(string)
				if !ok {
					t.Errorf("ObjectID conversion: expected string result, got %T", result)
					return
				}
				if len(resultStr) != 24 {
					t.Errorf("ObjectID conversion: expected 24-character hex string, got %s (length: %d)", resultStr, len(resultStr))
				}
				// Verify it's the same as the original ObjectID's hex representation.
				objID := tt.input.(primitive.ObjectID)
				expectedHex := objID.Hex()
				if resultStr != expectedHex {
					t.Errorf("ObjectID conversion: expected %s, got %s", expectedHex, resultStr)
				}
			case "primitive.M conversion", "nested primitive.M conversion":
				// For JSON objects, we need to parse and compare the structure.
				resultStr, ok := result.(string)
				if !ok {
					t.Errorf("primitive.M conversion: expected string result, got %T", result)
					return
				}
				expectedStr, ok := tt.expected.(string)
				if !ok {
					t.Errorf("primitive.M conversion: expected string in test data, got %T", tt.expected)
					return
				}
				var resultMap, expectedMap map[string]interface{}

				if err := json.Unmarshal([]byte(resultStr), &resultMap); err != nil {
					t.Errorf("Failed to unmarshal result JSON: %v", err)
					return
				}
				if err := json.Unmarshal([]byte(expectedStr), &expectedMap); err != nil {
					t.Errorf("Failed to unmarshal expected JSON: %v", err)
					return
				}

				if !reflect.DeepEqual(resultMap, expectedMap) {
					t.Errorf("convertID() = %v, want %v", result, tt.expected)
				}
			default:
				if !reflect.DeepEqual(result, tt.expected) {
					t.Errorf("convertID() = %v, want %v", result, tt.expected)
				}
			}
		})
	}
}

func TestConvertID_ObjectIDVariations(t *testing.T) {
	// Test with a known ObjectID.
	knownHex := "1234567890abcdef12345678"
	objID, err := primitive.ObjectIDFromHex(knownHex)
	if err != nil {
		t.Fatalf("Failed to create ObjectID from hex: %v", err)
	}

	result := convertID(objID)
	if result != knownHex {
		t.Errorf("convertID() with known ObjectID = %s, want %s", result, knownHex)
	}
}

func TestDocTransformer_Transform_LargeDataset(t *testing.T) {
	docTransformer := NewDocTransformer()

	// Create a large dataset to test dynamic worker scaling.
	input := make([]map[string]interface{}, 10000)
	for i := 0; i < 10000; i++ {
		input[i] = map[string]interface{}{
			"_id":     fmt.Sprintf("id_%d", i),
			"__v":     i,
			"_class":  "com.example.Entity",
			"name":    fmt.Sprintf("document_%d", i),
			"value":   i * 2,
			"active":  i%2 == 0,
			"created": time.Now().Unix(),
		}
	}

	output, err := docTransformer.Transform(context.Background(), input)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify output.
	if len(output) != len(input) {
		t.Errorf("expected output length %d, got %d", len(input), len(output))
	}

	// Verify transformation correctness.
	for i, doc := range output {
		// Check that _id was converted to id.
		if doc["id"] != fmt.Sprintf("id_%d", i) {
			t.Errorf("at index %d: expected id 'id_%d', got %v", i, i, doc["id"])
		}

		// Check that forbidden fields were removed.
		if _, exists := doc["_id"]; exists {
			t.Errorf("at index %d: _id should not be present", i)
		}
		if _, exists := doc["__v"]; exists {
			t.Errorf("at index %d: __v should not be present", i)
		}
		if _, exists := doc["_class"]; exists {
			t.Errorf("at index %d: _class should not be present", i)
		}

		// Check that other fields were preserved.
		if doc["name"] != fmt.Sprintf("document_%d", i) {
			t.Errorf("at index %d: expected name 'document_%d', got %v", i, i, doc["name"])
		}
	}
}

func TestDocTransformer_Transform_ConcurrentAccess(t *testing.T) {
	docTransformer := NewDocTransformer()

	// Create test data.
	input := make([]map[string]interface{}, 1000)
	for i := 0; i < 1000; i++ {
		input[i] = map[string]interface{}{
			"_id":    fmt.Sprintf("id_%d", i),
			"__v":    i,
			"_class": "com.example.Entity",
			"name":   fmt.Sprintf("doc_%d", i),
		}
	}

	// Run multiple transformations concurrently.
	numGoroutines := 10
	var wg sync.WaitGroup
	results := make([][]map[string]interface{}, numGoroutines)
	errors := make([]error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx], errors[idx] = docTransformer.Transform(context.Background(), input)
		}(i)
	}

	wg.Wait()

	// Verify all transformations succeeded.
	for i, err := range errors {
		if err != nil {
			t.Errorf("goroutine %d failed: %v", i, err)
		}
		if len(results[i]) != len(input) {
			t.Errorf("goroutine %d: expected %d results, got %d", i, len(input), len(results[i]))
		}
	}

	// Verify all results are identical.
	firstResult := results[0]
	for i := 1; i < numGoroutines; i++ {
		if !reflect.DeepEqual(firstResult, results[i]) {
			t.Errorf("goroutine %d result differs from first result", i)
		}
	}
}

func TestDocTransformer_Transform_WorkerScaling(t *testing.T) {
	docTransformer := NewDocTransformer()

	// Create a dataset that should trigger worker scaling.
	input := make([]map[string]interface{}, 5000)
	for i := 0; i < 5000; i++ {
		input[i] = map[string]interface{}{
			"_id":    fmt.Sprintf("id_%d", i),
			"__v":    i,
			"_class": "com.example.Entity",
			"name":   fmt.Sprintf("document_%d", i),
			"data":   make([]int, 100), // Add some complexity to make processing take time.
		}
	}

	output, err := docTransformer.Transform(context.Background(), input)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify output.
	if len(output) != len(input) {
		t.Errorf("expected output length %d, got %d", len(input), len(output))
	}

	// Verify transformation correctness.
	for i, doc := range output {
		if doc["id"] != fmt.Sprintf("id_%d", i) {
			t.Errorf("at index %d: expected id 'id_%d', got %v", i, i, doc["id"])
		}
	}
}
