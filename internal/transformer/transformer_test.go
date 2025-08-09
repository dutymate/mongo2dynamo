package transformer

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// mapContainsSubset returns true if all key/value pairs in subset exist in m with equal values.
func mapContainsSubset(m, subset map[string]any) bool {
	for k, v := range subset {
		if mv, ok := m[k]; !ok || !reflect.DeepEqual(mv, v) {
			return false
		}
	}
	return true
}

func TestDocTransformer_Transform(t *testing.T) {
	docTransformer := NewDocTransformer()
	input := []map[string]any{
		{"_id": "abc123", "name": "test"},
		{"_id": "def456", "name": "test2", "other": 42},
	}
	expected := []map[string]any{
		{"_id": "abc123", "name": "test"},
		{"_id": "def456", "name": "test2", "other": 42},
	}

	// Run the test multiple times with shuffled input to check for race conditions and order preservation.
	runs := 10
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < runs; i++ {
		shuffled := make([]map[string]any, len(input))
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
		// The order of output must match the input: input _id and output _id must be equal at each index.
		for idx, doc := range output {
			inID := shuffled[idx]["_id"]
			outID := doc["_id"]
			if inID != outID {
				t.Errorf("at idx %d: expected _id %v, got %v", idx, inID, outID)
			}
		}
		// Check that all expected field subsets are present in the output (ignore extra fields like __v/_class).
		for _, want := range expected {
			found := false
			for _, got := range output {
				if mapContainsSubset(got, want) {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("expected fields %+v not found in output", want)
			}
		}
	}
}

func TestDocTransformer_Transform_EmptyInput(t *testing.T) {
	docTransformer := NewDocTransformer()
	input := []map[string]any{}
	output, err := docTransformer.Transform(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(output) != 0 {
		t.Errorf("expected output length 0, got %d", len(output))
	}
}

func TestDocTransformer_Transform_LargeDataset(t *testing.T) {
	docTransformer := NewDocTransformer()

	// Create a large dataset to test dynamic worker scaling.
	input := make([]map[string]any, 10000)
	for i := 0; i < 10000; i++ {
		input[i] = map[string]any{
			"_id":     fmt.Sprintf("id_%d", i),
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
		// Check that _id was preserved.
		if doc["_id"] != fmt.Sprintf("id_%d", i) {
			t.Errorf("at index %d: expected _id 'id_%d', got %v", i, i, doc["_id"])
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
	input := make([]map[string]any, 1000)
	for i := 0; i < 1000; i++ {
		input[i] = map[string]any{
			"_id":  fmt.Sprintf("id_%d", i),
			"name": fmt.Sprintf("doc_%d", i),
		}
	}

	// Run multiple transformations concurrently.
	numGoroutines := 10
	var wg sync.WaitGroup
	results := make([][]map[string]any, numGoroutines)
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
	input := make([]map[string]any, 5000)
	for i := 0; i < 5000; i++ {
		input[i] = map[string]any{
			"_id":  fmt.Sprintf("id_%d", i),
			"name": fmt.Sprintf("document_%d", i),
			"data": make([]int, 100), // Add some complexity to make processing take time.
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
		if doc["_id"] != fmt.Sprintf("id_%d", i) {
			t.Errorf("at index %d: expected _id 'id_%d', got %v", i, i, doc["_id"])
		}
	}
}

func TestConvertValue(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected any
	}{
		{
			name:     "ObjectID conversion",
			input:    primitive.NewObjectID(),
			expected: "", // Will be set dynamically.
		},
		{
			name:     "string value",
			input:    "test123",
			expected: "test123",
		},
		{
			name:     "integer value",
			input:    42,
			expected: 42,
		},
		{
			name:     "float value",
			input:    3.14,
			expected: 3.14,
		},
		{
			name:     "boolean value",
			input:    true,
			expected: true,
		},
		{
			name:     "nil value",
			input:    nil,
			expected: nil,
		},
		{
			name: "array with ObjectIDs",
			input: []any{
				primitive.NewObjectID(),
				primitive.NewObjectID(),
				"string",
				42,
			},
			expected: []any{"", "", "string", 42}, // Will be set dynamically.
		},
		{
			name: "nested map with ObjectIDs",
			input: map[string]any{
				"user": map[string]any{
					"_id":  primitive.NewObjectID(),
					"name": "John Doe",
				},
				"timestamp": "2023-01-01T00:00:00Z",
			},
			expected: map[string]any{
				"user": map[string]any{
					"_id":  "", // Will be set dynamically.
					"name": "John Doe",
				},
				"timestamp": "2023-01-01T00:00:00Z",
			},
		},
		{
			name: "bson.M with ObjectIDs",
			input: bson.M{
				"userId": primitive.NewObjectID(),
				"data":   "some data",
			},
			expected: bson.M{
				"userId": "", // Will be set dynamically.
				"data":   "some data",
			},
		},
		{
			name: "bson.A with mixed types",
			input: bson.A{
				primitive.NewObjectID(),
				"string",
				42,
				bson.M{"nested": primitive.NewObjectID()},
			},
			expected: bson.A{
				"", // Will be set dynamically.
				"string",
				42,
				bson.M{"nested": ""}, // Will be set dynamically.
			},
		},
		{
			name:     "empty array",
			input:    []any{},
			expected: []any{},
		},
		{
			name:     "empty map",
			input:    map[string]any{},
			expected: map[string]any{},
		},
		{
			name:     "empty bson.M",
			input:    bson.M{},
			expected: map[string]any{},
		},
		{
			name:     "empty bson.A",
			input:    bson.A{},
			expected: []any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertValue(tt.input)

			// Special handling for ObjectID tests.
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
			case "array with ObjectIDs":
				// Verify array structure and ObjectID conversions.
				resultArr, ok := result.([]any)
				if !ok {
					t.Errorf("array conversion: expected []any result, got %T", result)
					return
				}
				inputArr := tt.input.([]any)
				if len(resultArr) != len(inputArr) {
					t.Errorf("array conversion: expected length %d, got %d", len(inputArr), len(resultArr))
					return
				}
				// Check ObjectID conversions in array.
				for i, item := range inputArr {
					if objID, isObjID := item.(primitive.ObjectID); isObjID {
						expectedHex := objID.Hex()
						if resultArr[i] != expectedHex {
							t.Errorf("array[%d]: expected %s, got %v", i, expectedHex, resultArr[i])
						}
					} else if resultArr[i] != item {
						t.Errorf("array[%d]: expected %v, got %v", i, item, resultArr[i])
					}
				}
			case "nested map with ObjectIDs":
				// Verify nested map structure and ObjectID conversions.
				resultMap, ok := result.(map[string]any)
				if !ok {
					t.Errorf("nested map conversion: expected map[string]any result, got %T", result)
					return
				}
				// Check user.id conversion.
				userMap, ok := resultMap["user"].(map[string]any)
				if !ok {
					t.Errorf("nested map conversion: expected user to be map[string]any, got %T", resultMap["user"])
					return
				}
				inputMap := tt.input.(map[string]any)
				inputUserMap := inputMap["user"].(map[string]any)
				inputObjID := inputUserMap["_id"].(primitive.ObjectID)
				expectedHex := inputObjID.Hex()
				if userMap["_id"] != expectedHex {
					t.Errorf("nested map user._id: expected %s, got %v", expectedHex, userMap["_id"])
				}
			case "bson.M with ObjectIDs":
				// Verify bson.M structure and ObjectID conversions.
				resultMap, ok := result.(map[string]any)
				if !ok {
					t.Errorf("bson.M conversion: expected map[string]any result, got %T", result)
					return
				}
				inputMap := tt.input.(bson.M)
				inputObjID := inputMap["userId"].(primitive.ObjectID)
				expectedHex := inputObjID.Hex()
				if resultMap["userId"] != expectedHex {
					t.Errorf("bson.M userId: expected %s, got %v", expectedHex, resultMap["userId"])
				}
			case "bson.A with mixed types":
				// Verify bson.A structure and ObjectID conversions.
				resultArr, ok := result.([]any)
				if !ok {
					t.Errorf("bson.A conversion: expected []any result, got %T", result)
					return
				}
				inputArr := tt.input.(bson.A)
				// Check first ObjectID.
				inputObjID := inputArr[0].(primitive.ObjectID)
				expectedHex := inputObjID.Hex()
				if resultArr[0] != expectedHex {
					t.Errorf("bson.A[0]: expected %s, got %v", expectedHex, resultArr[0])
				}
				// Check nested ObjectID in bson.M.
				resultNestedMap := resultArr[3].(map[string]any)
				inputNestedMap := inputArr[3].(bson.M)
				inputNestedObjID := inputNestedMap["nested"].(primitive.ObjectID)
				expectedNestedHex := inputNestedObjID.Hex()
				if resultNestedMap["nested"] != expectedNestedHex {
					t.Errorf("bson.A[3].nested: expected %s, got %v", expectedNestedHex, resultNestedMap["nested"])
				}
			default:
				if !reflect.DeepEqual(result, tt.expected) {
					t.Errorf("convertValue() = %v, want %v", result, tt.expected)
				}
			}
		})
	}
}

func TestConvertValue_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected any
	}{
		{
			name:     "zero ObjectID",
			input:    primitive.NilObjectID,
			expected: "000000000000000000000000",
		},
		{
			name: "deeply nested ObjectIDs",
			input: map[string]any{
				"level1": map[string]any{
					"level2": map[string]any{
						"level3": map[string]any{
							"level4": map[string]any{
								"_id": primitive.NewObjectID(),
							},
						},
					},
				},
			},
			expected: map[string]any{
				"level1": map[string]any{
					"level2": map[string]any{
						"level3": map[string]any{
							"level4": map[string]any{
								"_id": "", // Will be set dynamically.
							},
						},
					},
				},
			},
		},
		{
			name: "mixed array types",
			input: []any{
				primitive.NewObjectID(),
				[]any{primitive.NewObjectID(), "nested"},
				map[string]any{"_id": primitive.NewObjectID()},
				bson.M{"ref": primitive.NewObjectID()},
				bson.A{primitive.NewObjectID()},
			},
			expected: []any{
				"",
				[]any{"", "nested"},
				map[string]any{"_id": ""},
				bson.M{"ref": ""},
				bson.A{""},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertValue(tt.input)

			switch tt.name {
			case "zero ObjectID":
				if result != tt.expected {
					t.Errorf("zero ObjectID: expected %s, got %v", tt.expected, result)
				}
			case "deeply nested ObjectIDs":
				// Verify the deeply nested ObjectID is converted.
				resultMap := result.(map[string]any)
				level1 := resultMap["level1"].(map[string]any)
				level2 := level1["level2"].(map[string]any)
				level3 := level2["level3"].(map[string]any)
				level4 := level3["level4"].(map[string]any)

				inputMap := tt.input.(map[string]any)
				inputLevel1 := inputMap["level1"].(map[string]any)
				inputLevel2 := inputLevel1["level2"].(map[string]any)
				inputLevel3 := inputLevel2["level3"].(map[string]any)
				inputLevel4 := inputLevel3["level4"].(map[string]any)
				inputObjID := inputLevel4["_id"].(primitive.ObjectID)
				expectedHex := inputObjID.Hex()

				if level4["_id"] != expectedHex {
					t.Errorf("deeply nested ObjectID: expected %s, got %v", expectedHex, level4["_id"])
				}
			case "mixed array types":
				// Verify all ObjectIDs in the mixed array are converted.
				resultArr := result.([]any)
				inputArr := tt.input.([]any)

				// Check first ObjectID.
				inputObjID1 := inputArr[0].(primitive.ObjectID)
				expectedHex1 := inputObjID1.Hex()
				if resultArr[0] != expectedHex1 {
					t.Errorf("mixed array[0]: expected %s, got %v", expectedHex1, resultArr[0])
				}

				// Check nested array ObjectID.
				resultNestedArr := resultArr[1].([]any)
				inputNestedArr := inputArr[1].([]any)
				inputNestedObjID := inputNestedArr[0].(primitive.ObjectID)
				expectedNestedHex := inputNestedObjID.Hex()
				if resultNestedArr[0] != expectedNestedHex {
					t.Errorf("mixed array[1][0]: expected %s, got %v", expectedNestedHex, resultNestedArr[0])
				}

				// Check map ObjectID.
				resultMap := resultArr[2].(map[string]any)
				inputMap := inputArr[2].(map[string]any)
				inputMapObjID := inputMap["_id"].(primitive.ObjectID)
				expectedMapHex := inputMapObjID.Hex()
				if resultMap["_id"] != expectedMapHex {
					t.Errorf("mixed array[2]._id: expected %s, got %v", expectedMapHex, resultMap["_id"])
				}

				// Check bson.M ObjectID.
				resultBsonM := resultArr[3].(map[string]any)
				inputBsonM := inputArr[3].(bson.M)
				inputBsonMObjID := inputBsonM["ref"].(primitive.ObjectID)
				expectedBsonMHex := inputBsonMObjID.Hex()
				if resultBsonM["ref"] != expectedBsonMHex {
					t.Errorf("mixed array[3].ref: expected %s, got %v", expectedBsonMHex, resultBsonM["ref"])
				}

				// Check bson.A ObjectID.
				resultBsonA := resultArr[4].([]any)
				inputBsonA := inputArr[4].(bson.A)
				inputBsonAObjID := inputBsonA[0].(primitive.ObjectID)
				expectedBsonAHex := inputBsonAObjID.Hex()
				if resultBsonA[0] != expectedBsonAHex {
					t.Errorf("mixed array[4][0]: expected %s, got %v", expectedBsonAHex, resultBsonA[0])
				}
			}
		})
	}
}
