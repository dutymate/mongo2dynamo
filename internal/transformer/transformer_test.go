package transformer

import (
	"encoding/json"
	"math/rand"
	"reflect"
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
		output, err := docTransformer.Transform(shuffled)
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
	output, err := docTransformer.Transform(input)
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
		expected string
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
			expected: "42",
		},
		{
			name:     "float conversion",
			input:    3.14,
			expected: "3.14",
		},
		{
			name:     "boolean conversion",
			input:    true,
			expected: "true",
		},
		{
			name:     "nil conversion",
			input:    nil,
			expected: "null",
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
				if len(result) != 24 {
					t.Errorf("ObjectID conversion: expected 24-character hex string, got %s (length: %d)", result, len(result))
				}
				// Verify it's the same as the original ObjectID's hex representation.
				objID := tt.input.(primitive.ObjectID)
				expectedHex := objID.Hex()
				if result != expectedHex {
					t.Errorf("ObjectID conversion: expected %s, got %s", expectedHex, result)
				}
			case "primitive.M conversion", "nested primitive.M conversion":
				// For JSON objects, we need to parse and compare the structure.
				var resultMap, expectedMap map[string]interface{}

				if err := json.Unmarshal([]byte(result), &resultMap); err != nil {
					t.Errorf("Failed to unmarshal result JSON: %v", err)
					return
				}
				if err := json.Unmarshal([]byte(tt.expected), &expectedMap); err != nil {
					t.Errorf("Failed to unmarshal expected JSON: %v", err)
					return
				}

				if !reflect.DeepEqual(resultMap, expectedMap) {
					t.Errorf("convertID() = %v, want %v", result, tt.expected)
				}
			default:
				if result != tt.expected {
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

	// Test with multiple random ObjectIDs.
	for i := 0; i < 10; i++ {
		randomObjID := primitive.NewObjectID()
		result := convertID(randomObjID)
		expected := randomObjID.Hex()

		if result != expected {
			t.Errorf("convertID() with random ObjectID = %s, want %s", result, expected)
		}
	}
}
