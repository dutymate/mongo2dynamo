package transformer

import (
	"math/rand"
	"reflect"
	"testing"
	"time"
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
