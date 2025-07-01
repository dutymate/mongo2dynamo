package transformer

import (
	"reflect"
	"testing"
)

func TestMongoToDynamoTransformer_Transform(t *testing.T) {
	trans := NewMongoToDynamoTransformer()
	input := []map[string]interface{}{
		{"_id": "abc123", "__v": 1, "_class": "com.example.MyEntity", "name": "test"},
		{"_id": "def456", "__v": 2, "name": "test2", "other": 42},
	}
	expected := []map[string]interface{}{
		{"id": "abc123", "name": "test"},
		{"id": "def456", "name": "test2", "other": 42},
	}
	output, err := trans.Transform(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(expected, output) {
		t.Errorf("expected output to equal expected, got %v", output)
	}
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
