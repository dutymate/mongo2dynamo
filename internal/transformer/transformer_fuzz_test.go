package transformer

import (
	"encoding/json"
	"reflect"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// FuzzConvertValue tests the convertValue function with fuzzed inputs.
func FuzzConvertValue(f *testing.F) {
	// Add seed corpus for basic types.
	f.Add("test string", 42, 3.14, true)
	f.Add("another string", 100, 2.718, false)

	// Add seed corpus for ObjectID as string.
	f.Add(primitive.NewObjectID().Hex(), 0, 0.0, false)

	// Add seed corpus for JSON strings representing complex types.
	f.Add(`["string", 42, true]`, 0, 0.0, false)
	f.Add(`{"key": "value", "num": 42}`, 0, 0.0, false)
	f.Add(`{"_id": "`+primitive.NewObjectID().Hex()+`", "name": "test"}`, 0, 0.0, false)
	f.Add(`{"_id": "`+primitive.NewObjectID().Hex()+`", "name": "test", "nested": {"_id": "`+primitive.NewObjectID().Hex()+`", "name": "test"}}`, 0, 0.0, false)

	f.Fuzz(func(t *testing.T, inputStr string, inputInt int, inputFloat float64, inputBool bool) {
		// Create test inputs from fuzzed values.
		testInputs := []any{
			inputStr,
			inputInt,
			inputFloat,
			inputBool,
		}

		// Try to parse JSON if the string looks like JSON.
		if len(inputStr) > 0 && (inputStr[0] == '{' || inputStr[0] == '[') {
			var jsonInput any
			if err := json.Unmarshal([]byte(inputStr), &jsonInput); err == nil {
				testInputs = append(testInputs, jsonInput)
			}
		}

		// Test each input.
		for _, input := range testInputs {
			// Skip inputs that might cause infinite recursion or other issues.
			if shouldSkipInput(input) {
				continue
			}

			// Call convertValue and ensure it doesn't panic.
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("convertValue panicked with input %v: %v", input, r)
				}
			}()

			result := convertValue(input)

			// Basic validation: result should not be nil unless input was nil.
			if input == nil && result != nil {
				t.Errorf("convertValue(nil) should return nil, got %v", result)
			}

			// Validate ObjectID conversion.
			if objID, ok := input.(primitive.ObjectID); ok {
				expected := objID.Hex()
				if result != expected {
					t.Errorf("ObjectID conversion failed: expected %s, got %v", expected, result)
				}
			}

			// Validate array conversion.
			if arr, ok := input.([]any); ok {
				resultArr, ok := result.([]any)
				if !ok {
					t.Errorf("Array input should return array result, got %T", result)
					continue
				}
				if len(arr) != len(resultArr) {
					t.Errorf("Array length mismatch: expected %d, got %d", len(arr), len(resultArr))
					continue
				}
				// Recursively validate array elements.
				for i, item := range arr {
					expected := convertValue(item)
					if !reflect.DeepEqual(resultArr[i], expected) {
						t.Errorf("Array element %d mismatch: expected %v, got %v", i, expected, resultArr[i])
					}
				}
			}

			// Validate map conversion.
			if m, ok := input.(map[string]any); ok {
				resultMap, ok := result.(map[string]any)
				if !ok {
					t.Errorf("Map input should return map result, got %T", result)
					continue
				}
				if len(m) != len(resultMap) {
					t.Errorf("Map size mismatch: expected %d, got %d", len(m), len(resultMap))
					continue
				}
				// Recursively validate map values.
				for k, v := range m {
					expected := convertValue(v)
					if !reflect.DeepEqual(resultMap[k], expected) {
						t.Errorf("Map value for key %s mismatch: expected %v, got %v", k, expected, resultMap[k])
					}
				}
			}

			// Validate BSON.M conversion.
			if bsonM, ok := input.(bson.M); ok {
				resultMap, ok := result.(map[string]any)
				if !ok {
					t.Errorf("BSON.M input should return map result, got %T", result)
					continue
				}
				if len(bsonM) != len(resultMap) {
					t.Errorf("BSON.M size mismatch: expected %d, got %d", len(bsonM), len(resultMap))
					continue
				}
				// Recursively validate BSON.M values.
				for k, v := range bsonM {
					expected := convertValue(v)
					if !reflect.DeepEqual(resultMap[k], expected) {
						t.Errorf("BSON.M value for key %s mismatch: expected %v, got %v", k, expected, resultMap[k])
					}
				}
			}

			// Validate BSON.A conversion.
			if bsonA, ok := input.(bson.A); ok {
				resultArr, ok := result.([]any)
				if !ok {
					t.Errorf("BSON.A input should return array result, got %T", result)
					continue
				}
				if len(bsonA) != len(resultArr) {
					t.Errorf("BSON.A length mismatch: expected %d, got %d", len(bsonA), len(resultArr))
					continue
				}
				// Recursively validate BSON.A elements.
				for i, item := range bsonA {
					expected := convertValue(item)
					if !reflect.DeepEqual(resultArr[i], expected) {
						t.Errorf("BSON.A element %d mismatch: expected %v, got %v", i, expected, resultArr[i])
					}
				}
			}

			// For other types, ensure the result equals the input.
			if !isComplexType(input) && !reflect.DeepEqual(result, input) {
				t.Errorf("Simple type conversion failed: expected %v, got %v", input, result)
			}
		}
	})
}

// shouldSkipInput determines if an input should be skipped in fuzz testing.
func shouldSkipInput(input any) bool {
	// Skip inputs that might cause infinite recursion.
	if isCircularReference(input) {
		return true
	}

	// Skip inputs that are too deep or complex.
	if getDepth(input) > 10 {
		return true
	}

	// Skip inputs that might cause memory issues.
	if getSize(input) > 10000 {
		return true
	}

	return false
}

// isCircularReference checks if the input contains circular references.
func isCircularReference(input any) bool {
	visited := make(map[uintptr]bool)
	return hasCircularRef(input, visited)
}

// hasCircularRef recursively checks for circular references.
func hasCircularRef(input any, visited map[uintptr]bool) bool {
	if input == nil {
		return false
	}

	val := reflect.ValueOf(input)

	// Only call Pointer() on supported kinds to avoid panics.
	switch val.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Ptr, reflect.Slice, reflect.UnsafePointer:
		if val.IsNil() {
			return false
		}
		ptr := val.Pointer()
		if ptr != 0 {
			if visited[ptr] {
				return true
			}
			visited[ptr] = true
			defer delete(visited, ptr)
		}
	}

	switch val.Kind() {
	case reflect.Ptr, reflect.Interface:
		if val.IsNil() || !val.IsValid() {
			return false
		}
		elem := val.Elem()
		if !elem.IsValid() {
			return false
		}
		return hasCircularRef(elem.Interface(), visited)
	case reflect.Map:
		for _, key := range val.MapKeys() {
			if hasCircularRef(key.Interface(), visited) {
				return true
			}
			if hasCircularRef(val.MapIndex(key).Interface(), visited) {
				return true
			}
		}
		return false
	case reflect.Slice:
		for i := 0; i < val.Len(); i++ {
			if hasCircularRef(val.Index(i).Interface(), visited) {
				return true
			}
		}
		return false
	case reflect.Array:
		for i := 0; i < val.Len(); i++ {
			if hasCircularRef(val.Index(i).Interface(), visited) {
				return true
			}
		}
		return false
	case reflect.Struct:
		for i := 0; i < val.NumField(); i++ {
			if hasCircularRef(val.Field(i).Interface(), visited) {
				return true
			}
		}
		return false
	default:
		// Basic types (int, string, etc.) cannot be circular.
		return false
	}
}

// getDepth calculates the maximum depth of nested structures.
func getDepth(input any) int {
	if input == nil {
		return 0
	}

	visited := make(map[uintptr]bool)
	return getDepthWithVisited(input, visited)
}

// getDepthWithVisited calculates depth with visited tracking to prevent infinite recursion.
func getDepthWithVisited(input any, visited map[uintptr]bool) int {
	if input == nil {
		return 0
	}

	val := reflect.ValueOf(input)

	// Only call Pointer() on supported kinds to avoid panics.
	switch val.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Ptr, reflect.Slice, reflect.UnsafePointer:
		if val.IsNil() {
			return 0
		}
		ptr := val.Pointer()
		if ptr != 0 {
			if visited[ptr] {
				return 0 // Return 0 for circular references to prevent infinite recursion.
			}
			visited[ptr] = true
			defer delete(visited, ptr)
		}
	}

	maxDepth := 0
	iterateItems(input, func(item any) bool {
		depth := getDepthWithVisited(item, visited)
		if depth > maxDepth {
			maxDepth = depth
		}
		return false
	})
	return maxDepth + 1
}

// getSize estimates the size of the input structure.
func getSize(input any) int {
	if input == nil {
		return 0
	}

	visited := make(map[uintptr]bool)
	return getSizeWithVisited(input, visited)
}

// getSizeWithVisited calculates size with visited tracking to prevent infinite recursion.
func getSizeWithVisited(input any, visited map[uintptr]bool) int {
	if input == nil {
		return 0
	}

	val := reflect.ValueOf(input)

	// Only call Pointer() on supported kinds to avoid panics.
	switch val.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Ptr, reflect.Slice, reflect.UnsafePointer:
		if val.IsNil() {
			return 0
		}
		ptr := val.Pointer()
		if ptr != 0 {
			if visited[ptr] {
				return 0 // Return 0 for circular references to prevent infinite recursion.
			}
			visited[ptr] = true
			defer delete(visited, ptr)
		}
	}

	switch v := input.(type) {
	case string:
		return len(v)
	default:
		size := 0
		iterateItems(input, func(item any) bool {
			size += getSizeWithVisited(item, visited)
			return false
		})
		if m, ok := input.(map[string]any); ok {
			for k := range m {
				size += len(k)
			}
		} else if bsonM, ok := input.(bson.M); ok {
			for k := range bsonM {
				size += len(k)
			}
		}
		return size
	}
}

// iterateItems is a helper function that iterates over items in arrays and maps.
// It returns true if the callback returns true for any item.
func iterateItems(input any, callback func(item any) bool) bool {
	switch v := input.(type) {
	case []any:
		for _, item := range v {
			if callback(item) {
				return true
			}
		}
	case bson.A:
		for _, item := range v {
			if callback(item) {
				return true
			}
		}
	case map[string]any:
		for _, item := range v {
			if callback(item) {
				return true
			}
		}
	case bson.M:
		for _, item := range v {
			if callback(item) {
				return true
			}
		}
	}
	return false
}

// isComplexType checks if the input is a complex type that needs special handling.
func isComplexType(input any) bool {
	switch input.(type) {
	case []any, map[string]any, bson.M, bson.A, primitive.ObjectID:
		return true
	default:
		return false
	}
}
