package extractor

import (
	"testing"
)

func FuzzParseMongoFilter(f *testing.F) {
	// Seed corpus with various representative values.
	f.Add("")
	f.Add("{}")
	f.Add(`{"status": "active"}`)
	f.Add(`{"age": {"$gte": 18}}`)
	f.Add(`{"$and": [{"status": "active"}, {"age": {"$gte": 18}}]}`)
	f.Add(`{"invalid_field": {"$invalid_operator": "value"}}`)

	// Edge cases for malformed JSON.
	f.Add(`{"key": }`)                           // Missing value.
	f.Add(`{"key": "value",}`)                   // Trailing comma.
	f.Add(`{unclosed`)                           // Unclosed brace.
	f.Add(`{"key": "value"`)                     // Missing closing brace.
	f.Add(`{"key": "value", "key2":}`)           // Incomplete object.
	f.Add(`{"key": "value",, "key2": "value2"}`) // Double comma.
	f.Add(`{"key": "value", "key2": "value2",}`) // Trailing comma in object.

	// Nested structures with various depths.
	f.Add(`{"level1": {"level2": {"level3": {"level4": "deep"}}}}`)
	f.Add(`{"nested": {"array": [{"obj": {"field": "value"}}]}}`)
	f.Add(`{"$or": [{"$and": [{"field1": "value1"}, {"field2": "value2"}]}, {"field3": "value3"}]}`)

	// Empty and minimal structures.
	f.Add(`{"field": {}}`)    // Empty operator object.
	f.Add(`{"field": []}`)    // Empty array.
	f.Add(`{"field": ""}`)    // Empty string.
	f.Add(`{"field": null}`)  // Null value.
	f.Add(`{"field": 0}`)     // Zero number.
	f.Add(`{"field": false}`) // Boolean false.

	// MongoDB operators.
	f.Add(`{"age": {"$gt": 18, "$lt": 65}}`)
	f.Add(`{"status": {"$in": ["active", "pending", "completed"]}}`)
	f.Add(`{"tags": {"$all": ["tag1", "tag2", "tag3"]}}`)
	f.Add(`{"name": {"$regex": "^John", "$options": "i"}}`)
	f.Add(`{"location": {"$near": {"$geometry": {"type": "Point", "coordinates": [0, 0]}}}}`)

	// Complex logical operators.
	f.Add(`{"$nor": [{"status": "inactive"}, {"deleted": true}]}`)
	f.Add(`{"$not": {"status": "inactive"}}`)
	f.Add(`{"$expr": {"$gt": ["$field1", "$field2"]}}`)

	// Array operations.
	f.Add(`{"scores": {"$elemMatch": {"$gt": 80, "$lt": 100}}}`)
	f.Add(`{"tags": {"$size": 3}}`)
	f.Add(`{"comments": {"$slice": [0, 5]}}`)

	// Type-specific operators.
	f.Add(`{"field": {"$type": "string"}}`)
	f.Add(`{"field": {"$exists": true}}`)
	f.Add(`{"field": {"$mod": [10, 0]}}`)

	// Geospatial queries.
	f.Add(`{"location": {"$geoWithin": {"$center": [[0, 0], 10]}}}`)
	f.Add(`{"location": {"$geoIntersects": {"$geometry": {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]}}}}`)

	// Text search.
	f.Add(`{"$text": {"$search": "search term", "$language": "english"}}`)

	// Projection-like structures (should be handled gracefully).
	f.Add(`{"field": 1}`)
	f.Add(`{"field": 0}`)
	f.Add(`{"field": {"$meta": "textScore"}}`)

	f.Fuzz(func(t *testing.T, filterStr string) {
		filter, err := parseMongoFilter(filterStr)
		if err != nil {
			// Error is expected for invalid JSON, but should not panic.
			return
		}
		// Verify that the result is not nil when no error occurs.
		if filter == nil {
			t.Errorf("parseMongoFilter returned nil filter for input: %s", filterStr)
		}
	})
}
