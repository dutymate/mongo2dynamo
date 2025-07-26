package transformer

import (
	"encoding/json"
	"testing"
)

func FuzzConvertID(f *testing.F) {
	// Seed corpus with various representative values as JSON bytes.
	f.Add([]byte(`"string_id"`))
	f.Add([]byte(`42`))
	f.Add([]byte(`3.14`))
	f.Add([]byte(`true`))
	f.Add([]byte(`null`))
	f.Add([]byte(`{"nested": "object"}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Try to unmarshal as JSON to get various input types.
		var input any
		if err := json.Unmarshal(data, &input); err != nil {
			// If JSON unmarshaling fails, use the raw data as string.
			input = string(data)
		}

		result := convertID(input)

		// Verify that the result is always a string.
		if result == "" && input != nil && input != "" {
			// Empty result is only acceptable for nil or empty string input.
			t.Errorf("convertID returned empty string for non-empty input: %v", input)
		}
	})
}
