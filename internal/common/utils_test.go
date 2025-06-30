package common

import (
	"bytes"
	"testing"
)

func TestConfirmWithReader_yes(t *testing.T) {
	input := bytes.NewBufferString("yes\n")
	result := ConfirmWithReader(input, "Proceed? ")
	if !result {
		t.Errorf("expected true, got false")
	}
}

func TestConfirmWithReader_no(t *testing.T) {
	input := bytes.NewBufferString("no\n")
	result := ConfirmWithReader(input, "Proceed? ")
	if result {
		t.Errorf("expected false, got true")
	}
}

func TestFormatNumber(t *testing.T) {
	tests := []struct {
		name     string
		input    int
		expected string
	}{
		{"zero", 0, "0"},
		{"single digit", 5, "5"},
		{"double digit", 42, "42"},
		{"triple digit", 123, "123"},
		{"four digits", 1234, "1,234"},
		{"five digits", 12345, "12,345"},
		{"six digits", 123456, "123,456"},
		{"seven digits", 1234567, "1,234,567"},
		{"ten digits", 1234567890, "1,234,567,890"},
		{"negative number", -1234, "-1,234"},
		{"exact thousand", 1000, "1,000"},
		{"exact million", 1000000, "1,000,000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatNumber(tt.input)
			if result != tt.expected {
				t.Errorf("FormatNumber(%d) = %s, want %s", tt.input, result, tt.expected)
			}
		})
	}
}
