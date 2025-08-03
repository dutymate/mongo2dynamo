package common

import (
	"bytes"
	"testing"
	"time"
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

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		name     string
		input    time.Duration
		expected string
	}{
		{"negative duration", -1 * time.Second, "N/A"},
		{"zero duration", 0 * time.Second, "0s"},
		{"seconds only", 30 * time.Second, "30s"},
		{"minutes and seconds", 90 * time.Second, "1m 30s"},
		{"hours only", 2 * time.Hour, "2h 0m"},
		{"hours and minutes", 2*time.Hour + 30*time.Minute, "2h 30m"},
		{"complex duration", 25*time.Hour + 45*time.Minute + 30*time.Second, "25h 45m"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatDuration(tt.input)
			if result != tt.expected {
				t.Errorf("FormatDuration(%v) = %s, want %s", tt.input, result, tt.expected)
			}
		})
	}
}
