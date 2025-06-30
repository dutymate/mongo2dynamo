package version

import (
	"strings"
	"testing"
)

func TestGetVersionInfo(t *testing.T) {
	result := GetVersionInfo()

	// Check that the result contains expected fields.
	expectedFields := []string{
		"Version:",
		"Git Commit:",
		"Build Date:",
	}

	for _, field := range expectedFields {
		if !strings.Contains(result, field) {
			t.Errorf("GetVersionInfo() should contain '%s'", field)
		}
	}

	// Check that the result has the expected format (3 lines).
	lines := strings.Split(strings.TrimSpace(result), "\n")
	if len(lines) != 3 {
		t.Fatalf("GetVersionInfo() should return 3 lines, got %d", len(lines))
	}

	// Check that each line has the expected format.
	for i, line := range lines {
		if !strings.Contains(line, ": ") {
			t.Errorf("Line %d should contain ': ', got '%s'", i+1, line)
		}
	}
}
