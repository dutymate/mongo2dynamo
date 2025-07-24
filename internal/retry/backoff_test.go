package retry

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBackoffConfig_Calculate(t *testing.T) {
	config := NewBackoffConfig()

	tests := []struct {
		name    string
		attempt int
		min     time.Duration
		max     time.Duration
	}{
		{"attempt 0", 0, 50 * time.Millisecond, 150 * time.Millisecond},
		{"attempt 1", 1, 100 * time.Millisecond, 300 * time.Millisecond},
		{"attempt 2", 2, 200 * time.Millisecond, 600 * time.Millisecond},
		{"attempt 10", 10, 15 * time.Second, 30 * time.Second}, // Should be capped at maxDelay.
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delay := config.Calculate(tt.attempt)
			assert.GreaterOrEqual(t, delay, tt.min)
			assert.LessOrEqual(t, delay, tt.max)
		})
	}
}

func TestBackoffConfig_ThreadSafety(t *testing.T) {
	config := NewBackoffConfig()
	const numGoroutines = 100
	const callsPerGoroutine = 100

	var wg sync.WaitGroup
	results := make([]time.Duration, numGoroutines*callsPerGoroutine)
	resultIndex := 0
	var mu sync.Mutex

	// Start multiple goroutines that call Calculate concurrently.
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < callsPerGoroutine; j++ {
				delay := config.Calculate(j % 5) // Use different attempt values.

				mu.Lock()
				results[resultIndex] = delay
				resultIndex++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	// Verify that all results are valid (within expected range).
	for _, result := range results {
		assert.Greater(t, result, time.Duration(0), "Delay should be positive")
		assert.LessOrEqual(t, result, config.MaxDelay, "Delay should not exceed MaxDelay")
	}

	// Verify that we have some variation in results (jitter is working).
	uniqueDelays := make(map[time.Duration]bool)
	for _, result := range results {
		uniqueDelays[result] = true
	}

	// With jitter, we should have many different delay values.
	assert.Greater(t, len(uniqueDelays), 10, "Should have significant variation in delay values due to jitter")
}
