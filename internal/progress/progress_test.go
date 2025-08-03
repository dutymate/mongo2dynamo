package progress

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestProgressTracker(t *testing.T) {
	t.Run("NewProgressTracker", func(t *testing.T) {
		tracker := NewProgressTracker(1000, 1*time.Second)

		assert.NotNil(t, tracker)
		assert.Equal(t, int64(1000), tracker.totalItems)
		assert.Equal(t, 1*time.Second, tracker.updateInterval)
	})

	t.Run("UpdateProgress", func(t *testing.T) {
		tracker := NewProgressTracker(100, 1*time.Second)

		tracker.UpdateProgress(10)
		tracker.UpdateProgress(20)

		status := tracker.GetProgressStatus()
		assert.Equal(t, int64(30), status.Processed)
		assert.Equal(t, float64(30), status.Percentage)
	})

	t.Run("GetProgress", func(t *testing.T) {
		tracker := NewProgressTracker(100, 1*time.Second)
		tracker.startTime = time.Now().Add(-10 * time.Second) // Simulate 10 seconds elapsed.

		tracker.UpdateProgress(50)

		status := tracker.GetProgressStatus()
		assert.Equal(t, int64(50), status.Processed)
		assert.Equal(t, int64(100), status.Total)
		assert.Equal(t, float64(50), status.Percentage)
		assert.True(t, status.Elapsed >= 10*time.Second)
	})

	t.Run("StartAndStop", func(t *testing.T) {
		tracker := NewProgressTracker(100, 100*time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		tracker.Start(ctx)
		time.Sleep(50 * time.Millisecond)
		tracker.Stop()

		// Should not panic.
		assert.True(t, true)
	})
}
