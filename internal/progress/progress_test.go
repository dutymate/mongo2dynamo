package progress

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestProgressTracker(t *testing.T) {
	t.Run("NewProgressTracker", func(t *testing.T) {
		progressTracker := NewProgressTracker(1000, 1*time.Second)

		assert.NotNil(t, progressTracker)
		assert.Equal(t, int64(1000), progressTracker.totalItems)
		assert.Equal(t, 1*time.Second, progressTracker.updateInterval)
	})

	t.Run("UpdateProgress", func(t *testing.T) {
		progressTracker := NewProgressTracker(100, 1*time.Second)

		progressTracker.UpdateProgress(10)
		progressTracker.UpdateProgress(20)

		status := progressTracker.GetProgressStatus()
		assert.Equal(t, int64(30), status.Processed)
		assert.Equal(t, float64(30), status.Percentage)
	})

	t.Run("GetProgress", func(t *testing.T) {
		progressTracker := NewProgressTracker(100, 1*time.Second)
		progressTracker.startTime = time.Now().Add(-10 * time.Second) // Simulate 10 seconds elapsed.

		progressTracker.UpdateProgress(50)

		status := progressTracker.GetProgressStatus()
		assert.Equal(t, int64(50), status.Processed)
		assert.Equal(t, int64(100), status.Total)
		assert.Equal(t, float64(50), status.Percentage)
		assert.True(t, status.Elapsed >= 10*time.Second)
	})

	t.Run("StartAndStop", func(t *testing.T) {
		progressTracker := NewProgressTracker(100, 100*time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		progressTracker.Start(ctx)
		time.Sleep(50 * time.Millisecond)
		progressTracker.Stop()

		// Should not panic.
		assert.True(t, true)
	})
}
