package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDynamicWorkerPool_SuccessfulJobs(t *testing.T) {
	jobFunc := func(_ context.Context, job Job[int]) Result[int] {
		return Result[int]{JobID: job.ID, Value: job.Data * 2}
	}

	pool := NewDynamicWorkerPool(jobFunc, 2, 4, 10, 100*time.Millisecond)
	defer pool.Stop()

	jobs := []int{1, 2, 3, 4, 5}
	results, err := pool.Process(context.Background(), jobs)
	require.NoError(t, err)
	require.Len(t, results, 5)
	assert.ElementsMatch(t, []int{2, 4, 6, 8, 10}, results)
}

func TestDynamicWorkerPool_JobError(t *testing.T) {
	errorJobFunc := func(_ context.Context, job Job[int]) Result[int] {
		if job.Data == 3 {
			return Result[int]{JobID: job.ID, Err: errors.New("job error")}
		}
		return Result[int]{JobID: job.ID, Value: job.Data * 2}
	}

	pool := NewDynamicWorkerPool(errorJobFunc, 2, 4, 10, 100*time.Millisecond)
	defer pool.Stop()

	jobs := []int{1, 2, 3, 4, 5}
	_, err := pool.Process(context.Background(), jobs)
	require.Error(t, err)
}

func TestDynamicWorkerPool_ContextCancellation(t *testing.T) {
	cancellationJobFunc := func(_ context.Context, job Job[int]) Result[int] {
		time.Sleep(200 * time.Millisecond) // Ensure jobs take time.
		return Result[int]{JobID: job.ID, Value: job.Data}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	pool := NewDynamicWorkerPool(cancellationJobFunc, 2, 4, 10, 100*time.Millisecond)
	defer pool.Stop()

	jobs := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	_, err := pool.Process(ctx, jobs)

	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestDynamicWorkerPool_ScaleUp(t *testing.T) {
	// Create a worker pool with aggressive scaling for testing.
	pool := NewDynamicWorkerPool(
		func(_ context.Context, job Job[int]) Result[int] {
			// Simulate some work.
			time.Sleep(10 * time.Millisecond)
			return Result[int]{JobID: job.ID, Value: job.Data * 2}
		},
		2,                    // minWorkers.
		10,                   // maxWorkers.
		100,                  // queueSize.
		100*time.Millisecond, // scaleInterval.
	)

	ctx := context.Background()
	pool.Start(ctx)
	defer pool.Stop()

	// Submit many jobs to trigger scale up.
	jobs := make([]int, 50)
	for i := range jobs {
		jobs[i] = i
	}

	// Use a WaitGroup to synchronize the goroutine.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		results, err := pool.Process(ctx, jobs)
		require.NoError(t, err)
		assert.Len(t, results, 50)
	}()

	// Wait for scaling to occur.
	time.Sleep(200 * time.Millisecond)

	// Check that workers scaled up.
	stats := pool.GetStats()
	activeWorkers := stats["active_workers"].(int32)
	assert.Greater(t, activeWorkers, int32(2), "Should have scaled up from minimum workers")

	// Wait for the goroutine to finish.
	wg.Wait()
}

func TestDynamicWorkerPool_ScaleDown(t *testing.T) {
	// Create a worker pool with aggressive scaling for testing.
	pool := NewDynamicWorkerPool(
		func(_ context.Context, job Job[int]) Result[int] {
			// Simulate some work.
			time.Sleep(10 * time.Millisecond)
			return Result[int]{JobID: job.ID, Value: job.Data * 2}
		},
		2,                    // minWorkers.
		10,                   // maxWorkers.
		100,                  // queueSize.
		100*time.Millisecond, // scaleInterval.
	)

	ctx := context.Background()
	pool.Start(ctx)
	defer pool.Stop()

	// First, scale up with high load.
	highLoadJobs := make([]int, 100)
	for i := range highLoadJobs {
		highLoadJobs[i] = i
	}

	// Use a WaitGroup to synchronize the goroutine.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		results, err := pool.Process(ctx, highLoadJobs)
		require.NoError(t, err)
		assert.Len(t, results, 100)
	}()

	// Wait for scaling to stabilize.
	time.Sleep(400 * time.Millisecond)

	// Check that workers scaled up.
	stats := pool.GetStats()
	activeWorkers := stats["active_workers"].(int32)
	assert.Greater(t, activeWorkers, int32(2), "Should have scaled up from minimum workers")

	// Wait for all jobs to complete and then check scale down.
	time.Sleep(800 * time.Millisecond)

	// Check that workers scaled down.
	stats = pool.GetStats()
	activeWorkers = stats["active_workers"].(int32)
	assert.LessOrEqual(t, activeWorkers, int32(5), "Should have scaled down due to low load")

	// Wait for the goroutine to finish.
	wg.Wait()
}

func TestDynamicWorkerPool_MaintainMinimumWorkers(t *testing.T) {
	// Create a worker pool with aggressive scaling for testing.
	pool := NewDynamicWorkerPool(
		func(_ context.Context, job Job[int]) Result[int] {
			// Simulate some work.
			time.Sleep(10 * time.Millisecond)
			return Result[int]{JobID: job.ID, Value: job.Data * 2}
		},
		2,                    // minWorkers.
		10,                   // maxWorkers.
		100,                  // queueSize.
		100*time.Millisecond, // scaleInterval.
	)

	ctx := context.Background()
	pool.Start(ctx)
	defer pool.Stop()

	// Wait for scaling to stabilize.
	time.Sleep(200 * time.Millisecond)

	// Submit no jobs.
	jobs := make([]int, 0)
	results, err := pool.Process(ctx, jobs)
	require.NoError(t, err)
	assert.Len(t, results, 0)

	// Wait for potential scale down.
	time.Sleep(300 * time.Millisecond)

	// Check that we maintain minimum workers.
	stats := pool.GetStats()
	activeWorkers := stats["active_workers"].(int32)
	assert.GreaterOrEqual(t, activeWorkers, int32(2), "Should maintain minimum workers")
}

func TestDynamicWorkerPool_LoadFactorCalculation(t *testing.T) {
	pool := NewDynamicWorkerPool(
		func(_ context.Context, job Job[int]) Result[int] {
			return Result[int]{JobID: job.ID, Value: job.Data}
		},
		2,                    // minWorkers.
		10,                   // maxWorkers.
		100,                  // queueSize.
		100*time.Millisecond, // scaleInterval.
	)

	ctx := context.Background()
	pool.Start(ctx)
	defer pool.Stop()

	// Submit jobs and check load factor.
	jobs := make([]int, 20)
	for i := range jobs {
		jobs[i] = i
	}

	// Process jobs.
	results, err := pool.Process(ctx, jobs)
	require.NoError(t, err)
	assert.Len(t, results, 20)

	// Get stats and check load factor.
	stats := pool.GetStats()
	loadFactor := stats["load_factor"].(float64)
	activeWorkers := stats["active_workers"].(int32)
	pendingJobs := stats["pending_jobs"].(int64)

	// Load factor should be reasonable.
	assert.GreaterOrEqual(t, loadFactor, 0.0, "Load factor should be non-negative")
	assert.LessOrEqual(t, loadFactor, 10.0, "Load factor should be reasonable")

	t.Logf("Active workers: %d, Pending jobs: %d, Load factor: %.2f",
		activeWorkers, pendingJobs, loadFactor)
}

func TestDynamicWorkerPool_DefaultThresholds(t *testing.T) {
	pool := NewDynamicWorkerPool(
		func(_ context.Context, job Job[int]) Result[int] {
			return Result[int]{JobID: job.ID, Value: job.Data}
		},
		2,                    // minWorkers.
		10,                   // maxWorkers.
		100,                  // queueSize.
		100*time.Millisecond, // scaleInterval.
	)

	// Test default threshold values.
	stats := pool.GetStats()
	scaleUpThreshold := stats["scale_up_threshold"].(float64)
	scaleDownThreshold := stats["scale_down_threshold"].(float64)

	assert.Equal(t, 0.8, scaleUpThreshold, "Default scale up threshold should be 0.8")
	assert.Equal(t, 0.3, scaleDownThreshold, "Default scale down threshold should be 0.3")
}

func TestDynamicWorkerPool_ConcurrentScaling(t *testing.T) {
	pool := NewDynamicWorkerPool(
		func(_ context.Context, job Job[int]) Result[int] {
			// Simulate variable work time.
			time.Sleep(time.Duration(job.Data%10) * time.Millisecond)
			return Result[int]{JobID: job.ID, Value: job.Data * 2}
		},
		2,                    // minWorkers.
		8,                    // maxWorkers.
		50,                   // queueSize.
		100*time.Millisecond, // scaleInterval.
	)

	ctx := context.Background()
	pool.Start(ctx)
	defer pool.Stop()

	// Submit jobs in waves to test scaling.
	for wave := 0; wave < 3; wave++ {
		// High load wave.
		highLoadJobs := make([]int, 100)
		for i := range highLoadJobs {
			highLoadJobs[i] = i + wave*100
		}

		results, err := pool.Process(ctx, highLoadJobs)
		require.NoError(t, err)
		assert.Len(t, results, 100)

		// Check scaling up.
		stats := pool.GetStats()
		activeWorkers := stats["active_workers"].(int32)
		t.Logf("Wave %d - Active workers after high load: %d", wave+1, activeWorkers)

		// Low load wave.
		lowLoadJobs := make([]int, 2)
		for i := range lowLoadJobs {
			lowLoadJobs[i] = i
		}

		results, err = pool.Process(ctx, lowLoadJobs)
		require.NoError(t, err)
		assert.Len(t, results, 2)

		// Wait for potential scale down.
		time.Sleep(200 * time.Millisecond)

		// Check scaling down.
		stats = pool.GetStats()
		activeWorkers = stats["active_workers"].(int32)
		t.Logf("Wave %d - Active workers after low load: %d", wave+1, activeWorkers)
	}

	// Final check - should maintain minimum workers.
	stats := pool.GetStats()
	activeWorkers := stats["active_workers"].(int32)
	assert.GreaterOrEqual(t, activeWorkers, int32(2), "Should maintain minimum workers")
}
