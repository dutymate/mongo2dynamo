package worker

import (
	"context"
	"errors"
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
