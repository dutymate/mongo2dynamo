package worker

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultScaleInterval = 500 * time.Millisecond
)

// Job represents a unit of work to be processed, with an ID and payload.
type Job[T any] struct {
	ID   int
	Data T
}

// Result holds the outcome of a processed job.
type Result[V any] struct {
	JobID int
	Value V
	Err   error
}

// JobFunc defines the function signature for work to be performed on a job.
type JobFunc[T, V any] func(context.Context, Job[T]) Result[V]

// DynamicWorkerPool manages a pool of workers that scales based on workload.
type DynamicWorkerPool[T, V any] struct {
	jobFunc       JobFunc[T, V]
	minWorkers    int
	maxWorkers    int
	queueSize     int
	scaleInterval time.Duration

	jobs          chan Job[T]
	results       chan Result[V]
	wg            sync.WaitGroup
	pendingJobs   int64
	mu            sync.Mutex
	activeWorkers int32

	startOnce     sync.Once
	stopOnce      sync.Once
	closeJobsOnce sync.Once
	cancel        context.CancelFunc
}

// NewDynamicWorkerPool creates a new dynamic worker pool.
func NewDynamicWorkerPool[T, V any](
	jobFunc JobFunc[T, V],
	minWorkers, maxWorkers, queueSize int,
	scaleInterval time.Duration,
) *DynamicWorkerPool[T, V] {
	if minWorkers <= 0 {
		minWorkers = 1
	}
	if maxWorkers <= 0 {
		maxWorkers = runtime.NumCPU()
	}
	if queueSize <= 0 {
		queueSize = runtime.NumCPU()
	}
	if scaleInterval <= 0 {
		scaleInterval = DefaultScaleInterval
	}
	return &DynamicWorkerPool[T, V]{
		jobFunc:       jobFunc,
		minWorkers:    minWorkers,
		maxWorkers:    maxWorkers,
		queueSize:     queueSize,
		scaleInterval: scaleInterval,
	}
}

// Start initializes and starts the worker pool and its scaler.
func (p *DynamicWorkerPool[T, V]) Start(ctx context.Context) {
	p.startOnce.Do(func() {
		poolCtx, cancel := context.WithCancel(ctx)
		p.cancel = cancel

		p.jobs = make(chan Job[T], p.queueSize)
		p.results = make(chan Result[V], p.queueSize)

		initialWorkers := max(p.minWorkers, 1)
		p.activeWorkers = int32(initialWorkers)

		for i := 0; i < initialWorkers; i++ {
			p.wg.Add(1)
			go p.worker(poolCtx)
		}

		go p.scaler(poolCtx)
	})
}

// Process sends jobs to the pool and collects the results.
func (p *DynamicWorkerPool[T, V]) Process(ctx context.Context, jobsData []T) ([]V, error) {
	p.Start(ctx)

	numJobs := len(jobsData)
	outputs := make([]V, numJobs)

	go func() {
		defer p.closeJobs()
		for i, data := range jobsData {
			atomic.AddInt64(&p.pendingJobs, 1)
			select {
			case p.jobs <- Job[T]{ID: i, Data: data}:
			case <-ctx.Done():
				// The job was not sent, so decrement the counter.
				atomic.AddInt64(&p.pendingJobs, -1)
				return
			}
		}
	}()

	for i := 0; i < numJobs; i++ {
		select {
		case result := <-p.results:
			if result.Err != nil {
				p.Stop()
				p.drainJobs()
				return nil, result.Err
			}
			outputs[result.JobID] = result.Value
		case <-ctx.Done():
			p.Stop()
			return nil, fmt.Errorf("context canceled: %w", ctx.Err())
		}
	}

	return outputs, nil
}

// Stop gracefully shuts down the worker pool.
func (p *DynamicWorkerPool[T, V]) Stop() {
	p.stopOnce.Do(func() {
		if p.cancel != nil {
			p.cancel()
		}
		p.closeJobs()
		p.wg.Wait()
	})
}

// drainJobs drains any remaining jobs from the jobs channel.
func (p *DynamicWorkerPool[T, V]) drainJobs() {
	for range p.jobs {
		atomic.AddInt64(&p.pendingJobs, -1)
	}
}

// closeJobs closes the jobs channel.
func (p *DynamicWorkerPool[T, V]) closeJobs() {
	p.closeJobsOnce.Do(func() {
		close(p.jobs)
	})
}

// worker processes jobs from the jobs channel and sends results to the results channel.
func (p *DynamicWorkerPool[T, V]) worker(ctx context.Context) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-p.jobs:
			if !ok {
				return
			}
			func() {
				defer func() {
					if r := recover(); r != nil {
						p.results <- Result[V]{
							JobID: job.ID,
							Err:   fmt.Errorf("panic recovered in worker: %v", r),
						}
					}
					atomic.AddInt64(&p.pendingJobs, -1)
				}()
				p.results <- p.jobFunc(ctx, job)
			}()
		}
	}
}

// scaler adjusts the number of workers based on the pending jobs.
func (p *DynamicWorkerPool[T, V]) scaler(ctx context.Context) {
	ticker := time.NewTicker(p.scaleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.mu.Lock()
			pending := atomic.LoadInt64(&p.pendingJobs)
			workers := atomic.LoadInt32(&p.activeWorkers)
			if pending > int64(workers) && workers < int32(p.maxWorkers) {
				atomic.AddInt32(&p.activeWorkers, 1)
				p.wg.Add(1)
				go p.worker(ctx)
			}
			p.mu.Unlock()
		}
	}
}
