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
	DefaultScaleInterval       = 500 * time.Millisecond
	DefaultScaleUpThreshold    = 0.8
	DefaultScaleDownThreshold  = 0.3
	DefaultMinWorkers          = 1
	ScaleDownChannelBufferSize = 1
)

var (
	DefaultMaxWorkers = runtime.NumCPU()
	DefaultQueueSize  = runtime.NumCPU()
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
	jobFunc            JobFunc[T, V]
	minWorkers         int
	maxWorkers         int
	queueSize          int
	scaleInterval      time.Duration
	scaleUpThreshold   float64
	scaleDownThreshold float64

	jobs          chan Job[T]
	results       chan Result[V]
	wg            sync.WaitGroup
	pendingJobs   int64
	mu            sync.Mutex
	activeWorkers int32
	scaleDownChan chan struct{}
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
		minWorkers = DefaultMinWorkers
	}
	if maxWorkers <= 0 {
		maxWorkers = DefaultMaxWorkers
	}
	if queueSize <= 0 {
		queueSize = DefaultQueueSize
	}
	if scaleInterval <= 0 {
		scaleInterval = DefaultScaleInterval
	}
	scaleDownChan := make(chan struct{}, ScaleDownChannelBufferSize) // Buffered channel for scale down signals.
	return &DynamicWorkerPool[T, V]{
		jobFunc:            jobFunc,
		minWorkers:         minWorkers,
		maxWorkers:         maxWorkers,
		queueSize:          queueSize,
		scaleInterval:      scaleInterval,
		scaleUpThreshold:   DefaultScaleUpThreshold,
		scaleDownThreshold: DefaultScaleDownThreshold,
		scaleDownChan:      scaleDownChan,
	}
}

// Start initializes and starts the worker pool and its scaler.
func (p *DynamicWorkerPool[T, V]) Start(ctx context.Context) {
	p.startOnce.Do(func() {
		poolCtx, cancel := context.WithCancel(ctx)
		p.cancel = cancel

		p.jobs = make(chan Job[T], p.queueSize)
		p.results = make(chan Result[V], p.queueSize)

		initialWorkers := p.minWorkers
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

	// Create a dedicated context for sendJobs.
	sendCtx, sendCancel := context.WithCancel(ctx)

	// Use a WaitGroup to ensure sendJobs completes before canceling sendCtx.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.sendJobs(sendCtx, jobsData)
	}()

	// Collect results.
	results, err := p.collectResults(ctx, outputs)

	// Wait for sendJobs to complete before canceling the context.
	wg.Wait()
	sendCancel()

	return results, err
}

// sendJobs sends jobs to the worker pool.
func (p *DynamicWorkerPool[T, V]) sendJobs(ctx context.Context, jobsData []T) {
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
}

// collectResults collects results from the worker pool.
func (p *DynamicWorkerPool[T, V]) collectResults(ctx context.Context, outputs []V) ([]V, error) {
	numJobs := len(outputs)

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
		// Close scale down channel to signal all workers to stop.
		close(p.scaleDownChan)
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

// GetStats returns current pool statistics.
func (p *DynamicWorkerPool[T, V]) GetStats() map[string]any {
	activeWorkers := atomic.LoadInt32(&p.activeWorkers)
	pendingJobs := atomic.LoadInt64(&p.pendingJobs)
	loadFactor := p.calculateLoadFactor(pendingJobs, activeWorkers)

	return map[string]any{
		"active_workers":       activeWorkers,
		"pending_jobs":         pendingJobs,
		"load_factor":          loadFactor,
		"scale_up_threshold":   p.scaleUpThreshold,
		"scale_down_threshold": p.scaleDownThreshold,
		"min_workers":          p.minWorkers,
		"max_workers":          p.maxWorkers,
		"queue_size":           p.queueSize,
	}
}

// worker processes jobs from the jobs channel and sends results to the results channel.
func (p *DynamicWorkerPool[T, V]) worker(ctx context.Context) {
	defer p.wg.Done()

	workerCountDecremented := false
	defer func() {
		if !workerCountDecremented {
			atomic.AddInt32(&p.activeWorkers, -1)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			atomic.AddInt32(&p.activeWorkers, -1)
			workerCountDecremented = true
			return
		case <-p.scaleDownChan:
			// Decrement worker count immediately when scaling down.
			atomic.AddInt32(&p.activeWorkers, -1)
			workerCountDecremented = true

			// Received scale down signal, drain remaining jobs before exiting.
			for {
				select {
				case job, ok := <-p.jobs:
					if !ok {
						return // Channel is closed.
					}
					p.processJobWithRecovery(ctx, job)
				default:
					// No more jobs in channel, exit gracefully.
					return
				}
			}
		case job, ok := <-p.jobs:
			if !ok {
				atomic.AddInt32(&p.activeWorkers, -1)
				workerCountDecremented = true
				return
			}

			// Process the job.
			p.processJobWithRecovery(ctx, job)
		}
	}
}

// processJobWithRecovery processes a job with panic recovery and pending jobs tracking.
func (p *DynamicWorkerPool[T, V]) processJobWithRecovery(ctx context.Context, job Job[T]) {
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
			p.adjustWorkers(ctx)
		}
	}
}

// adjustWorkers makes scaling decisions and adjusts the number of workers.
func (p *DynamicWorkerPool[T, V]) adjustWorkers(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()

	pending := atomic.LoadInt64(&p.pendingJobs)
	workers := atomic.LoadInt32(&p.activeWorkers)
	loadFactor := p.calculateLoadFactor(pending, workers)

	// Scale up if needed.
	if p.shouldScaleUp(loadFactor, workers) {
		p.scaleUp(ctx)
	}

	// Scale down if needed.
	if p.shouldScaleDown(loadFactor, workers) {
		p.scaleDown()
	}
}

// calculateLoadFactor calculates the load factor (pending jobs per worker).
func (p *DynamicWorkerPool[T, V]) calculateLoadFactor(pending int64, workers int32) float64 {
	if workers == 0 {
		return 0.0
	}
	return float64(pending) / float64(workers)
}

// shouldScaleUp checks if we should scale up based on load factor and current workers.
func (p *DynamicWorkerPool[T, V]) shouldScaleUp(loadFactor float64, workers int32) bool {
	return loadFactor > p.scaleUpThreshold && workers < int32(p.maxWorkers)
}

// shouldScaleDown checks if we should scale down based on load factor and current workers.
func (p *DynamicWorkerPool[T, V]) shouldScaleDown(loadFactor float64, workers int32) bool {
	return loadFactor < p.scaleDownThreshold && workers > int32(p.minWorkers)
}

// scaleUp adds a new worker to the pool.
func (p *DynamicWorkerPool[T, V]) scaleUp(ctx context.Context) {
	atomic.AddInt32(&p.activeWorkers, 1)
	p.wg.Add(1)
	go p.worker(ctx)
}

// scaleDown signals a worker to stop by sending a signal to the scale down channel.
func (p *DynamicWorkerPool[T, V]) scaleDown() {
	// Send a scale down signal to one worker.
	select {
	case p.scaleDownChan <- struct{}{}:
		// Signal sent successfully. Worker will decrement counter when it exits.
	default:
		// Channel is full or no workers are listening, which is fine.
	}
}
