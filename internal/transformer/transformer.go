package transformer

import (
	"context"
	"runtime"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"mongo2dynamo/internal/common"
	"mongo2dynamo/internal/worker"
)

const (
	DefaultMinWorkers    = 2
	DefaultScaleInterval = 500 * time.Millisecond
)

var (
	DefaultMaxWorkers = runtime.NumCPU() * 2
	DefaultQueueSize  = DefaultMaxWorkers * 2
)

// transformFunc is the per-document transformation applied by the worker pool.
func transformFunc(_ context.Context, job worker.Job[map[string]any]) worker.Result[map[string]any] {
	doc := job.Data

	estimatedFields := len(doc)
	if estimatedFields == 0 {
		return worker.Result[map[string]any]{JobID: job.ID, Value: map[string]any{}}
	}

	newDoc := make(map[string]any, estimatedFields)
	for k, v := range doc {
		newDoc[k] = convertValue(v)
	}
	return worker.Result[map[string]any]{JobID: job.ID, Value: newDoc}
}

// DocTransformer transforms MongoDB documents using a shared, reusable worker pool.
type DocTransformer struct {
	mu         sync.Mutex
	pool       *worker.DynamicWorkerPool[map[string]any, map[string]any]
	poolCancel context.CancelFunc
	closed     bool
}

// NewDocTransformer creates a new DocTransformer with a shared worker pool that is reused across Transform calls.
// The pool lifetime is owned by the transformer, decoupled from any single Transform's ctx, so that pool workers survive across chunk boundaries.
func NewDocTransformer() common.Transformer {
	poolCtx, cancel := context.WithCancel(context.Background())
	pool := worker.NewDynamicWorkerPool(transformFunc, DefaultMinWorkers, DefaultMaxWorkers, DefaultQueueSize, DefaultScaleInterval)
	pool.SetBackpressureThreshold(0.95)
	pool.SetBackpressureTimeout(10 * time.Millisecond)
	pool.Start(poolCtx)
	return &DocTransformer{
		pool:       pool,
		poolCancel: cancel,
	}
}

// Transform converts MongoDB documents (e.g., ObjectID → hex string) using the shared worker pool.
// Concurrent callers are serialized because the underlying pool's Process is not safe for concurrent use (job IDs and result channels are shared).
func (t *DocTransformer) Transform(
	ctx context.Context,
	input []map[string]any,
) ([]map[string]any, error) {
	if len(input) == 0 {
		return []map[string]any{}, nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return []map[string]any{}, &common.TransformError{
			Reason: "transformer is closed",
		}
	}

	output, err := t.pool.Process(ctx, input)
	if err != nil {
		return []map[string]any{}, &common.TransformError{
			Reason: "document transformation failed",
			Err:    err,
		}
	}

	return output, nil
}

// Close shuts down the worker pool and releases its goroutines.
func (t *DocTransformer) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return
	}
	t.closed = true

	t.pool.Stop()
	t.poolCancel()
}

// convertValue recursively converts values, handling ObjectID references.
func convertValue(v any) any {
	switch val := v.(type) {
	case primitive.ObjectID:
		// Convert ObjectID to hex string for references.
		return val.Hex()
	case []any:
		// Handle arrays (e.g., array of ObjectIDs).
		result := make([]any, len(val))
		for i, item := range val {
			result[i] = convertValue(item)
		}
		return result
	case map[string]any:
		// Handle nested objects.
		result := make(map[string]any, len(val))
		for k, item := range val {
			result[k] = convertValue(item)
		}
		return result
	case bson.M:
		// Handle BSON maps.
		result := make(map[string]any, len(val))
		for k, item := range val {
			result[k] = convertValue(item)
		}
		return result
	case bson.A:
		// Handle BSON arrays.
		result := make([]any, len(val))
		for i, item := range val {
			result[i] = convertValue(item)
		}
		return result
	default:
		// For other types, return as-is.
		return v
	}
}
