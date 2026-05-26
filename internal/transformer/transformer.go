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
// For []any and map[string]any, the original is returned when no nested element needs conversion;
// callers may share the result with the input. Downstream consumers (attributevalue.MarshalMap) only read,
// so the shallow-share is safe and avoids per-document allocations on plain JSON-like sub-trees.
func convertValue(v any) any {
	switch val := v.(type) {
	case primitive.ObjectID:
		// Convert ObjectID to hex string for references.
		return val.Hex()
	case []any:
		if !needsConversion(val) {
			return v
		}
		result := make([]any, len(val))
		for i, item := range val {
			result[i] = convertValue(item)
		}
		return result
	case map[string]any:
		if !needsConversion(val) {
			return v
		}
		result := make(map[string]any, len(val))
		for k, item := range val {
			result[k] = convertValue(item)
		}
		return result
	case bson.M:
		// bson.M is always normalized to map[string]any per the established contract.
		result := make(map[string]any, len(val))
		for k, item := range val {
			result[k] = convertValue(item)
		}
		return result
	case bson.A:
		// bson.A is always normalized to []any per the established contract.
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

// needsConversion reports whether v (or any nested element) would change when passed through convertValue.
// It is allocation-free and lets convertValue skip allocating containers when nothing inside changes.
func needsConversion(v any) bool {
	switch val := v.(type) {
	case primitive.ObjectID:
		return true
	case bson.M, bson.A:
		// These types are always re-typed to map[string]any / []any.
		return true
	case []any:
		for _, item := range val {
			if needsConversion(item) {
				return true
			}
		}
		return false
	case map[string]any:
		for _, item := range val {
			if needsConversion(item) {
				return true
			}
		}
		return false
	default:
		return false
	}
}
