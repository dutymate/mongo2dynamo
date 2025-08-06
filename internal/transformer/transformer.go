package transformer

import (
	"context"
	"runtime"
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

// DocTransformer transforms MongoDB documents.
type DocTransformer struct{}

// NewDocTransformer creates a new DocTransformer.
func NewDocTransformer() common.Transformer {
	return &DocTransformer{}
}

// Transform renames the '_id' field to 'id' and removes '__v' and '_class' fields.
func (t *DocTransformer) Transform(
	ctx context.Context,
	input []map[string]any,
) ([]map[string]any, error) {
	if len(input) == 0 {
		return []map[string]any{}, nil
	}

	// This is the actual transformation logic for a single document.
	transformFunc := func(_ context.Context, job worker.Job[map[string]any]) worker.Result[map[string]any] {
		doc := job.Data

		// Use original document size as base capacity estimate.
		estimatedFields := len(doc)
		if estimatedFields == 0 {
			return worker.Result[map[string]any]{JobID: job.ID, Value: map[string]any{}}
		}

		newDoc := make(map[string]any, estimatedFields)
		for k, v := range doc {
			switch k {
			case "_id":
				newDoc["id"] = convertValue(v)
			case "__v", "_class":
				continue
			default:
				newDoc[k] = convertValue(v)
			}
		}
		return worker.Result[map[string]any]{JobID: job.ID, Value: newDoc}
	}

	// Configure and run the dynamic worker pool.
	pool := worker.NewDynamicWorkerPool(transformFunc, DefaultMinWorkers, DefaultMaxWorkers, DefaultQueueSize, DefaultScaleInterval)
	defer pool.Stop()

	pool.Start(ctx)
	output, err := pool.Process(ctx, input)
	if err != nil {
		return []map[string]any{}, &common.TransformError{
			Reason: "document transformation failed",
			Err:    err,
		}
	}

	return output, nil
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
