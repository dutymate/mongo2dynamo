package transformer

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"mongo2dynamo/internal/common"
	"mongo2dynamo/internal/worker"
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
				newDoc["id"] = convertID(v)
			case "__v", "_class":
				continue
			default:
				newDoc[k] = v
			}
		}
		return worker.Result[map[string]any]{JobID: job.ID, Value: newDoc}
	}

	// Configure and run the dynamic worker pool.
	minWorkers := 2
	maxWorkers := runtime.NumCPU() * 2
	pool := worker.NewDynamicWorkerPool(transformFunc, minWorkers, maxWorkers, maxWorkers, 0) // Use default scale interval.
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

func convertID(id any) any {
	switch v := id.(type) {
	case primitive.ObjectID:
		return v.Hex()
	case primitive.M:
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(jsonBytes)
	default:
		return v
	}
}
