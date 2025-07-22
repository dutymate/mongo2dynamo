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

var skipFields = map[string]struct{}{
	"__v":    {},
	"_class": {},
}

// DocTransformer transforms MongoDB documents.
type DocTransformer struct{}

// NewDocTransformer creates a new DocTransformer.
func NewDocTransformer() common.Transformer {
	return &DocTransformer{}
}

// Transform renames the '_id' field to 'id' and removes '__v' and '_class' fields.
func (t *DocTransformer) Transform(
	ctx context.Context,
	input []map[string]interface{},
) ([]map[string]interface{}, error) {
	if len(input) == 0 {
		return []map[string]interface{}{}, nil
	}

	// This is the actual transformation logic for a single document.
	transformFunc := func(_ context.Context, job worker.Job[map[string]interface{}]) worker.Result[map[string]interface{}] {
		doc := job.Data

		// Use original document size as base capacity estimate.
		estimatedFields := len(doc)
		if estimatedFields == 0 {
			return worker.Result[map[string]interface{}]{JobID: job.ID, Value: map[string]interface{}{}}
		}

		newDoc := make(map[string]interface{}, estimatedFields)
		for k, v := range doc {
			if k == "_id" {
				newDoc["id"] = convertID(v)
				continue
			}
			if _, skip := skipFields[k]; skip {
				continue
			}
			newDoc[k] = v
		}
		return worker.Result[map[string]interface{}]{JobID: job.ID, Value: newDoc}
	}

	// Configure and run the dynamic worker pool.
	minWorkers := 2
	maxWorkers := runtime.NumCPU() * 2
	pool := worker.NewDynamicWorkerPool(transformFunc, minWorkers, maxWorkers, maxWorkers, 0) // Use default scale interval.
	defer pool.Stop()

	pool.Start(ctx)
	output, err := pool.Process(ctx, input)
	if err != nil {
		return []map[string]interface{}{}, &common.TransformError{
			Reason: "document transformation failed",
			Err:    err,
		}
	}

	return output, nil
}

func convertID(id interface{}) interface{} {
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
