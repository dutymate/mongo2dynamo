package transformer

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"mongo2dynamo/internal/common"
)

// DocTransformer transforms MongoDB documents for DynamoDB.
// It renames the '_id' field to 'id' and removes the '__v' and '_class' fields.
type DocTransformer struct{}

// skipFields lists field names to be excluded from the output.
var skipFields = map[string]struct{}{
	"__v":    {},
	"_class": {},
}

// newDocTransformer creates a new DocTransformer.
func newDocTransformer() *DocTransformer {
	return &DocTransformer{}
}

// NewDocTransformer creates a new DocTransformer.
func NewDocTransformer() common.Transformer {
	return newDocTransformer()
}

// convertID converts MongoDB _id to DynamoDB compatible format while preserving types.
func convertID(id interface{}) interface{} {
	switch v := id.(type) {
	case primitive.ObjectID:
		return v.Hex()
	case primitive.M:
		// Convert MongoDB primitive.M objects to JSON string.
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			// Fallback to string representation if JSON marshaling fails.
			return fmt.Sprintf("%v", v)
		}
		return string(jsonBytes)
	default:
		// All other types (string, numbers, bool, etc.) pass through unchanged.
		// DynamoDB will handle type conversion automatically.
		return v
	}
}

// Transform renames the '_id' field to 'id' and removes the '__v' and '_class' fields from each document.
// The '_id' field from MongoDB is mapped to 'id' to match DynamoDB's primary key naming convention.
// The '__v' field, which is often used for versioning in MongoDB/Mongoose, is omitted from the output.
// The '_class' field, which is used by Spring Data for type information, is also omitted from the output.
// The resulting slice contains documents ready to be written to DynamoDB, with no '_id', '__v', or '_class' fields present.
// Returns an error only if an unexpected issue occurs during transformation (none in current implementation).
func (t *DocTransformer) Transform(ctx context.Context, input []map[string]interface{}) ([]map[string]interface{}, error) {
	output := make([]map[string]interface{}, len(input))
	if len(input) == 0 {
		return output, nil
	}

	// Dynamic worker pool configuration.
	minWorkers := 2
	maxWorkers := runtime.NumCPU() * 2 // Allow up to twice the number of CPU cores as workers.
	initialWorkers := max(min(runtime.NumCPU(), len(input)), minWorkers)

	type job struct {
		idx int
		doc map[string]interface{}
	}

	jobs := make(chan job, len(input))
	errChan := make(chan error, maxWorkers)
	var wg sync.WaitGroup
	var mu sync.Mutex
	activeWorkers := initialWorkers

	// Atomic counter for tracking pending jobs to avoid race conditions.
	var pendingJobs int64

	// Worker function.
	worker := func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				errChan <- &common.TransformError{
					Reason: fmt.Sprintf("worker panic during document transformation: %v", r),
					Err:    fmt.Errorf("panic: %v", r),
				}
			}
		}()

		for j := range jobs {
			// Decrement pending jobs counter when starting to process a job.
			atomic.AddInt64(&pendingJobs, -1)

			doc := j.doc
			// Count the number of fields to keep.
			kept := 0
			for k := range doc {
				if _, skip := skipFields[k]; skip {
					continue
				}
				kept++
			}

			if kept == 0 {
				continue
			}

			// Create a new document with the kept fields and id.
			newDoc := make(map[string]interface{}, kept+1)

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
			output[j.idx] = newDoc
		}
	}

	// Goroutine for dynamic worker adjustment.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		ticker := time.NewTicker(500 * time.Millisecond) // Check every 0.5 seconds.
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				currentPendingJobs := atomic.LoadInt64(&pendingJobs)

				// Perform all scaling checks within a single lock to prevent overscaling.
				mu.Lock()
				currentWorkers := activeWorkers

				// If there are many pending jobs and more workers can be added, scale up.
				if currentPendingJobs > int64(currentWorkers*2) && currentWorkers < maxWorkers {
					activeWorkers++
					wg.Add(1)
					go worker() // Spawn a new worker goroutine to handle additional load.
				}
				mu.Unlock()

				// No action needed for scaling down, as workers exit naturally when jobs channel closes.
			}
		}
	}()

	// Start initial workers.
	for i := 0; i < initialWorkers; i++ {
		wg.Add(1)
		go worker()
	}

	// Distribute jobs and increment pending jobs counter.
	for i, doc := range input {
		jobs <- job{idx: i, doc: doc}
		atomic.AddInt64(&pendingJobs, 1)
	}
	close(jobs)

	// Wait for all workers to finish.
	wg.Wait()
	close(errChan)

	// Check for errors.
	for err := range errChan {
		if err != nil {
			return nil, err
		}
	}

	return output, nil
}
