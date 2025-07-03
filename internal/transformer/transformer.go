package transformer

import (
	"fmt"
	"mongo2dynamo/internal/common"
	"runtime"
	"sync"
)

// Transformer provides an interface for transforming documents between formats.
type Transformer interface {
	// Transform returns a new slice of documents with the transformation applied.
	// Returns an error if transformation fails.
	Transform([]map[string]interface{}) ([]map[string]interface{}, error)
}

// MongoToDynamoTransformer transforms MongoDB documents for DynamoDB.
// It renames the '_id' field to 'id' and removes the '__v' and '_class' fields.
type MongoToDynamoTransformer struct{}

// skipFields lists field names to be excluded from the output.
var skipFields = map[string]struct{}{
	"__v":    {},
	"_class": {},
}

// NewMongoToDynamoTransformer returns a new MongoToDynamoTransformer.
// This transformer can be used to convert MongoDB documents to a DynamoDB-compatible format.
func NewMongoToDynamoTransformer() *MongoToDynamoTransformer {
	return &MongoToDynamoTransformer{}
}

// Transform renames the '_id' field to 'id' and removes the '__v' and '_class' fields from each document.
// The '_id' field from MongoDB is mapped to 'id' to match DynamoDB's primary key naming convention.
// The '__v' field, which is often used for versioning in MongoDB/Mongoose, is omitted from the output.
// The '_class' field, which is used by Spring Data for type information, is also omitted from the output.
// The resulting slice contains documents ready to be written to DynamoDB, with no '_id', '__v', or '_class' fields present.
// Returns an error only if an unexpected issue occurs during transformation (none in current implementation).
// This function uses a worker pool to parallelize transformation for better performance on large batches.
func (t *MongoToDynamoTransformer) Transform(input []map[string]interface{}) ([]map[string]interface{}, error) {
	output := make([]map[string]interface{}, len(input))
	if len(input) == 0 {
		return output, nil
	}

	numWorkers := min(runtime.NumCPU(), len(input)) // Number of workers is capped to the number of jobs.
	type job struct {
		idx int
		doc map[string]interface{}
	}
	jobs := make(chan job, len(input))
	errChan := make(chan error, numWorkers) // Channel to collect errors from workers.
	var wg sync.WaitGroup
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
			doc := j.doc
			// Count the number of fields to keep (including id).
			kept := 0
			for k := range doc {
				if k == "_id" {
					continue
				}
				if _, skip := skipFields[k]; skip {
					continue
				}
				kept++
			}
			// Create a new document with the kept fields and id.
			newDoc := make(map[string]interface{}, kept+1)
			for k, v := range doc {
				if k == "_id" {
					newDoc["id"] = v
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

	// Start the worker pool.
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker()
	}

	// Distribute jobs to workers.
	for i, doc := range input {
		jobs <- job{idx: i, doc: doc}
	}
	close(jobs)

	wg.Wait()
	close(errChan)
	// Return the first error encountered by any worker, if present.
	for err := range errChan {
		if err != nil {
			return nil, err
		}
	}
	return output, nil
}
