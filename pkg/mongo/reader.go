package mongo

import (
	"context"

	"mongo2dynamo/pkg/common"
	"mongo2dynamo/pkg/config"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Reader implements the DataReader interface for MongoDB.
type Reader struct {
	collection *mongo.Collection
	batchSize  int // Number of documents to read in each batch.
}

// newReader creates a new MongoDB reader with the specified collection.
func newReader(collection *mongo.Collection) *Reader {
	return &Reader{
		collection: collection,
		batchSize:  1000, // Default batch size for document processing.
	}
}

// Read retrieves documents from the MongoDB collection in fixed-size chunks and processes them using the provided callback.
func (r *Reader) Read(ctx context.Context, handleChunk func([]map[string]interface{}) error) error {
	const chunkSize = 1000
	findOptions := options.Find().SetBatchSize(chunkSize)
	cursor, err := r.collection.Find(ctx, bson.M{}, findOptions)
	if err != nil {
		return &common.DatabaseOperationError{Database: "MongoDB", Op: "find", Reason: err.Error(), Err: err}
	}
	defer cursor.Close(ctx)

	chunk := make([]map[string]interface{}, 0, chunkSize)
	for cursor.Next(ctx) {
		var doc map[string]interface{}
		if err := cursor.Decode(&doc); err != nil {
			return &common.DataValidationError{
				Database: "MongoDB",
				Op:       "decode",
				Reason:   err.Error(),
				Err:      err,
			}
		}
		chunk = append(chunk, doc)
		if len(chunk) >= chunkSize {
			if err := handleChunk(chunk); err != nil {
				return err
			}
			chunk = make([]map[string]interface{}, 0, chunkSize)
		}
	}

	// Check for any cursor errors after iteration.
	if err := cursor.Err(); err != nil {
		return &common.DatabaseOperationError{Database: "MongoDB", Op: "cursor", Reason: err.Error(), Err: err}
	}

	if len(chunk) > 0 {
		if err := handleChunk(chunk); err != nil {
			return err
		}
	}

	return nil
}

// NewDataReader creates a DataReader for MongoDB based on the configuration.
func NewDataReader(ctx context.Context, cfg *config.Config) (common.DataReader, error) {
	client, err := Connect(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return newReader(client.Database(cfg.MongoDB).Collection(cfg.MongoCollection)), nil
}
