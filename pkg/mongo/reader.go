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

// Read retrieves all documents from the MongoDB collection.
func (r *Reader) Read(ctx context.Context) ([]map[string]interface{}, error) {
	// Configure cursor with batch size for efficient document retrieval.
	findOptions := options.Find().SetBatchSize(int32(r.batchSize))
	cursor, err := r.collection.Find(ctx, bson.M{}, findOptions)
	if err != nil {
		return nil, &common.DatabaseOperationError{Database: "MongoDB", Op: "find", Reason: err.Error(), Err: err}
	}
	defer cursor.Close(ctx)

	documents := make([]map[string]interface{}, 0, r.batchSize)

	// Iterate through the cursor to read all documents.
	for cursor.Next(ctx) {
		var doc map[string]interface{}
		if err := cursor.Decode(&doc); err != nil {
			return nil, &common.DataValidationError{Field: "mongo decode", Reason: err.Error(), Err: err}
		}

		documents = append(documents, doc)
	}

	// Check for any cursor errors after iteration.
	if err := cursor.Err(); err != nil {
		return nil, &common.DatabaseOperationError{Database: "MongoDB", Op: "cursor", Reason: err.Error(), Err: err}
	}

	return documents, nil
}

// NewDataReader creates a DataReader for MongoDB based on the configuration.
func NewDataReader(ctx context.Context, cfg *config.Config) (common.DataReader, error) {
	client, err := Connect(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return newReader(client.Database(cfg.MongoDB).Collection(cfg.MongoCollection)), nil
}
