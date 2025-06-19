package mongo

import (
	"context"

	"mongo2dynamo/pkg/common"
	"mongo2dynamo/pkg/config"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// Reader implements the DataReader interface for MongoDB.
type Reader struct {
	collection *mongo.Collection
}

// newReader creates a new MongoDB reader.
func newReader(collection *mongo.Collection) *Reader {
	return &Reader{
		collection: collection,
	}
}

// Read retrieves all documents from the MongoDB collection.
func (r *Reader) Read(ctx context.Context) ([]map[string]interface{}, error) {
	cursor, err := r.collection.Find(ctx, bson.M{})
	if err != nil {
		return nil, &common.DatabaseOperationError{Database: "MongoDB", Op: "find", Reason: err.Error(), Err: err}
	}
	defer cursor.Close(ctx)

	var documents []map[string]interface{}
	if err := cursor.All(ctx, &documents); err != nil {
		return nil, &common.DataValidationError{Field: "mongo decode", Reason: err.Error(), Err: err}
	}

	return documents, nil
}

// NewDataReader creates a DataReader for MongoDB based on the configuration.
func NewDataReader(ctx context.Context, cfg *config.Config) (common.DataReader, error) {
	client, err := Connect(ctx, cfg)
	if err != nil {
		return nil, err // Already wrapped by Connect.
	}
	return newReader(client.Database(cfg.MongoDB).Collection(cfg.MongoCollection)), nil
}
