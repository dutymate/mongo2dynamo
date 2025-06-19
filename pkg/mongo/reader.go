package mongo

import (
	"context"
	"fmt"

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
		return nil, fmt.Errorf("failed to find documents: %w", err)
	}
	defer cursor.Close(ctx)

	var documents []map[string]interface{}
	if err := cursor.All(ctx, &documents); err != nil {
		return nil, fmt.Errorf("failed to decode documents: %w", err)
	}

	return documents, nil
}

// NewDataReader creates a DataReader for MongoDB based on the configuration.
func NewDataReader(ctx context.Context, cfg *config.Config) (common.DataReader, error) {
	client, err := Connect(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create MongoDB reader: %w", err)
	}
	return newReader(client.Database(cfg.MongoDB).Collection(cfg.MongoCollection)), nil
}
