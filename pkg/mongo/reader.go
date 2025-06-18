package mongo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// Reader implements the DataReader interface for MongoDB.
type Reader struct {
	collection *mongo.Collection
}

// NewReader creates a new MongoDB reader.
func NewReader(collection *mongo.Collection) *Reader {
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
