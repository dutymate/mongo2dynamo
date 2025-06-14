package mongo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
)

// Reader is a MongoDB data reader.
type Reader struct {
	collection *mongo.Collection
}

// NewReader creates a new MongoDB reader.
func NewReader(collection *mongo.Collection) *Reader {
	return &Reader{
		collection: collection,
	}
}

// Read reads all documents from the MongoDB collection.
func (r *Reader) Read(ctx context.Context) ([]map[string]interface{}, error) {
	cursor, err := r.collection.Find(ctx, map[string]interface{}{})
	if err != nil {
		return nil, fmt.Errorf("failed to find documents: %w", err)
	}
	defer cursor.Close(ctx)

	var results []map[string]interface{}
	if err := cursor.All(ctx, &results); err != nil {
		return nil, fmt.Errorf("failed to decode documents: %w", err)
	}

	return results, nil
}
