package mongo

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
)

type Reader struct {
	collection *mongo.Collection
}

func NewReader(collection *mongo.Collection) *Reader {
	return &Reader{
		collection: collection,
	}
}

func (r *Reader) Read(ctx context.Context) ([]map[string]interface{}, error) {
	cursor, err := r.collection.Find(ctx, map[string]interface{}{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var results []map[string]interface{}
	if err := cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	return results, nil
}
