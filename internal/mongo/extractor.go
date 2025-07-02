package mongo

import (
	"context"
	"mongo2dynamo/internal/common"
	"mongo2dynamo/internal/config"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Collection defines the interface for MongoDB collection operations needed by Extractor.
type Collection interface {
	Find(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (Cursor, error)
}

// Cursor defines the interface for MongoDB cursor operations needed by Extractor.
type Cursor interface {
	Next(ctx context.Context) bool
	Decode(val interface{}) error
	Close(ctx context.Context) error
	Err() error
}

// Extractor implements the DataExtractor interface for MongoDB.
type Extractor struct {
	collection Collection
	batchSize  int // Number of documents to fetch from MongoDB per batch.
	chunkSize  int // Number of documents to pass to handleChunk per chunk.
}

// newExtractor creates a new MongoDB extractor with the specified collection, batchSize, and chunkSize.
func newExtractor(collection Collection) *Extractor {
	return &Extractor{
		collection: collection,
		batchSize:  500,
		chunkSize:  1000,
	}
}

// Extract retrieves documents from the MongoDB collection in fixed-size chunks and processes them using the provided callback.
func (e *Extractor) Extract(ctx context.Context, handleChunk func([]map[string]interface{}) error) error {
	findOptions := options.Find().SetBatchSize(int32(e.batchSize))
	cursor, err := e.collection.Find(ctx, bson.M{}, findOptions)
	if err != nil {
		return &common.DatabaseOperationError{Database: "MongoDB", Op: "find", Reason: err.Error(), Err: err}
	}
	defer cursor.Close(ctx)

	chunk := make([]map[string]interface{}, 0, e.chunkSize)
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
		if len(chunk) >= e.chunkSize {
			if err := handleChunk(chunk); err != nil {
				return &common.ChunkCallbackError{Reason: "handleChunk failed", Err: err}
			}
			chunk = make([]map[string]interface{}, 0, e.chunkSize)
		}
	}

	if len(chunk) > 0 {
		if err := handleChunk(chunk); err != nil {
			return &common.ChunkCallbackError{Reason: "handleChunk failed", Err: err}
		}
	}

	// Only check cursor error if all chunks processed successfully.
	if err := cursor.Err(); err != nil {
		return &common.DatabaseOperationError{Database: "MongoDB", Op: "cursor", Reason: err.Error(), Err: err}
	}

	return nil
}

// NewDataExtractor creates a DataExtractor for MongoDB based on the configuration.
func NewDataExtractor(ctx context.Context, cfg *config.Config) (common.DataExtractor, error) {
	client, err := Connect(ctx, cfg)
	if err != nil {
		return nil, err
	}
	collection := client.Database(cfg.MongoDB).Collection(cfg.MongoCollection)
	return newExtractor(&mongoCollectionWrapper{collection}), nil
}

// mongoCollectionWrapper wraps *mongo.Collection to implement Collection interface.
type mongoCollectionWrapper struct {
	Collection *mongo.Collection
}

func (w *mongoCollectionWrapper) Find(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (Cursor, error) {
	cursor, err := w.Collection.Find(ctx, filter, opts...)
	if err != nil {
		return nil, &common.DatabaseOperationError{
			Database: "MongoDB",
			Op:       "find",
			Reason:   err.Error(),
			Err:      err,
		}
	}
	return &mongoCursorWrapper{cursor}, nil
}

// mongoCursorWrapper wraps *mongo.Cursor to implement Cursor interface.
type mongoCursorWrapper struct {
	*mongo.Cursor
}
