package extractor

import (
	"context"
	"encoding/json"

	"go.mongodb.org/mongo-driver/bson"
	goMongo "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"mongo2dynamo/internal/common"
	"mongo2dynamo/internal/mongo"
	"mongo2dynamo/internal/pool"
)

const (
	DefaultMongoBatchSize = 1000
	DefaultChunkSize      = 2000
)

// Collection defines the interface for MongoDB collection operations needed by Extractor.
type Collection interface {
	Find(ctx context.Context, filter any, opts ...*options.FindOptions) (Cursor, error)
}

// Cursor defines the interface for MongoDB cursor operations needed by Extractor.
type Cursor interface {
	Next(ctx context.Context) bool
	Decode(val any) error
	Close(ctx context.Context) error
	Err() error
}

// MongoExtractor extracts documents from a MongoDB collection.
type MongoExtractor struct {
	collection Collection
	batchSize  int // Number of documents to fetch from MongoDB per batch.
	chunkSize  int // Number of documents to pass to handleChunk per chunk.
	filter     bson.M
	projection bson.M
	chunkPool  *pool.ChunkPool
}

// mongoCollectionWrapper wraps *mongo.Collection to implement Collection interface.
type mongoCollectionWrapper struct {
	Collection *goMongo.Collection
}

// mongoCursorWrapper wraps *mongo.Cursor to implement Cursor interface.
type mongoCursorWrapper struct {
	*goMongo.Cursor
}

// Find executes a MongoDB find operation on the wrapped collection.
func (w *mongoCollectionWrapper) Find(ctx context.Context, filter any, opts ...*options.FindOptions) (Cursor, error) {
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

// newMongoExtractor creates a new MongoDB extractor with default values.
func newMongoExtractor(collection Collection, filter bson.M) *MongoExtractor {
	return &MongoExtractor{
		collection: collection,
		batchSize:  DefaultMongoBatchSize,
		chunkSize:  DefaultChunkSize,
		filter:     filter,
		chunkPool:  pool.NewChunkPool(DefaultChunkSize),
	}
}

// NewMongoExtractor creates a MongoExtractor for MongoDB based on the configuration.
func NewMongoExtractor(ctx context.Context, cfg common.ConfigProvider) (common.Extractor, error) {
	client, err := mongo.Connect(ctx, cfg)
	if err != nil {
		return nil, &common.DatabaseConnectionError{Database: "MongoDB", Reason: err.Error(), Err: err}
	}
	collection := client.Database(cfg.GetMongoDB()).Collection(cfg.GetMongoCollection())

	// Parse MongoDB filter if provided.
	var filter bson.M
	if cfg.GetMongoFilter() != "" {
		filter, err = parseMongoJSON(cfg.GetMongoFilter())
		if err != nil {
			return nil, err
		}
	}

	// Parse MongoDB projection if provided.
	var projection bson.M
	if cfg.GetMongoProjection() != "" {
		projection, err = parseMongoJSON(cfg.GetMongoProjection())
		if err != nil {
			return nil, err
		}
	}

	extractor := newMongoExtractor(&mongoCollectionWrapper{collection}, filter)
	extractor.projection = projection
	return extractor, nil
}

// Count returns the total number of documents that match the filter.
func (e *MongoExtractor) Count(ctx context.Context) (int64, error) {
	// Use the underlying mongo.Collection to count documents.
	if wrapper, ok := e.collection.(*mongoCollectionWrapper); ok {
		count, err := wrapper.Collection.CountDocuments(ctx, e.filter)
		if err != nil {
			return 0, &common.DatabaseOperationError{Database: "MongoDB", Op: "count", Reason: err.Error(), Err: err}
		}
		return count, nil
	}
	return 0, &common.DatabaseOperationError{Database: "MongoDB", Op: "count", Reason: "unable to access underlying collection", Err: nil}
}

// parseMongoJSON parses a JSON string into a MongoDB BSON document (bson.M).
// This can be used for both filter and projection parsing.
func parseMongoJSON(jsonStr string) (bson.M, error) {
	if jsonStr == "" {
		return bson.M{}, nil
	}

	var doc bson.M
	// First, try to parse as extended JSON.
	err := bson.UnmarshalExtJSON([]byte(jsonStr), false, &doc)
	if err == nil {
		return doc, nil
	}

	// If ExtJSON fails, fall back to standard JSON.
	err = json.Unmarshal([]byte(jsonStr), &doc)
	if err == nil {
		return doc, nil
	}

	// If both fail, return a comprehensive error.
	return nil, &common.ParseError{
		JSONString: jsonStr,
		Op:         "bson unmarshalextjson / json unmarshal",
		Reason:     "failed to parse as both extended and standard JSON",
		Err:        err,
	}
}

// Extract retrieves documents from the MongoDB collection in fixed-size chunks and processes them using the provided callback.
func (e *MongoExtractor) Extract(ctx context.Context, handleChunk common.ChunkHandler) error {
	findOptions := options.Find().SetBatchSize(int32(e.batchSize))

	// Parse MongoDB filter if provided.
	filter := bson.M{}
	if e.filter != nil {
		filter = e.filter
	}

	// Set projection if specified.
	if len(e.projection) > 0 {
		findOptions.SetProjection(e.projection)
	}

	cursor, err := e.collection.Find(ctx, filter, findOptions)
	if err != nil {
		return &common.DatabaseOperationError{Database: "MongoDB", Op: "find", Reason: err.Error(), Err: err}
	}
	defer cursor.Close(ctx)

	chunkPtr := e.chunkPool.Get()
	defer e.chunkPool.Put(chunkPtr)

	for cursor.Next(ctx) {
		var doc map[string]any
		if err := cursor.Decode(&doc); err != nil {
			return &common.DataValidationError{
				Database: "MongoDB",
				Op:       "decode",
				Reason:   err.Error(),
				Err:      err,
			}
		}
		*chunkPtr = append(*chunkPtr, doc)
		if len(*chunkPtr) >= e.chunkSize {
			if err := handleChunk(*chunkPtr); err != nil {
				return &common.ChunkCallbackError{Reason: "handleChunk failed", Err: err}
			}
			// Clear the slice and reuse the pointer.
			*chunkPtr = (*chunkPtr)[:0]
		}
	}

	if len(*chunkPtr) > 0 {
		if err := handleChunk(*chunkPtr); err != nil {
			return &common.ChunkCallbackError{Reason: "handleChunk failed", Err: err}
		}
	}

	// Only check cursor error if all chunks processed successfully.
	if err := cursor.Err(); err != nil {
		return &common.DatabaseOperationError{Database: "MongoDB", Op: "cursor", Reason: err.Error(), Err: err}
	}

	return nil
}
