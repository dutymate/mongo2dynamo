package extractor

import (
	"context"
	"encoding/json"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	goMongo "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"mongo2dynamo/internal/common"
	"mongo2dynamo/internal/mongo"
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

// MongoExtractor implements the Extractor interface for MongoDB.
type MongoExtractor struct {
	collection Collection
	batchSize  int // Number of documents to fetch from MongoDB per batch.
	chunkSize  int // Number of documents to pass to handleChunk per chunk.
	filter     primitive.M
	chunkPool  *common.ChunkPool
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

// newMongoExtractor creates a new MongoDB extractor with the specified collection, batchSize, and chunkSize.
func newMongoExtractor(collection Collection, filter primitive.M) *MongoExtractor {
	return &MongoExtractor{
		collection: collection,
		batchSize:  1000,
		chunkSize:  2000,
		filter:     filter,
		chunkPool:  common.NewChunkPool(2000), // Initialize chunk pool with a default size.
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
	var filter primitive.M
	if cfg.GetMongoFilter() != "" {
		filter, err = parseMongoFilter(cfg.GetMongoFilter())
		if err != nil {
			return nil, err
		}
	}

	return newMongoExtractor(&mongoCollectionWrapper{collection}, filter), nil
}

// parseMongoFilter parses a JSON string into a MongoDB BSON filter.
func parseMongoFilter(filterStr string) (primitive.M, error) {
	// Handle empty string case.
	if filterStr == "" {
		return primitive.M{}, nil
	}

	var filter map[string]interface{}
	if err := json.Unmarshal([]byte(filterStr), &filter); err != nil {
		return nil, &common.FilterParseError{
			Filter: filterStr,
			Op:     "json unmarshal",
			Reason: "invalid JSON syntax",
			Err:    err,
		}
	}

	// Convert to BSON document.
	bsonBytes, err := bson.Marshal(filter)
	if err != nil {
		return nil, &common.FilterParseError{
			Filter: filterStr,
			Op:     "bson marshal",
			Reason: "failed to convert JSON to BSON format",
			Err:    err,
		}
	}

	var bsonFilter primitive.M
	if err := bson.Unmarshal(bsonBytes, &bsonFilter); err != nil {
		return nil, &common.FilterParseError{
			Filter: filterStr,
			Op:     "bson unmarshal",
			Reason: "failed to convert BSON to MongoDB filter format",
			Err:    err,
		}
	}

	return bsonFilter, nil
}

// Extract retrieves documents from the MongoDB collection in fixed-size chunks and processes them using the provided callback.
func (e *MongoExtractor) Extract(ctx context.Context, handleChunk common.ChunkHandler) error {
	findOptions := options.Find().SetBatchSize(int32(e.batchSize))

	// Parse MongoDB filter if provided.
	filter := primitive.M{}
	if e.filter != nil {
		filter = e.filter
	}

	cursor, err := e.collection.Find(ctx, filter, findOptions)
	if err != nil {
		return &common.DatabaseOperationError{Database: "MongoDB", Op: "find", Reason: err.Error(), Err: err}
	}
	defer cursor.Close(ctx)

	chunkPtr := e.chunkPool.Get()
	defer e.chunkPool.Put(chunkPtr)

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
