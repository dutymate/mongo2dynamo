package common

import (
	"context"
)

// ChunkHandler defines the callback type for processing a chunk of documents.
type ChunkHandler func([]map[string]any) error

// Extractor defines the interface for extracting data from a source.
type Extractor interface {
	// Extract retrieves data from the source and processes it chunk by chunk using the provided callback.
	Extract(ctx context.Context, handleChunk ChunkHandler) error
	// Count returns the total number of documents that match the filter.
	Count(ctx context.Context) (int64, error)
}

// Transformer defines the interface for transforming data between formats.
type Transformer interface {
	// Transform transforms the provided documents into a new format.
	Transform(ctx context.Context, data []map[string]any) ([]map[string]any, error)
}

// Loader defines the interface for loading data to a destination.
type Loader interface {
	// Load saves the provided data to the destination.
	Load(ctx context.Context, data []map[string]any) error
}

// ConfigProvider is the interface for providing configuration settings.
type ConfigProvider interface {
	GetMongoHost() string
	GetMongoPort() string
	GetMongoUser() string
	GetMongoPassword() string
	GetMongoDB() string
	GetMongoCollection() string
	GetMongoFilter() string
	GetMongoProjection() string
	GetMongoURI() string
	GetDynamoEndpoint() string
	GetDynamoTable() string
	GetDynamoPartitionKey() string
	GetDynamoPartitionKeyType() string
	GetDynamoSortKey() string
	GetDynamoSortKeyType() string
	GetAWSRegion() string
	GetMaxRetries() int
	GetAutoApprove() bool
	GetDryRun() bool
}
