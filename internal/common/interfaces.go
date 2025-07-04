package common

import (
	"context"
)

// Extractor defines the interface for extracting data from a source.
type Extractor interface {
	// Extract retrieves data from the source and processes it chunk by chunk using the provided callback.
	Extract(ctx context.Context, handleChunk func([]map[string]interface{}) error) error
}

// Transformer defines the interface for transforming data between formats.
type Transformer interface {
	Transform([]map[string]interface{}) ([]map[string]interface{}, error)
}

// Loader defines the interface for loading data to a destination.
type Loader interface {
	// Load saves the provided data to the destination.
	// Returns any error that occurred during the load operation.
	Load(ctx context.Context, data []map[string]interface{}) error
}

// ConfigProvider is the interface for providing configuration settings.
type ConfigProvider interface {
	GetMongoURI() string
	GetMongoDB() string
	GetMongoCollection() string
	GetDynamoTable() string
	GetAutoApprove() bool
}
