package common

import (
	"context"
)

// DataReader defines the interface for reading data from a source.
type DataReader interface {
	// Read retrieves data from the source and processes it chunk by chunk using the provided callback.
	Read(ctx context.Context, handleChunk func([]map[string]interface{}) error) error
}

// DataWriter defines the interface for writing data to a destination.
type DataWriter interface {
	// Write saves the provided data to the destination.
	// Returns any error that occurred during the write operation.
	Write(ctx context.Context, data []map[string]interface{}) error
}

// MigrationService is the interface for migration services.
type MigrationService interface {
	Run(ctx context.Context) error
}

// ConfigProvider is the interface for providing configuration settings.
type ConfigProvider interface {
	GetMongoURI() string
	GetMongoDB() string
	GetMongoCollection() string
	GetDynamoTable() string
	GetAutoApprove() bool
}

type MongoStreamer interface {
	StreamAll(ctx context.Context) (<-chan map[string]interface{}, <-chan error)
}

type DynamoWriter interface {
	Write(item map[string]interface{}) error
	IncrementCount()
	GetCount() int
}
