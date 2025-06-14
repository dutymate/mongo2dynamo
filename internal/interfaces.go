package internal

import (
	"context"
)

// DataReader는 데이터를 읽는 인터페이스입니다.
type DataReader interface {
	Read(ctx context.Context) ([]map[string]interface{}, error)
}

// DataWriter는 데이터를 쓰는 인터페이스입니다.
type DataWriter interface {
	Write(ctx context.Context, data []map[string]interface{}) error
}

// MigrationService는 마이그레이션 서비스 인터페이스입니다.
type MigrationService interface {
	Run(ctx context.Context) error
}

// ConfigProvider는 설정을 제공하는 인터페이스입니다.
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
