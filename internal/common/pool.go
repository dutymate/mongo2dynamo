package common

import (
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// DocumentPool provides a pool of reusable document maps to reduce memory allocations.
type DocumentPool struct {
	pool sync.Pool
}

// NewDocumentPool creates a new document pool with optimized initial capacity.
func NewDocumentPool() *DocumentPool {
	return &DocumentPool{
		pool: sync.Pool{
			New: func() interface{} {
				m := make(map[string]interface{}, 10)
				return &m
			},
		},
	}
}

// Get retrieves a document map pointer from the pool or creates a new one.
func (p *DocumentPool) Get() *map[string]interface{} {
	return p.pool.Get().(*map[string]interface{})
}

// Put returns a document map pointer to the pool after clearing its contents.
func (p *DocumentPool) Put(doc *map[string]interface{}) {
	for k := range *doc {
		delete(*doc, k)
	}
	p.pool.Put(doc)
}

// ChunkPool provides a pool of reusable document chunks to reduce slice allocations.
type ChunkPool struct {
	pool sync.Pool
	size int
}

// NewChunkPool creates a new chunk pool with the specified chunk size.
func NewChunkPool(chunkSize int) *ChunkPool {
	return &ChunkPool{
		pool: sync.Pool{
			New: func() interface{} {
				c := make([]map[string]interface{}, 0, chunkSize)
				return &c
			},
		},
		size: chunkSize,
	}
}

// Get retrieves a document chunk pointer from the pool or creates a new one.
func (p *ChunkPool) Get() *[]map[string]interface{} {
	return p.pool.Get().(*[]map[string]interface{})
}

// Put returns a document chunk pointer to the pool after clearing its contents.
func (p *ChunkPool) Put(chunk *[]map[string]interface{}) {
	*chunk = (*chunk)[:0]
	p.pool.Put(chunk)
}

// WriteRequestPool provides a pool of reusable DynamoDB write requests.
type WriteRequestPool struct {
	pool sync.Pool
}

// NewWriteRequestPool creates a new write request pool.
func NewWriteRequestPool() *WriteRequestPool {
	return &WriteRequestPool{
		pool: sync.Pool{
			New: func() interface{} {
				w := make([]types.WriteRequest, 0, 25) // DynamoDB batch size.
				return &w
			},
		},
	}
}

// Get retrieves a write request slice pointer from the pool or creates a new one.
func (p *WriteRequestPool) Get() *[]types.WriteRequest {
	return p.pool.Get().(*[]types.WriteRequest)
}

// Put returns a write request slice pointer to the pool after clearing its contents.
func (p *WriteRequestPool) Put(requests *[]types.WriteRequest) {
	*requests = (*requests)[:0]
	p.pool.Put(requests)
}
