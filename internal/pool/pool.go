package pool

import (
	"sync"
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
