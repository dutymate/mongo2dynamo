package pool

import (
	"sync"
)

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
