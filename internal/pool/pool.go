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
			New: func() any {
				c := make([]map[string]any, 0, chunkSize)
				return &c
			},
		},
		size: chunkSize,
	}
}

// Get retrieves a document chunk pointer from the pool or creates a new one.
func (p *ChunkPool) Get() *[]map[string]any {
	return p.pool.Get().(*[]map[string]any)
}

// Put returns a document chunk pointer to the pool after clearing its contents.
func (p *ChunkPool) Put(chunk *[]map[string]any) {
	*chunk = (*chunk)[:0]
	p.pool.Put(chunk)
}
