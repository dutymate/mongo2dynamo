package pool

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChunkPool(t *testing.T) {
	t.Run("NewChunkPool", func(t *testing.T) {
		chunkSize := 100
		pool := NewChunkPool(chunkSize)

		assert.NotNil(t, pool)
		assert.Equal(t, chunkSize, pool.size)
	})

	t.Run("Get_ReturnsNewChunk", func(t *testing.T) {
		chunkSize := 50
		pool := NewChunkPool(chunkSize)
		chunk := pool.Get()

		assert.NotNil(t, chunk)
		assert.Equal(t, 0, len(*chunk))
		assert.Equal(t, chunkSize, cap(*chunk))
	})

	t.Run("Put_ClearsChunk", func(t *testing.T) {
		pool := NewChunkPool(10)
		chunk := pool.Get()

		// Add data to chunk.
		*chunk = []map[string]interface{}{
			{"key1": "value1"},
			{"key2": "value2"},
		}
		assert.Equal(t, 2, len(*chunk))

		// Return to pool.
		pool.Put(chunk)

		// Get again and check if it's empty.
		newChunk := pool.Get()
		assert.Equal(t, 0, len(*newChunk))
		// Capacity may differ from original due to pool reuse.
	})

	t.Run("ConcurrentAccess", func(_ *testing.T) {
		pool := NewChunkPool(20)
		const numGoroutines = 50
		const operationsPerGoroutine = 5

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < operationsPerGoroutine; j++ {
					chunk := pool.Get()
					*chunk = []map[string]interface{}{
						{"test": "value"},
					}
					pool.Put(chunk)
				}
			}()
		}

		wg.Wait()
	})

	t.Run("ChunkSizeVariations", func(t *testing.T) {
		testCases := []int{1, 10, 100, 1000}

		for _, size := range testCases {
			t.Run(fmt.Sprintf("Size_%d", size), func(t *testing.T) {
				pool := NewChunkPool(size)
				chunk := pool.Get()

				assert.Equal(t, 0, len(*chunk))
				assert.Equal(t, size, cap(*chunk))

				pool.Put(chunk)
			})
		}
	})
}

func TestChunkPoolMemoryEfficiency(t *testing.T) {
	pool := NewChunkPool(100)

	for i := 0; i < 500; i++ {
		chunk := pool.Get()
		*chunk = make([]map[string]interface{}, i%10)
		pool.Put(chunk)
	}

	finalChunk := pool.Get()
	assert.Equal(t, 0, len(*finalChunk))
	// Capacity may differ from original due to pool reuse.
	pool.Put(finalChunk)
}

func TestChunkPoolEdgeCases(t *testing.T) {
	t.Run("ChunkPool_ZeroSize", func(t *testing.T) {
		pool := NewChunkPool(0)
		chunk := pool.Get()

		assert.Equal(t, 0, len(*chunk))
		assert.Equal(t, 0, cap(*chunk))

		pool.Put(chunk)
	})

	t.Run("ChunkPool_NegativeSize", func(t *testing.T) {
		// Negative size should cause panic, so verify it panics.
		assert.Panics(t, func() {
			pool := NewChunkPool(-1)
			chunk := pool.Get()
			pool.Put(chunk)
		})
	})
}
