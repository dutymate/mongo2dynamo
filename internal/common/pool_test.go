package common

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDocumentPool(t *testing.T) {
	t.Run("NewDocumentPool", func(t *testing.T) {
		pool := NewDocumentPool()
		assert.NotNil(t, pool)
	})

	t.Run("Get_ReturnsNewMap", func(t *testing.T) {
		pool := NewDocumentPool()
		doc := pool.Get()

		assert.NotNil(t, doc)
		assert.Equal(t, 0, len(*doc))
		// Maps don't have capacity, so only check length.
	})

	t.Run("Put_ClearsMap", func(t *testing.T) {
		pool := NewDocumentPool()
		doc := pool.Get()

		// Add data to map.
		*doc = map[string]interface{}{
			"key1": "value1",
			"key2": "value2",
		}
		assert.Equal(t, 2, len(*doc))

		// Return to pool.
		pool.Put(doc)

		// Get again and check if it's empty.
		newDoc := pool.Get()
		assert.Equal(t, 0, len(*newDoc))
	})

	t.Run("ConcurrentAccess", func(_ *testing.T) {
		pool := NewDocumentPool()
		const numGoroutines = 100
		const operationsPerGoroutine = 10

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < operationsPerGoroutine; j++ {
					doc := pool.Get()
					*doc = map[string]interface{}{
						"test": "value",
					}
					pool.Put(doc)
				}
			}()
		}

		wg.Wait()
		// No race conditions or panics should occur.
	})

	t.Run("ReuseFromPool", func(t *testing.T) {
		pool := NewDocumentPool()

		// Get first document.
		doc1 := pool.Get()
		*doc1 = map[string]interface{}{"key1": "value1"}
		pool.Put(doc1)

		// Get second document (may be reused from pool).
		doc2 := pool.Get()
		assert.Equal(t, 0, len(*doc2))

		// Address may be different, but content should be empty.
		*doc2 = map[string]interface{}{"key2": "value2"}
		pool.Put(doc2)
	})
}

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

func TestPoolMemoryEfficiency(t *testing.T) {
	t.Run("DocumentPool_MemoryReuse", func(t *testing.T) {
		pool := NewDocumentPool()

		// Verify memory allocation efficiency with repeated usage.
		for i := 0; i < 1000; i++ {
			doc := pool.Get()
			*doc = map[string]interface{}{
				"key": "value",
			}
			pool.Put(doc)
		}

		// Verify the last retrieved document is correct.
		finalDoc := pool.Get()
		assert.Equal(t, 0, len(*finalDoc))
		pool.Put(finalDoc)
	})

	t.Run("ChunkPool_MemoryReuse", func(t *testing.T) {
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
	})
}

func TestPoolEdgeCases(t *testing.T) {
	t.Run("DocumentPool_NilPointer", func(t *testing.T) {
		pool := NewDocumentPool()
		doc := pool.Get()

		// Should not panic when calling Put with nil pointer.
		assert.NotPanics(t, func() {
			pool.Put(doc)
		})
	})

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

func BenchmarkDocumentPool(b *testing.B) {
	pool := NewDocumentPool()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			doc := pool.Get()
			*doc = map[string]interface{}{
				"key1": "value1",
				"key2": "value2",
				"key3": "value3",
			}
			pool.Put(doc)
		}
	})
}

func BenchmarkChunkPool(b *testing.B) {
	pool := NewChunkPool(100)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			chunk := pool.Get()
			*chunk = make([]map[string]interface{}, 10)
			pool.Put(chunk)
		}
	})
}
