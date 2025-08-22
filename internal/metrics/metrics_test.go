package metrics

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

func TestNewMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetricsWithRegistry(registry)

	assert.NotNil(t, metrics)
	assert.NotNil(t, metrics.migrationTotalDocuments)
	assert.NotNil(t, metrics.migrationProcessedDocuments)
	assert.NotNil(t, metrics.migrationFailedDocuments)
	assert.NotNil(t, metrics.migrationDuration)
	assert.NotNil(t, metrics.documentsPerSecond)
	assert.NotNil(t, metrics.extractionErrors)
	assert.NotNil(t, metrics.transformationErrors)
	assert.NotNil(t, metrics.loadingErrors)
	assert.NotNil(t, metrics.activeWorkers)
	assert.NotNil(t, metrics.queueSize)
}

func TestMetricsMethods(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetricsWithRegistry(registry)

	collection := "test_collection"
	database := "test_database"

	// Test SetTotalDocuments.
	metrics.SetTotalDocuments(collection, database, 100)

	// Test IncrementProcessedDocuments.
	metrics.IncrementProcessedDocuments(collection, database, 50)

	// Test IncrementFailedDocuments.
	metrics.IncrementFailedDocuments(collection, database, "test_error", 5)

	// Test RecordMigrationDuration.
	metrics.RecordMigrationDuration(collection, database, "success", 10*time.Second)

	// Test SetDocumentsPerSecond.
	metrics.SetDocumentsPerSecond(collection, database, 25.5)

	// Test error metrics.
	metrics.IncrementExtractionErrors(collection, database, "connection_error")
	metrics.IncrementTransformationErrors(collection, database, "parse_error")
	metrics.IncrementLoadingErrors(collection, database, "write_error")

	// Test system metrics.
	metrics.SetActiveWorkers("extractor", 3)
	metrics.SetQueueSize("transformer", 10)

	// Verify test completion.
	t.Log("All metrics methods executed successfully")
}

func TestMetricsHandler(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetricsWithRegistry(registry)

	// Set values for metrics.
	metrics.SetTotalDocuments("test_collection", "test_database", 100)
	metrics.IncrementProcessedDocuments("test_collection", "test_database", 50)

	handler := metrics.GetMetricsHandlerWithRegistry(registry)
	assert.NotNil(t, handler)

	// Test if HTTP handler works properly.
	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	assert.Contains(t, body, "mongo2dynamo_migration_total_documents")
	assert.Contains(t, body, "mongo2dynamo_migration_processed_documents")
}

func TestMetricsServer(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetricsWithRegistry(registry)

	// Start server.
	go func() {
		err := metrics.StartMetricsServer(context.Background(), ":0") // Port 0 automatically selects an available port.
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("Failed to start metrics server: %v", err)
		}
	}()

	// Wait for server to start.
	time.Sleep(100 * time.Millisecond)

	// Stop server.
	err := metrics.StopMetricsServer()
	assert.NoError(t, err)
}
