package metrics

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	namespace = "mongo2dynamo"
	subsystem = "migration"
)

// Metrics struct manages all Prometheus metrics.
type Metrics struct {
	// Migration progress metrics.
	migrationTotalDocuments     *prometheus.CounterVec
	migrationProcessedDocuments *prometheus.CounterVec
	migrationFailedDocuments    *prometheus.CounterVec
	migrationDuration           *prometheus.HistogramVec

	// Processing rate metrics.
	documentsPerSecond *prometheus.GaugeVec

	// Error metrics.
	extractionErrors     *prometheus.CounterVec
	transformationErrors *prometheus.CounterVec
	loadingErrors        *prometheus.CounterVec

	// System metrics.
	activeWorkers *prometheus.GaugeVec
	queueSize     *prometheus.GaugeVec

	// Backpressure metrics.
	backpressureHits  *prometheus.CounterVec
	flowControlEvents *prometheus.CounterVec

	// HTTP server.
	server *http.Server
}

// NewMetrics creates a new Metrics instance.
func NewMetrics() *Metrics {
	m := &Metrics{}

	m.initMetrics()
	return m
}

// NewMetricsWithRegistry creates a new Metrics instance (for testing).
func NewMetricsWithRegistry(registry *prometheus.Registry) *Metrics {
	m := &Metrics{}

	m.initMetricsWithRegistry(registry)
	return m
}

// initMetrics initializes all Prometheus metrics.
func (m *Metrics) initMetrics() {
	m.initMetricsWithRegistry(prometheus.DefaultRegisterer)
}

// initMetricsWithRegistry initializes metrics in the specified registry.
func (m *Metrics) initMetricsWithRegistry(registry prometheus.Registerer) {
	// Migration progress metrics.
	m.migrationTotalDocuments = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "total_documents",
			Help:      "Total number of documents to migrate",
		},
		[]string{"collection", "database"},
	)

	m.migrationProcessedDocuments = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "processed_documents",
			Help:      "Number of successfully processed documents",
		},
		[]string{"collection", "database"},
	)

	m.migrationFailedDocuments = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "failed_documents",
			Help:      "Number of documents that failed to process",
		},
		[]string{"collection", "database", "error_type"},
	)

	m.migrationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "duration_seconds",
			Help:      "Time taken to complete migration (seconds)",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"collection", "database", "status"},
	)

	// Processing rate metrics.
	m.documentsPerSecond = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "documents_per_second",
			Help:      "Number of documents processed per second",
		},
		[]string{"collection", "database"},
	)

	// Error metrics.
	m.extractionErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "extraction_errors_total",
			Help:      "Total number of errors during data extraction from MongoDB",
		},
		[]string{"collection", "database", "error_type"},
	)

	m.transformationErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "transformation_errors_total",
			Help:      "Total number of errors during document transformation",
		},
		[]string{"collection", "database", "error_type"},
	)

	m.loadingErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "loading_errors_total",
			Help:      "Total number of errors during data loading to DynamoDB",
		},
		[]string{"collection", "database", "error_type"},
	)

	// System metrics.
	m.activeWorkers = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "active_workers",
			Help:      "Current number of active workers",
		},
		[]string{"stage"},
	)

	m.queueSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "queue_size",
			Help:      "Current number of documents waiting in queue",
		},
		[]string{"stage"},
	)

	// Backpressure metrics.
	m.backpressureHits = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "backpressure_hits_total",
			Help:      "Total number of backpressure events triggered",
		},
		[]string{"stage"},
	)

	m.flowControlEvents = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "flow_control_events_total",
			Help:      "Total number of flow control events",
		},
		[]string{"stage"},
	)

	// Register all metrics in the specified registry.
	registry.MustRegister(
		m.migrationTotalDocuments,
		m.migrationProcessedDocuments,
		m.migrationFailedDocuments,
		m.migrationDuration,
		m.documentsPerSecond,
		m.extractionErrors,
		m.transformationErrors,
		m.loadingErrors,
		m.activeWorkers,
		m.queueSize,
		m.backpressureHits,
		m.flowControlEvents,
	)
}

// StartMetricsServer starts an HTTP server to expose metrics.
func (m *Metrics) StartMetricsServer(ctx context.Context, addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	m.server = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	fmt.Printf("Starting metrics server: %s\n", addr)

	// Start server in a goroutine.
	errChan := make(chan error, 1)
	go func() {
		if err := m.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- fmt.Errorf("failed to start metrics server: %w", err)
		}
	}()

	// Wait for context cancellation or server error.
	select {
	case <-ctx.Done():
		// Context cancelled, shutdown server gracefully.
		shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		if err := m.server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("failed to shutdown metrics server: %w", err)
		}
		return nil
	case err := <-errChan:
		return err
	}
}

// StopMetricsServer stops the metrics server.
func (m *Metrics) StopMetricsServer() error {
	if m.server != nil {
		if err := m.server.Close(); err != nil {
			return fmt.Errorf("failed to stop metrics server: %w", err)
		}
	}
	return nil
}

// SetTotalDocuments sets the total number of documents to migrate.
func (m *Metrics) SetTotalDocuments(collection, database string, count int64) {
	m.migrationTotalDocuments.WithLabelValues(collection, database).Add(float64(count))
}

// IncrementProcessedDocuments increments the number of successfully processed documents.
func (m *Metrics) IncrementProcessedDocuments(collection, database string, count int64) {
	m.migrationProcessedDocuments.WithLabelValues(collection, database).Add(float64(count))
}

// IncrementFailedDocuments increments the number of documents that failed to process.
func (m *Metrics) IncrementFailedDocuments(collection, database, errorType string, count int64) {
	m.migrationFailedDocuments.WithLabelValues(collection, database, errorType).Add(float64(count))
}

// RecordMigrationDuration records the time taken to complete migration.
func (m *Metrics) RecordMigrationDuration(collection, database, status string, duration time.Duration) {
	m.migrationDuration.WithLabelValues(collection, database, status).Observe(duration.Seconds())
}

// SetDocumentsPerSecond sets the number of documents processed per second.
func (m *Metrics) SetDocumentsPerSecond(collection, database string, rate float64) {
	m.documentsPerSecond.WithLabelValues(collection, database).Set(rate)
}

// IncrementExtractionErrors increments the number of extraction errors.
func (m *Metrics) IncrementExtractionErrors(collection, database, errorType string) {
	m.extractionErrors.WithLabelValues(collection, database, errorType).Inc()
}

// IncrementTransformationErrors increments the number of transformation errors.
func (m *Metrics) IncrementTransformationErrors(collection, database, errorType string) {
	m.transformationErrors.WithLabelValues(collection, database, errorType).Inc()
}

// IncrementLoadingErrors increments the number of loading errors.
func (m *Metrics) IncrementLoadingErrors(collection, database, errorType string) {
	m.loadingErrors.WithLabelValues(collection, database, errorType).Inc()
}

// SetActiveWorkers sets the current number of active workers.
func (m *Metrics) SetActiveWorkers(stage string, count int) {
	m.activeWorkers.WithLabelValues(stage).Set(float64(count))
}

// SetQueueSize sets the current number of documents waiting in queue.
func (m *Metrics) SetQueueSize(stage string, size int) {
	m.queueSize.WithLabelValues(stage).Set(float64(size))
}

// IncrementBackpressureHits increments the number of backpressure events.
func (m *Metrics) IncrementBackpressureHits(stage string) {
	m.backpressureHits.WithLabelValues(stage).Inc()
}

// IncrementFlowControlEvents increments the number of flow control events.
func (m *Metrics) IncrementFlowControlEvents(stage string) {
	m.flowControlEvents.WithLabelValues(stage).Inc()
}

// GetMetricsHandler returns the metrics HTTP handler.
func (m *Metrics) GetMetricsHandler() http.Handler {
	return promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{})
}

// GetMetricsHandlerWithRegistry returns a metrics HTTP handler using the specified registry.
func (m *Metrics) GetMetricsHandlerWithRegistry(registry prometheus.Gatherer) http.Handler {
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
}
