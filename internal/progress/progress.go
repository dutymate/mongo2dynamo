package progress

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"mongo2dynamo/internal/common"
)

const (
	DefaultUpdateInterval = 1 * time.Second
)

// Status contains information about the migration progress.
type Status struct {
	Processed  int64
	Total      int64
	Percentage float64
	Rate       float64
	ETA        time.Duration
	Elapsed    time.Duration
}

// Tracker tracks migration progress.
type Tracker struct {
	totalItems     int64
	processedItems int64
	startTime      time.Time
	updateInterval time.Duration
	mu             sync.RWMutex

	// Rate calculation.
	lastProcessed int64
	lastRateTime  time.Time
	currentRate   float64

	// Display control.
	stopChan chan struct{}
}

// NewProgressTracker creates a new progress tracker.
func NewProgressTracker(totalItems int64, updateInterval time.Duration) *Tracker {
	if updateInterval <= 0 {
		updateInterval = DefaultUpdateInterval
	}

	return &Tracker{
		totalItems:     totalItems,
		updateInterval: updateInterval,
		startTime:      time.Now(),
		lastRateTime:   time.Now(),
		stopChan:       make(chan struct{}),
	}
}

// Start begins the progress tracking and display.
func (pt *Tracker) Start(ctx context.Context) {
	go pt.displayProgress(ctx)
}

// Stop stops the progress tracking.
func (pt *Tracker) Stop() {
	close(pt.stopChan)
}

// ClearProgress clears the current progress line from the terminal.
func (pt *Tracker) ClearProgress() {
	// Clear the current line.
	fmt.Print("\r\033[K")
}

// UpdateProgress updates the progress with the number of processed items.
func (pt *Tracker) UpdateProgress(processed int64) {
	atomic.AddInt64(&pt.processedItems, processed)
}

// GetProgressStatus returns the current progress status.
func (pt *Tracker) GetProgressStatus() Status {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	processed := atomic.LoadInt64(&pt.processedItems)
	elapsed := time.Since(pt.startTime)

	var percentage float64
	if pt.totalItems > 0 {
		percentage = float64(processed) / float64(pt.totalItems) * 100
	}

	var eta time.Duration
	if pt.currentRate > 0 {
		remaining := pt.totalItems - processed
		if remaining > 0 {
			eta = time.Duration(float64(remaining)/pt.currentRate) * time.Second
		}
	}

	return Status{
		Processed:  processed,
		Total:      pt.totalItems,
		Percentage: percentage,
		Rate:       pt.currentRate,
		ETA:        eta,
		Elapsed:    elapsed,
	}
}

// displayProgress displays progress updates at regular intervals.
func (pt *Tracker) displayProgress(ctx context.Context) {
	ticker := time.NewTicker(pt.updateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-pt.stopChan:
			return
		case <-ticker.C:
			pt.updateRate()
			pt.displayProgressLine()
		}
	}
}

// updateRate calculates the current processing rate.
func (pt *Tracker) updateRate() {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	now := time.Now()
	currentProcessed := atomic.LoadInt64(&pt.processedItems)

	if !pt.lastRateTime.IsZero() {
		timeDiff := now.Sub(pt.lastRateTime).Seconds()
		if timeDiff > 0 {
			itemsDiff := currentProcessed - pt.lastProcessed
			pt.currentRate = float64(itemsDiff) / timeDiff
		}
	}

	pt.lastProcessed = currentProcessed
	pt.lastRateTime = now
}

// displayProgressLine displays a single line with progress information.
func (pt *Tracker) displayProgressLine() {
	status := pt.GetProgressStatus()

	if status.Percentage >= 100.0 {
		status.Rate = float64(status.Total) / status.Elapsed.Seconds()
	}

	// Clear the current line.
	fmt.Print("\r\033[K")

	// Format the progress line.
	line := fmt.Sprintf(
		"▶ %s/%s items (%.1f%%) | %s items/sec | %s left",
		common.FormatNumber(int(status.Processed)),
		common.FormatNumber(int(status.Total)),
		status.Percentage,
		common.FormatNumber(int(status.Rate)),
		common.FormatDuration(status.ETA),
	)

	fmt.Print(line)
}
