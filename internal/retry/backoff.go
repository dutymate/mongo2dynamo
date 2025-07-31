package retry

import (
	"math/rand"
	"time"
)

const (
	DefaultBaseDelay = 100 * time.Millisecond
	DefaultMaxDelay  = 30 * time.Second
	DefaultJitterMin = 0.5
	DefaultJitterMax = 1.5
)

// BackoffConfig holds configuration for exponential backoff with jitter.
type BackoffConfig struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
	JitterMin float64
	JitterMax float64
}

// NewBackoffConfig creates a new backoff configuration with default values.
func NewBackoffConfig() *BackoffConfig {
	return &BackoffConfig{
		BaseDelay: DefaultBaseDelay,
		MaxDelay:  DefaultMaxDelay,
		JitterMin: DefaultJitterMin,
		JitterMax: DefaultJitterMax,
	}
}

// Calculate calculates exponential backoff with jitter to prevent thundering herd.
func (c *BackoffConfig) Calculate(attempt int) time.Duration {
	exponentialDelay := min(time.Duration(1<<attempt)*c.BaseDelay, c.MaxDelay)

	jitterRange := c.JitterMax - c.JitterMin
	jitter := c.JitterMin + rand.Float64()*jitterRange
	jitteredDelay := min(time.Duration(float64(exponentialDelay)*jitter), c.MaxDelay)

	return jitteredDelay
}
