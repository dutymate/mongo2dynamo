package retry

import (
	"context"
	"fmt"
	"time"
)

const (
	DefaultMaxRetries = 5
)

// RetryableFunc defines a function that can be retried and returns only an error.
type RetryableFunc func() error

// Config holds configuration for retry operations.
type Config struct {
	MaxRetries    int
	BackoffConfig *BackoffConfig
}

// NewConfig creates a new retry configuration with default values.
func NewConfig() *Config {
	return &Config{
		MaxRetries:    DefaultMaxRetries,
		BackoffConfig: NewBackoffConfig(),
	}
}

// WithMaxRetries sets the maximum number of retry attempts.
func (c *Config) WithMaxRetries(maxRetries int) *Config {
	c.MaxRetries = maxRetries
	return c
}

// DoWithConfig executes a function with retry logic using the provided configuration.
// This function is designed for operations that only need error handling.
func DoWithConfig(ctx context.Context, config *Config, fn RetryableFunc) error {
	for attempt := 0; attempt <= config.MaxRetries; attempt++ {
		// Check context cancellation before each attempt.
		select {
		case <-ctx.Done():
			return fmt.Errorf("context error: %w", ctx.Err())
		default:
		}

		if err := fn(); err == nil {
			return nil // Success.
		} else if attempt == config.MaxRetries {
			return err // Return the last error after exhausting retries.
		}

		// Calculate backoff delay and wait.
		delay := config.BackoffConfig.Calculate(attempt)

		select {
		case <-ctx.Done():
			return fmt.Errorf("context error: %w", ctx.Err())
		case <-time.After(delay):
			// Continue to next attempt.
		}
	}

	return nil
}
