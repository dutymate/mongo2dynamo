package retry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDoWithConfig_Success(t *testing.T) {
	ctx := context.Background()
	config := NewConfig()

	err := DoWithConfig(ctx, config, func() error {
		return nil // Success on first attempt.
	})

	require.NoError(t, err)
}

func TestDoWithConfig_RetryAndSuccess(t *testing.T) {
	ctx := context.Background()
	config := NewConfig().WithMaxRetries(2)
	attempts := 0

	err := DoWithConfig(ctx, config, func() error {
		attempts++
		if attempts < 3 {
			return errors.New("temporary error")
		}
		return nil // Success on third attempt.
	})

	require.NoError(t, err)
	assert.Equal(t, 3, attempts)
}

func TestDoWithConfig_MaxRetriesExceeded(t *testing.T) {
	ctx := context.Background()
	config := NewConfig().WithMaxRetries(2)
	attempts := 0

	err := DoWithConfig(ctx, config, func() error {
		attempts++
		return errors.New("persistent error")
	})

	require.Error(t, err)
	assert.Equal(t, 3, attempts) // Initial attempt + 2 retries.
	assert.Contains(t, err.Error(), "persistent error")
}

func TestDoWithConfig_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	config := NewConfig()
	config.BackoffConfig.BaseDelay = 100 * time.Millisecond

	attempts := 0
	err := DoWithConfig(ctx, config, func() error {
		attempts++
		return errors.New("error")
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.LessOrEqual(t, attempts, 2) // Should not complete many attempts due to timeout.
}
