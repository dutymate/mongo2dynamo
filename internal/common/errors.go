package common

import "fmt"

// ConfigError is returned for general configuration loading and validation errors.
type ConfigError struct {
	Op     string
	Reason string
	Err    error
}

func (e *ConfigError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("configuration error during '%s': %s: %v", e.Op, e.Reason, e.Err)
	}
	return fmt.Sprintf("configuration error during '%s': %s", e.Op, e.Reason)
}

func (e *ConfigError) Unwrap() error {
	return e.Err
}

// DataValidationError is returned for data validation errors not related to config.
type DataValidationError struct {
	Database string
	Op       string
	Reason   string
	Err      error
}

func (e *DataValidationError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("data validation error on '%s' (%s): %s: %v", e.Database, e.Op, e.Reason, e.Err)
	}
	return fmt.Sprintf("data validation error on '%s' (%s): %s", e.Database, e.Op, e.Reason)
}

func (e *DataValidationError) Unwrap() error {
	return e.Err
}

// DatabaseConnectionError is returned when a connection to a database fails.
type DatabaseConnectionError struct {
	Database string
	Reason   string
	Err      error
}

func (e *DatabaseConnectionError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("failed to connect to database '%s': %s: %v", e.Database, e.Reason, e.Err)
	}
	return fmt.Sprintf("failed to connect to database '%s': %s", e.Database, e.Reason)
}

func (e *DatabaseConnectionError) Unwrap() error {
	return e.Err
}

// DatabaseOperationError is returned for database operation (read/write/update/delete) errors.
type DatabaseOperationError struct {
	Database string
	Op       string
	Reason   string
	Err      error
}

func (e *DatabaseOperationError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("database operation error on '%s' (%s): %s: %v", e.Database, e.Op, e.Reason, e.Err)
	}
	return fmt.Sprintf("database operation error on '%s' (%s): %s", e.Database, e.Op, e.Reason)
}

func (e *DatabaseOperationError) Unwrap() error {
	return e.Err
}

// FileIOError is returned for file I/O related errors.
type FileIOError struct {
	Op     string
	Reason string
	Err    error
}

func (e *FileIOError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("file I/O error during '%s': %s: %v", e.Op, e.Reason, e.Err)
	}
	return fmt.Sprintf("file I/O error during '%s': %s", e.Op, e.Reason)
}

func (e *FileIOError) Unwrap() error {
	return e.Err
}

// AuthError is returned for authentication/authorization errors.
type AuthError struct {
	Database string
	Op       string
	Reason   string
	Err      error
}

func (e *AuthError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("authentication/authorization error on '%s' (%s): %s: %v", e.Database, e.Op, e.Reason, e.Err)
	}
	return fmt.Sprintf("authentication/authorization error on '%s' (%s): %s", e.Database, e.Op, e.Reason)
}

func (e *AuthError) Unwrap() error {
	return e.Err
}

// PlanError is returned for unexpected errors during the plan command.
type PlanError struct {
	Reason string
	Err    error
}

func (e *PlanError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("plan error: %s: %v", e.Reason, e.Err)
	}
	return fmt.Sprintf("plan error: %s", e.Reason)
}

func (e *PlanError) Unwrap() error {
	return e.Err
}

// ApplyError is returned for unexpected errors during the apply command.
type ApplyError struct {
	Reason string
	Err    error
}

func (e *ApplyError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("apply error: %s: %v", e.Reason, e.Err)
	}
	return fmt.Sprintf("apply error: %s", e.Reason)
}

func (e *ApplyError) Unwrap() error {
	return e.Err
}

// ExtractError is returned when creating or initializing an extractor fails.
type ExtractError struct {
	Reason string
	Err    error
}

func (e *ExtractError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("extract error: %s: %v", e.Reason, e.Err)
	}
	return fmt.Sprintf("extract error: %s", e.Reason)
}

func (e *ExtractError) Unwrap() error {
	return e.Err
}

// LoadError is returned when creating, initializing, or loading to the destination fails.
type LoadError struct {
	Reason string
	Err    error
}

func (e *LoadError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("load error: %s: %v", e.Reason, e.Err)
	}
	return fmt.Sprintf("load error: %s", e.Reason)
}

func (e *LoadError) Unwrap() error {
	return e.Err
}

// TransformError is returned when a data transformation fails.
type TransformError struct {
	Reason string
	Err    error
}

func (e *TransformError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("transform error: %s: %v", e.Reason, e.Err)
	}
	return fmt.Sprintf("transform error: %s", e.Reason)
}

func (e *TransformError) Unwrap() error {
	return e.Err
}

// ChunkCallbackError is returned when a chunk processing callback returns an error.
type ChunkCallbackError struct {
	Reason string
	Err    error
}

func (e *ChunkCallbackError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("chunk callback error: %s: %v", e.Reason, e.Err)
	}
	return fmt.Sprintf("chunk callback error: %s", e.Reason)
}

func (e *ChunkCallbackError) Unwrap() error {
	return e.Err
}
