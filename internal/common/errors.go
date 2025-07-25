package common

import "fmt"

// ConfigError is returned for general configuration loading and validation errors.
type ConfigError struct {
	Op     string
	Reason string
	Err    error
}

func (e *ConfigError) Error() string {
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
	return fmt.Sprintf("authentication/authorization error on '%s' (%s): %s", e.Database, e.Op, e.Reason)
}

func (e *AuthError) Unwrap() error {
	return e.Err
}

// ParseError is returned when MongoDB json parsing fails.
type ParseError struct {
	JSONString string
	Op         string
	Reason     string
	Err        error
}

func (e *ParseError) Error() string {
	if e.JSONString != "" {
		return fmt.Sprintf("MongoDB json parse error during '%s' for document '%s': %s", e.Op, e.JSONString, e.Reason)
	}
	return fmt.Sprintf("MongoDB json parse error during '%s': %s", e.Op, e.Reason)
}

func (e *ParseError) Unwrap() error {
	return e.Err
}

// ExtractError is returned when creating or initializing an extractor fails.
type ExtractError struct {
	Reason string
	Err    error
}

func (e *ExtractError) Error() string {
	return fmt.Sprintf("extract error: %s", e.Reason)
}

func (e *ExtractError) Unwrap() error {
	return e.Err
}

// TransformError is returned when a data transformation fails.
type TransformError struct {
	Reason string
	Err    error
}

func (e *TransformError) Error() string {
	return fmt.Sprintf("transform error: %s", e.Reason)
}

func (e *TransformError) Unwrap() error {
	return e.Err
}

// LoadError is returned when creating, initializing, or loading to the destination fails.
type LoadError struct {
	Reason string
	Err    error
}

func (e *LoadError) Error() string {
	return fmt.Sprintf("load error: %s", e.Reason)
}

func (e *LoadError) Unwrap() error {
	return e.Err
}

// ChunkCallbackError is returned when a chunk processing callback returns an error.
type ChunkCallbackError struct {
	Reason string
	Err    error
}

func (e *ChunkCallbackError) Error() string {
	return fmt.Sprintf("chunk callback error: %s", e.Reason)
}

func (e *ChunkCallbackError) Unwrap() error {
	return e.Err
}
