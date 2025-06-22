package common

import "fmt"

// ConfigFieldError is returned when configuration is invalid or missing required fields.
type ConfigFieldError struct {
	Field  string
	Reason string
}

func (e *ConfigFieldError) Error() string {
	return fmt.Sprintf("invalid configuration for field '%s': %s", e.Field, e.Reason)
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

// MigrationStepError is returned when a migration operation fails at a specific step.
type MigrationStepError struct {
	Step   string
	Reason string
	Err    error
}

func (e *MigrationStepError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("migration failed during '%s' step: %s: %v", e.Step, e.Reason, e.Err)
	}
	return fmt.Sprintf("migration failed during '%s' step: %s", e.Step, e.Reason)
}

func (e *MigrationStepError) Unwrap() error {
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
	Op     string
	Reason string
	Err    error
}

func (e *AuthError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("authentication/authorization error during '%s': %s: %v", e.Op, e.Reason, e.Err)
	}
	return fmt.Sprintf("authentication/authorization error during '%s': %s", e.Op, e.Reason)
}

func (e *AuthError) Unwrap() error {
	return e.Err
}
