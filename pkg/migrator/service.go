package migrator

import (
	"context"
	"fmt"
	"mongo2dynamo/pkg/common"
)

// Service handles the migration process between MongoDB and DynamoDB.
type Service struct {
	reader common.DataReader
	writer common.DataWriter
	dryRun bool
}

// NewService creates a new migration service with the given reader and writer.
func NewService(reader common.DataReader, writer common.DataWriter, dryRun bool) *Service {
	return &Service{
		reader: reader,
		writer: writer,
		dryRun: dryRun,
	}
}

// Run executes the migration process.
func (s *Service) Run(ctx context.Context) error {
	processChunks := func(processFunc func([]map[string]interface{}) error) error {
		return s.reader.Read(ctx, func(chunk []map[string]interface{}) error {
			return processFunc(chunk)
		})
	}

	if s.dryRun {
		total := 0
		err := processChunks(func(chunk []map[string]interface{}) error {
			total += len(chunk)
			return nil
		})
		if err != nil {
			return &common.MigrationStepError{Step: "read", Reason: err.Error(), Err: err}
		}
		fmt.Printf("Found %s documents to migrate\n", common.FormatNumber(total))
		return nil
	}

	migrated := 0
	err := processChunks(func(chunk []map[string]interface{}) error {
		if err := s.writer.Write(ctx, chunk); err != nil {
			return &common.MigrationStepError{Step: "write", Reason: err.Error(), Err: err}
		}
		migrated += len(chunk)
		return nil
	})
	if err != nil {
		return &common.MigrationStepError{Step: "write", Reason: err.Error(), Err: err}
	}
	fmt.Printf("Successfully migrated %s documents\n", common.FormatNumber(migrated))
	return nil
}
