package migrator

import (
	"context"
	"fmt"
	"mongo2dynamo/internal"
)

// Service handles the migration process between MongoDB and DynamoDB
type Service struct {
	reader internal.DataReader
	writer internal.DataWriter
	dryRun bool
}

// NewService creates a new migration service with the given reader and writer
func NewService(reader internal.DataReader, writer internal.DataWriter, dryRun bool) *Service {
	return &Service{
		reader: reader,
		writer: writer,
		dryRun: dryRun,
	}
}

// Run executes the migration process
func (s *Service) Run(ctx context.Context) error {
	data, err := s.reader.Read(ctx)
	if err != nil {
		return fmt.Errorf("failed to read data: %w", err)
	}

	fmt.Printf("Found %d documents to migrate\n", len(data))

	if s.dryRun {
		return nil
	}

	if err := s.writer.Write(ctx, data); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	return nil
}
