package flags_test

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"

	"mongo2dynamo/internal/flags"
)

func TestAddMongoFlags(t *testing.T) {
	cmd := &cobra.Command{}
	flags.AddMongoFlags(cmd)

	hostFlag := cmd.Flags().Lookup("mongo-host")
	assert.NotNil(t, hostFlag, "mongo-host flag should be registered")
	assert.Equal(t, "localhost", hostFlag.DefValue)

	portFlag := cmd.Flags().Lookup("mongo-port")
	assert.NotNil(t, portFlag, "mongo-port flag should be registered")
	assert.Equal(t, "27017", portFlag.DefValue)

	userFlag := cmd.Flags().Lookup("mongo-user")
	assert.NotNil(t, userFlag, "mongo-user flag should be registered")

	passwordFlag := cmd.Flags().Lookup("mongo-password")
	assert.NotNil(t, passwordFlag, "mongo-password flag should be registered")

	dbFlag := cmd.Flags().Lookup("mongo-db")
	assert.NotNil(t, dbFlag, "mongo-db flag should be registered")

	collectionFlag := cmd.Flags().Lookup("mongo-collection")
	assert.NotNil(t, collectionFlag, "mongo-collection flag should be registered")
}

func TestAddDynamoFlags(t *testing.T) {
	cmd := &cobra.Command{}
	flags.AddDynamoFlags(cmd)

	endpointFlag := cmd.Flags().Lookup("dynamo-endpoint")
	assert.NotNil(t, endpointFlag, "dynamo-endpoint flag should be registered")
	assert.Equal(t, "http://localhost:8000", endpointFlag.DefValue)

	tableFlag := cmd.Flags().Lookup("dynamo-table")
	assert.NotNil(t, tableFlag, "dynamo-table flag should be registered")

	regionFlag := cmd.Flags().Lookup("aws-region")
	assert.NotNil(t, regionFlag, "aws-region flag should be registered")
	assert.Equal(t, "us-east-1", regionFlag.DefValue)

	maxRetriesFlag := cmd.Flags().Lookup("max-retries")
	assert.NotNil(t, maxRetriesFlag, "max-retries flag should be registered")
	assert.Equal(t, "5", maxRetriesFlag.DefValue)
}
