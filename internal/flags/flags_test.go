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

	filterFlag := cmd.Flags().Lookup("mongo-filter")
	assert.NotNil(t, filterFlag, "mongo-filter flag should be registered")
	assert.Equal(t, "", filterFlag.DefValue)

	projectionFlag := cmd.Flags().Lookup("mongo-projection")
	assert.NotNil(t, projectionFlag, "mongo-projection flag should be registered")
	assert.Equal(t, "{\"__v\":0,\"_class\":0}", projectionFlag.DefValue)
}

func TestAddDynamoFlags(t *testing.T) {
	cmd := &cobra.Command{}
	flags.AddDynamoFlags(cmd)

	endpointFlag := cmd.Flags().Lookup("dynamo-endpoint")
	assert.NotNil(t, endpointFlag, "dynamo-endpoint flag should be registered")
	assert.Equal(t, "http://localhost:8000", endpointFlag.DefValue)

	tableFlag := cmd.Flags().Lookup("dynamo-table")
	assert.NotNil(t, tableFlag, "dynamo-table flag should be registered")

	partitionKeyFlag := cmd.Flags().Lookup("dynamo-partition-key")
	assert.NotNil(t, partitionKeyFlag, "dynamo-partition-key flag should be registered")
	assert.Equal(t, "id", partitionKeyFlag.DefValue)

	partitionKeyTypeFlag := cmd.Flags().Lookup("dynamo-partition-key-type")
	assert.NotNil(t, partitionKeyTypeFlag, "dynamo-partition-key-type flag should be registered")
	assert.Equal(t, "S", partitionKeyTypeFlag.DefValue)

	sortKeyFlag := cmd.Flags().Lookup("dynamo-sort-key")
	assert.NotNil(t, sortKeyFlag, "dynamo-sort-key flag should be registered")
	assert.Equal(t, "", sortKeyFlag.DefValue)

	sortKeyTypeFlag := cmd.Flags().Lookup("dynamo-sort-key-type")
	assert.NotNil(t, sortKeyTypeFlag, "dynamo-sort-key-type flag should be registered")
	assert.Equal(t, "S", sortKeyTypeFlag.DefValue)

	regionFlag := cmd.Flags().Lookup("aws-region")
	assert.NotNil(t, regionFlag, "aws-region flag should be registered")
	assert.Equal(t, "us-east-1", regionFlag.DefValue)

	maxRetriesFlag := cmd.Flags().Lookup("max-retries")
	assert.NotNil(t, maxRetriesFlag, "max-retries flag should be registered")
	assert.Equal(t, "5", maxRetriesFlag.DefValue)
}

func TestAddAutoApproveFlag(t *testing.T) {
	cmd := &cobra.Command{}
	flags.AddAutoApproveFlag(cmd)

	autoApproveFlag := cmd.Flags().Lookup("auto-approve")
	assert.NotNil(t, autoApproveFlag, "auto-approve flag should be registered")
	assert.Equal(t, "false", autoApproveFlag.DefValue)
}

func TestAddNoProgressFlag(t *testing.T) {
	cmd := &cobra.Command{}
	flags.AddNoProgressFlag(cmd)

	noProgressFlag := cmd.Flags().Lookup("no-progress")
	assert.NotNil(t, noProgressFlag, "no-progress flag should be registered")
	assert.Equal(t, "false", noProgressFlag.DefValue)
}

func TestAddMetricsFlags(t *testing.T) {
	cmd := &cobra.Command{}
	flags.AddMetricsFlags(cmd)

	metricsEnabledFlag := cmd.Flags().Lookup("metrics-enabled")
	assert.NotNil(t, metricsEnabledFlag, "metrics-enabled flag should be registered")
	assert.Equal(t, "false", metricsEnabledFlag.DefValue)

	metricsAddrFlag := cmd.Flags().Lookup("metrics-addr")
	assert.NotNil(t, metricsAddrFlag, "metrics-addr flag should be registered")
	assert.Equal(t, ":2112", metricsAddrFlag.DefValue)
}
