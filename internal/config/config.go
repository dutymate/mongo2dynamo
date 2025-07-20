package config

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/viper"

	"mongo2dynamo/internal/common"
)

// Config holds all configuration for the application.
type Config struct {
	// MongoDB configuration.
	MongoHost       string
	MongoPort       string
	MongoUser       string
	MongoPassword   string
	MongoDB         string
	MongoCollection string
	MongoFilter     string

	// DynamoDB configuration.
	DynamoTable            string
	DynamoEndpoint         string
	DynamoPartitionKey     string
	DynamoPartitionKeyType string
	DynamoSortKey          string
	DynamoSortKeyType      string
	AWSRegion              string

	// Application configuration.
	AutoApprove bool
	DryRun      bool
	MaxRetries  int
}

// Ensure Config implements the interface.
var _ common.ConfigProvider = (*Config)(nil)

// Load loads configuration from environment variables and config file.
func (c *Config) Load() error {
	v := viper.New()

	// Set default values.
	v.SetDefault("mongo_host", "localhost")
	v.SetDefault("mongo_port", "27017")
	v.SetDefault("mongo_filter", "")
	v.SetDefault("dynamo_endpoint", "http://localhost:8000")
	v.SetDefault("dynamo_partition_key", "id")
	v.SetDefault("dynamo_partition_key_type", "S")
	v.SetDefault("dynamo_sort_key", "")
	v.SetDefault("dynamo_sort_key_type", "S")
	v.SetDefault("aws_region", "us-east-1")
	v.SetDefault("max_retries", 5)

	// Read from environment variables.
	v.SetEnvPrefix("MONGO2DYNAMO")
	v.AutomaticEnv()

	// Read from config file if it exists.
	home, err := os.UserHomeDir()
	if err != nil {
		return &common.FileIOError{Op: "get user home dir", Reason: err.Error(), Err: err}
	}
	configPath := filepath.Join(home, ".mongo2dynamo")
	v.AddConfigPath(configPath)
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	if err := v.ReadInConfig(); err != nil {
		// Ignore error if config file doesn't exist, but wrap other errors.
		var notFoundErr viper.ConfigFileNotFoundError
		if !errors.As(err, &notFoundErr) {
			return &common.FileIOError{Op: "read config file", Reason: err.Error(), Err: err}
		}
	}

	// Only set values if they are not already set by flags.
	if c.MongoHost == "" {
		c.MongoHost = v.GetString("mongo_host")
	}
	if c.MongoPort == "" {
		c.MongoPort = v.GetString("mongo_port")
	}
	if c.MongoUser == "" {
		c.MongoUser = v.GetString("mongo_user")
	}
	if c.MongoPassword == "" {
		c.MongoPassword = v.GetString("mongo_password")
	}
	if c.MongoDB == "" {
		c.MongoDB = v.GetString("mongo_db")
	}
	if c.MongoCollection == "" {
		c.MongoCollection = v.GetString("mongo_collection")
	}
	if c.MongoFilter == "" {
		c.MongoFilter = v.GetString("mongo_filter")
	}
	if c.DynamoTable == "" {
		c.DynamoTable = v.GetString("dynamo_table")
	}
	if c.DynamoEndpoint == "" {
		c.DynamoEndpoint = v.GetString("dynamo_endpoint")
	}
	if c.DynamoPartitionKey == "" {
		c.DynamoPartitionKey = v.GetString("dynamo_partition_key")
	}
	if c.DynamoPartitionKeyType == "" {
		c.DynamoPartitionKeyType = v.GetString("dynamo_partition_key_type")
	}
	if c.DynamoSortKey == "" {
		c.DynamoSortKey = v.GetString("dynamo_sort_key")
	}
	if c.DynamoSortKeyType == "" {
		c.DynamoSortKeyType = v.GetString("dynamo_sort_key_type")
	}
	if c.AWSRegion == "" {
		c.AWSRegion = v.GetString("aws_region")
	}
	if !c.AutoApprove {
		c.AutoApprove = v.GetBool("auto_approve")
	}
	if !c.DryRun {
		c.DryRun = v.GetBool("dry_run")
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = v.GetInt("max_retries")
	}

	return nil
}

// Validate checks if all required fields are set.
func (c *Config) Validate() error {
	if c.MongoDB == "" {
		return &common.ConfigError{Op: "validate", Reason: "mongo_db field is required"}
	}
	if c.MongoCollection == "" {
		return &common.ConfigError{Op: "validate", Reason: "mongo_collection field is required"}
	}
	if c.DynamoPartitionKey == "" {
		return &common.ConfigError{Op: "validate", Reason: "dynamo_partition_key field is required"}
	}
	if !isValidKeyType(c.DynamoPartitionKeyType) {
		return &common.ConfigError{Op: "validate", Reason: "dynamo_partition_key_type must be one of 'S', 'N', or 'B'"}
	}
	if c.DynamoSortKey != "" && !isValidKeyType(c.DynamoSortKeyType) {
		return &common.ConfigError{Op: "validate", Reason: "dynamo_sort_key_type must be one of 'S', 'N', or 'B'"}
	}
	if c.MaxRetries <= 0 {
		return &common.ConfigError{Op: "validate", Reason: "max_retries must be greater than 0"}
	}

	// If DynamoTable is not set and not in DryRun mode, use the collection name as the DynamoDB table name.
	if !c.DryRun && c.DynamoTable == "" {
		// If AutoApprove is false, prompt the user for confirmation before proceeding.
		if !c.AutoApprove && !common.Confirm("Use collection name as DynamoDB table name? (y/N) ") {
			return fmt.Errorf("required field 'dynamo_table' not set: user declined to use collection name: %w", context.Canceled)
		}
		fmt.Printf("Using collection name '%s' as DynamoDB table name.\n", c.MongoCollection)
		c.DynamoTable = c.MongoCollection
	}

	return nil
}

func isValidKeyType(keyType string) bool {
	switch keyType {
	case "S", "N", "B":
		return true
	default:
		return false
	}
}

func (c *Config) GetMongoHost() string {
	return c.MongoHost
}

func (c *Config) GetMongoPort() string {
	return c.MongoPort
}

func (c *Config) GetMongoUser() string {
	return c.MongoUser
}

func (c *Config) GetMongoPassword() string {
	return c.MongoPassword
}

func (c *Config) GetMongoDB() string {
	return c.MongoDB
}

func (c *Config) GetMongoCollection() string {
	return c.MongoCollection
}

func (c *Config) GetMongoFilter() string {
	return c.MongoFilter
}

func (c *Config) GetMongoURI() string {
	if c.MongoUser != "" && c.MongoPassword != "" {
		return fmt.Sprintf("mongodb://%s:%s@%s:%s",
			c.MongoUser, c.MongoPassword, c.MongoHost, c.MongoPort)
	}
	return fmt.Sprintf("mongodb://%s:%s", c.MongoHost, c.MongoPort)
}

func (c *Config) GetDynamoEndpoint() string {
	return c.DynamoEndpoint
}

func (c *Config) GetDynamoTable() string {
	return c.DynamoTable
}

func (c *Config) GetAutoApprove() bool {
	return c.AutoApprove
}

func (c *Config) GetDryRun() bool {
	return c.DryRun
}

func (c *Config) GetMaxRetries() int {
	return c.MaxRetries
}

func (c *Config) GetDynamoPartitionKey() string {
	return c.DynamoPartitionKey
}

func (c *Config) GetDynamoPartitionKeyType() string {
	return c.DynamoPartitionKeyType
}

func (c *Config) GetDynamoSortKey() string {
	return c.DynamoSortKey
}

func (c *Config) GetDynamoSortKeyType() string {
	return c.DynamoSortKeyType
}

func (c *Config) GetAWSRegion() string {
	return c.AWSRegion
}

func (c *Config) SetDryRun(dryRun bool) {
	c.DryRun = dryRun
}
