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
	MongoHost       string `mapstructure:"mongo_host"`
	MongoPort       string `mapstructure:"mongo_port"`
	MongoUser       string `mapstructure:"mongo_user"`
	MongoPassword   string `mapstructure:"mongo_password"`
	MongoDB         string `mapstructure:"mongo_db"`
	MongoCollection string `mapstructure:"mongo_collection"`
	MongoFilter     string `mapstructure:"mongo_filter"`
	MongoProjection string `mapstructure:"mongo_projection"`

	// DynamoDB configuration.
	DynamoTable            string `mapstructure:"dynamo_table"`
	DynamoEndpoint         string `mapstructure:"dynamo_endpoint"`
	DynamoPartitionKey     string `mapstructure:"dynamo_partition_key"`
	DynamoPartitionKeyType string `mapstructure:"dynamo_partition_key_type"`
	DynamoSortKey          string `mapstructure:"dynamo_sort_key"`
	DynamoSortKeyType      string `mapstructure:"dynamo_sort_key_type"`
	AWSRegion              string `mapstructure:"aws_region"`
	MaxRetries             int    `mapstructure:"max_retries"`

	// Application configuration.
	AutoApprove bool `mapstructure:"auto_approve"`
	NoProgress  bool `mapstructure:"no_progress"`

	// Dry run mode.
	dryRun bool
}

// Load loads configuration from environment variables and config file.
func (c *Config) Load() error {
	// Set default values.
	viper.SetDefault("mongo_host", "localhost")
	viper.SetDefault("mongo_port", "27017")
	viper.SetDefault("mongo_filter", "")
	viper.SetDefault("mongo_projection", "")
	viper.SetDefault("dynamo_endpoint", "http://localhost:8000")
	viper.SetDefault("dynamo_partition_key", "id")
	viper.SetDefault("dynamo_partition_key_type", "S")
	viper.SetDefault("dynamo_sort_key", "")
	viper.SetDefault("dynamo_sort_key_type", "S")
	viper.SetDefault("aws_region", "us-east-1")
	viper.SetDefault("max_retries", 5)

	// Read from config file if it exists.
	home, err := os.UserHomeDir()
	if err != nil {
		return &common.FileIOError{Op: "get user home dir", Reason: err.Error(), Err: err}
	}

	configPath := filepath.Join(home, ".mongo2dynamo")
	viper.AddConfigPath(configPath)
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")

	// Read from environment variables.
	viper.AutomaticEnv()
	viper.SetEnvPrefix("MONGO2DYNAMO")
	viper.SetOptions(viper.ExperimentalBindStruct())

	if err := viper.ReadInConfig(); err != nil {
		// Ignore error if config file doesn't exist, but wrap other errors.
		var notFoundErr viper.ConfigFileNotFoundError
		if !errors.As(err, &notFoundErr) {
			return &common.FileIOError{Op: "read config file", Reason: err.Error(), Err: err}
		}
	}

	// Unmarshal the config into the struct.
	if err := viper.Unmarshal(&c); err != nil {
		return &common.ConfigError{Op: "unmarshal config", Reason: err.Error(), Err: err}
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
	if !c.IsDryRun() && c.DynamoTable == "" {
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

// SetDryRun sets the dry run mode.
func (c *Config) SetDryRun(dryRun bool) {
	c.dryRun = dryRun
}

// IsDryRun returns whether dry run mode is enabled.
func (c *Config) IsDryRun() bool {
	return c.dryRun
}

// BuildMongoURI builds the MongoDB connection URI.
func (c *Config) BuildMongoURI() string {
	if c.MongoUser != "" && c.MongoPassword != "" {
		return fmt.Sprintf("mongodb://%s:%s@%s:%s",
			c.MongoUser, c.MongoPassword, c.MongoHost, c.MongoPort)
	}
	return fmt.Sprintf("mongodb://%s:%s", c.MongoHost, c.MongoPort)
}
