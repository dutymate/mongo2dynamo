package config

import (
	"errors"
	"fmt"
	"mongo2dynamo/internal/common"
	"os"
	"path/filepath"

	"github.com/spf13/viper"
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

	// DynamoDB configuration.
	DynamoTable    string
	DynamoEndpoint string
	AWSRegion      string

	// Application configuration.
	AutoApprove bool
	DryRun      bool
}

// Load loads configuration from environment variables and config file.
func (c *Config) Load() error {
	v := viper.New()

	// Set default values.
	v.SetDefault("mongo_host", "localhost")
	v.SetDefault("mongo_port", "27017")
	v.SetDefault("dynamo_endpoint", "http://localhost:8000")
	v.SetDefault("aws_region", "us-east-1")

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
	if c.DynamoTable == "" {
		c.DynamoTable = v.GetString("dynamo_table")
	}
	if c.DynamoEndpoint == "" {
		c.DynamoEndpoint = v.GetString("dynamo_endpoint")
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
	if c.DynamoTable == "" && !c.DryRun {
		return &common.ConfigError{Op: "validate", Reason: "dynamo_table field is required unless dry run"}
	}
	return nil
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

func (c *Config) GetAWSRegion() string {
	return c.AWSRegion
}

func (c *Config) GetAutoApprove() bool {
	return c.AutoApprove
}

func (c *Config) GetDryRun() bool {
	return c.DryRun
}

// SetDryRun sets whether this is a dry run.
func (c *Config) SetDryRun(dryRun bool) {
	c.DryRun = dryRun
}
