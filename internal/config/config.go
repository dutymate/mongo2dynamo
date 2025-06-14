package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Config holds all configuration for the application
type Config struct {
	// MongoDB configuration
	MongoHost       string
	MongoPort       string
	MongoUser       string
	MongoPassword   string
	MongoDB         string
	MongoCollection string

	// DynamoDB configuration
	DynamoTable    string
	DynamoEndpoint string
	AWSRegion      string

	// Application configuration
	AutoApprove bool
	DryRun      bool
}

// Load loads configuration from environment variables and config file
func (c *Config) Load() error {
	v := viper.New()

	// Set default values
	v.SetDefault("mongo_host", "localhost")
	v.SetDefault("mongo_port", "27017")
	v.SetDefault("dynamo_endpoint", "http://localhost:8000")
	v.SetDefault("aws_region", "us-east-1")

	// Read from environment variables
	v.SetEnvPrefix("MONGO2DYNAMO")
	v.AutomaticEnv()

	// Read from config file if it exists
	home, err := os.UserHomeDir()
	if err == nil {
		configPath := filepath.Join(home, ".mongo2dynamo")
		v.AddConfigPath(configPath)
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		_ = v.ReadInConfig() // Ignore error if config file doesn't exist
	}

	// Only set values if they are not already set by flags
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

	if err := c.validate(); err != nil {
		return err
	}

	return nil
}

// validate checks if all required fields are set
func (c *Config) validate() error {
	if c.MongoDB == "" {
		return fmt.Errorf("mongo_db is required")
	}
	if c.MongoCollection == "" {
		return fmt.Errorf("mongo_collection is required")
	}
	if c.DynamoTable == "" && !c.DryRun {
		return fmt.Errorf("dynamo_table is required")
	}
	return nil
}

// GetMongoURI returns the MongoDB connection URI
func (c *Config) GetMongoURI() string {
	if c.MongoUser != "" && c.MongoPassword != "" {
		return fmt.Sprintf("mongodb://%s:%s@%s:%s",
			c.MongoUser, c.MongoPassword, c.MongoHost, c.MongoPort)
	}
	return fmt.Sprintf("mongodb://%s:%s", c.MongoHost, c.MongoPort)
}

func (c *Config) GetMongoDB() string {
	return c.MongoDB
}

func (c *Config) GetMongoCollection() string {
	return c.MongoCollection
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

func RegisterMongoDynamoFlags(cmd *cobra.Command) {
	cmd.Flags().String("mongo-host", "localhost", "MongoDB host")
	cmd.Flags().String("mongo-port", "27017", "MongoDB port")
	cmd.Flags().String("mongo-user", "", "MongoDB username")
	cmd.Flags().String("mongo-password", "", "MongoDB password")
	cmd.Flags().String("mongo-db", "", "MongoDB database name")
	cmd.Flags().String("mongo-collection", "", "MongoDB collection name")
	cmd.Flags().String("dynamo-table", "", "DynamoDB table name")
}

// SetDryRun sets whether this is a dry run
func (c *Config) SetDryRun(dryRun bool) {
	c.DryRun = dryRun
}
