package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"mongo2dynamo/internal/common"
)

const defaultMaxRetries = 5

func TestConfig_Load(t *testing.T) {
	tests := []struct {
		name    string
		envVars map[string]string
		config  *Config
		wantErr bool
	}{
		{
			name: "load with environment variables",
			envVars: map[string]string{
				"MONGO2DYNAMO_MONGO_HOST":       "test-host",
				"MONGO2DYNAMO_MONGO_PORT":       "27018",
				"MONGO2DYNAMO_MONGO_DB":         "testdb",
				"MONGO2DYNAMO_MONGO_COLLECTION": "testcollection",
				"MONGO2DYNAMO_MONGO_FILTER":     `{"status": "active"}`,
				"MONGO2DYNAMO_DYNAMO_ENDPOINT":  "http://test:8000",
				"MONGO2DYNAMO_DYNAMO_TABLE":     "testtable",
				"MONGO2DYNAMO_AWS_REGION":       "us-west-2",
			},
			config:  &Config{},
			wantErr: false,
		},
		{
			name: "load with default values",
			envVars: map[string]string{
				"MONGO2DYNAMO_MONGO_DB":         "testdb",
				"MONGO2DYNAMO_MONGO_COLLECTION": "testcollection",
				"MONGO2DYNAMO_DYNAMO_TABLE":     "testtable",
			},
			config:  &Config{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set environment variables.
			for k, v := range tt.envVars {
				os.Setenv(k, v)
				defer os.Unsetenv(k)
			}

			err := tt.config.Load()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Load() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				// Check that required fields are set.
				if tt.config.MongoDB == "" {
					t.Error("MongoDB field should be set")
				}
				if tt.config.MongoCollection == "" {
					t.Error("MongoCollection field should be set")
				}
				if tt.config.DynamoTable == "" {
					t.Error("DynamoTable field should be set")
				}

				// Check that environment variables are loaded correctly.
				if tt.envVars["MONGO2DYNAMO_MONGO_HOST"] != "" && tt.config.MongoHost != tt.envVars["MONGO2DYNAMO_MONGO_HOST"] {
					t.Errorf("MongoHost should be '%s', got '%s'", tt.envVars["MONGO2DYNAMO_MONGO_HOST"], tt.config.MongoHost)
				}
				if tt.envVars["MONGO2DYNAMO_MONGO_PORT"] != "" && tt.config.MongoPort != tt.envVars["MONGO2DYNAMO_MONGO_PORT"] {
					t.Errorf("MongoPort should be '%s', got '%s'", tt.envVars["MONGO2DYNAMO_MONGO_PORT"], tt.config.MongoPort)
				}
				if tt.envVars["MONGO2DYNAMO_MONGO_FILTER"] != "" && tt.config.MongoFilter != tt.envVars["MONGO2DYNAMO_MONGO_FILTER"] {
					t.Errorf("MongoFilter should be '%s', got '%s'", tt.envVars["MONGO2DYNAMO_MONGO_FILTER"], tt.config.MongoFilter)
				}
				if tt.envVars["MONGO2DYNAMO_DYNAMO_ENDPOINT"] != "" && tt.config.DynamoEndpoint != tt.envVars["MONGO2DYNAMO_DYNAMO_ENDPOINT"] {
					t.Errorf("DynamoEndpoint should be '%s', got '%s'", tt.envVars["MONGO2DYNAMO_DYNAMO_ENDPOINT"], tt.config.DynamoEndpoint)
				}
				if tt.envVars["MONGO2DYNAMO_AWS_REGION"] != "" && tt.config.AWSRegion != tt.envVars["MONGO2DYNAMO_AWS_REGION"] {
					t.Errorf("AWSRegion should be '%s', got '%s'", tt.envVars["MONGO2DYNAMO_AWS_REGION"], tt.config.AWSRegion)
				}

				// Check default values when not set via environment.
				if tt.envVars["MONGO2DYNAMO_MONGO_HOST"] == "" && tt.config.MongoHost != "localhost" {
					t.Errorf("MongoHost should default to 'localhost', got '%s'", tt.config.MongoHost)
				}
				if tt.envVars["MONGO2DYNAMO_MONGO_PORT"] == "" && tt.config.MongoPort != "27017" {
					t.Errorf("MongoPort should default to '27017', got '%s'", tt.config.MongoPort)
				}
				if tt.envVars["MONGO2DYNAMO_DYNAMO_ENDPOINT"] == "" && tt.config.DynamoEndpoint != "http://localhost:8000" {
					t.Errorf("DynamoEndpoint should default to 'http://localhost:8000', got '%s'", tt.config.DynamoEndpoint)
				}
				if tt.envVars["MONGO2DYNAMO_AWS_REGION"] == "" && tt.config.AWSRegion != "us-east-1" {
					t.Errorf("AWSRegion should default to 'us-east-1', got '%s'", tt.config.AWSRegion)
				}
			}
		})
	}
}

func TestConfig_Load_WithConfigFile(t *testing.T) {
	// Create temporary config file.
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("Failed to get user home dir: %v", err)
	}

	configDir := filepath.Join(home, ".mongo2dynamo")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		t.Fatalf("Failed to create config directory: %v", err)
	}
	defer os.RemoveAll(configDir)

	configFile := filepath.Join(configDir, "config.yaml")
	configContent := `
mongo_host: "config-host"
mongo_port: "27019"
mongo_db: "configdb"
mongo_collection: "configcollection"
dynamo_table: "configtable"
`

	if err := os.WriteFile(configFile, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	cfg := &Config{}
	err = cfg.Load()
	if err != nil {
		t.Fatalf("Config.Load() error = %v", err)
	}

	// Check that values from config file are loaded.
	if cfg.MongoHost != "config-host" {
		t.Errorf("Expected MongoHost to be 'config-host', got '%s'", cfg.MongoHost)
	}
	if cfg.MongoPort != "27019" {
		t.Errorf("Expected MongoPort to be '27019', got '%s'", cfg.MongoPort)
	}
	if cfg.MongoDB != "configdb" {
		t.Errorf("Expected MongoDB to be 'configdb', got '%s'", cfg.MongoDB)
	}
	if cfg.MongoCollection != "configcollection" {
		t.Errorf("Expected MongoCollection to be 'configcollection', got '%s'", cfg.MongoCollection)
	}
	if cfg.DynamoTable != "configtable" {
		t.Errorf("Expected DynamoTable to be 'configtable', got '%s'", cfg.DynamoTable)
	}
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name        string
		config      *Config
		wantErr     bool
		expectedErr string
	}{
		{
			name: "valid config",
			config: &Config{
				MongoDB:                "testdb",
				MongoCollection:        "testcollection",
				DynamoTable:            "testtable",
				DynamoPartitionKey:     "id",
				DynamoPartitionKeyType: "S",
				MaxRetries:             defaultMaxRetries,
			},
			wantErr: false,
		},
		{
			name: "missing mongo_db",
			config: &Config{
				MongoCollection:        "testcollection",
				DynamoTable:            "testtable",
				DynamoPartitionKey:     "id",
				DynamoPartitionKeyType: "S",
				MaxRetries:             defaultMaxRetries,
			},
			wantErr:     true,
			expectedErr: "mongo_db",
		},
		{
			name: "missing mongo_collection",
			config: &Config{
				MongoDB:                "testdb",
				DynamoTable:            "testtable",
				DynamoPartitionKey:     "id",
				DynamoPartitionKeyType: "S",
				MaxRetries:             defaultMaxRetries,
			},
			wantErr:     true,
			expectedErr: "mongo_collection",
		},
		{
			name: "missing dynamo_partition_key",
			config: &Config{
				MongoDB:                "testdb",
				MongoCollection:        "testcollection",
				DynamoTable:            "testtable",
				DynamoPartitionKey:     "",
				DynamoPartitionKeyType: "S",
				MaxRetries:             defaultMaxRetries,
			},
			wantErr:     true,
			expectedErr: "dynamo_partition_key field is required",
		},
		{
			name: "invalid dynamo_partition_key_type",
			config: &Config{
				MongoDB:                "testdb",
				MongoCollection:        "testcollection",
				DynamoTable:            "testtable",
				DynamoPartitionKey:     "id",
				DynamoPartitionKeyType: "X", // Invalid type.
				MaxRetries:             defaultMaxRetries,
			},
			wantErr:     true,
			expectedErr: "dynamo_partition_key_type must be one of 'S', 'N', or 'B'",
		},
		{
			name: "invalid dynamo_sort_key_type",
			config: &Config{
				MongoDB:                "testdb",
				MongoCollection:        "testcollection",
				DynamoTable:            "testtable",
				DynamoPartitionKey:     "id",
				DynamoPartitionKeyType: "S",
				DynamoSortKey:          "timestamp",
				DynamoSortKeyType:      "X", // Invalid type.
				MaxRetries:             defaultMaxRetries,
			},
			wantErr:     true,
			expectedErr: "dynamo_sort_key_type must be one of 'S', 'N', or 'B'",
		},
		{
			name: "valid dynamo_sort_key_type",
			config: &Config{
				MongoDB:                "testdb",
				MongoCollection:        "testcollection",
				DynamoTable:            "testtable",
				DynamoPartitionKey:     "id",
				DynamoPartitionKeyType: "S",
				DynamoSortKey:          "timestamp",
				DynamoSortKeyType:      "N",
				MaxRetries:             defaultMaxRetries,
			},
			wantErr: false,
		},
		{
			name: "missing dynamo_table but dry run",
			config: func() *Config {
				cfg := &Config{
					MongoDB:                "testdb",
					MongoCollection:        "testcollection",
					DynamoPartitionKey:     "id",
					DynamoPartitionKeyType: "S",
					MaxRetries:             defaultMaxRetries,
				}
				cfg.SetDryRun(true)
				return cfg
			}(),
			wantErr: false,
		},
		{
			name: "missing dynamo_table with auto-approve should set DynamoTable to collection name",
			config: func() *Config {
				cfg := &Config{
					MongoDB:                "testdb",
					MongoCollection:        "testcollection",
					AutoApprove:            true,
					DynamoPartitionKey:     "id",
					DynamoPartitionKeyType: "S",
					MaxRetries:             defaultMaxRetries,
				}
				cfg.SetDryRun(false)
				return cfg
			}(),
			wantErr: false,
		},
		{
			name: "max_retries is zero",
			config: &Config{
				MongoDB:                "testdb",
				MongoCollection:        "testcollection",
				DynamoTable:            "testtable",
				DynamoPartitionKey:     "id",
				DynamoPartitionKeyType: "S",
				MaxRetries:             0,
			},
			wantErr:     true,
			expectedErr: "max_retries",
		},
		{
			name: "max_retries is negative",
			config: &Config{
				MongoDB:                "testdb",
				MongoCollection:        "testcollection",
				DynamoTable:            "testtable",
				DynamoPartitionKey:     "id",
				DynamoPartitionKeyType: "S",
				MaxRetries:             -3,
			},
			wantErr:     true,
			expectedErr: "max_retries",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.validate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr && err != nil {
				// Check error type.
				var configErr *common.ConfigError
				if !errors.As(err, &configErr) {
					t.Errorf("Expected ConfigError, got %T", err)
				}

				// Check error message content.
				if tt.expectedErr != "" {
					errMsg := err.Error()
					if !strings.Contains(errMsg, tt.expectedErr) {
						t.Errorf("Error message should contain '%s', got: %s", tt.expectedErr, errMsg)
					}
				}

				// Check ConfigError fields.
				if !strings.Contains(configErr.Reason, tt.expectedErr) {
					t.Errorf("ConfigError.Reason should contain '%s', got '%s'", tt.expectedErr, configErr.Reason)
				}
			}

			// Check if DynamoTable is automatically set to MongoCollection when empty.
			// Note: In dry run mode, DynamoTable is not set automatically.
			if !tt.config.IsDryRun() && tt.config.MongoCollection != "" && tt.config.DynamoTable == "" {
				// This should not happen after Validate() is called.
				t.Errorf("DynamoTable should be set to MongoCollection '%s' when empty, but it's still empty", tt.config.MongoCollection)
			}
		})
	}
}

func TestConfig_BuildMongoURI(t *testing.T) {
	tests := []struct {
		name     string
		config   *Config
		expected string
	}{
		{
			name: "with credentials",
			config: &Config{
				MongoHost:     "localhost",
				MongoPort:     "27017",
				MongoUser:     "user",
				MongoPassword: "password",
			},
			expected: "mongodb://user:password@localhost:27017",
		},
		{
			name: "without credentials",
			config: &Config{
				MongoHost: "localhost",
				MongoPort: "27017",
			},
			expected: "mongodb://localhost:27017",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.config.BuildMongoURI()
			if result != tt.expected {
				t.Errorf("BuildMongoURI() = %v, want %v", result, tt.expected)
			}
		})
	}
}
