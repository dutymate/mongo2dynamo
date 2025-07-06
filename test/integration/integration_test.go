package integration

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// setupTestContainers sets up MongoDB and LocalStack containers for testing.
func setupTestContainers(t *testing.T) (mongoC, lsC testcontainers.Container, mongoHost, mongoPort, lsHost, lsPort string) {
	// Start MongoDB container.
	mongoC, err := testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "mongo:latest",
			ExposedPorts: []string{"27017/tcp"},
			WaitingFor:   wait.ForListeningPort(nat.Port("27017/tcp")),
		},
		Started: true,
	})
	require.NoError(t, err)

	// Get MongoDB host and port.
	mongoHost, err = mongoC.Host(context.Background())
	require.NoError(t, err)
	mongoPortMap, err := mongoC.MappedPort(context.Background(), "27017")
	require.NoError(t, err)
	mongoPort = mongoPortMap.Port()

	// Start LocalStack container.
	lsC, err = testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "localstack/localstack:latest",
			ExposedPorts: []string{"4566/tcp"},
			Env: map[string]string{
				"SERVICES":            "dynamodb",
				"DEFAULT_REGION":      "us-east-1",
				"SKIP_SSL_VALIDATION": "1",
			},
			WaitingFor: wait.ForListeningPort(nat.Port("4566/tcp")),
		},
		Started: true,
	})
	require.NoError(t, err)

	// Get LocalStack host and port.
	lsHost, err = lsC.Host(context.Background())
	require.NoError(t, err)
	lsPortMap, err := lsC.MappedPort(context.Background(), "4566")
	require.NoError(t, err)
	lsPort = lsPortMap.Port()

	return mongoC, lsC, mongoHost, mongoPort, lsHost, lsPort
}

// setupMongoDB sets up MongoDB with test data.
func setupMongoDB(t *testing.T, mongoHost, mongoPort string) *mongo.Client {
	ctx := context.Background()
	mongoURI := "mongodb://" + mongoHost + ":" + mongoPort
	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	require.NoError(t, err)

	// Insert test data.
	collection := mongoClient.Database("testdb").Collection("testcol")
	_, err = collection.InsertOne(ctx, bson.M{
		"_id":    "mongoid-001",
		"__v":    7,
		"_class": "com.example.MyEntity",
		"name":   "test",
		"value":  123,
	})
	require.NoError(t, err)

	return mongoClient
}

// setupDynamoDB sets up DynamoDB client (table will be created automatically by the loader).
func setupDynamoDB(t *testing.T, lsHost, lsPort string) *dynamodb.Client {
	ctx := context.Background()
	awsCfg, err := awsConfig.LoadDefaultConfig(ctx)
	require.NoError(t, err)

	client := dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://" + lsHost + ":" + lsPort)
	})

	return client
}

// createDynamoTable creates a DynamoDB table manually for testing.
func createDynamoTable(t *testing.T, client *dynamodb.Client, tableName string) {
	ctx := context.Background()

	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       types.KeyTypeHash,
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	// Wait for table to be created.
	for {
		table, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(tableName),
		})
		require.NoError(t, err)
		if table.Table.TableStatus == types.TableStatusActive {
			break
		}
		time.Sleep(500 * time.Millisecond) // Poll every 500ms.
	}
}

// TestApplyCommand_WithExistingTable tests the apply command when DynamoDB table already exists.
func TestApplyCommand_WithExistingTable(t *testing.T) {
	// Set up test containers.
	mongoC, lsC, mongoHost, mongoPort, lsHost, lsPort := setupTestContainers(t)
	defer func() {
		if err := mongoC.Terminate(context.Background()); err != nil {
			t.Logf("Warning: failed to terminate MongoDB container: %v", err)
		}
		if err := lsC.Terminate(context.Background()); err != nil {
			t.Logf("Warning: failed to terminate LocalStack container: %v", err)
		}
	}()

	// Set up AWS environment variables.
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	// Set up MongoDB.
	mongoClient := setupMongoDB(t, mongoHost, mongoPort)
	defer func() {
		if err := mongoClient.Disconnect(context.Background()); err != nil {
			t.Logf("Warning: failed to disconnect from MongoDB: %v", err)
		}
	}()

	// Set up DynamoDB and create table manually.
	dynamoClient := setupDynamoDB(t, lsHost, lsPort)
	createDynamoTable(t, dynamoClient, "test_table")

	// Run the apply command.
	cmd := exec.Command("go", "run", "../../main.go", "apply",
		"--mongo-host", mongoHost,
		"--mongo-port", mongoPort,
		"--mongo-db", "testdb",
		"--mongo-collection", "testcol",
		"--dynamo-table", "test_table",
		"--dynamo-endpoint", "http://"+lsHost+":"+lsPort,
		"--auto-approve",
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("CLI apply failed: %v\nOutput:\n%s", err, output)
		t.Fail()
	}

	// Verify apply command output messages.
	outputStr := string(output)
	require.Contains(t, outputStr, "Successfully migrated 1 documents", "Apply output should show successful migration")
	require.Contains(t, outputStr, "Table 'test_table' already exists.", "Should detect existing table")

	// Verify data in DynamoDB.
	result, err := dynamoClient.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String("test_table"),
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: "mongoid-001"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, result.Item)

	var item map[string]interface{}
	err = attributevalue.UnmarshalMap(result.Item, &item)
	require.NoError(t, err)
	require.Equal(t, "mongoid-001", item["id"])
	require.Equal(t, "test", item["name"])
	require.Equal(t, float64(123), item["value"])
	require.NotContains(t, item, "_id")
	require.NotContains(t, item, "__v")
	require.NotContains(t, item, "_class")
}

// TestApplyCommand_WithAutoCreateTable tests the apply command when DynamoDB table needs to be created automatically.
func TestApplyCommand_WithAutoCreateTable(t *testing.T) {
	// Set up test containers.
	mongoC, lsC, mongoHost, mongoPort, lsHost, lsPort := setupTestContainers(t)
	defer func() {
		if err := mongoC.Terminate(context.Background()); err != nil {
			t.Logf("Warning: failed to terminate MongoDB container: %v", err)
		}
		if err := lsC.Terminate(context.Background()); err != nil {
			t.Logf("Warning: failed to terminate LocalStack container: %v", err)
		}
	}()

	// Set up AWS environment variables.
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	// Set up MongoDB.
	mongoClient := setupMongoDB(t, mongoHost, mongoPort)
	defer func() {
		if err := mongoClient.Disconnect(context.Background()); err != nil {
			t.Logf("Warning: failed to disconnect from MongoDB: %v", err)
		}
	}()

	// Set up DynamoDB (table will be created automatically).
	dynamoClient := setupDynamoDB(t, lsHost, lsPort)

	// Run the apply command with auto-approve enabled.
	cmd := exec.Command("go", "run", "../../main.go", "apply",
		"--mongo-host", mongoHost,
		"--mongo-port", mongoPort,
		"--mongo-db", "testdb",
		"--mongo-collection", "testcol",
		"--dynamo-table", "test_table_auto",
		"--dynamo-endpoint", "http://"+lsHost+":"+lsPort,
		"--auto-approve",
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("CLI apply failed: %v\nOutput:\n%s", err, output)
		t.Fail()
	}

	// Verify apply command output messages.
	outputStr := string(output)
	require.Contains(t, outputStr, "Successfully migrated 1 documents", "Apply output should show successful migration")
	require.Contains(t, outputStr, "Auto-creating table 'test_table_auto'...", "Should show auto-creation message")
	require.Contains(t, outputStr, "Creating DynamoDB table 'test_table_auto'...", "Should show table creation message")
	require.Contains(t, outputStr, "Waiting for table 'test_table_auto' to become active...", "Should show waiting message")
	require.Contains(t, outputStr, "Table 'test_table_auto' is now active and ready for use.", "Should show table ready message")

	// Verify data in DynamoDB.
	result, err := dynamoClient.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String("test_table_auto"),
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: "mongoid-001"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, result.Item)

	var item map[string]interface{}
	err = attributevalue.UnmarshalMap(result.Item, &item)
	require.NoError(t, err)
	require.Equal(t, "mongoid-001", item["id"])
	require.Equal(t, "test", item["name"])
	require.Equal(t, float64(123), item["value"])
	require.NotContains(t, item, "_id")
	require.NotContains(t, item, "__v")
	require.NotContains(t, item, "_class")
}

// TestPlanCommand tests the plan command functionality.
func TestPlanCommand(t *testing.T) {
	// Set up test containers.
	mongoC, lsC, mongoHost, mongoPort, _, _ := setupTestContainers(t)
	defer func() {
		if err := mongoC.Terminate(context.Background()); err != nil {
			t.Logf("Warning: failed to terminate MongoDB container: %v", err)
		}
		if err := lsC.Terminate(context.Background()); err != nil {
			t.Logf("Warning: failed to terminate LocalStack container: %v", err)
		}
	}()

	// Set up MongoDB.
	mongoClient := setupMongoDB(t, mongoHost, mongoPort)
	defer func() {
		if err := mongoClient.Disconnect(context.Background()); err != nil {
			t.Logf("Warning: failed to disconnect from MongoDB: %v", err)
		}
	}()

	// Run the plan command.
	cmd := exec.Command("go", "run", "../../main.go", "plan",
		"--mongo-host", mongoHost,
		"--mongo-port", mongoPort,
		"--mongo-db", "testdb",
		"--mongo-collection", "testcol",
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("CLI plan failed: %v\nOutput:\n%s", err, output)
		t.Fail()
	}

	// Verify plan command output messages.
	outputStr := string(output)
	require.Contains(t, outputStr, "Found 1 documents to migrate", "Plan output should show found documents")
	require.NotContains(t, outputStr, "Successfully migrated", "Plan output should not show success message in dry run mode")
}

// TestVersionCommand tests the version command functionality.
func TestVersionCommand(t *testing.T) {
	// Run the version command.
	cmd := exec.Command("go", "run", "../../main.go", "version")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "CLI version command should not fail")

	// Verify version output format.
	outputStr := string(output)
	require.Contains(t, outputStr, "Version:")
	require.Contains(t, outputStr, "Git Commit:")
	require.Contains(t, outputStr, "Build Date:")

	// Verify that the output has the expected structure.
	lines := strings.Split(strings.TrimSpace(outputStr), "\n")
	require.Len(t, lines, 3, "Version output should have exactly 3 lines")

	// Verify each line format.
	require.Regexp(t, `^Version: .+$`, lines[0], "Version line should match expected format")
	require.Regexp(t, `^Git Commit: .+$`, lines[1], "Git Commit line should match expected format")
	require.Regexp(t, `^Build Date: .+$`, lines[2], "Build Date line should match expected format")
}
