package integration

import (
	"context"
	"os/exec"
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

// setupTestContainers sets up MongoDB and LocalStack containers for testing
func setupTestContainers(t *testing.T) (mongoC, lsC testcontainers.Container, mongoHost, mongoPort, lsHost, lsPort string) {
	// Start MongoDB container
	mongoC, err := testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "mongo:latest",
			ExposedPorts: []string{"27017/tcp"},
			WaitingFor:   wait.ForListeningPort(nat.Port("27017/tcp")),
		},
		Started: true,
	})
	require.NoError(t, err)

	// Get MongoDB host and port
	mongoHost, err = mongoC.Host(context.Background())
	require.NoError(t, err)
	mongoPortMap, err := mongoC.MappedPort(context.Background(), "27017")
	require.NoError(t, err)
	mongoPort = mongoPortMap.Port()

	// Start LocalStack container
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

	// Get LocalStack host and port
	lsHost, err = lsC.Host(context.Background())
	require.NoError(t, err)
	lsPortMap, err := lsC.MappedPort(context.Background(), "4566")
	require.NoError(t, err)
	lsPort = lsPortMap.Port()

	return mongoC, lsC, mongoHost, mongoPort, lsHost, lsPort
}

// setupMongoDB sets up MongoDB with test data
func setupMongoDB(t *testing.T, mongoHost, mongoPort string) *mongo.Client {
	ctx := context.Background()
	mongoURI := "mongodb://" + mongoHost + ":" + mongoPort
	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	require.NoError(t, err)

	// Insert test data
	collection := mongoClient.Database("testdb").Collection("testcol")
	_, err = collection.InsertOne(ctx, bson.M{"name": "test", "value": 123})
	require.NoError(t, err)

	return mongoClient
}

// setupDynamoDB sets up DynamoDB table
func setupDynamoDB(t *testing.T, lsHost, lsPort string) *dynamodb.Client {
	ctx := context.Background()
	awsCfg, err := awsConfig.LoadDefaultConfig(ctx)
	require.NoError(t, err)

	client := dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://" + lsHost + ":" + lsPort)
	})

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("test_table"),
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("name"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("name"),
				KeyType:       types.KeyTypeHash,
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	// Wait for table to be created
	time.Sleep(2 * time.Second)

	return client
}

// TestApplyCommand tests the apply command functionality
func TestApplyCommand(t *testing.T) {
	// Set up test containers
	mongoC, lsC, mongoHost, mongoPort, lsHost, lsPort := setupTestContainers(t)
	defer mongoC.Terminate(context.Background())
	defer lsC.Terminate(context.Background())

	// Set up AWS environment variables
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ENDPOINT_URL", "http://"+lsHost+":"+lsPort)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	// Set up MongoDB
	mongoClient := setupMongoDB(t, mongoHost, mongoPort)
	defer mongoClient.Disconnect(context.Background())

	// Set up DynamoDB
	dynamoClient := setupDynamoDB(t, lsHost, lsPort)

	// Run the apply command
	cmd := exec.Command("go", "run", "../main.go", "apply",
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

	// Verify data in DynamoDB
	result, err := dynamoClient.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String("test_table"),
		Key: map[string]types.AttributeValue{
			"name": &types.AttributeValueMemberS{Value: "test"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, result.Item)

	var item map[string]interface{}
	err = attributevalue.UnmarshalMap(result.Item, &item)
	require.NoError(t, err)
	require.Equal(t, "test", item["name"])
	require.Equal(t, float64(123), item["value"])
}

// TestPlanCommand tests the plan command functionality
func TestPlanCommand(t *testing.T) {
	// Set up test containers
	mongoC, lsC, mongoHost, mongoPort, _, _ := setupTestContainers(t)
	defer mongoC.Terminate(context.Background())
	defer lsC.Terminate(context.Background())

	// Set up MongoDB
	mongoClient := setupMongoDB(t, mongoHost, mongoPort)
	defer mongoClient.Disconnect(context.Background())

	// Run the plan command
	cmd := exec.Command("go", "run", "../main.go", "plan",
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

	// Verify plan output
	require.Contains(t, string(output), "Found 1 documents to migrate")
}
