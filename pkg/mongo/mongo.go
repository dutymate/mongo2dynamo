package mongo

import (
	"context"
	"mongo2dynamo/pkg/common"
	"mongo2dynamo/pkg/config"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Connect establishes a connection to MongoDB.
func Connect(ctx context.Context, cfg *config.Config) (*mongo.Client, error) {
	clientOpts := options.Client().ApplyURI(cfg.GetMongoURI())
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, &common.DatabaseConnectionError{Database: "MongoDB", Reason: err.Error(), Err: err}
	}

	// Ping the database to verify connection.
	if err := client.Ping(ctx, nil); err != nil {
		return nil, &common.DatabaseConnectionError{Database: "MongoDB", Reason: err.Error(), Err: err}
	}

	return client, nil
}
