package mongo

import (
	"context"
	"mongo2dynamo/internal/config"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Connect establishes a connection to MongoDB
func Connect(cfg *config.Config) (*mongo.Client, error) {
	clientOpts := options.Client().ApplyURI(cfg.GetMongoURI())
	client, err := mongo.Connect(context.Background(), clientOpts)
	if err != nil {
		return nil, err
	}

	// Ping the database to verify connection
	if err := client.Ping(context.Background(), nil); err != nil {
		return nil, err
	}

	return client, nil
}
