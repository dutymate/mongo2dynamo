package mongo

import (
	"context"
	"errors"
	"strings"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"mongo2dynamo/internal/common"
)

const (
	MongoErrorCodeUnauthorized         = 13
	MongoErrorCodeAuthenticationFailed = 18
	MongoErrorCodeIllegalOperation     = 35
	MongoErrorCodeMechanismUnavailable = 334
)

// mongoAuthErrorPatterns contains common authentication failure patterns for MongoDB errors.
var mongoAuthErrorPatterns = []string{
	"authentication failed",
	"auth failed",
	"unauthorized",
	"invalid credentials",
	"authentication error",
	"sasl authentication",
	"bad auth",
	"auth source",
	"authentication mechanism",
}

// Connect establishes a connection to MongoDB.
func Connect(ctx context.Context, cfg common.ConfigProvider) (*mongo.Client, error) {
	clientOpts := options.Client().ApplyURI(cfg.GetMongoURI())

	// Explicitly set authentication credentials if provided.
	if cfg.GetMongoUser() != "" && cfg.GetMongoPassword() != "" {
		clientOpts.SetAuth(options.Credential{
			Username: cfg.GetMongoUser(),
			Password: cfg.GetMongoPassword(),
		})
	}

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		if isMongoAuthError(err) {
			return nil, &common.AuthError{
				Database: "MongoDB",
				Op:       "connect",
				Reason:   err.Error(),
				Err:      err,
			}
		}
		return nil, &common.DatabaseConnectionError{Database: "MongoDB", Reason: err.Error(), Err: err}
	}

	// Ping the database to verify connection.
	if err := client.Ping(ctx, nil); err != nil {
		// Disconnect the client to avoid resource leaks.
		_ = client.Disconnect(ctx)
		if isMongoAuthError(err) {
			return nil, &common.AuthError{
				Database: "MongoDB",
				Op:       "ping",
				Reason:   err.Error(),
				Err:      err,
			}
		}
		return nil, &common.DatabaseConnectionError{Database: "MongoDB", Reason: err.Error(), Err: err}
	}

	return client, nil
}

// isMongoAuthError checks if the error is related to MongoDB authentication failure.
func isMongoAuthError(err error) bool {
	if err == nil {
		return false
	}

	// Check for CommandError type.
	var cmdErr mongo.CommandError
	if errors.As(err, &cmdErr) {
		return isAuthErrorCode(cmdErr.Code)
	}

	// Check for WriteException type.
	var writeErr mongo.WriteException
	if errors.As(err, &writeErr) {
		for _, we := range writeErr.WriteErrors {
			if isAuthErrorCode(int32(we.Code)) {
				return true
			}
		}
	}

	// Fallback to string pattern matching.
	return containsAuthErrorPattern(err.Error())
}

// isAuthErrorCode returns true if the code is a known MongoDB auth error code.
func isAuthErrorCode(code int32) bool {
	switch code {
	case MongoErrorCodeUnauthorized, MongoErrorCodeAuthenticationFailed, MongoErrorCodeMechanismUnavailable, MongoErrorCodeIllegalOperation:
		return true
	default:
		return false
	}
}

// containsAuthErrorPattern checks if the error message contains any authentication failure patterns.
func containsAuthErrorPattern(errMsg string) bool {
	errMsg = strings.ToLower(errMsg)
	for _, pattern := range mongoAuthErrorPatterns {
		if strings.Contains(errMsg, pattern) {
			return true
		}
	}
	return false
}
