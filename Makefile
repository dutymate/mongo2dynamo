GIT_COMMIT := $(shell git rev-parse HEAD)
BUILD_DATE := $(shell date +%Y-%m-%dT%H:%M:%S%z)

BIN_DIR := bin

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

# Install dependencies.
.PHONY: deps
deps:
	@echo "Installing dependencies..."
	go version
	go mod tidy
	go mod vendor

# Lint the code.
.PHONY: lint
lint:
	golangci-lint run

# Format the code.
.PHONY: fmt
fmt:
	go fmt ./...
	goimports -w .

# Build the application.
.PHONY: build
build: deps
	@echo "Building mongo2dynamo..."
	@echo "GIT_COMMIT=$(GIT_COMMIT)"
	@echo "BUILD_DATE=$(BUILD_DATE)"
	go build -ldflags="-s -w -X mongo2dynamo/internal/version.GitCommit=$(GIT_COMMIT) -X mongo2dynamo/internal/version.BuildDate=$(BUILD_DATE)" -o $(BIN_DIR)/mongo2dynamo main.go
	@echo "Done"

# Run all tests.
.PHONY: test
test:
	go test -v ./...

# Run integration tests.
.PHONY: test-integration
test-integration:
	go test -v ./test/integration/...

# Run tests with coverage.
.PHONY: test-coverage
test-coverage:
	go test -v -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out

# Clean up test artifacts.
.PHONY: clean
clean:
	@echo "Cleaning up..."
	rm -rf $(BIN_DIR)

.DEFAULT_GOAL := build
