.PHONY: test test-integration

# Run all tests
test:
	go test -v ./...

# Run integration tests
test-integration:
	go test -v -tags=integration ./integration/...

# Run tests with coverage
test-coverage:
	go test -v -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out

# Clean up test artifacts
clean:
	rm -f coverage.out 