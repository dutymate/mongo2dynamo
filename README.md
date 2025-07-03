# mongo2dynamo

<p align="center">
  <img src="images/logo.png" alt="mongo2dynamo Logo" width="200"/>
</p>

**mongo2dynamo** is a command-line tool for migrating data from MongoDB to DynamoDB.

[![Build](https://github.com/dutymate/mongo2dynamo/actions/workflows/build.yaml/badge.svg)](https://github.com/dutymate/mongo2dynamo/actions/workflows/build.yaml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

- [Features](#features)
- [Why mongo2dynamo?](#why-mongo2dynamo)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [How It Works](#how-it-works)
- [License](#license)

## Features

- **ETL-based MongoDB → DynamoDB migration**: Extracts data from MongoDB collections, transforms it, and loads it into DynamoDB tables, minimizing the risk of data loss or duplication.
- **Batch-based, memory-efficient processing**: Extracts and loads data in configurable batches (default: 1000 documents per chunk), allowing efficient handling of large datasets without excessive memory usage.
- **Auto-approve and interactive confirmation**: Supports both automated ETL runs (for CI/CD or scripting) and interactive confirmation prompts to prevent accidental data transfers.
- **Flexible configuration**: Easily configure all options via command-line flags, environment variables, or a YAML config file—whichever fits your workflow best.
- **Error handling and retry logic**: Automatically retries failed extract/load operations with exponential backoff, and provides clear error messages to help you quickly resolve issues.
- **Dry-run support**: Use the `plan` command to preview ETL operations before performing any actual data transfer.

## Why mongo2dynamo?

- **Reliability**: Safely extracts, transforms, and loads data without loss or duplication.
- **Scalability**: Handles millions of documents efficiently with ETL best practices.
- **Easy to Use**: Intuitive CLI and configuration options.

## Installation

### Homebrew

```bash
brew tap dutymate/tap
brew install mongo2dynamo
```

### Download Binary

Download the latest release from the [releases page](https://github.com/dutymate/mongo2dynamo/releases).

## Quick Start

### 1. Preview Migration Plan

```bash
mongo2dynamo plan \
  --mongo-host localhost \
  --mongo-port 27017 \
  --mongo-db your_database \
  --mongo-collection your_collection
```

### 2. Run Actual Migration

```bash
mongo2dynamo apply \
  --mongo-host localhost \
  --mongo-port 27017 \
  --mongo-db your_database \
  --mongo-collection your_collection \
  --dynamo-endpoint your_endpoint \
  --dynamo-table your_table
```

## Configuration

### Environment Variables

```bash
export MONGO2DYNAMO_MONGO_HOST=localhost
export MONGO2DYNAMO_MONGO_PORT=27017
export MONGO2DYNAMO_MONGO_USER=your_username
export MONGO2DYNAMO_MONGO_PASSWORD=your_password
export MONGO2DYNAMO_MONGO_DB=your_database
export MONGO2DYNAMO_MONGO_COLLECTION=your_collection
export MONGO2DYNAMO_DYNAMO_TABLE=your_table
export MONGO2DYNAMO_DYNAMO_ENDPOINT=http://localhost:8000
export MONGO2DYNAMO_AWS_REGION=us-east-1
```

### Config File

Configuration file at `~/.mongo2dynamo/config.yaml`:

```yaml
mongo_host: localhost
mongo_port: 27017
mongo_user: your_username
mongo_password: your_password
mongo_db: your_database
mongo_collection: your_collection
dynamo_table: your_table
dynamo_endpoint: http://localhost:8000
aws_region: us-east-1
```

## How It Works

mongo2dynamo follows a robust ETL (Extract, Transform, Load) process:

### 1. Plan Phase
The `plan` command performs a dry-run of the ETL process:
- **Connection Setup**: Establishes connection to MongoDB using the Extractor.
- **Document Counting**: Counts documents in the specified collection to estimate migration scope.
- **Preview Display**: Shows the total number of documents that would be processed.
- **No Data Transfer**: No actual extraction, transformation, or loading occurs during this phase.

### 2. Apply Phase
The `apply` command executes the full ETL pipeline:
- **Connection Setup**: Establishes connections to both MongoDB and DynamoDB.
- **Extraction**: Extracts documents from MongoDB in configurable batches (default: 1000 documents per chunk).
- **Transformation**: Transforms MongoDB BSON documents to DynamoDB-compatible format.
- **Loading**: Loads transformed data into DynamoDB using the BatchWriteItem API.
- **Error Handling**: Implements retry logic with exponential backoff for failed operations.

## License

Licensed under the [MIT License](LICENSE).
