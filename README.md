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

- **Reliable MongoDB → DynamoDB migration**
- **Batch-based, memory-efficient processing**
- **Auto-approve and interactive confirmation**
- **Flexible configuration** (CLI flags, environment variables, config file)
- **Error handling and retry logic**

## Why mongo2dynamo?

- **Reliability**: Safely migrates data without loss or duplication
- **Scalability**: Handles millions of documents efficiently
- **Easy to Use**: Intuitive CLI and configuration options

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

mongo2dynamo follows a simple yet robust two-step process:

### 1. Plan Phase
The `plan` command performs a dry-run of the migration:
- Connects to MongoDB and counts documents in the specified collection
- Displays the total number of documents that would be migrated
- No actual data transfer occurs during this phase

### 2. Apply Phase
The `apply` command executes the actual migration:
- **Connection Setup**: Establishes connections to both MongoDB and DynamoDB
- **Batch Reading**: Reads documents from MongoDB in configurable chunks (default: 1000 documents)
- **Data Processing**: Converts MongoDB BSON documents to DynamoDB format
- **Batch Writing**: Writes data to DynamoDB using BatchWriteItem API
- **Error Handling**: Implements retry logic with exponential backoff for failed operations

## License

Licensed under the [MIT License](LICENSE).
