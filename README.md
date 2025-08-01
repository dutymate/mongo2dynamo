# mongo2dynamo

<p align="center">
  <img src="images/logo.png" alt="mongo2dynamo Logo" width="200"/>
</p>

**mongo2dynamo** is a high-performance, command-line tool for migrating data from MongoDB to DynamoDB.

[![Build](https://github.com/dutymate/mongo2dynamo/actions/workflows/build.yaml/badge.svg)](https://github.com/dutymate/mongo2dynamo/actions/workflows/build.yaml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Commands](#commands)
- [How It Works](#how-it-works)
- [License](#license)

## Features

mongo2dynamo is designed for efficient and reliable data migration, incorporating several key features for performance and stability.

-   **High-Performance Transformation**: Utilizes a **dynamic worker pool** that scales based on CPU cores (from 2 to 2x `runtime.NumCPU()`) with real-time workload monitoring. Workers auto-scale every 500ms based on pending jobs, maximizing parallel processing efficiency.
-   **Optimized Memory Management**: Implements strategic memory allocation - extractor uses `ChunkPool` for efficient slice reuse during document streaming, while transformer uses direct allocation with pre-calculated capacity for optimal performance based on benchmarking.
-   **Robust Loading Mechanism**: Implements a reliable data loading strategy for DynamoDB using the `BatchWriteItem` API with a **concurrent worker pool**. Features **Exponential Backoff with Jitter** algorithm to automatically handle DynamoDB throttling exceptions, ensuring smooth migration process.
-   **Memory-Efficient Extraction**: Employs a streaming approach to extract data from MongoDB in configurable chunks (default: 2000 documents), minimizing memory footprint even with large datasets. Supports MongoDB query filters and projections for selective migration.
-   **Intelligent Field Processing**: Automatically converts MongoDB-specific fields (`_id` → `id`) and removes framework metadata (`__v`, `_class`). Pre-calculates output document capacity to minimize memory allocations during transformation.
-   **Fine-Grained Error Handling**: Defines domain-specific custom error types for each stage of the ETL process (Extract, Transform, Load). This enables precise error identification and facilitates targeted recovery logic.
-   **Comprehensive CLI**: Built with `Cobra`, providing a user-friendly command-line interface with `plan` (dry-run) and `apply` commands, flexible configuration options (flags, env vars, config file), and an `--auto-approve` flag for non-interactive execution.
-   **Automatic Table Management**: Automatically creates DynamoDB tables if they don't exist, with user confirmation prompts (unless auto-approved). **Supports custom primary keys (Partition and Sort Keys).** Waits for table activation before proceeding with migration.

## Installation

### Homebrew

```bash
brew tap dutymate/tap
brew install mongo2dynamo
```

### Download Binary

Download the latest release from the [releases page](https://github.com/dutymate/mongo2dynamo/releases).

### Build from Source

```bash
git clone https://github.com/dutymate/mongo2dynamo.git
cd mongo2dynamo
make build
```

## Quick Start

```bash
# Preview migration
mongo2dynamo plan --mongo-db mydb --mongo-collection users

# Execute migration with a custom primary key (Partition + Sort Key)
mongo2dynamo apply --mongo-db mydb --mongo-collection events \
  --dynamo-table user-events \
  --dynamo-partition-key event_id \
  --dynamo-partition-key-type S \
  --dynamo-sort-key timestamp \
  --dynamo-sort-key-type N

# With filter and auto-approve
mongo2dynamo apply --mongo-db mydb --mongo-collection users \
  --mongo-filter '{"status": "active"}' \
  --auto-approve

# With projection to select specific fields
mongo2dynamo apply --mongo-db mydb --mongo-collection users \
  --mongo-projection '{"name": 1, "email": 1, "_id": 0}' \
  --auto-approve
```

## Configuration

Configuration can be provided via command-line flags, environment variables, or a YAML configuration file. The order of precedence is:
1. Command-Line Flags
2. Environment Variables
3. Configuration File
4. Default Values

### Command-Line Flags

**MongoDB Flags**

| Flag | Description | Default |
| --- | --- | --- |
| `--mongo-host` | MongoDB host. | `localhost` |
| `--mongo-port` | MongoDB port. | `27017` |
| `--mongo-user` | MongoDB username. | ` ` |
| `--mongo-password` | MongoDB password. | ` ` |
| `--mongo-db` | **(Required)** MongoDB database name. | ` ` |
| `--mongo-collection` | **(Required)** MongoDB collection name. | ` ` |
| `--mongo-filter` | MongoDB query filter as a JSON string. | ` ` |
| `--mongo-projection` | MongoDB projection as a JSON string to select specific fields. | ` ` |

**DynamoDB Flags**

| Flag | Description | Default |
| --- | --- | --- |
| `--dynamo-endpoint` | DynamoDB endpoint. | `http://localhost:8000` |
| `--dynamo-table` | DynamoDB table name. | MongoDB collection name |
| `--dynamo-partition-key` | The attribute name for the partition key. | `id` |
| `--dynamo-partition-key-type` | The attribute type for the partition key (S, N, B). | `S` |
| `--dynamo-sort-key` | The attribute name for the sort key. (Optional) | ` ` |
| `--dynamo-sort-key-type` | The attribute type for the sort key (S, N, B). | `S` |
| `--aws-region` | AWS region. | `us-east-1` |
| `--max-retries` | Maximum retries for failed DynamoDB batch writes. | `5` |

**Apply Control Flags**

| Flag | Description | Default |
| --- | --- | --- |
| `--auto-approve` | Skip all confirmation prompts. | `false` |

### Environment Variables

```bash
export MONGO2DYNAMO_MONGO_HOST=localhost
export MONGO2DYNAMO_MONGO_PORT=27017
export MONGO2DYNAMO_MONGO_USER=your_username
export MONGO2DYNAMO_MONGO_PASSWORD=your_password
export MONGO2DYNAMO_MONGO_DB=your_database
export MONGO2DYNAMO_MONGO_COLLECTION=your_collection
export MONGO2DYNAMO_MONGO_FILTER='{"status": "active"}'
export MONGO2DYNAMO_MONGO_PROJECTION='{"name": 1, "email": 1, "_id": 0}'
export MONGO2DYNAMO_DYNAMO_ENDPOINT=http://localhost:8000
export MONGO2DYNAMO_DYNAMO_TABLE=your_table
export MONGO2DYNAMO_DYNAMO_PARTITION_KEY=id
export MONGO2DYNAMO_DYNAMO_PARTITION_KEY_TYPE=S
export MONGO2DYNAMO_DYNAMO_SORT_KEY=timestamp
export MONGO2DYNAMO_DYNAMO_SORT_KEY_TYPE=N
export MONGO2DYNAMO_AWS_REGION=us-east-1
export MONGO2DYNAMO_MAX_RETRIES=5
export MONGO2DYNAMO_AUTO_APPROVE=false
```

### Config File

Create `~/.mongo2dynamo/config.yaml`:

```yaml
mongo_host: localhost
mongo_port: 27017
mongo_user: your_username
mongo_password: your_password
mongo_db: your_database
mongo_collection: your_collection
mongo_filter: '{"status": "active"}'
mongo_projection: '{"name": 1, "email": 1, "_id": 0}'
dynamo_endpoint: http://localhost:8000
dynamo_table: your_table
dynamo_partition_key: id
dynamo_partition_key_type: S
dynamo_sort_key: timestamp
dynamo_sort_key_type: N
aws_region: us-east-1
max_retries: 5
auto_approve: false
```

## Commands

### `plan` - Preview Migration

Performs a dry-run to preview the migration by executing the full ETL pipeline without loading to DynamoDB.

**Features:**
- Connects to MongoDB and validates configuration.
- Extracts documents from MongoDB (with filters and projections if specified).
- Transforms documents to DynamoDB format using dynamic worker pools.
- Counts the total number of documents that would be migrated.
- No data is loaded to DynamoDB (dry-run mode).

**Example Output:**
```text
Starting migration plan analysis...
Found 1,234 documents to migrate.
```

### `apply` - Execute Migration

Executes the complete ETL pipeline to migrate data from MongoDB to DynamoDB.

**Features:**
- Full ETL pipeline execution (Extract → Transform → Load).
- Configuration validation and user confirmation prompts.
- Automatic DynamoDB table creation (with confirmation).
- Batch processing with optimized chunk sizes (1000 documents per MongoDB batch, 2000 documents per extraction chunk, 25 documents per DynamoDB batch, concurrent loader workers).
- Dynamic worker pool scaling for optimal performance.
- Retry logic for failed operations (configurable via `--max-retries`).

**Example Output:**
```text
Creating DynamoDB table 'users'...
Waiting for table 'users' to become active...
Table 'users' is now active and ready for use.
Starting data migration from MongoDB to DynamoDB...
Successfully migrated 1,234 documents.
```

### `version` - Show Version

Displays version information including Git commit and build date.

## How It Works

mongo2dynamo follows a standard Extract, Transform, Load (ETL) architecture with parallel processing capabilities. Each stage is designed to perform its task efficiently and reliably.

### Pipeline Architecture
- **Parallel Processing**: The ETL stages run concurrently using Go channels with a buffer size of 10, allowing extraction, transformation, and loading to happen simultaneously for maximum throughput.
- **Strategic Memory Optimization**: Components use independent memory strategies optimized for their specific workloads - extractor leverages `ChunkPool` for slice reuse, while transformer uses direct allocation for maximum speed.

```mermaid
%%{init: { 'theme': 'neutral' } }%%
flowchart LR
    subgraph Input Source
        MongoDB[(fa:fa-database MongoDB)]
    end

    subgraph mongo2dynamo
        direction LR
        subgraph "Extract"
            Extractor("fa:fa-cloud-download Extractor<br/><br/>Streams documents<br/>from the source collection.")
        end
        
        subgraph "Transform"
            Transformer("fa:fa-cogs Transformer<br/><br/>Uses a dynamic worker pool to process documents in parallel.")
        end

        subgraph "Load"
            Loader("fa:fa-upload Loader<br/><br/>Writes data using BatchWriteItem API.<br/>Handles throttling with<br/>Exponential Backoff + Jitter.")
        end
    end

    subgraph Output Target
        DynamoDB[(fa:fa-database DynamoDB)]
    end

    MongoDB -- Documents --> Extractor
    Extractor -- Raw Documents --> Transformer
    Transformer -- Transformed Items --> Loader
    Loader -- Batched Items --> DynamoDB

    style Extractor fill:#e6f3ff,stroke:#333
    style Transformer fill:#fff2e6,stroke:#333
    style Loader fill:#e6ffed,stroke:#333
```

### 1. Extraction
- Connects to MongoDB using optimized connection settings with configurable batch sizes (default: 1000 documents per batch).
- Uses a streaming approach with `ChunkPool` memory reuse to handle large datasets efficiently.
- Processes documents in configurable chunks (default: 2000 documents) to maintain low memory footprint.
- Applies user-defined filters (`--mongo-filter`) with JSON-to-BSON conversion for selective data migration.
- Implements robust error handling for connection, decode, and cursor operations.

### 2. Transformation
- Utilizes a **dynamic worker pool** starting with CPU core count, scaling up to 2x CPU cores based on workload.
- **Intelligent scaling**: Workers auto-adjust every 500ms with optimized thresholds (scale up at 80% load, scale down at 30% load).
- **Bidirectional scaling**: Automatically scales down when workload decreases to optimize resource usage.
- **Memory optimization**: Pre-calculates field counts to allocate maps with optimal capacity, reducing garbage collection overhead.
- **Field processing**: Converts `_id` to `id` with intelligent type handling (ObjectID → hex, bson.M → JSON), removes `__v` and `_class` fields.
- Implements panic recovery and comprehensive error reporting for worker failures.

### 3. Loading
- Uses a **concurrent worker pool** to maximize DynamoDB throughput with parallel batch processing.
- Groups documents into optimal batches of 25 items per `BatchWriteItem` request (DynamoDB limit).
- **Advanced retry logic**: Implements exponential backoff with jitter (100ms to 30s) for unprocessed items, with configurable max retries (default: 5).
- **Automatic table management**: Creates tables with hash key schema if they don't exist, waits for table activation.
- Handles context cancellation gracefully across all worker goroutines.

## License

Licensed under the [MIT License](LICENSE).
