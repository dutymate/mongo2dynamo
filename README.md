# mongo2dynamo

A command-line tool for migrating data from MongoDB to DynamoDB.

## Features

- Migrate data from MongoDB to DynamoDB
- Support for MongoDB authentication
- Support for local DynamoDB
- Dry run mode for testing
- Auto-approve option for automation

## Usage

mongo2dynamo provides two main commands: `plan` and `apply`.

### Plan Command

The `plan` command shows what would be migrated without actually performing the migration:

```bash
mongo2dynamo plan \
  --mongo-host localhost \
  --mongo-port 27017 \
  --mongo-db your_database \
  --mongo-collection your_collection \
  --dynamo-table your_table
```

### Apply Command

The `apply` command performs the actual migration:

```bash
mongo2dynamo apply \
  --mongo-host localhost \
  --mongo-port 27017 \
  --mongo-db your_database \
  --mongo-collection your_collection \
  --dynamo-table your_table
```

### Command-line Flags

> [!NOTE]
> \* Required for `apply` command, optional for `plan` command.

| Flag | Description | Required | Default |
|------|-------------|----------|---------|
| `--mongo-host` | MongoDB host | No | localhost |
| `--mongo-port` | MongoDB port | No | 27017 |
| `--mongo-user` | MongoDB username | No | - |
| `--mongo-password` | MongoDB password | No | - |
| `--mongo-db` | MongoDB database name | Yes | - |
| `--mongo-collection` | MongoDB collection name | Yes | - |
| `--dynamo-endpoint` | DynamoDB endpoint | No | http://localhost:8000 |
| `--dynamo-table` | DynamoDB table name | Yes* | - |
| `--aws-region` | AWS region | No | us-east-1 |
| `--auto-approve` | Skip confirmation prompt | No | false |
| `--dry-run` | Show what would be migrated without actually migrating | No | false |

## Environment Variables

You can also set configuration using environment variables:

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

## Configuration File

You can also create a configuration file at `~/.mongo2dynamo/config.yaml`:

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

## Contributing

Check out [Contributing guide](.github/CONTRIBUTING.md) for ideas on contributing and setup steps for getting our repositories up.

## License

Licensed under the [MIT License](LICENSE).
