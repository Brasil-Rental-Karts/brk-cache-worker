# BRK Cache Worker

A Node.js worker service that consumes messages from RabbitMQ and updates a Redis cache based on the operation type (INSERT, UPDATE, DELETE).

## Overview

This worker is designed to maintain a Redis cache that mirrors data from a primary database. It listens for change events via RabbitMQ messages and performs the corresponding operations on the Redis cache.

### Supported Operations

- **INSERT**: Adds a new entry to the Redis cache
- **UPDATE**: Updates an existing entry in the Redis cache
- **DELETE**: Removes an entry from the Redis cache

## Message Format

The worker expects messages in the following JSON format:

```json
{
  "operation": "INSERT|UPDATE|DELETE",
  "table": "EntityName",
  "timestamp": 1747002033.158324,
  "data": {
    "id": "uuid-value",
    "field1": "value1",
    "field2": "value2"
    // ... other fields
  }
}
```

## Redis Key Format

Keys in Redis are stored using the format: `tableName:id`

For example: `clubs:dae88056-e830-461a-b299-000aac64b4ce`

## Setup

### Prerequisites

- Node.js (v14 or higher)
- RabbitMQ server
- Redis server

### Installation

1. Clone the repository
2. Install dependencies:
   ```
   npm install
   ```
3. Create a `.env` file based on the `.env.example` template:
   ```
   cp .env.example .env
   ```
4. Update the `.env` file with your RabbitMQ and Redis configuration

## Running the Worker

### Development

```
npm run dev
```

### Production

```
npm start
```

## Environment Variables

### RabbitMQ Configuration
- `RABBITMQ_URL`: Connection URL for RabbitMQ (default: `amqp://guest:guest@localhost:5672`)
- `RABBITMQ_QUEUE`: Queue name to consume messages from
- `RABBITMQ_EXCHANGE`: Exchange name
- `RABBITMQ_ROUTING_KEY`: Routing key for binding queue to exchange

#### Dead Letter Queue (DLQ) Configuration
The worker now supports sending messages with unsupported operations to a Dead Letter Queue (DLQ) instead of throwing errors. The following environment variables can be configured:

- `RABBITMQ_DLQ_QUEUE`: DLQ queue name (default: `{RABBITMQ_QUEUE}_dlq`)
- `RABBITMQ_DLQ_EXCHANGE`: DLQ exchange name (default: `{RABBITMQ_EXCHANGE}_dlq`)
- `RABBITMQ_DLQ_ROUTING_KEY`: DLQ routing key (default: `{RABBITMQ_ROUTING_KEY}.dlq`)

Messages are sent to the DLQ in the following cases:
- When the operation type is not supported (not INSERT, UPDATE, or DELETE)
- When the message format is invalid (missing required fields)

### Redis Configuration
- `REDIS_HOST`: Redis server hostname (default: `localhost`)
- `REDIS_PORT`: Redis server port (default: `6379`)
- `REDIS_PASSWORD`: Redis password (if required)
- `REDIS_DB`: Redis database number (default: `0`)

### Logging
- `LOG_LEVEL`: Logging level (default: `info`)