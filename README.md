# SQS Consumer Library

A simple Python library for consuming messages from Amazon SQS queues with built-in health checks, graceful shutdown handling, and dead letter queue support.

## Features

- **Async Message Processing**: Threaded SQS message fetching with configurable batching
- **Health Check Endpoint**: Built-in HTTP health check server for container orchestration
- **Graceful Shutdown**: Proper signal handling and message requeuing on shutdown
- **Dead Letter Queue**: Automatic DLQ setup with configurable retry limits
- **Long Polling**: Efficient SQS long polling to reduce API calls
- **Configurable**: Environment-based configuration with sensible defaults
- **Logging**: Structured logging with performance metrics

## Installation

Install directly from the GitHub release:

```bash
pip install https://github.com/OneStepRND/sqs-consumer/releases/download/v0.8.0/sqs_consumer-0.8.0-py3-none-any.whl
```

## Quick Start

```python
from sqs_consumer import consume, GracefulShutdown, Message

def my_message_handler(message: Message):
    print(f"Processing message: {message.body}")
    # Your message processing logic here

# Set up graceful shutdown
shutdown = GracefulShutdown.with_signal()

# Start consuming (requires SQS_QUEUE_NAME environment variable)
consume(
    handler=my_message_handler,
    shutdown=shutdown
)
```

## Configuration

Configure the consumer using environment variables with the `SQS_` prefix:

### Required Configuration

```bash
export SQS_QUEUE_NAME="my-work-queue"
export SQS_DLQ_QUEUE_NAME="my-work-queue-dlq"
```

### Optional Configuration

```bash
# AWS SQS Settings
export SQS_ENDPOINT_URL="http://localhost:4566"  # For elasticmq
export SQS_MAX_MESSAGES=10                       # Messages per poll (1-10)
export SQS_WAIT_TIME_SECONDS=20                  # Long polling wait time (0-20)
export SQS_VISIBILITY_TIMEOUT=300                # Message visibility timeout (seconds)

# Consumer Settings
export SQS_FETCH_MIN_SLEEP=1                     # Min sleep between fetches (seconds)
export SQS_FETCH_MAX_SLEEP=3                     # Max sleep between fetches (seconds)
export SQS_FETCH_MIN_MESSAGES=5                  # Min messages in queue before sleeping

# Health Check
export SQS_HEALTH_CHECK_PORT=8080                # Health check HTTP port
export SQS_HEALTH_MAX_AGE=180                    # Max seconds between heartbeats

# Dead Letter Queue
export SQS_MAX_RETRIES_UNTIL_DLQ=3               # Retries before sending to DLQ
```

## Advanced Usage

### Custom Configuration

```python
from sqs_consumer import consume, Config, GracefulShutdown, Message

config = Config(
    queue_name="my-queue",
    dlq_queue_name="my-queue-dlq",
    max_messages=5,
    wait_time_seconds=10
)

def handler(message: Message):
    print(f"Message ID: {message.id}")
    print(f"Body: {message.body}")
    print(f"Receipt Handle: {message.receipt}")

shutdown = GracefulShutdown.with_signal()
consume(handler=handler, config=config, shutdown=shutdown)
```

### Custom SQS Client

```python
import boto3
from sqs_consumer import consume, GracefulShutdown

# Custom SQS client (e.g., for specific region or credentials)
sqs_client = boto3.client(
    'sqs',
    region_name='us-west-2',
    aws_access_key_id='your-key',
    aws_secret_access_key='your-secret'
)

shutdown = GracefulShutdown.with_signal()
consume(
    handler=my_handler,
    sqs=sqs_client,
    shutdown=shutdown
)
```

### Manual Shutdown Control

```python
from sqs_consumer import consume, GracefulShutdown
import threading
import time

shutdown = GracefulShutdown()

# Start consumer in background thread
consumer_thread = threading.Thread(
    target=consume,
    kwargs={
        'handler': my_handler,
        'shutdown': shutdown
    }
)
consumer_thread.start()

# Shutdown after 60 seconds
time.sleep(60)
shutdown.shutdown()
consumer_thread.join()
```

## Health Checks

The library provides a built-in HTTP health check endpoint at `/health`:

- **200 OK**: Consumer is healthy (received heartbeat within configured time)
- **503 Service Unavailable**: Consumer is unhealthy
- **404 Not Found**: Any other endpoint

```bash
curl http://localhost:8080/health
```

Perfect for Docker health checks:

```dockerfile
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1
```

## Docker Example

```dockerfile
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY . .

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

CMD ["python", "main.py"]
```

## Message Processing

The `Message` object provides access to SQS message data:

```python
def handler(message: Message):
    # Raw message body (string)
    body = message.body

    # Unique message ID
    message_id = message.id

    # Receipt handle (for manual operations)
    receipt_handle = message.receipt

    # Parse JSON if needed
    import json
    try:
        data = json.loads(body)
        process_data(data)
    except json.JSONDecodeError:
        print(f"Invalid JSON in message {message_id}")
```

## Error Handling

- **Processing Errors**: If your handler raises an exception, the message remains in the queue and will be retried
- **Max Retries**: After `max_retries_until_dlq` attempts, messages are moved to the dead letter queue
- **Graceful Shutdown**: On shutdown, any unprocessed messages are returned to the queue

## Queue Management

The library automatically:

1. **Creates queues** if they don't exist
2. **Sets up DLQ** with proper redrive policy
3. **Configures timeouts** and retention periods
4. **Handles queue attributes** for optimal performance

## Monitoring and Logging

The library uses Python's standard logging with structured extra fields. **Recommended**: Use JSON logging for better observability:


### Example Consumer Application

```python
# app.py
import json
import logging
from sqs_consumer import consume, GracefulShutdown, Message

logging.basicConfig(level=logging.INFO)

def process_message(message: Message):
    try:
        data = json.loads(message.body)
        print(f"Processing: {data}")

        # Simulate processing time
        import time
        time.sleep(1)

        print(f"Completed message {data.get('id', 'unknown')}")
    except json.JSONDecodeError:
        print(f"Invalid JSON in message: {message.body}")
    except Exception as e:
        print(f"Error processing message: {e}")
        raise  # Re-raise to trigger retry

if __name__ == "__main__":
    shutdown = GracefulShutdown.with_signal()
    consume(handler=process_message, shutdown=shutdown)
```
## Best Practices

1. **Idempotent Handlers**: Design message handlers to be idempotent in case of retries
2. **Error Handling**: Catch and handle exceptions in your message handler appropriately
3. **Timeouts**: Set `visibility_timeout` longer than your maximum processing time
4. **Batch Size**: Adjust `max_messages` based on your processing speed and memory constraints
5. **Monitoring**: Monitor the health check endpoint and DLQ for stuck messages