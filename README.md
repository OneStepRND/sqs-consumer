# SQS Consumer Library

A simple Python library for consuming messages from Amazon SQS queues with built-in health checks, graceful shutdown handling, and dead letter queue support.

## Features

- **Sync and async handlers**: A coroutine handler runs on a single long-lived event loop owned by the consume loop, one message at a time
- **Threaded prefetch**: Background SQS fetching into a bounded local buffer with configurable batching
- **Visibility heartbeat**: The in-flight message keeps its visibility extended, and the liveness heartbeat stays fresh, for as long as the handler runs
- **Bounded processing time**: `max_processing_time` cancels an async handler and leaves the message for redelivery
- **Health Check Endpoint**: Built-in HTTP health check server for container orchestration
- **Graceful Shutdown**: Signal handling, a grace period for the in-flight handler, and message requeuing on shutdown
- **Error policy**: Crash on a handler exception, or log it and keep consuming
- **FIFO queues**: FIFO queue and DLQ creation, plus a `send` helper for message group and deduplication ids
- **Dead Letter Queue**: Automatic DLQ setup with configurable retry limits
- **Pre-provisioned queues**: Consume from a queue URL, or resolve it by name, without queue-management permissions
- **Long Polling**: Efficient SQS long polling to reduce API calls
- **Configurable**: Environment-based configuration with sensible defaults
- **Logging**: Structured logging with performance metrics

## Installation

Install directly from the GitHub release:

```bash
pip install https://github.com/OneStepRND/sqs-consumer/releases/download/v0.11.0/sqs_consumer-0.11.0-py3-none-any.whl
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

### Queue Identity

Either a queue name or an explicit queue URL is required. With a name and
`SQS_CREATE_QUEUES` enabled the queue and its DLQ are created when missing, which
needs queue-management permissions:

```bash
export SQS_QUEUE_NAME="my-work-queue"
export SQS_DLQ_QUEUE_NAME="my-work-queue-dlq"
```

For a pre-provisioned queue, pass the URL (no queue lookup at all) or the name
with creation disabled (one `GetQueueUrl` call at startup):

```bash
export SQS_QUEUE_URL="https://sqs.us-east-1.amazonaws.com/123456789012/my-work-queue"
# or
export SQS_QUEUE_NAME="my-work-queue"
export SQS_CREATE_QUEUES=false
```

### Optional Configuration

```bash
# AWS SQS Settings
export SQS_ENDPOINT_URL="http://localhost:4566"  # For elasticmq
export SQS_MAX_MESSAGES=10                       # Messages per poll (1-10)
export SQS_WAIT_TIME_SECONDS=20                  # Long polling wait time (0-20)
export SQS_VISIBILITY_TIMEOUT=300                # Message visibility timeout (seconds)

# Handler Settings
export SQS_ON_ERROR=crash                        # crash | continue on a handler exception
export SQS_MAX_PROCESSING_TIME=600               # Cancel the async handler after N seconds
export SQS_SHUTDOWN_GRACE_PERIOD=30              # Seconds the in-flight handler gets on shutdown

# Consumer Settings
export SQS_FETCH_MIN_SLEEP=1                     # Min sleep between fetches (seconds)
export SQS_FETCH_MAX_SLEEP=3                     # Max sleep between fetches (seconds)
export SQS_FETCH_MIN_MESSAGES=5                  # Min messages in queue before sleeping
export SQS_MAX_BUFFERED_MESSAGES=30              # Local buffer capacity (default: max_messages * 3)

# Health Check
export SQS_HEALTH_CHECK_PORT=8080                # Health check HTTP port
export SQS_HEALTH_MAX_AGE=180                    # Max seconds between heartbeats

# Dead Letter Queue
export SQS_MAX_RETRIES_UNTIL_DLQ=3               # Retries before sending to DLQ
```

## Advanced Usage

### Async Handlers

A coroutine handler runs on a single event loop owned by the consume loop, one
message at a time:

```python
from sqs_consumer import consume, Config, GracefulShutdown, Message

async def handler(message: Message) -> None:
    await do_work(message.body)

config = Config(
    queue_name="my-queue",
    dlq_queue_name="my-queue-dlq",
    max_messages=1,
    max_buffered_messages=1,
    visibility_timeout=300,
    max_processing_time=900,
    on_error="continue",
)

consume(handler=handler, config=config, shutdown=GracefulShutdown.with_signal())
```

### Long-Running Handlers

When the handler starts, the consumer extends the visibility of the in-flight
message immediately — rebasing the visibility window onto the handler rather than
onto the earlier receive — and then every `visibility_timeout / 2` seconds. It
refreshes the liveness heartbeat on the same tick, so a handler longer than
`health_max_age` keeps passing the health probe. `visibility_timeout` therefore
does not need to exceed your processing time.

`max_processing_time` bounds that: when it is reached the handler task is
cancelled and the message is left undeleted, so it is redelivered once its
visibility expires and counted towards `max_retries_until_dlq`. With
`on_error="crash"` the consumer terminates with a `TimeoutError` after the
cancellation. Cancellation requires an async handler — `max_processing_time`
with a synchronous handler is rejected at startup, since a handler thread cannot
be interrupted.

Once a handler is past its deadline the heartbeat stops, so a handler that
ignores cancellation fails the health probe and the orchestrator can replace the
process. Without `max_processing_time` there is no deadline, and a hung handler
keeps the probe green for as long as it runs.

On shutdown the consumer stops taking new messages and gives the in-flight
handler `shutdown_grace_period` seconds before cancelling it the same way. A
synchronous handler cannot be cancelled, so it runs to completion and its message
is deleted as usual — the grace period only bounds an async handler.

A handler cancelled by shutdown has its message released, as do the messages left
in the buffer, so the successor picks the work up at once instead of waiting out
`visibility_timeout`; this holds on a clean shutdown and on a crash alike. A
message cancelled by `max_processing_time`, or failed under `on_error="continue"`,
keeps its visibility instead, so its retry is spaced out rather than immediate.

Releasing a message does not reset its `ApproximateReceiveCount`, which SQS
increments on every receive. Interrupting the same long-running message on
successive deploys therefore walks it towards the DLQ, so give
`max_retries_until_dlq` room for the interruptions you expect.

### Prefetch and Buffering

The fetch thread keeps at most `max_buffered_messages` messages in the local
buffer (`max_messages * 3` by default), stops receiving while the buffer is full,
and backs off between `fetch_min_sleep` and `fetch_max_sleep` seconds when a poll
comes back empty. A buffered message gets no visibility extension until its
handler starts, so a consumer with long handlers should keep the buffer small:

```python
config = Config(
    queue_name="my-queue",
    dlq_queue_name="my-queue-dlq",
    max_messages=1,
    max_buffered_messages=1,
)
```

### Error Policy

```python
config = Config(
    queue_name="my-queue",
    dlq_queue_name="my-queue-dlq",
    on_error="continue",
)
```

- `crash` (default): the handler exception propagates out of `consume`, the
  message stays undeleted and is redelivered
- `continue`: the exception is logged, the message stays undeleted so redrive
  counting applies, and the consumer keeps processing

### FIFO Queues

A queue identified by a name or URL ending in `.fifo` is treated as a FIFO queue,
and a `.fifo` name is created as one; the DLQ name must end in `.fifo` too.

```python
config = Config(queue_name="repair.fifo", dlq_queue_name="repair-dlq.fifo")
```

SQS serializes delivery per message group, but one poll can return several
messages of the same group at once, and those siblings sit in the local buffer
behind the one being handled. So that a failure cannot be overtaken by a later
message of its group, a FIFO consumer releases the rest of its buffer whenever a
message is not processed successfully; SQS then redelivers the group in order,
behind the still-invisible failed message. Handling one group at a time with
`max_messages=1` avoids the round trip.

`send` accepts `group_id` and `dedup_id`, both of which FIFO requires unless the
queue has `ContentBasedDeduplication` enabled — the library does not set that
attribute on the queues it creates.

### Producing Messages

```python
import boto3
from sqs_consumer import send

sqs = boto3.client("sqs")
send(sqs, queue_url, '{"case_id": 42}', group_id="widget-7", dedup_id="outbox-1051")
```

`group_id` and `dedup_id` map to `MessageGroupId` and
`MessageDeduplicationId`, and are only accepted by FIFO queues. The
deduplication id must be unique per publish: FIFO ignores a message whose
deduplication id was seen in the last five minutes.

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

- **Processing Errors**: If your handler raises an exception, the message remains in the queue and will be retried; `on_error` decides whether the consumer terminates or keeps consuming
- **Max Retries**: After `max_retries_until_dlq` attempts, messages are moved to the dead letter queue
- **Timeouts**: A handler that hits `max_processing_time` is cancelled and its message is redelivered
- **Graceful Shutdown**: On shutdown, any unprocessed messages are returned to the queue

## Queue Management

With `create_queues` enabled (the default) and a `queue_name`, the library:

1. **Creates queues** if they don't exist, as FIFO queues for a `.fifo` name
2. **Sets up DLQ** with proper redrive policy
3. **Configures timeouts** and retention periods
4. **Handles queue attributes** for optimal performance

With a `queue_url`, or with `create_queues` disabled, the consumer only needs
`ReceiveMessage`, `DeleteMessage`, `ChangeMessageVisibility` and
`ChangeMessageVisibilityBatch` on an already provisioned queue, plus
`GetQueueUrl` when it resolves the queue by name.

## Monitoring and Logging

The library uses Python's standard logging with structured extra fields, so a JSON
formatter surfaces the per-message `message_id`, `runtime` and `processed` fields.


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
3. **Timeouts**: Bound handlers with `max_processing_time` rather than by inflating
   `visibility_timeout`, which the consumer extends for you while the handler runs
4. **Batch Size**: Adjust `max_messages` based on your processing speed and memory constraints
5. **Monitoring**: Monitor the health check endpoint and DLQ for stuck messages