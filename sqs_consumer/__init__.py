import dataclasses
import itertools
import logging
import random
import signal
import threading
import time
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from queue import Empty, Queue
from typing import TYPE_CHECKING, Any, Callable

import boto3
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

if TYPE_CHECKING:
    from mypy_boto3_sqs.client import SQSClient

log = logging.getLogger(__name__)


class Message(BaseModel):
    body: str = Field(alias="Body")
    id: str = Field(alias="MessageId")
    receipt: str = Field(alias="ReceiptHandle")


MessageQueue = Queue[Message]


@dataclasses.dataclass(kw_only=True)
class Health:
    last_heartbeat: datetime | None = dataclasses.field(default=None)
    heartbeat_max_age: timedelta = dataclasses.field(default=timedelta(minutes=3))

    @property
    def healthy(self):
        if self.last_heartbeat is None:
            return False

        return (
            datetime.now(timezone.utc) - self.last_heartbeat
        ) < self.heartbeat_max_age

    def heartbeat(self):
        self.last_heartbeat = datetime.now(timezone.utc)


class GracefulShutdown:
    """Simple graceful shutdown handler."""

    def __init__(self):
        self.shutdown_requested = False

    def _signal_handler(self, signum: int, frame: Any):
        self.shutdown()

    def shutdown(self):
        self.shutdown_requested = True

    @classmethod
    def with_signal(cls):
        instance = cls()

        def handler(signum: int, frame: Any):
            instance.shutdown()

        signal.signal(signal.SIGTERM, handler)
        signal.signal(signal.SIGINT, handler)

        return instance


class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        env_prefix="SQS_",
        extra="ignore",
    )

    # Required - will be loaded from SQS_QUEUE_URL env var
    queue_url: str = Field(..., description="SQS queue URL")

    # Optional - these have the SQS_ prefix automatically added
    endpoint_url: str | None = Field(default=None, description="Custom SQS endpoint")
    max_messages: int = Field(
        default=10, ge=1, le=10, description="Max messages per poll"
    )
    wait_time_seconds: int = Field(
        default=20, ge=0, le=20, description="Long polling wait time"
    )
    visibility_timeout: int = Field(
        default=int(timedelta(minutes=5).total_seconds()),
        ge=0,
        description="Message visibility timeout",
    )
    health_check_port: int = Field(default=8080)
    health_max_age: int = Field(default=int(timedelta(minutes=3).total_seconds()))
    fetch_min_sleep: int = Field(default=int(timedelta(seconds=1).total_seconds()))
    fetch_max_sleep: int = Field(default=int(timedelta(seconds=3).total_seconds()))
    fetch_min_messages: int = Field(default=5)


class HealthCheckHandler(BaseHTTPRequestHandler):
    """HTTP request handler for health checks using stdlib."""

    def __init__(self, health: Health, *args, **kwargs):  # type: ignore
        self.health = health
        super().__init__(*args, **kwargs)  # type: ignore

    def log_message(self, format, *args):  # type: ignore
        """Override to use our logger instead of printing to stderr."""
        log.debug(f"Health check: {format % args}")

    def do_GET(self):
        if self.path != "/health":
            self.send_error(404)
            return
        self.send_response(200 if self.health.healthy else 503)
        self.end_headers()


def create_health_server(health: Health, host: str, port: int) -> HTTPServer:
    """Create an HTTP server for health checks."""

    # Create a handler factory that includes the health object
    def handler_factory(*args, **kwargs):  # type: ignore
        return HealthCheckHandler(health, *args, **kwargs)  # type: ignore

    server = HTTPServer((host, port), handler_factory)  # type: ignore
    return server


def _fetch_sqs_thread(
    sqs: "SQSClient",
    shutdown: GracefulShutdown,
    config: Config,
    queue: MessageQueue,
):
    def run():
        while not shutdown.shutdown_requested:
            qsize = queue.qsize()
            if qsize > config.fetch_min_messages:
                sleep = random.uniform(config.fetch_min_sleep, config.fetch_max_sleep)
                log.debug(f"{qsize=} sleep for {sleep:.2f}")
                time.sleep(sleep)
                continue

            log.debug("fetching message from sqs")

            response = sqs.receive_message(
                QueueUrl=config.queue_url,
                WaitTimeSeconds=config.wait_time_seconds,
                MaxNumberOfMessages=config.max_messages,
                VisibilityTimeout=config.visibility_timeout,
            )
            messages = response.get("Messages", [])
            log.debug(f"got {len(messages)} from queue")
            for m in response.get("Messages", []):
                queue.put_nowait(Message.model_validate(m))

        sqs.close()

    return run


def _get_all[T](queue: Queue[T]) -> Iterable[T]:
    while True:
        try:
            yield queue.get_nowait()
        except Empty:
            break


def _requeue(sqs: "SQSClient", msgs: Iterable[Message], queue_url: str):
    for batch in itertools.batched(msgs, 10):
        sqs.change_message_visibility_batch(
            QueueUrl=queue_url,
            Entries=[
                {
                    "ReceiptHandle": m.receipt,
                    "VisibilityTimeout": 0,
                    "Id": m.id,
                }
                for m in batch
            ],
        )


def consume(
    *,
    handler: Callable[[Message], None],
    config: Config | None = None,
    health: Health | None = None,
    shutdown: GracefulShutdown,
    sqs: "SQSClient | None" = None,
):
    config = config or Config()  # type: ignore
    health = health or Health(
        heartbeat_max_age=timedelta(seconds=config.health_max_age)
    )
    queue_to_process: MessageQueue = Queue(config.max_messages * 3)
    sqs = sqs or boto3.client("sqs", endpoint_url=config.endpoint_url)  # pyright: ignore[reportUnknownMemberType]

    health_server = create_health_server(health, "0.0.0.0", config.health_check_port)
    server_thread = threading.Thread(
        target=health_server.serve_forever,
        daemon=True,
        name="health_check",
    )
    server_thread.start()
    sqs_fetch_thread = threading.Thread(
        target=_fetch_sqs_thread(sqs, shutdown, config, queue_to_process),
        name="sqs_fetch",
        daemon=True,
    )

    sqs_fetch_thread.start()

    log.info(
        "Starting consumption",
        extra={
            "config": config.model_dump(mode="json"),
        },
    )
    total_messages_processed = 0

    while not shutdown.shutdown_requested:
        health.heartbeat()
        try:
            m = queue_to_process.get(block=True, timeout=0.2)
        except Empty:
            continue

        log.debug(f"calling : {handler} with : {m.id}")
        start = time.time()
        handler(m)
        total_messages_processed += 1
        runtime = time.time() - start
        log.info(
            f"runtime for {handler} with {m.id} : {runtime:.2f}",
            extra={
                "runtime": runtime,
                "message_id": m.id,
                "total_messages_processed": total_messages_processed,
            },
        )
        sqs.delete_message(
            QueueUrl=config.queue_url,
            ReceiptHandle=m.receipt,
        )
        queue_to_process.task_done()
        health.heartbeat()

    log.info("starting queue shutdown set messages VisibilityTimeout back to 0")

    sqs_fetch_thread.join(timeout=config.wait_time_seconds + 2)
    if sqs_fetch_thread.is_alive():
        log.error("sqs thread still alive after join")
    _requeue(sqs, _get_all(queue_to_process), config.queue_url)

    health_server.shutdown()
