import dataclasses
import json
import logging
import random
import signal
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from queue import Empty, Queue
from typing import Any, Callable

import boto3
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

log = logging.getLogger(__name__)


class Message(BaseModel):
    body: str = Field(alias="Body")
    id: str = Field(alias="MessageId")
    receipt: str = Field(alias="ReceiptHandle")


MessageQueue = Queue[Message]


@dataclasses.dataclass(kw_only=True)
class Health:
    ready: bool = dataclasses.field(default=False)
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
        self.shutdown_requested = threading.Event()
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum: int, frame: Any):
        log.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.shutdown_requested.set()


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
    fetch_max_sleep: int = Field(default=int(timedelta(seconds=2).total_seconds()))
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
        if self.path == "/health":
            ok = self.health.healthy
        elif self.path == "/ready":
            ok = self.health.ready
        else:
            self.send_error(404)
            return
        status_code = 200 if ok else 503
        self.send_response(status_code)
        response_body = json.dumps({"ok": ok}).encode("utf-8")
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response_body)))
        self.end_headers()
        self.wfile.write(response_body)


def create_health_server(health: Health, host: str, port: int) -> HTTPServer:
    """Create an HTTP server for health checks."""

    # Create a handler factory that includes the health object
    def handler_factory(*args, **kwargs):  # type: ignore
        return HealthCheckHandler(health, *args, **kwargs)  # type: ignore

    server = HTTPServer((host, port), handler_factory)  # type: ignore
    return server


def _fetch_sqs(
    shutdown: GracefulShutdown,
    config: Config,
    queue: MessageQueue,
):
    sqs = boto3.client("sqs", endpoint_url=config.endpoint_url)  # pyright: ignore[reportUnknownMemberType]
    while not shutdown.shutdown_requested.is_set():
        qsize = queue.qsize()
        if qsize > config.fetch_min_messages:
            sleep = random.random() * config.fetch_max_sleep
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


def consume(
    handler: Callable[[Message], None],
    config: Config | None = None,
    health: Health | None = None,
    shutdown: GracefulShutdown | None = None,
):
    config = config or Config()  # type: ignore
    health = health or Health(
        heartbeat_max_age=timedelta(seconds=config.health_max_age)
    )
    queue_to_process: MessageQueue = Queue(config.max_messages * 3)
    shutdown = shutdown or GracefulShutdown()

    health_server = create_health_server(health, "0.0.0.0", config.health_check_port)
    server_thread = threading.Thread(
        target=health_server.serve_forever,
        daemon=True,
        name="health_check",
    )
    server_thread.start()
    sqs_fetch_thread = threading.Thread(
        target=_fetch_sqs,
        name="sqs_fetch",
        daemon=True,
        kwargs={
            "shutdown": shutdown,
            "config": config,
            "queue": queue_to_process,
        },
    )
    sqs_fetch_thread.start()

    sqs = boto3.client("sqs", endpoint_url=config.endpoint_url)  # pyright: ignore[reportUnknownMemberType]

    log.info(
        "Starting consumption",
        extra={
            "config": config.model_dump(mode="json"),
        },
    )
    total_messages_processed = 0

    while not shutdown.shutdown_requested.is_set():
        health.heartbeat()
        try:
            m = queue_to_process.get(block=True, timeout=0.2)
        except Empty:
            continue

        try:
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
            health.heartbeat()
            if total_messages_processed == 1:
                health.ready = True
        except Exception as e:
            log.exception(f"failed to process {e}")
        finally:
            queue_to_process.task_done()
            health.heartbeat()

    log.info("starting queue shutdown set messages VisibilityTimeout back to 0")
    while not queue_to_process.empty():
        try:
            m = queue_to_process.get_nowait()
            sqs.change_message_visibility(
                QueueUrl=config.queue_url,
                ReceiptHandle=m.receipt,
                VisibilityTimeout=0,
            )
            queue_to_process.task_done()
        except Empty:
            break
        except Exception as e:
            log.error(f"Failed to return message to SQS: {e}")
            queue_to_process.task_done()

    sqs.close()
    health_server.shutdown()
    queue_to_process.join()
