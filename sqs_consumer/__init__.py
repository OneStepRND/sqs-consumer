import logging
import signal
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

import boto3
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

if TYPE_CHECKING:
    from mypy_boto3_sqs.type_defs import MessageTypeDef
    from mypy_boto3_sqs import SQSClient

log = logging.getLogger(__name__)


class SimpleHealthCheck:
    def __init__(self, health_dir: str):
        self.health_dir = Path(health_dir)
        self.health_dir.mkdir(parents=True, exist_ok=True)
        self.heartbeat_file = self.health_dir / "heartbeat"
        self.ready_file = self.health_dir / "ready"

    @staticmethod
    def iso_now():
        return datetime.now(timezone.utc).isoformat()

    def heartbeat(self):
        timestamp = self.iso_now()
        temp_file = self.heartbeat_file.with_suffix(".tmp")
        temp_file.write_text(timestamp)
        temp_file.replace(self.heartbeat_file)
        log.debug("heartbeat")

    def mark_ready(self):
        self.ready_file.write_text(self.iso_now())
        log.info("marked ready")

    def is_healthy(
        self,
        max_age: timedelta = timedelta(minutes=2),
    ) -> tuple[bool, str]:
        if not self.heartbeat_file.exists():
            return False, "No heartbeat file"
        now = datetime.now(timezone.utc)
        last = datetime.fromisoformat(self.heartbeat_file.read_text())
        age = now - last
        if age > max_age:
            return False, f"Heartbeat too old: {age}s"

        return True, "Healthy"

    def is_ready(self):
        return self.ready_file.exists()


class GracefulShutdown:
    """Simple graceful shutdown handler."""

    def __init__(self):
        self.shutdown_requested = False
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum: int, frame: Any):
        log.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.shutdown_requested = True


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
    health_dir: str = Field(default="/tmp/sqs-consumer/health")
    health_max_age: int = Field(default=int(timedelta(minutes=2).total_seconds()))


def _re_enqueue(
    *,
    sqs: "SQSClient",
    messages: list["MessageTypeDef"],
    queue_url: str,
):
    if not messages:
        return

    sqs.change_message_visibility_batch(
        QueueUrl=queue_url,
        Entries=[
            {
                "Id": m["MessageId"],
                "ReceiptHandle": m["ReceiptHandle"],
                "VisibilityTimeout": 0,
            }
            for m in messages
            if "MessageId" in m and "ReceiptHandle" in m
        ],
    )
    log.debug(f"re enqueue : {len(messages)}")
    messages.clear()


def _ack(
    *,
    sqs: "SQSClient",
    messages: list["MessageTypeDef"],
    queue_url: str,
):
    sqs.delete_message_batch(
        QueueUrl=queue_url,
        Entries=[
            {
                "Id": m["MessageId"],
                "ReceiptHandle": m["ReceiptHandle"],
            }
            for m in messages
            if "MessageId" in m and "ReceiptHandle" in m
        ],
    )
    log.debug(f"ack : {len(messages)}")


def consume(
    handler: Callable[["MessageTypeDef"], None],
    config: Config | None = None,
    health: SimpleHealthCheck | None = None,
    shutdown: GracefulShutdown | None = None,
):
    config = config or Config()  # type: ignore
    health = health or SimpleHealthCheck(config.health_dir)
    shutdown = shutdown or GracefulShutdown()

    sqs = (
        boto3.client("sqs", endpoint_url=config.endpoint_url)  # type: ignore
        if config.endpoint_url
        else boto3.client("sqs")  # type: ignore
    )

    log.info(
        "Starting consumption",
        extra={
            "config": config.model_dump(mode="json"),
        },
    )
    to_delete: list["MessageTypeDef"] = []
    to_process: list["MessageTypeDef"] = []
    total_messages_processed = 0

    while not shutdown.shutdown_requested:
        log.debug(f"calling receive_message from {config.queue_url}")
        health.heartbeat()
        response = sqs.receive_message(
            QueueUrl=config.queue_url,
            WaitTimeSeconds=config.wait_time_seconds,
            MaxNumberOfMessages=config.max_messages,
            VisibilityTimeout=config.visibility_timeout,
        )
        to_process = response.get("Messages", [])

        while to_process:
            m = to_process.pop(0)
            health.heartbeat()
            if shutdown.shutdown_requested:
                to_process.append(m)
                log.debug(
                    f"shutdown signal stopping processing : {len(to_process)} messages pending in memory"
                )
                break

            try:
                assert "MessageId" in m, "MessageId is required its a type lib issue"
                message_id = m["MessageId"]
                log.debug(f"calling : {handler} with : {message_id}")
                start = time.time()
                handler(m)
                total_messages_processed += 1
            except Exception:
                log.exception(f"failed to process message : {m}")
            else:
                runtime = time.time() - start
                log.info(
                    f"runtime for {handler} with {message_id} : {runtime:.2f}",
                    extra={
                        "runtime": runtime,
                        "message_id": message_id,
                        "total_messages_processed": total_messages_processed,
                    },
                )
                to_delete.append(m)
                health.heartbeat()
                if total_messages_processed == 1:
                    health.mark_ready()

        _ack(sqs=sqs, queue_url=config.queue_url, messages=to_delete)

    _ack(sqs=sqs, queue_url=config.queue_url, messages=to_delete)
    _re_enqueue(sqs=sqs, queue_url=config.queue_url, messages=to_process)
    sqs.close()


def check_ready(config: Config | None = None, health: SimpleHealthCheck | None = None):
    config = config or Config()  # type: ignore
    health = health or SimpleHealthCheck(config.health_dir)
    return 0 if health.is_ready() else 1


def check_health(config: Config | None = None, health: SimpleHealthCheck | None = None):
    config = config or Config()  # type: ignore
    health = health or SimpleHealthCheck(config.health_dir)
    healthy, message = health.is_healthy(
        max_age=timedelta(seconds=config.health_max_age)
    )
    print(message)
    return 0 if healthy else 1
