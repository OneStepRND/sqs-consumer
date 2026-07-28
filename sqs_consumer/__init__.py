import asyncio
import dataclasses
import inspect
import itertools
import json
import logging
import random
import signal
import threading
import time
from collections.abc import Awaitable, Iterable
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from queue import Empty, Full, Queue
from typing import TYPE_CHECKING, Any, Callable, Literal, TypeIs, cast

import boto3
from botocore.exceptions import ClientError
from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

if TYPE_CHECKING:
    from mypy_boto3_sqs.client import SQSClient
    from mypy_boto3_sqs.literals import QueueAttributeNameType

log = logging.getLogger(__name__)


class Message(BaseModel):
    body: str = Field(alias="Body")
    id: str = Field(alias="MessageId")
    receipt: str = Field(alias="ReceiptHandle")


MessageQueue = Queue[Message]

SyncHandler = Callable[[Message], None]
AsyncHandler = Callable[[Message], Awaitable[None]]
Handler = SyncHandler | AsyncHandler

CancelReason = Literal["max_processing_time", "shutdown"]

_SUPERVISOR_TICK = 0.5


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

    queue_name: str | None = Field(default=None, description="SQS queue NAME")
    dlq_queue_name: str | None = Field(
        default=None, description="SQS dead letter queue NAME"
    )
    queue_url: str | None = Field(
        default=None,
        description="Explicit SQS queue URL, used as is without any queue lookup",
    )
    create_queues: bool = Field(
        default=True,
        description="Create the queue and its DLQ when missing, otherwise resolve the URL by name",
    )

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
    max_processing_time: int | None = Field(
        default=None,
        ge=1,
        description="Seconds a handler may run before it is cancelled, async handlers only",
    )
    on_error: Literal["crash", "continue"] = Field(
        default="crash",
        description="Terminate the consumer on a handler exception, or log it and keep consuming",
    )
    shutdown_grace_period: int = Field(
        default=int(timedelta(seconds=30).total_seconds()),
        ge=0,
        description="Seconds an in-flight handler gets after shutdown is requested",
    )
    health_check_port: int = Field(default=8080)
    health_max_age: int = Field(default=int(timedelta(minutes=3).total_seconds()))
    fetch_min_sleep: int = Field(default=int(timedelta(seconds=1).total_seconds()))
    fetch_max_sleep: int = Field(default=int(timedelta(seconds=3).total_seconds()))
    fetch_min_messages: int = Field(default=5)
    max_buffered_messages: int | None = Field(
        default=None,
        ge=1,
        description="Local buffer capacity, defaults to max_messages * 3",
    )
    max_retries_until_dlq: int = Field(default=3)

    @property
    def buffer_capacity(self) -> int:
        return self.max_buffered_messages or self.max_messages * 3

    @property
    def is_fifo(self) -> bool:
        return (self.queue_name or "").endswith(".fifo")

    @field_validator(
        "endpoint_url", "queue_url", "queue_name", "dlq_queue_name", mode="before"
    )
    def _empty_str_to_none(cls, value: str | None) -> str | None:
        return value if value != "" else None

    @model_validator(mode="after")
    def _validate_queue_identity(self):
        if self.queue_url is None and self.queue_name is None:
            raise ValueError("either queue_url or queue_name is required")

        if self.queue_url is not None or not self.create_queues:
            return self

        if self.dlq_queue_name is None:
            raise ValueError("dlq_queue_name is required when create_queues is enabled")

        if self.is_fifo != self.dlq_queue_name.endswith(".fifo"):
            raise ValueError(
                "a FIFO queue requires a FIFO dead letter queue, both names must end with .fifo"
            )

        return self


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

    def do_HEAD(self):
        """Handle HEAD requests by sending the same response as GET but without body."""
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


def send(
    sqs: "SQSClient",
    queue_url: str,
    body: str,
    *,
    group_id: str | None = None,
    dedup_id: str | None = None,
) -> str:
    fifo_args: dict[str, Any] = {}
    if group_id is not None:
        fifo_args["MessageGroupId"] = group_id
    if dedup_id is not None:
        fifo_args["MessageDeduplicationId"] = dedup_id

    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=body,
        **fifo_args,
    )
    return response["MessageId"]


def _get_or_create_queue_url(sqs: "SQSClient", config: Config) -> str:
    if config.queue_name is None or config.dlq_queue_name is None:
        raise ValueError("queue_name and dlq_queue_name are required to create queues")

    max_retention = str(int(timedelta(days=14).total_seconds()))
    fifo: dict["QueueAttributeNameType", str] = (
        {"FifoQueue": "true"} if config.is_fifo else {}
    )
    try:
        dlq = sqs.create_queue(
            QueueName=config.dlq_queue_name,
            Attributes={
                "MessageRetentionPeriod": max_retention,
                **fifo,
            },
        )

        queue_arn = sqs.get_queue_attributes(
            QueueUrl=dlq["QueueUrl"],
            AttributeNames=["QueueArn"],
        )["Attributes"]["QueueArn"]

        queue_response = sqs.create_queue(
            QueueName=config.queue_name,
            Attributes={
                "MessageRetentionPeriod": max_retention,
                "VisibilityTimeout": str(config.visibility_timeout),
                "ReceiveMessageWaitTimeSeconds": str(config.wait_time_seconds),
                "RedrivePolicy": json.dumps(
                    {
                        "deadLetterTargetArn": queue_arn,
                        "maxReceiveCount": str(config.max_retries_until_dlq),
                    }
                ),
                **fifo,
            },
        )
        return queue_response["QueueUrl"]
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "QueueAlreadyExists":
            return sqs.get_queue_url(QueueName=config.queue_name)["QueueUrl"]

        raise e


def resolve_queue_url(sqs: "SQSClient", config: Config) -> str:
    if config.queue_url is not None:
        return config.queue_url

    if config.queue_name is None:
        raise ValueError("either queue_url or queue_name is required")

    if not config.create_queues:
        return sqs.get_queue_url(QueueName=config.queue_name)["QueueUrl"]

    return _get_or_create_queue_url(sqs, config)


def _fetch_sqs_thread(
    sqs: "SQSClient",
    shutdown: GracefulShutdown,
    config: Config,
    queue: MessageQueue,
    queue_url: str,
):
    def run():
        try:
            while not shutdown.shutdown_requested:
                qsize = queue.qsize()
                free_capacity = max(queue.maxsize - qsize, 0)
                if free_capacity == 0 or qsize > config.fetch_min_messages:
                    sleep = random.uniform(
                        config.fetch_min_sleep, config.fetch_max_sleep
                    )
                    log.debug(f"{qsize=} {free_capacity=} sleep for {sleep:.2f}")
                    time.sleep(sleep)
                    continue

                log.debug("fetching message from sqs")

                response = sqs.receive_message(
                    QueueUrl=queue_url,
                    WaitTimeSeconds=config.wait_time_seconds,
                    MaxNumberOfMessages=min(config.max_messages, free_capacity),
                    VisibilityTimeout=config.visibility_timeout,
                )
                messages = response.get("Messages", [])
                log.debug(f"got {len(messages)} from queue")
                for m in messages:
                    message = Message.model_validate(m)
                    try:
                        queue.put_nowait(message)
                    except Full:
                        log.warning(f"local buffer is full, releasing {message.id}")
                        requeue(sqs, [message], queue_url)

            sqs.close()
        except Exception:
            log.exception("failed to fetch sqs message")
            raise

    return run


def _get_all[T](queue: Queue[T]) -> Iterable[T]:
    while True:
        try:
            yield queue.get_nowait()
        except Empty:
            break


def requeue(sqs: "SQSClient", msgs: Iterable[Message], queue_url: str):
    for batch in itertools.batched(msgs, 10):
        log.info(f"resetting VisibilityTimeout for {len(batch)} messages")
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


def _is_async_handler(handler: Handler) -> TypeIs[AsyncHandler]:
    return inspect.iscoroutinefunction(handler)


class _SyncInvoker:
    def __init__(self, handler: SyncHandler):
        self.handler = handler

    def invoke(self, message: Message) -> None:
        self.handler(message)

    def cancel(self) -> bool:
        log.error(f"{self.handler} runs synchronously and cannot be cancelled")
        return False

    def close(self) -> None:
        return None


class _AsyncInvoker:
    def __init__(self, handler: AsyncHandler):
        self.handler = handler
        self._runner = asyncio.Runner()
        self._lock = threading.Lock()
        self._task: asyncio.Task[Any] | None = None

    def invoke(self, message: Message) -> None:
        self._runner.run(self._run(message))

    async def _run(self, message: Message) -> None:
        with self._lock:
            self._task = asyncio.current_task()
        try:
            await self.handler(message)
        finally:
            with self._lock:
                self._task = None

    def cancel(self) -> bool:
        with self._lock:
            task = self._task
        if task is None or task.done():
            return False
        self._runner.get_loop().call_soon_threadsafe(task.cancel)
        return True

    def close(self) -> None:
        self._runner.close()


Invoker = _SyncInvoker | _AsyncInvoker


def _make_invoker(handler: Handler) -> Invoker:
    if _is_async_handler(handler):
        return _AsyncInvoker(handler)

    return _SyncInvoker(cast(SyncHandler, handler))


class _InFlightSupervisor:
    """Extends the visibility of the in-flight message and keeps the liveness heartbeat fresh."""

    def __init__(
        self,
        *,
        sqs: "SQSClient",
        queue_url: str,
        message: Message,
        config: Config,
        health: Health,
        shutdown: GracefulShutdown,
        invoker: Invoker,
    ):
        self.sqs = sqs
        self.queue_url = queue_url
        self.message = message
        self.config = config
        self.health = health
        self.shutdown = shutdown
        self.invoker = invoker
        self.cancel_reason: CancelReason | None = None
        self._cancel_attempt: CancelReason | None = None
        self._extension_interval = max(config.visibility_timeout / 2, _SUPERVISOR_TICK)
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="visibility_heartbeat",
            daemon=True,
        )

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self._stop.set()
        self._thread.join(timeout=_SUPERVISOR_TICK * 4)
        if self._thread.is_alive():
            log.error(f"visibility heartbeat of {self.message.id} is still alive")

    def _extend_visibility(self) -> None:
        if self.config.visibility_timeout == 0:
            return

        try:
            self.sqs.change_message_visibility(
                QueueUrl=self.queue_url,
                ReceiptHandle=self.message.receipt,
                VisibilityTimeout=self.config.visibility_timeout,
            )
        except Exception:
            log.exception(f"failed to extend visibility of {self.message.id}")

    def _cancel(self, reason: CancelReason) -> None:
        self._cancel_attempt = reason
        log.error(f"cancelling the handler of {self.message.id} : {reason}")
        if self.invoker.cancel():
            self.cancel_reason = reason

    def _run(self) -> None:
        started = time.monotonic()
        next_extension = started + self._extension_interval
        shutdown_deadline: float | None = None

        while not self._stop.wait(_SUPERVISOR_TICK):
            now = time.monotonic()
            self.health.heartbeat()

            if self.shutdown.shutdown_requested and shutdown_deadline is None:
                shutdown_deadline = now + self.config.shutdown_grace_period

            if self._cancel_attempt is None:
                max_processing_time = self.config.max_processing_time
                if (
                    max_processing_time is not None
                    and now - started >= max_processing_time
                ):
                    self._cancel("max_processing_time")
                elif shutdown_deadline is not None and now >= shutdown_deadline:
                    self._cancel("shutdown")

            if now >= next_extension:
                self._extend_visibility()
                next_extension = now + self._extension_interval


def _handle_message(
    *,
    message: Message,
    invoker: Invoker,
    sqs: "SQSClient",
    queue_url: str,
    config: Config,
    health: Health,
    shutdown: GracefulShutdown,
) -> bool:
    failure: BaseException | None = None

    with _InFlightSupervisor(
        sqs=sqs,
        queue_url=queue_url,
        message=message,
        config=config,
        health=health,
        shutdown=shutdown,
        invoker=invoker,
    ) as supervisor:
        try:
            invoker.invoke(message)
        except (Exception, asyncio.CancelledError) as e:
            failure = e

    if supervisor.cancel_reason is not None:
        log.error(
            f"handler of {message.id} was cancelled : {supervisor.cancel_reason}",
            extra={
                "message_id": message.id,
                "cancel_reason": supervisor.cancel_reason,
            },
        )
        if (
            supervisor.cancel_reason == "max_processing_time"
            and config.on_error == "crash"
        ):
            raise TimeoutError(
                f"handler of {message.id} exceeded max_processing_time of {config.max_processing_time}s"
            )
        return False

    if failure is not None:
        if config.on_error == "crash":
            raise failure

        log.error(
            f"handler of {message.id} failed",
            exc_info=failure,
            extra={"message_id": message.id},
        )
        return False

    sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=message.receipt,
    )
    return True


def consume(
    *,
    handler: Handler,
    config: Config | None = None,
    health: Health | None = None,
    shutdown: GracefulShutdown,
    sqs: "SQSClient | None" = None,
):
    start_timestamp = datetime.now(timezone.utc)
    config = config or Config()
    invoker = _make_invoker(handler)
    if config.max_processing_time is not None and isinstance(invoker, _SyncInvoker):
        raise ValueError(
            "max_processing_time is only supported with an async handler, a synchronous handler cannot be cancelled"
        )

    health = health or Health(
        heartbeat_max_age=timedelta(seconds=config.health_max_age)
    )
    queue_to_process: MessageQueue = Queue(config.buffer_capacity)
    sqs = sqs or boto3.client("sqs", endpoint_url=config.endpoint_url)  # pyright: ignore[reportUnknownMemberType]
    queue_url = resolve_queue_url(sqs, config)
    health_server = create_health_server(health, "0.0.0.0", config.health_check_port)
    server_thread = threading.Thread(
        target=health_server.serve_forever,
        daemon=True,
        name="health_check",
    )
    server_thread.start()
    sqs_fetch_thread = threading.Thread(
        target=_fetch_sqs_thread(
            sqs,
            shutdown,
            config,
            queue_to_process,
            queue_url,
        ),
        name="sqs_fetch",
        daemon=True,
    )

    sqs_fetch_thread.start()

    log.info(
        f"Starting consumption : {queue_url}",
        extra={
            "config": config.model_dump(mode="json"),
        },
    )
    total_messages_processed = 0

    while not shutdown.shutdown_requested:
        if not sqs_fetch_thread.is_alive():
            log.error("sqs_fetch_thread is dead shutting down")
            shutdown.shutdown()
            break

        health.heartbeat()
        try:
            m = queue_to_process.get(block=True, timeout=0.2)
        except Empty:
            continue

        log.debug(f"calling : {handler} with : {m.id}")
        start = time.time()
        processed = _handle_message(
            message=m,
            invoker=invoker,
            sqs=sqs,
            queue_url=queue_url,
            config=config,
            health=health,
            shutdown=shutdown,
        )
        total_messages_processed += int(processed)
        runtime = time.time() - start
        log.info(
            f"runtime for {handler} with {m.id} : {runtime:.2f}",
            extra={
                "runtime": runtime,
                "message_id": m.id,
                "processed": processed,
                "total_messages_processed": total_messages_processed,
                "uptime": str(datetime.now(timezone.utc) - start_timestamp),
            },
        )
        queue_to_process.task_done()
        health.heartbeat()

    log.info("shutting down consumer")

    if sqs_fetch_thread.is_alive():
        wait_time = config.wait_time_seconds + 2
        log.info(f"waiting : {wait_time} seconds for sqs_fetch_thread to finish")
        sqs_fetch_thread.join(timeout=wait_time)

    if sqs_fetch_thread.is_alive():
        log.error("sqs thread still alive after join")

    requeue(sqs, _get_all(queue_to_process), queue_url)
    invoker.close()
    health_server.shutdown()
