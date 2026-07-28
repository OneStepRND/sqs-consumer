import asyncio
import dataclasses
import functools
import inspect
import itertools
import json
import logging
import random
import signal
import socketserver
import threading
import time
import types
from collections.abc import Awaitable, Iterable
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from queue import Empty, Full, Queue
from typing import TYPE_CHECKING, Any, Callable, Literal, Protocol, TypeIs, cast

import boto3
from botocore.exceptions import ClientError
from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

if TYPE_CHECKING:
    from mypy_boto3_sqs.client import SQSClient
    from mypy_boto3_sqs.literals import QueueAttributeNameType

log = logging.getLogger(__name__)

__all__ = [
    "AsyncHandler",
    "CancelReason",
    "Config",
    "GracefulShutdown",
    "Handler",
    "Health",
    "Invoker",
    "Message",
    "SyncHandler",
    "consume",
    "create_health_server",
    "requeue",
    "resolve_queue_url",
    "send",
]


class Message(BaseModel):
    body: str = Field(alias="Body")
    id: str = Field(alias="MessageId")
    receipt: str = Field(alias="ReceiptHandle")


_MessageQueue = Queue[Message]

SyncHandler = Callable[[Message], None]
AsyncHandler = Callable[[Message], Awaitable[None]]
Handler = SyncHandler | AsyncHandler

CancelReason = Literal["max_processing_time", "shutdown"]

_SUPERVISOR_TICK = 0.5
_FIFO_SUFFIX = ".fifo"


@dataclasses.dataclass(kw_only=True)
class Health:
    last_heartbeat: datetime | None = dataclasses.field(default=None)
    heartbeat_max_age: timedelta = dataclasses.field(default=timedelta(minutes=3))

    @property
    def healthy(self) -> bool:
        if self.last_heartbeat is None:
            return False

        return (
            datetime.now(timezone.utc) - self.last_heartbeat
        ) < self.heartbeat_max_age

    def heartbeat(self) -> None:
        self.last_heartbeat = datetime.now(timezone.utc)


class GracefulShutdown:
    """Simple graceful shutdown handler."""

    def __init__(self) -> None:
        self.shutdown_requested = False

    def shutdown(self) -> None:
        self.shutdown_requested = True

    @classmethod
    def with_signal(cls) -> "GracefulShutdown":
        instance = cls()

        def handler(signum: int, frame: types.FrameType | None) -> None:
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
    max_processing_time: float | None = Field(
        default=None,
        gt=0,
        description="Seconds a handler may run before it is cancelled, async handlers only",
    )
    on_error: Literal["crash", "continue"] = Field(
        default="crash",
        description="Terminate the consumer on a handler exception, or log it and keep consuming",
    )
    shutdown_grace_period: float = Field(
        default=30,
        ge=0,
        description="Seconds an in-flight handler gets after shutdown is requested",
    )
    health_check_port: int = Field(
        default=8080, ge=1, le=65535, description="Health check HTTP port"
    )
    health_max_age: int = Field(
        default=int(timedelta(minutes=3).total_seconds()),
        ge=1,
        description="Seconds without a heartbeat before the health probe fails",
    )
    fetch_min_sleep: float = Field(
        default=1, ge=0, description="Min sleep between fetches"
    )
    fetch_max_sleep: float = Field(
        default=3, ge=0, description="Max sleep between fetches"
    )
    fetch_min_messages: int = Field(
        default=5,
        ge=0,
        description="Buffered messages above which fetching pauses",
    )
    max_buffered_messages: int | None = Field(
        default=None,
        ge=1,
        description="Local buffer capacity, defaults to max_messages * 3",
    )
    max_retries_until_dlq: int = Field(
        default=3,
        ge=1,
        le=1000,
        description="Receives of a message before it is moved to the DLQ",
    )

    @property
    def buffer_capacity(self) -> int:
        return self.max_buffered_messages or self.max_messages * 3

    @property
    def is_fifo(self) -> bool:
        return (self.queue_name or self.queue_url or "").endswith(_FIFO_SUFFIX)

    @field_validator(
        "endpoint_url", "queue_url", "queue_name", "dlq_queue_name", mode="before"
    )
    @classmethod
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

        if self.is_fifo != self.dlq_queue_name.endswith(_FIFO_SUFFIX):
            raise ValueError(
                "a FIFO queue requires a FIFO dead letter queue, both names must end with .fifo"
            )

        return self

    @model_validator(mode="after")
    def _validate_fetch_pacing(self):
        if self.fetch_min_sleep > self.fetch_max_sleep:
            raise ValueError("fetch_min_sleep must not exceed fetch_max_sleep")

        return self


class HealthCheckHandler(BaseHTTPRequestHandler):
    """HTTP request handler for health checks using stdlib."""

    def __init__(
        self,
        health: Health,
        request: Any,
        client_address: Any,
        server: socketserver.BaseServer,
    ) -> None:
        self.health = health
        super().__init__(request, client_address, server)

    def log_message(self, format: str, *args: Any) -> None:
        log.debug(f"Health check: {format % args}")

    def do_GET(self) -> None:
        if self.path != "/health":
            self.send_error(404)
            return
        self.send_response(200 if self.health.healthy else 503)
        self.send_header("Content-Length", "0")
        self.end_headers()

    do_HEAD = do_GET


def create_health_server(health: Health, host: str, port: int) -> HTTPServer:
    """Create an HTTP server for health checks."""

    return HTTPServer((host, port), functools.partial(HealthCheckHandler, health))


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


def _create_queue(
    sqs: "SQSClient", name: str, attributes: dict["QueueAttributeNameType", str]
) -> str:
    try:
        return sqs.create_queue(QueueName=name, Attributes=attributes)["QueueUrl"]
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") != "QueueAlreadyExists":
            raise

        log.warning(
            f"{name} already exists with different attributes, keeping the existing ones",
            extra={"queue_name": name, "requested_attributes": attributes},
        )
        return sqs.get_queue_url(QueueName=name)["QueueUrl"]


def _get_or_create_queue_url(sqs: "SQSClient", config: Config) -> str:
    if config.queue_name is None or config.dlq_queue_name is None:
        raise ValueError("queue_name and dlq_queue_name are required to create queues")

    max_retention = str(int(timedelta(days=14).total_seconds()))
    fifo: dict["QueueAttributeNameType", str] = (
        {"FifoQueue": "true"} if config.is_fifo else {}
    )
    dlq_url = _create_queue(
        sqs,
        config.dlq_queue_name,
        {"MessageRetentionPeriod": max_retention, **fifo},
    )
    dlq_arn = sqs.get_queue_attributes(
        QueueUrl=dlq_url,
        AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]

    return _create_queue(
        sqs,
        config.queue_name,
        {
            "MessageRetentionPeriod": max_retention,
            "VisibilityTimeout": str(config.visibility_timeout),
            "ReceiveMessageWaitTimeSeconds": str(config.wait_time_seconds),
            "RedrivePolicy": json.dumps(
                {
                    "deadLetterTargetArn": dlq_arn,
                    "maxReceiveCount": str(config.max_retries_until_dlq),
                }
            ),
            **fifo,
        },
    )


def resolve_queue_url(sqs: "SQSClient", config: Config) -> str:
    if config.queue_url is not None:
        return config.queue_url

    if config.queue_name is None:
        raise ValueError("either queue_url or queue_name is required")

    if not config.create_queues:
        return sqs.get_queue_url(QueueName=config.queue_name)["QueueUrl"]

    return _get_or_create_queue_url(sqs, config)


def _has_free_capacity(qsize: int, capacity: int, fetch_min_messages: int) -> bool:
    """Whether the local buffer has room worth filling with another poll."""

    return qsize < capacity and qsize <= fetch_min_messages


def _fetch_sqs_thread(
    sqs: "SQSClient",
    *,
    stop: threading.Event,
    drained: threading.Event,
    shutdown: GracefulShutdown,
    config: Config,
    queue: _MessageQueue,
    queue_url: str,
    backoff: Callable[[threading.Event], None] | None = None,
) -> Callable[[], None]:
    def jittered_backoff(until: threading.Event) -> None:
        until.wait(random.uniform(config.fetch_min_sleep, config.fetch_max_sleep))

    pause = backoff or jittered_backoff

    def run() -> None:
        try:
            while not (stop.is_set() or shutdown.shutdown_requested):
                qsize = queue.qsize()
                free_capacity = max(queue.maxsize - qsize, 0)
                if not _has_free_capacity(
                    qsize, queue.maxsize, config.fetch_min_messages
                ):
                    log.debug(f"{qsize=} {free_capacity=} waiting for the consumer")
                    pause(drained)
                    drained.clear()
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
                        log.warning(
                            f"local buffer is full, releasing {message.id}",
                            extra={"message_id": message.id},
                        )
                        requeue(sqs, [message], queue_url)

                if not messages:
                    pause(stop)
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


def requeue(sqs: "SQSClient", msgs: Iterable[Message], queue_url: str) -> None:
    for batch in itertools.batched(msgs, 10):
        log.info(f"resetting VisibilityTimeout for {len(batch)} messages")
        response = sqs.change_message_visibility_batch(
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
        for failure in response.get("Failed", []):
            log.error(
                f"failed to reset the VisibilityTimeout of {failure.get('Id')} : {failure.get('Message')}",
                extra={"message_id": failure.get("Id")},
            )


def _is_async_handler(handler: Handler) -> TypeIs[AsyncHandler]:
    return inspect.iscoroutinefunction(handler)


class Invoker(Protocol):
    """Runs a handler over one message, and cancels it when it outstays its deadline."""

    @property
    def cancellable(self) -> bool: ...

    def invoke(self, message: Message) -> None: ...

    def cancel(self) -> None: ...

    def close(self) -> None: ...


class _SyncInvoker:
    cancellable = False

    def __init__(self, handler: SyncHandler):
        self.handler = handler

    def invoke(self, message: Message) -> None:
        self.handler(message)

    def cancel(self) -> None:
        return None

    def close(self) -> None:
        return None


class _AsyncInvoker:
    cancellable = True

    def __init__(self, handler: AsyncHandler):
        self.handler = handler
        self._runner = asyncio.Runner()
        self._lock = threading.Lock()
        self._task: asyncio.Task[None] | None = None

    def invoke(self, message: Message) -> None:
        self._runner.run(self._run(message))

    async def _run(self, message: Message) -> None:
        with self._lock:
            self._task = cast(asyncio.Task[None], asyncio.current_task())
        try:
            await self.handler(message)
        finally:
            with self._lock:
                self._task = None

    def cancel(self) -> None:
        with self._lock:
            task = self._task
        if task is None or task.done():
            return

        self._runner.get_loop().call_soon_threadsafe(task.cancel)

    def close(self) -> None:
        self._runner.close()


def _make_invoker(handler: Handler) -> Invoker:
    if _is_async_handler(handler):
        return _AsyncInvoker(handler)

    return _SyncInvoker(cast(SyncHandler, handler))


class _InFlightSupervisor:
    """Extends the visibility of the in-flight message, keeps the liveness heartbeat
    fresh while the handler is within its deadline, and cancels it once it is not."""

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
        self._extends_visibility = config.visibility_timeout > 0
        self._extension_interval = max(config.visibility_timeout / 2, _SUPERVISOR_TICK)
        self._started = 0.0
        self._next_extension = 0.0
        self._shutdown_deadline: float | None = None
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name="in_flight_supervisor",
            daemon=True,
        )

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self._stop.set()
        self._thread.join(timeout=_SUPERVISOR_TICK * 4)
        if self._thread.is_alive():
            log.error(
                f"the supervisor of {self.message.id} is still alive",
                extra={"message_id": self.message.id},
            )

    def _extend_visibility(self) -> bool:
        try:
            self.sqs.change_message_visibility(
                QueueUrl=self.queue_url,
                ReceiptHandle=self.message.receipt,
                VisibilityTimeout=self.config.visibility_timeout,
            )
            return True
        except Exception:
            log.exception(
                f"failed to extend visibility of {self.message.id}",
                extra={"message_id": self.message.id},
            )
            return False

    def _overdue_reason(
        self, elapsed: float, shutdown_overdue: bool
    ) -> CancelReason | None:
        max_processing_time = self.config.max_processing_time
        if max_processing_time is not None and elapsed >= max_processing_time:
            return "max_processing_time"

        return "shutdown" if shutdown_overdue else None

    def _cancel(self, reason: CancelReason) -> None:
        self.cancel_reason = reason
        if not self.invoker.cancellable:
            log.warning(
                f"the handler of {self.message.id} is overdue : {reason}, and runs synchronously so it cannot be cancelled",
                extra={"message_id": self.message.id, "cancel_reason": reason},
            )
            return

        log.warning(
            f"cancelling the handler of {self.message.id} : {reason}",
            extra={"message_id": self.message.id, "cancel_reason": reason},
        )
        self.invoker.cancel()

    def start(self, now: float) -> None:
        """Rebases the visibility clock onto the handler, which starts long after the
        message was received and may have waited out most of its visibility in the
        local buffer."""

        self._started = now
        if self._extends_visibility:
            self._extend_visibility()
        self._next_extension = now + self._extension_interval

    def tick(self, now: float) -> None:
        if self.shutdown.shutdown_requested and self._shutdown_deadline is None:
            self._shutdown_deadline = now + self.config.shutdown_grace_period

        reason = self._overdue_reason(
            now - self._started,
            self._shutdown_deadline is not None and now >= self._shutdown_deadline,
        )
        if reason is None:
            self.health.heartbeat()
        elif self.cancel_reason is None:
            self._cancel(reason)

        if self._extends_visibility and now >= self._next_extension:
            extended = self._extend_visibility()
            self._next_extension = now + (
                self._extension_interval if extended else _SUPERVISOR_TICK * 2
            )

    def _run(self) -> None:
        self.start(time.monotonic())

        while not self._stop.wait(_SUPERVISOR_TICK):
            self.tick(time.monotonic())


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

    cancel_reason = (
        supervisor.cancel_reason
        if isinstance(failure, asyncio.CancelledError)
        else None
    )

    if cancel_reason is not None:
        log.error(
            f"handler of {message.id} was cancelled : {cancel_reason}",
            extra={
                "message_id": message.id,
                "cancel_reason": cancel_reason,
            },
        )
        if cancel_reason == "shutdown":
            requeue(sqs, [message], queue_url)
            return False

        if config.on_error == "crash":
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
    shutdown: GracefulShutdown,
    config: Config | None = None,
    health: Health | None = None,
    sqs: "SQSClient | None" = None,
) -> None:
    start_timestamp = datetime.now(timezone.utc)
    config = config or Config()
    invoker = _make_invoker(handler)
    if config.max_processing_time is not None and not invoker.cancellable:
        raise ValueError(
            "max_processing_time is only supported with an async handler, a synchronous handler cannot be cancelled"
        )

    health = health or Health(
        heartbeat_max_age=timedelta(seconds=config.health_max_age)
    )
    queue_to_process: _MessageQueue = Queue(config.buffer_capacity)
    owns_client = sqs is None
    sqs = sqs or boto3.client("sqs", endpoint_url=config.endpoint_url)  # pyright: ignore[reportUnknownMemberType]
    queue_url = resolve_queue_url(sqs, config)
    health_server = create_health_server(health, "0.0.0.0", config.health_check_port)
    server_thread = threading.Thread(
        target=health_server.serve_forever,
        daemon=True,
        name="health_check",
    )
    stop_fetching = threading.Event()
    drained = threading.Event()
    sqs_fetch_thread = threading.Thread(
        target=_fetch_sqs_thread(
            sqs,
            stop=stop_fetching,
            drained=drained,
            shutdown=shutdown,
            config=config,
            queue=queue_to_process,
            queue_url=queue_url,
        ),
        name="sqs_fetch",
        daemon=True,
    )

    server_thread.start()
    sqs_fetch_thread.start()

    log.info(
        f"Starting consumption : {queue_url}",
        extra={
            "config": config.model_dump(mode="json"),
            "buffer_capacity": config.buffer_capacity,
        },
    )
    total_messages_processed = 0

    try:
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
            start = time.monotonic()
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
            runtime = time.monotonic() - start
            log.info(
                f"runtime for {m.id} : {runtime:.2f}",
                extra={
                    "runtime": runtime,
                    "message_id": m.id,
                    "processed": processed,
                    "total_messages_processed": total_messages_processed,
                    "uptime": (
                        datetime.now(timezone.utc) - start_timestamp
                    ).total_seconds(),
                },
            )
            queue_to_process.task_done()
            drained.set()

            if not processed and config.is_fifo:
                log.warning(
                    f"releasing the messages buffered behind {m.id} to preserve the FIFO order",
                    extra={"message_id": m.id},
                )
                requeue(sqs, _get_all(queue_to_process), queue_url)

            health.heartbeat()
    finally:
        log.info("shutting down consumer")
        stop_fetching.set()

        if sqs_fetch_thread.is_alive():
            wait_time = config.wait_time_seconds + 2
            log.info(f"waiting : {wait_time} seconds for sqs_fetch_thread to finish")
            sqs_fetch_thread.join(timeout=wait_time)

        if sqs_fetch_thread.is_alive():
            log.error("sqs thread still alive after join")

        requeue(sqs, _get_all(queue_to_process), queue_url)
        invoker.close()
        health_server.shutdown()
        health_server.server_close()
        if owns_client:
            sqs.close()
