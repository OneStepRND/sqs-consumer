import asyncio
import inspect
import logging
import random
import signal
import threading
import time
import types
from collections.abc import Awaitable, Iterable
from datetime import datetime, timedelta, timezone
from queue import Empty, Full, Queue
from typing import TYPE_CHECKING, Callable, Literal, Protocol, TypeIs, cast

import boto3
from botocore.exceptions import (
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ReadTimeoutError,
)

from sqs_consumer.config import Config
from sqs_consumer.health import Health, create_health_server
from sqs_consumer.queues import Message, requeue, resolve_queue_url

if TYPE_CHECKING:
    from mypy_boto3_sqs.client import SQSClient

log = logging.getLogger(__name__)

SyncHandler = Callable[[Message], None]
AsyncHandler = Callable[[Message], Awaitable[None]]
Handler = SyncHandler | AsyncHandler

CancelReason = Literal["max_processing_time", "shutdown"]

SUPERVISOR_TICK = 0.5

UNREACHABLE_ERRORS = (
    EndpointConnectionError,
    ConnectTimeoutError,
    ReadTimeoutError,
    ConnectionClosedError,
)
"""What botocore raises, once its own retries are spent, when it cannot reach the endpoint
or loses the connection to it: a broker that is restarting or moving. TLS and proxy errors
are left out on purpose, they are settings to fix and must fail at once."""

OUTAGE_RETRY_FIRST = 1.0
OUTAGE_RETRY_MAX = 15.0
"""The pause before the first retry of an unreachable endpoint, doubling up to the max."""

SHUTDOWN_POLL = 0.5


def _pause_unless_shutdown(shutdown: "GracefulShutdown", seconds: float) -> bool:
    """Sleeps up to `seconds`, waking early on a shutdown request. The flag is set from a
    signal handler, so it is polled rather than waited on through a lock."""

    deadline = time.monotonic() + seconds
    while not shutdown.shutdown_requested:
        left = deadline - time.monotonic()
        if left <= 0:
            break
        time.sleep(min(left, SHUTDOWN_POLL))
    return shutdown.shutdown_requested


def _pause_and_heartbeat(
    shutdown: "GracefulShutdown", health: Health, seconds: float
) -> bool:
    """A pause of the main loop, which owns the heartbeat: the liveness probe must see a
    consumer waiting out an outage as alive, not as hung."""

    health.heartbeat()
    interrupted = _pause_unless_shutdown(shutdown, seconds)
    health.heartbeat()
    return interrupted


def through_outage[T](
    call: Callable[[], T],
    *,
    what: str,
    limit: float,
    interrupted: Callable[[float], bool],
) -> T | None:
    """Runs `call`, retrying it while the SQS endpoint cannot be reached, for at most `limit`
    seconds of one outage; past that the last error propagates. Every failed attempt is
    logged with its traceback. Returns None when `interrupted` reports a stop during a pause.

    botocore's own retry settings could stretch a call over the outage too, but a call
    retrying inside botocore can't be cut short by a shutdown and logs its retries only at
    debug level, so the outage is ridden out here, between calls."""

    outage_start: float | None = None
    delay = OUTAGE_RETRY_FIRST
    while True:
        attempt_start = time.monotonic()
        try:
            result = call()
        except UNREACHABLE_ERRORS:
            now = time.monotonic()
            if outage_start is None:
                outage_start = attempt_start
            elapsed = now - outage_start
            if elapsed >= limit:
                log.error(
                    f"{what} : the SQS endpoint stayed unreachable for {elapsed:.0f}s, past endpoint_outage_limit={limit:g}s",
                    extra={"outage_seconds": elapsed},
                )
                raise

            pause = min(delay, limit - elapsed)
            log.warning(
                f"{what} : the SQS endpoint is unreachable, retrying in {pause:.1f}s",
                exc_info=True,
                extra={"outage_seconds": elapsed},
            )
            if interrupted(pause):
                return None
            delay = min(delay * 2, OUTAGE_RETRY_MAX)
            continue

        if outage_start is not None:
            elapsed = time.monotonic() - outage_start
            log.info(
                f"{what} : the SQS endpoint is reachable again after {elapsed:.0f}s",
                extra={"outage_seconds": elapsed},
            )
        return result


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


class MessageBuffer(Queue[Message]):
    """The local prefetch buffer, which counts a message as owned by the process from
    the poll that fetched it until `task_done` settles it — so the message the consumer
    has popped and is handling still takes up capacity."""

    @property
    def owned(self) -> int:
        with self.mutex:
            return self.unfinished_tasks


def has_free_capacity(owned: int, capacity: int, fetch_min_messages: int) -> bool:
    """Whether the process owns few enough messages for another poll to be worth it."""

    return owned < capacity and owned <= fetch_min_messages


def fetch_sqs_thread(
    sqs: "SQSClient",
    *,
    stop: threading.Event,
    drained: threading.Event,
    shutdown: GracefulShutdown,
    config: Config,
    queue: MessageBuffer,
    queue_url: str,
    backoff: Callable[[threading.Event], None] | None = None,
    failed: threading.Event | None = None,
) -> Callable[[], None]:
    def jittered_backoff(until: threading.Event) -> None:
        until.wait(random.uniform(config.fetch_min_sleep, config.fetch_max_sleep))

    pause = backoff or jittered_backoff

    def run() -> None:
        try:
            while not (stop.is_set() or shutdown.shutdown_requested):
                owned = queue.owned
                free_capacity = max(queue.maxsize - owned, 0)
                if not has_free_capacity(
                    owned, queue.maxsize, config.fetch_min_messages
                ):
                    log.debug(f"{owned=} {free_capacity=} waiting for the consumer")
                    pause(drained)
                    drained.clear()
                    continue

                log.debug("fetching message from sqs")

                response = through_outage(
                    lambda: sqs.receive_message(
                        QueueUrl=queue_url,
                        WaitTimeSeconds=config.wait_time_seconds,
                        MaxNumberOfMessages=min(config.max_messages, free_capacity),
                        VisibilityTimeout=config.visibility_timeout,
                        MessageSystemAttributeNames=[
                            "MessageGroupId",
                            "ApproximateReceiveCount",
                        ],
                    ),
                    what="receiving messages",
                    limit=config.endpoint_outage_limit,
                    interrupted=lambda seconds: (
                        stop.wait(seconds) or shutdown.shutdown_requested
                    ),
                )
                if response is None:
                    continue
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
                        through_outage(
                            lambda: requeue(sqs, [message], queue_url),
                            what=f"releasing {message.id}",
                            limit=config.endpoint_outage_limit,
                            interrupted=lambda seconds: (
                                stop.wait(seconds) or shutdown.shutdown_requested
                            ),
                        )

                if not messages:
                    pause(stop)
        except Exception:
            log.exception("failed to fetch sqs message")
            if failed is not None:
                failed.set()
            raise

    return run


def _drain(queue: MessageBuffer) -> Iterable[Message]:
    while True:
        try:
            message = queue.get_nowait()
        except Empty:
            break

        queue.task_done()
        yield message


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


class InFlightSupervisor:
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
        self._extension_interval = max(config.visibility_timeout / 2, SUPERVISOR_TICK)
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
        self._thread.join(timeout=SUPERVISOR_TICK * 4)
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
                self._extension_interval if extended else SUPERVISOR_TICK * 2
            )

    def _run(self) -> None:
        self.start(time.monotonic())

        while not self._stop.wait(SUPERVISOR_TICK):
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

    with InFlightSupervisor(
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

    deleted = through_outage(
        lambda: sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=message.receipt,
        ),
        what=f"deleting {message.id}",
        limit=config.endpoint_outage_limit,
        interrupted=lambda seconds: _pause_and_heartbeat(shutdown, health, seconds),
    )
    if deleted is None:
        log.warning(
            f"shutdown requested before {message.id} could be deleted, it will be delivered again",
            extra={"message_id": message.id},
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
    queue_to_process = MessageBuffer(config.buffer_capacity)
    owns_client = sqs is None
    sqs = sqs or boto3.client("sqs", endpoint_url=config.endpoint_url)  # pyright: ignore[reportUnknownMemberType]
    queue_url = through_outage(
        lambda: resolve_queue_url(sqs, config),
        what="resolving the queue url",
        limit=config.endpoint_outage_limit,
        interrupted=lambda seconds: _pause_unless_shutdown(shutdown, seconds),
    )
    if queue_url is None:
        log.info("shutdown requested while the SQS endpoint was unreachable")
        if owns_client:
            sqs.close()
        return

    health_server = create_health_server(health, "0.0.0.0", config.health_check_port)
    server_thread = threading.Thread(
        target=health_server.serve_forever,
        daemon=True,
        name="health_check",
    )
    stop_fetching = threading.Event()
    drained = threading.Event()
    fetch_failed = threading.Event()
    sqs_fetch_thread = threading.Thread(
        target=fetch_sqs_thread(
            sqs,
            stop=stop_fetching,
            drained=drained,
            shutdown=shutdown,
            config=config,
            queue=queue_to_process,
            queue_url=queue_url,
            failed=fetch_failed,
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
                behind = list(_drain(queue_to_process))
                through_outage(
                    lambda: requeue(sqs, behind, queue_url),
                    what=f"releasing the messages buffered behind {m.id}",
                    limit=config.endpoint_outage_limit,
                    interrupted=lambda seconds: _pause_and_heartbeat(
                        shutdown, health, seconds
                    ),
                )

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

        try:
            requeue(sqs, _drain(queue_to_process), queue_url)
        except UNREACHABLE_ERRORS:
            # Releasing only spares the buffered messages their visibility timeout; they
            # return either way. Logged, and the clean-up and the exit's own error go on.
            log.exception("could not release the buffered messages on shutdown")
        invoker.close()
        health_server.shutdown()
        health_server.server_close()
        if owns_client:
            sqs.close()

    # Set by the fetch thread itself, so a failure that races a shutdown still surfaces.
    if fetch_failed.is_set():
        raise RuntimeError(
            "the sqs fetch thread died, its traceback is logged above as 'failed to fetch sqs message'"
        )
