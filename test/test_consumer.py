import asyncio
import contextlib
import json
import os
import signal
import socket
import threading
from collections.abc import Callable, Iterator
from datetime import timedelta
from queue import Queue
from typing import Any, NamedTuple
from unittest.mock import Mock

import boto3
import freezegun
import pytest
import urllib3
from botocore.exceptions import ClientError
from moto import mock_aws
from mypy_boto3_sqs import SQSClient
from pydantic import ValidationError

from sqs_consumer import (
    Config,
    GracefulShutdown,
    Handler,
    Health,
    Message,
    consume,
    create_health_server,
    requeue,
    resolve_queue_url,
    send,
)
from sqs_consumer import consumer as consumer_module
from sqs_consumer.consumer import (
    InFlightSupervisor,
    fetch_sqs_thread,
    has_free_capacity,
)

TIMEOUT = 10.0
"""Every blocking wait in this suite is bounded by a condition, never by a sleep."""


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        s.listen(1)
        return s.getsockname()[1]


def message(index: int = 0) -> Message:
    return Message.model_validate(
        {
            "Body": f"message {index}",
            "MessageId": f"id-{index}",
            "ReceiptHandle": f"receipt-{index}",
        }
    )


class Recorder:
    """A thread-safe list a test can block on until it reaches a given size."""

    def __init__(self) -> None:
        self.items: list[Any] = []
        self._changed = threading.Condition()

    def add(self, item: Any) -> None:
        with self._changed:
            self.items.append(item)
            self._changed.notify_all()

    def wait_for(self, count: int, timeout: float = TIMEOUT) -> bool:
        with self._changed:
            return self._changed.wait_for(lambda: len(self.items) >= count, timeout)

    def __len__(self) -> int:
        with self._changed:
            return len(self.items)


def spy_on(monkeypatch: pytest.MonkeyPatch, sqs: "SQSClient", method: str) -> Recorder:
    """Records the keyword arguments of every call made to `method`, and delegates."""
    calls = Recorder()
    original: Callable[..., Any] = getattr(sqs, method)

    def record(**kwargs: Any) -> Any:
        calls.add(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(sqs, method, record)
    return calls


def spy_on_delivered_messages(
    monkeypatch: pytest.MonkeyPatch, sqs: "SQSClient"
) -> Recorder:
    """Records every message the fetch thread actually received, not just the polls."""
    delivered = Recorder()
    original = sqs.receive_message

    def record(**kwargs: Any) -> Any:
        response = original(**kwargs)
        for m in response.get("Messages", []):
            delivered.add(m.get("MessageId"))
        return response

    monkeypatch.setattr(sqs, "receive_message", record)
    return delivered


def entries_of(calls: Recorder) -> list[dict[str, Any]]:
    return [entry for call in calls.items for entry in call["Entries"]]


def receipts_of(calls: Recorder) -> list[str]:
    return [str(call["ReceiptHandle"]) for call in calls.items]


def receive(sqs: "SQSClient", queue_url: str, **kwargs: Any) -> list[Any]:
    return sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=0, **kwargs).get(
        "Messages", []
    )


@pytest.fixture(scope="session", autouse=True)
def aws_envvars():
    """set mocks for tests to make sure moto doesn't connect to aws"""
    env_vars = dict(
        AWS_ACCESS_KEY_ID="testing",
        AWS_SECRET_ACCESS_KEY="testing",
        AWS_SECURITY_TOKEN="testing",
        AWS_SESSION_TOKEN="testing",
        AWS_DEFAULT_REGION="us-east-1",
    )
    os.environ.update(env_vars)
    return env_vars


@pytest.fixture(autouse=True)
def without_sqs_env(monkeypatch: pytest.MonkeyPatch):
    """Config reads SQS_* from the environment, which must not leak into a test."""
    for key in list(os.environ):
        if key.startswith("SQS_"):
            monkeypatch.delenv(key)


@pytest.fixture(autouse=True)
def fast_supervisor_tick(monkeypatch: pytest.MonkeyPatch):
    """The supervisor polls its deadlines on a wall-clock tick, which only needs to be
    coarse enough in production. Nothing in the suite waits out the default."""
    monkeypatch.setattr(consumer_module, "SUPERVISOR_TICK", 0.01)


@pytest.fixture()
def sqs():
    with mock_aws():
        yield boto3.client("sqs")  # pyright: ignore[reportUnknownMemberType]


@pytest.fixture()
def health():
    return Health()


@pytest.fixture
def queue_name():
    return "foo"


@pytest.fixture()
def config(queue_name: str):
    return Config(
        queue_name=queue_name,
        dlq_queue_name=f"{queue_name}-dlq",
        wait_time_seconds=0,
        fetch_min_sleep=0,
        fetch_max_sleep=0.05,
        health_check_port=get_free_port(),
    )


@pytest.fixture
def sqs_queue_url(sqs: "SQSClient", queue_name: str):
    return sqs.create_queue(QueueName=queue_name)["QueueUrl"]


def worker_config(queue_name: str, **overrides: Any) -> Config:
    settings: dict[str, Any] = {
        "queue_name": queue_name,
        "dlq_queue_name": f"{queue_name}-dlq",
        "wait_time_seconds": 0,
        "visibility_timeout": 30,
        "max_messages": 1,
        "max_buffered_messages": 1,
        "fetch_min_sleep": 0,
        "fetch_max_sleep": 0.05,
        "health_check_port": get_free_port(),
    }
    return Config(**(settings | overrides))


@contextlib.contextmanager
def running_consumer(
    *,
    handler: Handler,
    config: Config,
    health: Health,
    sqs: "SQSClient",
) -> Iterator[tuple[GracefulShutdown, threading.Thread]]:
    shutdown = GracefulShutdown()
    thread = threading.Thread(
        target=consume,
        kwargs={
            "handler": handler,
            "config": config,
            "shutdown": shutdown,
            "health": health,
            "sqs": sqs,
        },
        daemon=True,
    )
    thread.start()
    try:
        yield shutdown, thread
    finally:
        shutdown.shutdown()
        thread.join(timeout=TIMEOUT)
        assert not thread.is_alive(), "the consumer did not shut down"


class FakeInvoker:
    def __init__(self, *, cancellable: bool = True) -> None:
        self.cancellable = cancellable
        self.cancels = 0

    def invoke(self, message: Message) -> None:
        return None

    def cancel(self) -> None:
        self.cancels += 1

    def close(self) -> None:
        return None


def make_supervisor(
    **overrides: Any,
) -> tuple[InFlightSupervisor, Any, Health, GracefulShutdown, FakeInvoker]:
    invoker = FakeInvoker(cancellable=overrides.pop("cancellable", True))
    sqs = Mock(spec=SQSClient)
    health = Health()
    shutdown = GracefulShutdown()
    settings: dict[str, Any] = {
        "queue_url": "https://sqs.us-east-1.amazonaws.com/123456789012/foo",
        "visibility_timeout": 100,
    }
    supervisor = InFlightSupervisor(
        sqs=sqs,
        queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/foo",
        message=message(),
        config=Config(**(settings | overrides)),
        health=health,
        shutdown=shutdown,
        invoker=invoker,
    )
    return supervisor, sqs, health, shutdown, invoker


def test_supervisor_extends_visibility_when_the_handler_starts():
    """The visibility clock starts at receive, so a message that waited in the local
    buffer must be extended before the handler's first tick, not half a timeout later."""
    supervisor, sqs, _, _, _ = make_supervisor(visibility_timeout=100)

    supervisor.start(now=1000.0)

    sqs.change_message_visibility.assert_called_once_with(
        QueueUrl="https://sqs.us-east-1.amazonaws.com/123456789012/foo",
        ReceiptHandle="receipt-0",
        VisibilityTimeout=100,
    )


def test_supervisor_extends_visibility_every_half_timeout():
    supervisor, sqs, _, _, _ = make_supervisor(visibility_timeout=100)

    supervisor.start(now=0.0)
    for now in (10.0, 49.0, 50.0, 99.0, 100.0):
        supervisor.tick(now=now)

    assert sqs.change_message_visibility.call_count == 3


def test_supervisor_retries_a_failed_extension_sooner_than_the_full_interval():
    supervisor, sqs, _, _, _ = make_supervisor(visibility_timeout=100)
    sqs.change_message_visibility.side_effect = ClientError(
        {"Error": {"Code": "RequestThrottled"}}, "ChangeMessageVisibility"
    )

    supervisor.start(now=0.0)
    assert sqs.change_message_visibility.call_count == 1

    supervisor.tick(now=50.0)
    assert sqs.change_message_visibility.call_count == 2

    supervisor.tick(now=50.05)
    assert sqs.change_message_visibility.call_count == 3, (
        "a swallowed failure must be retried well before the message expires"
    )


def test_supervisor_does_not_extend_a_zero_visibility_timeout():
    supervisor, sqs, _, _, _ = make_supervisor(visibility_timeout=0)

    supervisor.start(now=0.0)
    supervisor.tick(now=1000.0)

    sqs.change_message_visibility.assert_not_called()


def test_supervisor_heartbeats_while_the_handler_is_within_its_deadline():
    supervisor, _, health, _, _ = make_supervisor(max_processing_time=60)

    supervisor.start(now=0.0)
    assert health.last_heartbeat is None

    supervisor.tick(now=59.0)
    assert health.last_heartbeat is not None


def test_supervisor_stops_heartbeating_once_the_handler_is_overdue():
    """A wedged handler must fail the liveness probe so the orchestrator restarts it."""
    supervisor, _, health, _, _ = make_supervisor(max_processing_time=60)

    supervisor.start(now=0.0)
    supervisor.tick(now=59.0)
    last_heartbeat = health.last_heartbeat

    supervisor.tick(now=60.0)
    supervisor.tick(now=120.0)

    assert health.last_heartbeat == last_heartbeat


def test_supervisor_cancels_the_handler_after_max_processing_time():
    supervisor, _, _, _, invoker = make_supervisor(max_processing_time=60)

    supervisor.start(now=0.0)
    supervisor.tick(now=59.9)
    assert invoker.cancels == 0
    assert supervisor.cancel_reason is None

    supervisor.tick(now=60.0)
    assert invoker.cancels == 1
    assert supervisor.cancel_reason == "max_processing_time"


def test_supervisor_cancels_the_handler_once_the_shutdown_grace_period_lapses():
    supervisor, _, _, shutdown, invoker = make_supervisor(shutdown_grace_period=30)

    supervisor.start(now=0.0)
    supervisor.tick(now=1000.0)
    assert invoker.cancels == 0, (
        "an idle shutdown must not start the grace period early"
    )

    shutdown.shutdown()
    supervisor.tick(now=1001.0)
    assert invoker.cancels == 0

    supervisor.tick(now=1031.0)
    assert invoker.cancels == 1
    assert supervisor.cancel_reason == "shutdown"


def test_supervisor_cancels_at_most_once():
    supervisor, _, _, _, invoker = make_supervisor(max_processing_time=10)

    supervisor.start(now=0.0)
    for now in (10.0, 11.0, 12.0, 100.0):
        supervisor.tick(now=now)

    assert invoker.cancels == 1


def test_supervisor_does_not_try_to_cancel_a_sync_handler(
    caplog: pytest.LogCaptureFixture,
):
    supervisor, _, _, _, invoker = make_supervisor(
        max_processing_time=10, cancellable=False
    )

    supervisor.start(now=0.0)
    supervisor.tick(now=10.0)

    assert invoker.cancels == 0
    assert supervisor.cancel_reason == "max_processing_time"
    assert "cannot be cancelled" in caplog.text
    assert "ERROR" not in caplog.text, "expected shutdown behaviour is not an error"


@pytest.mark.parametrize(
    ("qsize", "capacity", "fetch_min_messages", "expected"),
    [
        (0, 30, 5, True),
        (5, 30, 5, True),
        (6, 30, 5, False),
        (30, 30, 5, False),
        (1, 1, 5, False),
        (0, 1, 5, True),
    ],
)
def test_has_free_capacity(
    qsize: int, capacity: int, fetch_min_messages: int, expected: bool
):
    assert has_free_capacity(qsize, capacity, fetch_min_messages) is expected


class FetchRun(NamedTuple):
    buffer: Queue[Message]
    paused_on: list[threading.Event]
    drained: threading.Event
    stop: threading.Event


def run_fetch_loop(
    sqs: Any, config: Config, *, backoffs: int, buffer: Queue[Message] | None = None
) -> FetchRun:
    """Drives the fetch loop on this thread, stopping it after `backoffs` pauses."""
    run = FetchRun(
        buffer=buffer if buffer is not None else Queue(config.buffer_capacity),
        paused_on=[],
        drained=threading.Event(),
        stop=threading.Event(),
    )

    def backoff(until: threading.Event) -> None:
        run.paused_on.append(until)
        if len(run.paused_on) >= backoffs:
            run.stop.set()

    fetch_sqs_thread(
        sqs,
        stop=run.stop,
        drained=run.drained,
        shutdown=GracefulShutdown(),
        config=config,
        queue=run.buffer,
        queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/foo",
        backoff=backoff,
    )()
    return run


def test_fetch_polls_once_per_backoff_while_the_queue_is_empty():
    """An empty queue must not turn the fetch loop into a billable hot loop."""
    sqs = Mock(spec=SQSClient)
    sqs.receive_message.return_value = {}
    config = Config(queue_url="https://example.com/q", wait_time_seconds=0)

    run = run_fetch_loop(sqs, config, backoffs=3)

    assert sqs.receive_message.call_count == 3
    assert run.paused_on == [run.stop, run.stop, run.stop]


def test_fetch_waits_for_the_consumer_to_drain_a_full_buffer():
    sqs = Mock(spec=SQSClient)
    config = Config(queue_url="https://example.com/q", max_buffered_messages=1)
    buffer: Queue[Message] = Queue(config.buffer_capacity)
    buffer.put_nowait(message())

    run = run_fetch_loop(sqs, config, backoffs=1, buffer=buffer)

    sqs.receive_message.assert_not_called()
    assert run.paused_on == [run.drained], "it must wake as soon as the consumer drains"


def test_fetch_clamps_the_poll_to_the_free_capacity():
    sqs = Mock(spec=SQSClient)
    sqs.receive_message.return_value = {}
    config = Config(
        queue_url="https://example.com/q", max_messages=10, max_buffered_messages=3
    )
    buffer: Queue[Message] = Queue(config.buffer_capacity)
    buffer.put_nowait(message(1))

    run_fetch_loop(sqs, config, backoffs=1, buffer=buffer)

    assert sqs.receive_message.call_args.kwargs["MaxNumberOfMessages"] == 2


def test_fetch_releases_a_message_that_does_not_fit_in_the_buffer():
    sqs = Mock(spec=SQSClient)
    sqs.receive_message.return_value = {
        "Messages": [
            {"Body": "a", "MessageId": "id-a", "ReceiptHandle": "receipt-a"},
            {"Body": "b", "MessageId": "id-b", "ReceiptHandle": "receipt-b"},
        ]
    }
    sqs.change_message_visibility_batch.return_value = {}
    config = Config(queue_url="https://example.com/q", max_buffered_messages=1)

    run = run_fetch_loop(sqs, config, backoffs=1)

    assert run.buffer.qsize() == 1
    released = sqs.change_message_visibility_batch.call_args.kwargs["Entries"]
    assert [entry["Id"] for entry in released] == ["id-b"]
    assert released[0]["VisibilityTimeout"] == 0


def test_requeue(sqs: "SQSClient", sqs_queue_url: str):
    for i in range(10):
        sqs.send_message(QueueUrl=sqs_queue_url, MessageBody=f"message number : {i}")
    msgs = receive(sqs, sqs_queue_url, MaxNumberOfMessages=10)
    assert len(msgs) == 10
    assert receive(sqs, sqs_queue_url, MaxNumberOfMessages=10) == []

    requeue(sqs, [Message.model_validate(x) for x in msgs], sqs_queue_url)

    assert len(receive(sqs, sqs_queue_url, MaxNumberOfMessages=10)) == 10


def test_requeue_splits_into_batches_of_ten(
    sqs: "SQSClient", sqs_queue_url: str, monkeypatch: pytest.MonkeyPatch
):
    calls = spy_on(monkeypatch, sqs, "change_message_visibility_batch")

    requeue(sqs, [message(i) for i in range(25)], sqs_queue_url)

    assert [len(call["Entries"]) for call in calls.items] == [10, 10, 5]
    assert {entry["VisibilityTimeout"] for entry in entries_of(calls)} == {0}


def test_requeue_logs_the_failed_entries(
    sqs: "SQSClient",
    sqs_queue_url: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    monkeypatch.setattr(
        sqs,
        "change_message_visibility_batch",
        lambda **_: {
            "Failed": [
                {"Id": "id-0", "Message": "ReceiptHandleIsInvalid", "SenderFault": True}
            ]
        },
    )

    requeue(sqs, [message()], sqs_queue_url)

    assert "failed to reset the VisibilityTimeout of id-0" in caplog.text


def test_consume_creates_the_queue_and_its_dlq(
    sqs: "SQSClient", config: Config, health: Health, monkeypatch: pytest.MonkeyPatch
):
    polls = spy_on(monkeypatch, sqs, "receive_message")

    with running_consumer(
        handler=lambda m: None, config=config, health=health, sqs=sqs
    ):
        assert polls.wait_for(1), "the consumer never started polling"

    attributes = sqs.get_queue_attributes(
        QueueUrl=sqs.get_queue_url(QueueName="foo")["QueueUrl"],
        AttributeNames=["RedrivePolicy", "VisibilityTimeout", "MessageRetentionPeriod"],
    )["Attributes"]
    redrive = json.loads(attributes["RedrivePolicy"])
    assert int(redrive["maxReceiveCount"]) == config.max_retries_until_dlq
    assert redrive["deadLetterTargetArn"].endswith("foo-dlq")
    assert attributes["VisibilityTimeout"] == str(config.visibility_timeout)
    assert attributes["MessageRetentionPeriod"] == str(
        int(timedelta(days=14).total_seconds())
    )


def test_consume(
    sqs: "SQSClient",
    config: Config,
    health: Health,
    sqs_queue_url: str,
    monkeypatch: pytest.MonkeyPatch,
):
    deletes = spy_on(monkeypatch, sqs, "delete_message")
    sent = [
        sqs.send_message(QueueUrl=sqs_queue_url, MessageBody=f"message number : {i}")[
            "MessageId"
        ]
        for i in range(10)
    ]
    processed = Recorder()

    with running_consumer(
        handler=lambda m: processed.add(m.id), config=config, health=health, sqs=sqs
    ):
        assert processed.wait_for(len(sent))
        assert health.healthy

    assert sorted(processed.items) == sorted(sent)
    assert len(deletes) == len(sent)
    assert receive(sqs, sqs_queue_url) == []


def test_consume_async_handler(
    sqs: "SQSClient",
    config: Config,
    health: Health,
    sqs_queue_url: str,
    monkeypatch: pytest.MonkeyPatch,
):
    deletes = spy_on(monkeypatch, sqs, "delete_message")
    sent = [
        sqs.send_message(QueueUrl=sqs_queue_url, MessageBody=f"message number : {i}")[
            "MessageId"
        ]
        for i in range(3)
    ]
    processed = Recorder()

    async def handler(message: Message) -> None:
        processed.add(message.id)

    with running_consumer(handler=handler, config=config, health=health, sqs=sqs):
        assert processed.wait_for(len(sent))

    assert sorted(processed.items) == sorted(sent)
    assert len(deletes) == len(sent)
    assert receive(sqs, sqs_queue_url) == []


def test_async_handler_reuses_one_event_loop(
    sqs: "SQSClient", config: Config, health: Health, sqs_queue_url: str
):
    loops = Recorder()
    for i in range(2):
        sqs.send_message(QueueUrl=sqs_queue_url, MessageBody=f"message {i}")

    async def handler(message: Message) -> None:
        loops.add(id(asyncio.get_running_loop()))

    with running_consumer(handler=handler, config=config, health=health, sqs=sqs):
        assert loops.wait_for(2)

    assert len(set(loops.items)) == 1


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT])
def test_with_signal_requests_shutdown(monkeypatch: pytest.MonkeyPatch, signum: int):
    handlers: dict[int, Any] = {}
    monkeypatch.setattr(
        signal, "signal", lambda num, handler: handlers.__setitem__(num, handler)
    )

    shutdown = GracefulShutdown.with_signal()

    assert set(handlers) == {signal.SIGTERM, signal.SIGINT}
    assert not shutdown.shutdown_requested

    handlers[signum](signum, None)

    assert shutdown.shutdown_requested


@pytest.fixture
def idle_consumer_config() -> Config:
    return Config(
        queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/foo",
        wait_time_seconds=0,
        health_check_port=get_free_port(),
    )


def test_consume_closes_a_client_it_created(
    health: Health, idle_consumer_config: Config, monkeypatch: pytest.MonkeyPatch
):
    client = Mock(spec=SQSClient)
    client.receive_message.return_value = {}
    monkeypatch.setattr(boto3, "client", lambda *_, **__: client)
    shutdown = GracefulShutdown()
    shutdown.shutdown()

    consume(
        handler=lambda m: None,
        config=idle_consumer_config,
        health=health,
        shutdown=shutdown,
    )

    client.close.assert_called_once()


def test_consume_leaves_a_caller_owned_client_open(
    health: Health, idle_consumer_config: Config
):
    """Closing a client the caller passed in would break their next call."""
    client = Mock(spec=SQSClient)
    client.receive_message.return_value = {}
    shutdown = GracefulShutdown()
    shutdown.shutdown()

    consume(
        handler=lambda m: None,
        config=idle_consumer_config,
        health=health,
        shutdown=shutdown,
        sqs=client,
    )

    client.close.assert_not_called()


def test_the_consume_loop_keeps_the_heartbeat_fresh(
    sqs: "SQSClient", config: Config, health: Health, monkeypatch: pytest.MonkeyPatch
):
    beats = Recorder()
    heartbeat = health.heartbeat

    def record() -> None:
        heartbeat()
        beats.add(health.last_heartbeat)

    monkeypatch.setattr(health, "heartbeat", record)

    with running_consumer(
        handler=lambda m: None, config=config, health=health, sqs=sqs
    ):
        assert beats.wait_for(2), "an idle consume loop must keep beating"
        assert health.healthy


def test_health_check(health: Health):
    assert health.healthy is False
    health.heartbeat()
    assert health.healthy is True
    with freezegun.freeze_time(timedelta(minutes=1)):
        assert health.healthy is True

    with freezegun.freeze_time(timedelta(minutes=3)):
        assert health.healthy is False


@pytest.fixture
def health_server(health: Health) -> Iterator[str]:
    port = get_free_port()
    server = create_health_server(health, "127.0.0.1", port)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=TIMEOUT)


@pytest.mark.parametrize("method", ["GET", "HEAD"])
def test_health_server(health: Health, health_server: str, method: str):
    http = urllib3.PoolManager()
    try:
        assert http.request(method, f"{health_server}/health").status == 503

        health.heartbeat()

        assert http.request(method, f"{health_server}/health").status == 200
        assert http.request(method, f"{health_server}/nonexistent").status == 404
    finally:
        http.clear()


def test_shutdown_cancels_the_in_flight_async_handler(
    sqs: "SQSClient",
    health: Health,
    sqs_queue_url: str,
    queue_name: str,
    monkeypatch: pytest.MonkeyPatch,
):
    config = worker_config(queue_name, shutdown_grace_period=0)
    deletes = spy_on(monkeypatch, sqs, "delete_message")
    released = spy_on(monkeypatch, sqs, "change_message_visibility_batch")
    sent = sqs.send_message(QueueUrl=sqs_queue_url, MessageBody="endless")
    started = threading.Event()
    cancelled = threading.Event()

    async def handler(message: Message) -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    with running_consumer(handler=handler, config=config, health=health, sqs=sqs) as (
        shutdown,
        _,
    ):
        assert started.wait(TIMEOUT)
        shutdown.shutdown()
        assert cancelled.wait(TIMEOUT)

    assert deletes.items == []
    assert [entry["Id"] for entry in entries_of(released)] == [sent["MessageId"]], (
        "the interrupted message must be handed back for the successor to pick up"
    )
    assert {entry["VisibilityTimeout"] for entry in entries_of(released)} == {0}
    assert len(receive(sqs, sqs_queue_url)) == 1, (
        "it must be receivable at once, not after the visibility timeout"
    )


def test_shutdown_deletes_the_message_of_a_sync_handler_that_ran_past_the_grace_period(
    sqs: "SQSClient",
    health: Health,
    sqs_queue_url: str,
    queue_name: str,
    monkeypatch: pytest.MonkeyPatch,
):
    config = worker_config(queue_name, shutdown_grace_period=0)
    deletes = spy_on(monkeypatch, sqs, "delete_message")
    sqs.send_message(QueueUrl=sqs_queue_url, MessageBody="slow")
    started = threading.Event()
    release = threading.Event()

    def handler(message: Message) -> None:
        started.set()
        assert release.wait(TIMEOUT)

    with running_consumer(handler=handler, config=config, health=health, sqs=sqs) as (
        shutdown,
        _,
    ):
        assert started.wait(TIMEOUT)
        shutdown.shutdown()
        release.set()
        assert deletes.wait_for(1), "a sync handler runs to completion and is deleted"

    assert len(deletes) == 1


def test_shutdown_releases_the_buffered_backlog(
    sqs: "SQSClient",
    health: Health,
    sqs_queue_url: str,
    queue_name: str,
    monkeypatch: pytest.MonkeyPatch,
):
    config = worker_config(queue_name, max_buffered_messages=3)
    for i in range(4):
        sqs.send_message(QueueUrl=sqs_queue_url, MessageBody=f"message {i}")
    released = spy_on(monkeypatch, sqs, "change_message_visibility_batch")
    delivered = spy_on_delivered_messages(monkeypatch, sqs)
    started = threading.Event()
    release = threading.Event()

    def handler(message: Message) -> None:
        started.set()
        assert release.wait(TIMEOUT)

    with running_consumer(handler=handler, config=config, health=health, sqs=sqs) as (
        shutdown,
        _,
    ):
        assert started.wait(TIMEOUT)
        assert delivered.wait_for(4), "the fetch thread never filled the buffer"
        shutdown.shutdown()
        release.set()

    assert len(entries_of(released)) == 3, "every buffered message must be released"
    assert {entry["VisibilityTimeout"] for entry in entries_of(released)} == {0}
    assert len(receive(sqs, sqs_queue_url, MaxNumberOfMessages=10)) == 3


def test_max_processing_time_cancels_the_handler_and_keeps_the_message(
    sqs: "SQSClient",
    health: Health,
    sqs_queue_url: str,
    queue_name: str,
    monkeypatch: pytest.MonkeyPatch,
):
    config = worker_config(queue_name, max_processing_time=0.05, on_error="continue")
    deletes = spy_on(monkeypatch, sqs, "delete_message")
    released = spy_on(monkeypatch, sqs, "change_message_visibility_batch")
    sqs.send_message(QueueUrl=sqs_queue_url, MessageBody="endless")
    cancelled = threading.Event()

    async def handler(message: Message) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    with running_consumer(handler=handler, config=config, health=health, sqs=sqs) as (
        shutdown,
        _,
    ):
        assert cancelled.wait(TIMEOUT)
        shutdown.shutdown()

    assert deletes.items == [], "a cancelled message must be left for redelivery"
    assert entries_of(released) == [], (
        "the in-flight message keeps its visibility, SQS redelivers it on expiry"
    )


def test_max_processing_time_crashes_the_consumer_with_a_timeout_error(
    sqs: "SQSClient", health: Health, sqs_queue_url: str, queue_name: str
):
    config = worker_config(queue_name, max_processing_time=0.05)
    assert config.on_error == "crash"
    sqs.send_message(QueueUrl=sqs_queue_url, MessageBody="endless")

    async def handler(message: Message) -> None:
        await asyncio.Event().wait()

    with pytest.raises(TimeoutError, match="max_processing_time"):
        consume(
            handler=handler,
            config=config,
            health=health,
            shutdown=GracefulShutdown(),
            sqs=sqs,
        )


def test_max_processing_time_requires_an_async_handler(
    sqs: "SQSClient", health: Health, queue_name: str
):
    config = worker_config(queue_name, max_processing_time=1)

    with pytest.raises(ValueError, match="async handler"):
        consume(
            handler=lambda m: None,
            config=config,
            health=health,
            shutdown=GracefulShutdown(),
            sqs=sqs,
        )


def test_bounded_prefetch_drains_the_whole_queue(
    sqs: "SQSClient",
    health: Health,
    sqs_queue_url: str,
    queue_name: str,
    monkeypatch: pytest.MonkeyPatch,
):
    config = worker_config(queue_name, max_messages=10, max_buffered_messages=2)
    received = spy_on(monkeypatch, sqs, "receive_message")
    deletes = spy_on(monkeypatch, sqs, "delete_message")
    for i in range(5):
        sqs.send_message(QueueUrl=sqs_queue_url, MessageBody=f"message number : {i}")
    processed = Recorder()

    with running_consumer(
        handler=lambda m: processed.add(m.id), config=config, health=health, sqs=sqs
    ) as (shutdown, _):
        assert processed.wait_for(5)
        assert not shutdown.shutdown_requested

    assert len(deletes) == 5
    assert all(
        call["MaxNumberOfMessages"] <= config.buffer_capacity for call in received.items
    )


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_a_dead_fetch_thread_shuts_down_the_consumer(health: Health):
    config = Config(
        queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/foo",
        wait_time_seconds=0,
        health_check_port=get_free_port(),
    )
    sqs = Mock(spec=SQSClient)
    sqs.receive_message.side_effect = RuntimeError("sqs is unreachable")
    shutdown = GracefulShutdown()

    consume(
        handler=lambda m: None,
        config=config,
        health=health,
        shutdown=shutdown,
        sqs=sqs,
    )

    assert shutdown.shutdown_requested


def test_consume_with_pre_provisioned_queue_url(
    sqs: "SQSClient",
    health: Health,
    sqs_queue_url: str,
    monkeypatch: pytest.MonkeyPatch,
):
    config = Config(
        queue_url=sqs_queue_url,
        wait_time_seconds=0,
        fetch_min_sleep=0,
        fetch_max_sleep=0.05,
        health_check_port=get_free_port(),
    )
    created = spy_on(monkeypatch, sqs, "create_queue")
    resolved = spy_on(monkeypatch, sqs, "get_queue_url")
    send(sqs, sqs_queue_url, "hello")
    processed = Recorder()

    with running_consumer(
        handler=lambda m: processed.add(m.body), config=config, health=health, sqs=sqs
    ):
        assert processed.wait_for(1)

    assert processed.items == ["hello"]
    assert created.items == []
    assert resolved.items == []


def test_consume_with_pre_provisioned_queue_name(
    sqs: "SQSClient",
    health: Health,
    sqs_queue_url: str,
    queue_name: str,
    monkeypatch: pytest.MonkeyPatch,
):
    config = Config(
        queue_name=queue_name,
        create_queues=False,
        wait_time_seconds=0,
        fetch_min_sleep=0,
        fetch_max_sleep=0.05,
        health_check_port=get_free_port(),
    )
    created = spy_on(monkeypatch, sqs, "create_queue")
    resolved = spy_on(monkeypatch, sqs, "get_queue_url")
    send(sqs, sqs_queue_url, "hello")
    processed = Recorder()

    with running_consumer(
        handler=lambda m: processed.add(m.body), config=config, health=health, sqs=sqs
    ):
        assert processed.wait_for(1)

    assert created.items == []
    assert [call["QueueName"] for call in resolved.items] == [queue_name]


def test_on_error_continue_keeps_consuming(
    sqs: "SQSClient",
    health: Health,
    sqs_queue_url: str,
    queue_name: str,
    monkeypatch: pytest.MonkeyPatch,
):
    config = worker_config(queue_name, on_error="continue")
    for body in ("fail", "ok-1", "ok-2"):
        sqs.send_message(QueueUrl=sqs_queue_url, MessageBody=body)
    deletes = spy_on(monkeypatch, sqs, "delete_message")
    succeeded = Recorder()
    failed = Recorder()

    def handler(message: Message) -> None:
        if message.body == "fail":
            failed.add(message.receipt)
            raise RuntimeError("boom")
        succeeded.add(message.receipt)

    with running_consumer(handler=handler, config=config, health=health, sqs=sqs) as (
        shutdown,
        _,
    ):
        assert succeeded.wait_for(2)
        assert len(failed) == 1
        assert not shutdown.shutdown_requested

    assert receipts_of(deletes) == succeeded.items
    assert set(receipts_of(deletes)).isdisjoint(failed.items)


def test_on_error_crash_terminates_the_consumer_and_releases_the_buffer(
    sqs: "SQSClient",
    config: Config,
    health: Health,
    sqs_queue_url: str,
    monkeypatch: pytest.MonkeyPatch,
):
    assert config.on_error == "crash"
    deletes = spy_on(monkeypatch, sqs, "delete_message")
    released = spy_on(monkeypatch, sqs, "change_message_visibility_batch")
    for i in range(5):
        sqs.send_message(QueueUrl=sqs_queue_url, MessageBody=f"message {i}")

    def handler(message: Message) -> None:
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        consume(
            handler=handler,
            config=config,
            health=health,
            shutdown=GracefulShutdown(),
            sqs=sqs,
        )

    assert deletes.items == []
    assert entries_of(released), "the buffered backlog was not released on a crash"
    assert {entry["VisibilityTimeout"] for entry in entries_of(released)} == {0}


def test_fifo_queue_creation(sqs: "SQSClient"):
    config = Config(queue_name="repair.fifo", dlq_queue_name="repair-dlq.fifo")
    queue_url = resolve_queue_url(sqs, config)

    attributes = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["FifoQueue", "RedrivePolicy"],
    )["Attributes"]
    dlq_attributes = sqs.get_queue_attributes(
        QueueUrl=sqs.get_queue_url(QueueName="repair-dlq.fifo")["QueueUrl"],
        AttributeNames=["FifoQueue", "QueueArn"],
    )["Attributes"]

    assert attributes["FifoQueue"] == "true"
    assert dlq_attributes["FifoQueue"] == "true"
    assert (
        json.loads(attributes["RedrivePolicy"])["deadLetterTargetArn"]
        == dlq_attributes["QueueArn"]
    )


def test_fifo_releases_the_messages_buffered_behind_a_failure(
    sqs: "SQSClient", health: Health, monkeypatch: pytest.MonkeyPatch
):
    """SQS hands over a whole message group in one poll, so the siblings of a failed
    message are already buffered locally. Handling them would reorder the group."""
    config = Config(
        queue_name="repair.fifo",
        dlq_queue_name="repair-dlq.fifo",
        wait_time_seconds=0,
        max_messages=3,
        max_buffered_messages=3,
        on_error="continue",
        fetch_min_sleep=0,
        fetch_max_sleep=0.05,
        health_check_port=get_free_port(),
    )
    queue_url = resolve_queue_url(sqs, config)
    for i in range(3):
        send(sqs, queue_url, f"message {i}", group_id="g", dedup_id=f"dedup-{i}")

    delivered = spy_on_delivered_messages(monkeypatch, sqs)
    released = spy_on(monkeypatch, sqs, "change_message_visibility_batch")
    handled = Recorder()
    release = threading.Event()

    def handler(message: Message) -> None:
        handled.add(message.body)
        assert release.wait(TIMEOUT)
        raise RuntimeError("boom")

    with running_consumer(handler=handler, config=config, health=health, sqs=sqs) as (
        shutdown,
        _,
    ):
        assert handled.wait_for(1)
        assert delivered.wait_for(3), "the whole group must arrive in one poll"
        release.set()
        assert released.wait_for(1)
        shutdown.shutdown()

    assert len(released.items[0]["Entries"]) == 2, (
        "both siblings of the failed message must be released"
    )
    assert {entry["VisibilityTimeout"] for entry in entries_of(released)} == {0}
    assert handled.items == ["message 0"], "no sibling may be handled past the failure"
    assert {entry["VisibilityTimeout"] for entry in entries_of(released)} == {0}


def test_an_existing_queue_keeps_its_attributes(
    sqs: "SQSClient", queue_name: str, caplog: pytest.LogCaptureFixture
):
    existing = sqs.create_queue(
        QueueName=queue_name, Attributes={"VisibilityTimeout": "42"}
    )["QueueUrl"]
    config = Config(
        queue_name=queue_name,
        dlq_queue_name=f"{queue_name}-dlq",
        visibility_timeout=99,
    )

    assert resolve_queue_url(sqs, config) == existing
    assert (
        sqs.get_queue_attributes(
            QueueUrl=existing, AttributeNames=["VisibilityTimeout"]
        )["Attributes"]["VisibilityTimeout"]
        == "42"
    )
    assert "already exists with different attributes" in caplog.text


def test_an_existing_dlq_does_not_block_creating_the_queue(
    sqs: "SQSClient", queue_name: str
):
    sqs.create_queue(
        QueueName=f"{queue_name}-dlq", Attributes={"MessageRetentionPeriod": "60"}
    )
    config = Config(queue_name=queue_name, dlq_queue_name=f"{queue_name}-dlq")

    queue_url = resolve_queue_url(sqs, config)

    assert queue_url == sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    redrive = json.loads(
        sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["RedrivePolicy"])[
            "Attributes"
        ]["RedrivePolicy"]
    )
    assert redrive["deadLetterTargetArn"].endswith(f"{queue_name}-dlq")


def test_an_unexpected_client_error_propagates(
    sqs: "SQSClient", queue_name: str, monkeypatch: pytest.MonkeyPatch
):
    def denied(**_: Any) -> Any:
        raise ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "nope"}}, "CreateQueue"
        )

    monkeypatch.setattr(sqs, "create_queue", denied)
    config = Config(queue_name=queue_name, dlq_queue_name=f"{queue_name}-dlq")

    with pytest.raises(ClientError, match="AccessDenied"):
        resolve_queue_url(sqs, config)


def test_fifo_queue_requires_a_fifo_dlq():
    with pytest.raises(ValidationError, match="FIFO"):
        Config(queue_name="repair.fifo", dlq_queue_name="repair-dlq")


def test_config_requires_a_queue_name_or_url():
    with pytest.raises(ValidationError, match="queue_url or queue_name"):
        Config()


def test_config_requires_a_dlq_name_when_creating_queues():
    with pytest.raises(ValidationError, match="dlq_queue_name is required"):
        Config(queue_name="foo")


def test_config_rejects_inverted_fetch_sleeps():
    with pytest.raises(ValidationError, match="fetch_min_sleep"):
        Config(queue_url="https://example.com/q", fetch_min_sleep=3, fetch_max_sleep=1)


@pytest.mark.parametrize("blank", ["", None])
def test_config_treats_a_blank_queue_name_as_missing(blank: str | None):
    with pytest.raises(ValidationError, match="queue_url or queue_name"):
        Config(queue_name=blank, queue_url=blank)


def test_config_defaults_the_buffer_to_three_polls():
    assert (
        Config(queue_url="https://example.com/q", max_messages=4).buffer_capacity == 12
    )
    assert (
        Config(
            queue_url="https://example.com/q", max_messages=4, max_buffered_messages=2
        ).buffer_capacity
        == 2
    )


def test_config_detects_a_fifo_queue_url():
    assert Config(queue_url="https://example.com/repair.fifo").is_fifo
    assert not Config(queue_url="https://example.com/repair").is_fifo


def test_message_reads_the_system_attributes():
    parsed = Message.model_validate(
        {
            "Body": "body",
            "MessageId": "id-0",
            "ReceiptHandle": "receipt-0",
            "Attributes": {"MessageGroupId": "g", "ApproximateReceiveCount": "3"},
        }
    )

    assert parsed.group_id == "g"
    assert parsed.receive_count == 3


def test_message_without_system_attributes_leaves_them_unset():
    parsed = Message.model_validate(
        {"Body": "body", "MessageId": "id-0", "ReceiptHandle": "receipt-0"}
    )

    assert parsed.group_id is None
    assert parsed.receive_count is None


def test_the_handler_sees_the_group_and_the_receive_count(
    sqs: "SQSClient", health: Health
):
    config = Config(
        queue_name="repair.fifo",
        dlq_queue_name="repair-dlq.fifo",
        wait_time_seconds=0,
        on_error="continue",
        fetch_min_sleep=0,
        fetch_max_sleep=0.05,
        health_check_port=get_free_port(),
    )
    queue_url = resolve_queue_url(sqs, config)
    send(sqs, queue_url, "case-1", group_id="widget-7", dedup_id="outbox-1")
    handled = Recorder()

    with running_consumer(
        handler=lambda m: handled.add((m.group_id, m.receive_count)),
        config=config,
        health=health,
        sqs=sqs,
    ):
        assert handled.wait_for(1)

    assert handled.items == [("widget-7", 1)]


def test_send_with_fifo_parameters(sqs: "SQSClient", monkeypatch: pytest.MonkeyPatch):
    queue_url = sqs.create_queue(
        QueueName="repair.fifo", Attributes={"FifoQueue": "true"}
    )["QueueUrl"]
    sent = spy_on(monkeypatch, sqs, "send_message")

    message_id = send(
        sqs, queue_url, "case-1", group_id="widget-1", dedup_id="outbox-1"
    )

    assert sent.items == [
        {
            "QueueUrl": queue_url,
            "MessageBody": "case-1",
            "MessageGroupId": "widget-1",
            "MessageDeduplicationId": "outbox-1",
        }
    ]
    received = receive(sqs, queue_url, MessageSystemAttributeNames=["All"])[0]
    assert Message.model_validate(received).id == message_id
    assert received.get("Attributes", {})["MessageGroupId"] == "widget-1"


def test_send_omits_the_fifo_parameters_for_a_standard_queue(
    sqs: "SQSClient", sqs_queue_url: str, monkeypatch: pytest.MonkeyPatch
):
    sent = spy_on(monkeypatch, sqs, "send_message")

    send(sqs, sqs_queue_url, "case-1")

    assert sent.items == [{"QueueUrl": sqs_queue_url, "MessageBody": "case-1"}]
