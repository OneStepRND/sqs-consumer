import asyncio
import contextlib
import json
import os
import socket
import threading
import time
from collections.abc import Callable, Iterator
from datetime import timedelta
from typing import Any

import boto3
import freezegun
import pytest
import urllib3
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


def get_free_port():
    """Find a free port to use for testing."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        s.listen(1)
        port = s.getsockname()[1]
    return port


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


@pytest.fixture()
def sqs():
    with mock_aws():
        yield boto3.client("sqs")  # pyright: ignore[reportUnknownMemberType]


@pytest.fixture()
def health():
    return Health()


@pytest.fixture()
def config(queue_name: str):
    return Config(
        queue_name=queue_name,
        dlq_queue_name=f"{queue_name}-dlq",
        wait_time_seconds=0,
        health_check_port=get_free_port(),
    )


@pytest.fixture
def queue_name():
    return "foo"


@pytest.fixture
def sqs_queue_url(sqs: "SQSClient"):
    response = sqs.create_queue(QueueName="foo")
    return response["QueueUrl"]


def worker_config(queue_name: str, **overrides: Any) -> Config:
    return Config(
        queue_name=queue_name,
        dlq_queue_name=f"{queue_name}-dlq",
        wait_time_seconds=0,
        visibility_timeout=2,
        max_messages=1,
        max_buffered_messages=1,
        health_check_port=get_free_port(),
        **overrides,
    )


def wait_for(predicate: Callable[[], bool], timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return predicate()


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
        thread.join(timeout=15)


def queue_depth(sqs: "SQSClient", queue_url: str) -> int:
    attributes = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
        ],
    )["Attributes"]
    return int(attributes["ApproximateNumberOfMessages"]) + int(
        attributes["ApproximateNumberOfMessagesNotVisible"]
    )


class VisibilitySpy:
    """Records every change_message_visibility call made on the client."""

    def __init__(self, sqs: "SQSClient"):
        self.timeouts: list[int] = []
        original = sqs.change_message_visibility

        def record(**kwargs: Any) -> Any:
            self.timeouts.append(int(kwargs["VisibilityTimeout"]))
            return original(**kwargs)

        setattr(sqs, "change_message_visibility", record)


class QueueManagementSpy:
    """Records the queue management calls a pre-provisioned consumer must not make."""

    def __init__(self, sqs: "SQSClient"):
        self.created: list[str] = []
        self.resolved: list[str] = []
        create_queue = sqs.create_queue
        get_queue_url = sqs.get_queue_url

        def record_create(**kwargs: Any) -> Any:
            self.created.append(str(kwargs["QueueName"]))
            return create_queue(**kwargs)

        def record_get_url(**kwargs: Any) -> Any:
            self.resolved.append(str(kwargs["QueueName"]))
            return get_queue_url(**kwargs)

        setattr(sqs, "create_queue", record_create)
        setattr(sqs, "get_queue_url", record_get_url)


def test_requeue(sqs: "SQSClient", sqs_queue_url: str):
    for i in range(10):
        sqs.send_message(
            QueueUrl=sqs_queue_url,
            MessageBody=f"message number : {i}",
        )
    msgs = sqs.receive_message(QueueUrl=sqs_queue_url, MaxNumberOfMessages=10)[
        "Messages"
    ]
    assert len(msgs) == 10
    with pytest.raises(KeyError):
        sqs.receive_message(QueueUrl=sqs_queue_url, MaxNumberOfMessages=10)["Messages"]

    requeue(
        sqs,
        [Message.model_validate(x) for x in msgs],
        sqs_queue_url,
    )
    msgs = sqs.receive_message(QueueUrl=sqs_queue_url, MaxNumberOfMessages=10)[
        "Messages"
    ]
    assert len(msgs) == 10


def test_consume_with_create_queue(sqs: "SQSClient", config: Config, health: Health):
    def handler(message: Message):
        pass

    shutdown = GracefulShutdown()
    consumer_thread = threading.Thread(
        target=consume,
        kwargs={
            "handler": handler,
            "config": config,
            "shutdown": shutdown,
            "health": health,
            "sqs": sqs,
        },
    )
    consumer_thread.start()
    timeout = time.time() + 5
    while not health.healthy and time.time() <= timeout:
        time.sleep(0.1)
    assert health.healthy
    shutdown.shutdown()
    consumer_thread.join(timeout=10)


def test_consume(sqs: "SQSClient", config: Config, health: Health, sqs_queue_url: str):
    processed_messages: list[str] = []
    test_messages = [
        sqs.send_message(
            QueueUrl=sqs_queue_url,
            MessageBody=f"message number : {i}",
        )
        for i in range(10)
    ]

    def handler(message: Message):
        processed_messages.append(message.id)

    shutdown = GracefulShutdown()
    consumer_thread = threading.Thread(
        target=consume,
        kwargs={
            "handler": handler,
            "config": config,
            "shutdown": shutdown,
            "health": health,
            "sqs": sqs,
        },
    )
    consumer_thread.start()
    timeout = time.time() + 5
    while len(processed_messages) < len(test_messages) and time.time() <= timeout:
        time.sleep(0.1)

    assert health.healthy
    shutdown.shutdown()
    consumer_thread.join(timeout=10)
    assert len(test_messages) == len(processed_messages)
    response = sqs.receive_message(QueueUrl=sqs_queue_url, WaitTimeSeconds=0)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    with pytest.raises(KeyError):
        response["Messages"]


def test_health_check(health: Health):
    assert health.healthy is False
    health.heartbeat()
    assert health.healthy is True
    with freezegun.freeze_time(timedelta(minutes=1)):
        assert health.healthy is True

    with freezegun.freeze_time(timedelta(minutes=3)):
        assert health.healthy is False


def test_health_server(health: Health, config: Config):
    """Test the health check server using urllib3."""
    # Start health server
    server = create_health_server(health, "127.0.0.1", config.health_check_port)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    # Give server time to start
    time.sleep(0.1)

    # Create urllib3 pool manager
    http = urllib3.PoolManager()
    base_url = f"http://127.0.0.1:{config.health_check_port}"

    # Test health endpoint - should return 503 when not healthy
    resp = http.request("GET", f"{base_url}/health")
    assert resp.status == 503

    # Make healthy
    health.heartbeat()

    # Test health endpoint - should return 200 when healthy
    resp = http.request("GET", f"{base_url}/health")
    assert resp.status == 200

    # Test non-existent endpoint
    resp = http.request("GET", f"{base_url}/nonexistent")
    assert resp.status == 404

    # Cleanup
    server.shutdown()
    http.clear()


def test_consume_async_handler(
    sqs: "SQSClient", config: Config, health: Health, sqs_queue_url: str
):
    processed: list[str] = []
    sent = [
        sqs.send_message(QueueUrl=sqs_queue_url, MessageBody=f"message number : {i}")
        for i in range(3)
    ]

    async def handler(message: Message) -> None:
        await asyncio.sleep(0)
        processed.append(message.id)

    with running_consumer(handler=handler, config=config, health=health, sqs=sqs) as (
        shutdown,
        thread,
    ):
        assert wait_for(lambda: len(processed) == len(sent))
        shutdown.shutdown()
        thread.join(timeout=15)
        assert not thread.is_alive()

    assert queue_depth(sqs, sqs_queue_url) == 0


def test_visibility_heartbeat_extends_the_in_flight_message(
    sqs: "SQSClient", health: Health, sqs_queue_url: str, queue_name: str
):
    config = worker_config(queue_name)
    spy = VisibilitySpy(sqs)
    sqs.send_message(QueueUrl=sqs_queue_url, MessageBody="slow")
    handled = threading.Event()

    async def handler(message: Message) -> None:
        await asyncio.sleep(2.6)
        handled.set()

    with running_consumer(handler=handler, config=config, health=health, sqs=sqs):
        assert wait_for(handled.is_set, timeout=15)

    assert len(spy.timeouts) >= 2
    assert set(spy.timeouts) == {config.visibility_timeout}


def test_health_stays_green_during_a_long_handler(
    sqs: "SQSClient", sqs_queue_url: str, queue_name: str
):
    health = Health(heartbeat_max_age=timedelta(seconds=2))
    config = worker_config(queue_name, health_max_age=2)
    sqs.send_message(QueueUrl=sqs_queue_url, MessageBody="slow")
    started = threading.Event()
    handled = threading.Event()

    async def handler(message: Message) -> None:
        started.set()
        await asyncio.sleep(4)
        handled.set()

    with running_consumer(handler=handler, config=config, health=health, sqs=sqs):
        assert wait_for(started.is_set)
        deadline = time.monotonic() + 3.5
        while time.monotonic() < deadline:
            assert health.healthy
            time.sleep(0.1)
        assert wait_for(handled.is_set, timeout=10)


def test_max_processing_time_cancels_the_handler_and_keeps_the_message(
    sqs: "SQSClient", health: Health, sqs_queue_url: str, queue_name: str
):
    config = worker_config(queue_name, max_processing_time=1, on_error="continue")
    sqs.send_message(QueueUrl=sqs_queue_url, MessageBody="endless")
    cancelled = threading.Event()

    async def handler(message: Message) -> None:
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    with running_consumer(handler=handler, config=config, health=health, sqs=sqs) as (
        _,
        thread,
    ):
        assert wait_for(cancelled.is_set, timeout=15)
        assert thread.is_alive()
        assert queue_depth(sqs, sqs_queue_url) == 1


def test_max_processing_time_requires_an_async_handler(
    sqs: "SQSClient", health: Health, queue_name: str
):
    config = worker_config(queue_name, max_processing_time=1)

    def handler(message: Message) -> None:
        return None

    with pytest.raises(ValueError, match="async handler"):
        consume(
            handler=handler,
            config=config,
            health=health,
            shutdown=GracefulShutdown(),
            sqs=sqs,
        )


def test_shutdown_cancels_the_in_flight_async_handler(
    sqs: "SQSClient", health: Health, sqs_queue_url: str, queue_name: str
):
    config = worker_config(queue_name, shutdown_grace_period=1)
    sqs.send_message(QueueUrl=sqs_queue_url, MessageBody="endless")
    started = threading.Event()
    cancelled = threading.Event()

    async def handler(message: Message) -> None:
        started.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    with running_consumer(handler=handler, config=config, health=health, sqs=sqs) as (
        shutdown,
        thread,
    ):
        assert wait_for(started.is_set)
        shutdown.shutdown()
        assert wait_for(cancelled.is_set, timeout=15)
        thread.join(timeout=15)
        assert not thread.is_alive()

    assert queue_depth(sqs, sqs_queue_url) == 1


def test_shutdown_deletes_the_message_of_a_sync_handler_that_ran_past_the_grace_period(
    sqs: "SQSClient", health: Health, sqs_queue_url: str, queue_name: str
):
    config = worker_config(queue_name, shutdown_grace_period=0)
    sqs.send_message(QueueUrl=sqs_queue_url, MessageBody="slow")
    started = threading.Event()
    handled = threading.Event()

    def handler(message: Message) -> None:
        started.set()
        time.sleep(1)
        handled.set()

    with running_consumer(handler=handler, config=config, health=health, sqs=sqs) as (
        shutdown,
        thread,
    ):
        assert wait_for(started.is_set)
        shutdown.shutdown()
        assert wait_for(handled.is_set, timeout=10)
        thread.join(timeout=15)
        assert not thread.is_alive()

    assert queue_depth(sqs, sqs_queue_url) == 0


def test_bounded_prefetch_keeps_the_fetch_thread_alive(
    sqs: "SQSClient", health: Health, sqs_queue_url: str, queue_name: str
):
    config = worker_config(queue_name, fetch_min_sleep=0, fetch_max_sleep=1)
    for i in range(5):
        sqs.send_message(QueueUrl=sqs_queue_url, MessageBody=f"message number : {i}")
    processed: list[str] = []

    def handler(message: Message) -> None:
        time.sleep(0.2)
        processed.append(message.id)

    with running_consumer(handler=handler, config=config, health=health, sqs=sqs) as (
        shutdown,
        thread,
    ):
        assert wait_for(lambda: len(processed) == 5, timeout=30)
        assert not shutdown.shutdown_requested
        assert thread.is_alive()

    assert queue_depth(sqs, sqs_queue_url) == 0


def test_consume_with_pre_provisioned_queue_url(
    sqs: "SQSClient", health: Health, sqs_queue_url: str
):
    config = Config(
        queue_url=sqs_queue_url,
        wait_time_seconds=0,
        health_check_port=get_free_port(),
    )
    spy = QueueManagementSpy(sqs)
    send(sqs, sqs_queue_url, "hello")
    processed: list[str] = []

    def handler(message: Message) -> None:
        processed.append(message.body)

    with running_consumer(handler=handler, config=config, health=health, sqs=sqs):
        assert wait_for(lambda: processed == ["hello"])

    assert spy.created == []
    assert spy.resolved == []


def test_consume_with_pre_provisioned_queue_name(
    sqs: "SQSClient", health: Health, sqs_queue_url: str, queue_name: str
):
    config = Config(
        queue_name=queue_name,
        create_queues=False,
        wait_time_seconds=0,
        health_check_port=get_free_port(),
    )
    spy = QueueManagementSpy(sqs)
    send(sqs, sqs_queue_url, "hello")
    processed: list[str] = []

    def handler(message: Message) -> None:
        processed.append(message.body)

    with running_consumer(handler=handler, config=config, health=health, sqs=sqs):
        assert wait_for(lambda: processed == ["hello"])

    assert spy.created == []
    assert spy.resolved == [queue_name]


def test_on_error_continue_keeps_consuming(
    sqs: "SQSClient", health: Health, sqs_queue_url: str, queue_name: str
):
    config = worker_config(
        queue_name,
        on_error="continue",
        fetch_min_sleep=0,
        fetch_max_sleep=1,
    )
    for body in ("fail", "ok-1", "ok-2"):
        sqs.send_message(QueueUrl=sqs_queue_url, MessageBody=body)
    succeeded: list[str] = []
    failed: list[str] = []

    def handler(message: Message) -> None:
        if message.body == "fail":
            failed.append(message.id)
            raise RuntimeError("boom")
        succeeded.append(message.body)

    with running_consumer(handler=handler, config=config, health=health, sqs=sqs) as (
        shutdown,
        thread,
    ):
        assert wait_for(lambda: sorted(succeeded) == ["ok-1", "ok-2"], timeout=30)
        assert failed
        assert thread.is_alive()
        assert not shutdown.shutdown_requested

    assert queue_depth(sqs, sqs_queue_url) == 1


def test_on_error_crash_terminates_the_consumer(
    sqs: "SQSClient", config: Config, health: Health, sqs_queue_url: str
):
    assert config.on_error == "crash"
    sqs.send_message(QueueUrl=sqs_queue_url, MessageBody="boom")
    shutdown = GracefulShutdown()

    def handler(message: Message) -> None:
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        consume(
            handler=handler,
            config=config,
            health=health,
            shutdown=shutdown,
            sqs=sqs,
        )

    shutdown.shutdown()
    assert queue_depth(sqs, sqs_queue_url) == 1


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


def test_fifo_queue_requires_a_fifo_dlq():
    with pytest.raises(ValidationError, match="FIFO"):
        Config(queue_name="repair.fifo", dlq_queue_name="repair-dlq")


def test_config_requires_a_queue_name_or_url():
    with pytest.raises(ValidationError, match="queue_url or queue_name"):
        Config()


def test_send_with_fifo_parameters(sqs: "SQSClient"):
    queue_url = sqs.create_queue(
        QueueName="repair.fifo",
        Attributes={"FifoQueue": "true"},
    )["QueueUrl"]

    message_id = send(
        sqs,
        queue_url,
        "case-1",
        group_id="widget-1",
        dedup_id="outbox-1",
    )
    assert message_id

    received = sqs.receive_message(
        QueueUrl=queue_url,
        MessageSystemAttributeNames=["All"],
        WaitTimeSeconds=0,
    )["Messages"][0]
    assert Message.model_validate(received).body == "case-1"
    assert received.get("Attributes", {})["MessageGroupId"] == "widget-1"
