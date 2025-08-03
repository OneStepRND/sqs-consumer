import os
import socket
import threading
import time
from datetime import timedelta

import boto3
import freezegun
import pytest
import urllib3
from moto import mock_aws
from mypy_boto3_sqs import SQSClient

from sqs_consumer import (
    Config,
    GracefulShutdown,
    Health,
    Message,
    consume,
    create_health_server,
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
def config(queue_url: str):
    return Config(
        queue_url=queue_url,
        wait_time_seconds=0,
        health_check_port=get_free_port(),
    )


@pytest.fixture
def queue_url(sqs: "SQSClient"):
    response = sqs.create_queue(QueueName="foo")
    return response["QueueUrl"]


def test_consume(sqs: "SQSClient", config: Config, health: Health):
    processed_messages: list[str] = []
    test_messages = [
        sqs.send_message(
            QueueUrl=config.queue_url,
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
    response = sqs.receive_message(QueueUrl=config.queue_url, WaitTimeSeconds=0)
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
