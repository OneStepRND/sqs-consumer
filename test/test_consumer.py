from datetime import timedelta
import os
import threading
import time
from pathlib import Path

import boto3
import pytest
from moto import mock_aws
from mypy_boto3_sqs import SQSClient
from mypy_boto3_sqs.type_defs import MessageTypeDef
import freezegun
from sqs_consumer import Config, GracefulShutdown, SimpleHealthCheck, consume


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
def health_dir(tmp_path: Path):
    health_path = tmp_path / "health"
    health_path.mkdir(parents=True, exist_ok=True)
    return health_path.absolute().as_posix()


@pytest.fixture()
def health(health_dir: str):
    return SimpleHealthCheck(health_dir)


@pytest.fixture()
def config(queue_url: str, health_dir: str):
    return Config(
        queue_url=queue_url,
        health_dir=health_dir,
        wait_time_seconds=0,
    )


@pytest.fixture()
def sqs(aws_envvars: dict[str, str]):
    with mock_aws():
        client: "SQSClient" = boto3.client("sqs")  # type: ignore
        yield client


@pytest.fixture
def queue_url(sqs: "SQSClient"):
    response = sqs.create_queue(QueueName="foo")
    return response["QueueUrl"]


def test_consume(sqs: "SQSClient", config: Config, health: SimpleHealthCheck):
    processed_messages: list[str] = []
    test_messages = [
        sqs.send_message(
            QueueUrl=config.queue_url,
            MessageBody=f"message number : {i}",
        )
        for i in range(10)
    ]

    def handler(message: MessageTypeDef):
        processed_messages.append(message.get("Body", ""))

    shutdown = GracefulShutdown()
    consumer_thread = threading.Thread(
        target=consume,
        kwargs={
            "handler": handler,
            "config": config,
            "shutdown": shutdown,
            "health": health,
        },
        daemon=True,
    )
    consumer_thread.start()
    timeout = time.time() + 5

    while not health.is_ready() and time.time() < timeout:
        time.sleep(0.1)

    assert health.is_ready()
    ok, msg = health.is_healthy()
    assert ok, msg
    shutdown.shutdown_requested = True
    consumer_thread.join(timeout=10)
    assert len(test_messages) == len(processed_messages)
    response = sqs.receive_message(QueueUrl=config.queue_url, WaitTimeSeconds=0)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    with pytest.raises(KeyError):
        response["Messages"]


def test_health_check(health: SimpleHealthCheck):
    assert health.is_ready() is False
    assert health.is_healthy()[0] is False
    health.mark_ready()
    assert health.is_ready() is True
    health.heartbeat()
    assert health.is_healthy()[0] is True
    with freezegun.freeze_time(timedelta(minutes=1)):
        assert health.is_healthy()[0] is True

    with freezegun.freeze_time(timedelta(minutes=3)):
        assert health.is_healthy()[0] is False

    health.heartbeat_file.unlink()
    assert health.is_healthy()[0] is False
    health.ready_file.unlink()
    assert health.is_ready() is False
