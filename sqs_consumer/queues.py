import itertools
import json
import logging
from collections.abc import Iterable
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from botocore.exceptions import ClientError
from pydantic import AliasPath, BaseModel, Field

from sqs_consumer.config import Config

if TYPE_CHECKING:
    from mypy_boto3_sqs.client import SQSClient
    from mypy_boto3_sqs.literals import QueueAttributeNameType

log = logging.getLogger(__name__)


class Message(BaseModel):
    body: str = Field(alias="Body")
    id: str = Field(alias="MessageId")
    receipt: str = Field(alias="ReceiptHandle")
    group_id: str | None = Field(
        default=None,
        validation_alias=AliasPath("Attributes", "MessageGroupId"),
        description="The FIFO message group, to publish a follow up into the same group",
    )
    receive_count: int | None = Field(
        default=None,
        validation_alias=AliasPath("Attributes", "ApproximateReceiveCount"),
        description="Deliveries of this message so far, 1 on the first, compare against max_retries_until_dlq",
    )


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


QUEUE_EXISTS_CODES = frozenset({"QueueAlreadyExists", "QueueNameExists"})
"""SQS reports a name taken by a queue with other attributes under either code."""


def _create_queue(
    sqs: "SQSClient", name: str, attributes: dict["QueueAttributeNameType", str]
) -> str:
    try:
        return sqs.create_queue(QueueName=name, Attributes=attributes)["QueueUrl"]
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in QUEUE_EXISTS_CODES:
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
