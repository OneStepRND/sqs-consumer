from datetime import timedelta
from typing import Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

FIFO_SUFFIX = ".fifo"


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
        return (self.queue_name or self.queue_url or "").endswith(FIFO_SUFFIX)

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

        if self.is_fifo != self.dlq_queue_name.endswith(FIFO_SUFFIX):
            raise ValueError(
                "a FIFO queue requires a FIFO dead letter queue, both names must end with .fifo"
            )

        return self

    @model_validator(mode="after")
    def _validate_fetch_pacing(self):
        if self.fetch_min_sleep > self.fetch_max_sleep:
            raise ValueError("fetch_min_sleep must not exceed fetch_max_sleep")

        return self
