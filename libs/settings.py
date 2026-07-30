"""Shared validated runtime settings for all Saga services."""

from typing import Self

from pydantic import PositiveFloat, PositiveInt, PrivateAttr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class RuntimeSettings(BaseSettings):
    """Validate common retry and Outbox configuration during process startup."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str
    message_retry_attempts: PositiveInt = 3
    message_backoff_seconds: str = "1,2,4"
    outbox_poll_seconds: PositiveFloat = 0.5
    outbox_batch_size: PositiveInt = 100
    log_level: str = "INFO"

    _backoff_delays: tuple[float, ...] = PrivateAttr()

    @model_validator(mode="after")
    def validate_backoff(self) -> Self:
        try:
            delays = tuple(
                float(value.strip())
                for value in self.message_backoff_seconds.split(",")
            )
        except ValueError as exc:
            raise ValueError("message backoff delays must be numbers") from exc
        if not delays or any(delay <= 0 for delay in delays):
            raise ValueError("message backoff delays must be positive")
        if len(delays) < self.message_retry_attempts:
            raise ValueError("one delay is required for every attempt")
        self._backoff_delays = delays
        return self

    @property
    def backoff_delays(self) -> tuple[float, ...]:
        return self._backoff_delays
