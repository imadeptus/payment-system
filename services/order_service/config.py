"""Environment-driven Order Service settings."""

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class OrderSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    order_database_url: SecretStr
    kafka_bootstrap_servers: str
    payment_commands_topic: str = "payments.commands.v1"
    payment_events_topic: str = "payments.events.v1"
    inventory_commands_topic: str = "inventory.commands.v1"
    inventory_events_topic: str = "inventory.events.v1"
    dlq_topic: str = "saga.dlq.v1"
    message_retry_attempts: int = 3
    message_backoff_seconds: str = "1,2,4"
    outbox_poll_seconds: float = 0.5
    outbox_batch_size: int = 100
    log_level: str = "INFO"
