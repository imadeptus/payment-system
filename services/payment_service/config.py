"""Environment-driven Payment Service settings."""

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class PaymentSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    payment_database_url: SecretStr
    kafka_bootstrap_servers: str
    payment_commands_topic: str = "payments.commands.v1"
    payment_events_topic: str = "payments.events.v1"
    dlq_topic: str = "saga.dlq.v1"
    reject_order_ids: str = ""
    refund_failure_order_ids: str = ""
    message_retry_attempts: int = 3
    message_backoff_seconds: str = "1,2,4"
    outbox_poll_seconds: float = 0.5
    outbox_batch_size: int = 100
    log_level: str = "INFO"
