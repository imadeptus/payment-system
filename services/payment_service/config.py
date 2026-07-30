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
