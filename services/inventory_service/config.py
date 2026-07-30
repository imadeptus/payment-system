"""Environment-driven Inventory Service settings."""

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class InventorySettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    inventory_database_url: SecretStr
    kafka_bootstrap_servers: str
    inventory_commands_topic: str = "inventory.commands.v1"
    inventory_events_topic: str = "inventory.events.v1"
    dlq_topic: str = "saga.dlq.v1"
