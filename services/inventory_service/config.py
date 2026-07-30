"""Environment-driven Inventory Service settings."""

from pydantic import SecretStr

from libs.settings import RuntimeSettings


class InventorySettings(RuntimeSettings):
    inventory_database_url: SecretStr
    inventory_commands_topic: str = "inventory.commands.v1"
    inventory_events_topic: str = "inventory.events.v1"
    dlq_topic: str = "saga.dlq.v1"
