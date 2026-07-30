"""Stable routing for version-one Saga messages."""

import os

PAYMENT_COMMANDS_TOPIC = "payments.commands.v1"
PAYMENT_EVENTS_TOPIC = "payments.events.v1"
INVENTORY_COMMANDS_TOPIC = "inventory.commands.v1"
INVENTORY_EVENTS_TOPIC = "inventory.events.v1"
DLQ_TOPIC = "saga.dlq.v1"

_TOPICS_BY_TYPE = {
    "AuthorizePayment": ("PAYMENT_COMMANDS_TOPIC", PAYMENT_COMMANDS_TOPIC),
    "RefundPayment": ("PAYMENT_COMMANDS_TOPIC", PAYMENT_COMMANDS_TOPIC),
    "ReserveInventory": ("INVENTORY_COMMANDS_TOPIC", INVENTORY_COMMANDS_TOPIC),
    "ReleaseInventory": ("INVENTORY_COMMANDS_TOPIC", INVENTORY_COMMANDS_TOPIC),
    "PaymentAuthorized": ("PAYMENT_EVENTS_TOPIC", PAYMENT_EVENTS_TOPIC),
    "PaymentRejected": ("PAYMENT_EVENTS_TOPIC", PAYMENT_EVENTS_TOPIC),
    "PaymentRefunded": ("PAYMENT_EVENTS_TOPIC", PAYMENT_EVENTS_TOPIC),
    "PaymentRefundFailed": ("PAYMENT_EVENTS_TOPIC", PAYMENT_EVENTS_TOPIC),
    "InventoryReserved": ("INVENTORY_EVENTS_TOPIC", INVENTORY_EVENTS_TOPIC),
    "InventoryRejected": ("INVENTORY_EVENTS_TOPIC", INVENTORY_EVENTS_TOPIC),
    "InventoryReleased": ("INVENTORY_EVENTS_TOPIC", INVENTORY_EVENTS_TOPIC),
    "InventoryReleaseFailed": ("INVENTORY_EVENTS_TOPIC", INVENTORY_EVENTS_TOPIC),
}


def topic_for(message_type: str) -> str:
    """Return the versioned Kafka topic for a known message contract."""

    try:
        environment_name, default = _TOPICS_BY_TYPE[message_type]
    except KeyError as exc:
        raise ValueError(f"Unknown message type: {message_type}") from exc
    return os.getenv(environment_name, default)
