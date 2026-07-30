"""Public message contracts."""

from libs.contracts.envelope import MessageEnvelope
from libs.contracts.messages import (
    AuthorizePayment,
    InventoryRejected,
    InventoryReleased,
    InventoryReleaseFailed,
    InventoryReserved,
    PaymentAuthorized,
    PaymentRefunded,
    PaymentRefundFailed,
    PaymentRejected,
    RefundPayment,
    ReleaseInventory,
    ReserveInventory,
)
from libs.contracts.topics import (
    DLQ_TOPIC,
    INVENTORY_COMMANDS_TOPIC,
    INVENTORY_EVENTS_TOPIC,
    PAYMENT_COMMANDS_TOPIC,
    PAYMENT_EVENTS_TOPIC,
    topic_for,
)

__all__ = [
    "DLQ_TOPIC",
    "INVENTORY_COMMANDS_TOPIC",
    "INVENTORY_EVENTS_TOPIC",
    "PAYMENT_COMMANDS_TOPIC",
    "PAYMENT_EVENTS_TOPIC",
    "AuthorizePayment",
    "InventoryRejected",
    "InventoryReleaseFailed",
    "InventoryReleased",
    "InventoryReserved",
    "MessageEnvelope",
    "PaymentAuthorized",
    "PaymentRefundFailed",
    "PaymentRefunded",
    "PaymentRejected",
    "RefundPayment",
    "ReleaseInventory",
    "ReserveInventory",
    "topic_for",
]
