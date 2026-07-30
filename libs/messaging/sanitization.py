"""Centralized allowlist-based DLQ message sanitization."""

from typing import Any

from pydantic import BaseModel, ValidationError

from libs.contracts import (
    AuthorizePayment,
    InventoryRejected,
    InventoryReleased,
    InventoryReleaseFailed,
    InventoryReserved,
    MessageEnvelope,
    PaymentAuthorized,
    PaymentRefunded,
    PaymentRefundFailed,
    PaymentRejected,
    RefundPayment,
    ReleaseInventory,
    ReserveInventory,
)

DLQ_ENVELOPE_METADATA_FIELDS = (
    "message_id",
    "message_type",
    "schema_version",
    "occurred_at",
    "correlation_id",
    "causation_id",
    "order_id",
)

PAYLOAD_MODELS: dict[str, type[BaseModel]] = {
    "AuthorizePayment": AuthorizePayment,
    "RefundPayment": RefundPayment,
    "ReserveInventory": ReserveInventory,
    "ReleaseInventory": ReleaseInventory,
    "PaymentAuthorized": PaymentAuthorized,
    "PaymentRejected": PaymentRejected,
    "PaymentRefunded": PaymentRefunded,
    "PaymentRefundFailed": PaymentRefundFailed,
    "InventoryReserved": InventoryReserved,
    "InventoryRejected": InventoryRejected,
    "InventoryReleased": InventoryReleased,
    "InventoryReleaseFailed": InventoryReleaseFailed,
}


def sanitize_decoded_message(
    raw: dict[str, Any],
    *,
    include_validated_payload: bool,
) -> dict[str, Any]:
    """Keep only envelope metadata and known, validated payload fields."""

    try:
        canonical = MessageEnvelope[Any].model_validate(raw).model_dump(mode="json")
    except ValidationError:
        return {}
    sanitized = {
        field: canonical.get(field)
        for field in DLQ_ENVELOPE_METADATA_FIELDS
    }
    message_type = canonical.get("message_type")
    if message_type not in PAYLOAD_MODELS:
        sanitized["message_type"] = "unknown"
    if not include_validated_payload:
        return sanitized

    payload_model = (
        PAYLOAD_MODELS.get(message_type)
        if isinstance(message_type, str)
        else None
    )
    payload = canonical.get("payload")
    if payload_model is None or not isinstance(payload, dict):
        return sanitized

    allowlisted_payload = {
        field: payload[field]
        for field in payload_model.model_fields
        if field in payload
    }
    try:
        sanitized["payload"] = payload_model.model_validate(
            allowlisted_payload
        ).model_dump(mode="json")
    except ValidationError:
        pass
    return sanitized
