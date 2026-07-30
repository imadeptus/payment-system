from datetime import UTC, datetime
from uuid import UUID

import pytest
from pydantic import ValidationError

from libs.contracts import (
    AuthorizePayment,
    InventoryRejected,
    MessageEnvelope,
    PaymentAuthorized,
    topic_for,
)


def test_utc_envelope_round_trips_with_typed_payload() -> None:
    envelope = MessageEnvelope[AuthorizePayment](
        message_id=UUID("00000000-0000-0000-0000-000000000001"),
        message_type="AuthorizePayment",
        schema_version=1,
        occurred_at=datetime(2026, 7, 30, 8, 0, tzinfo=UTC),
        correlation_id=UUID("00000000-0000-0000-0000-000000000002"),
        causation_id=None,
        order_id=UUID("00000000-0000-0000-0000-000000000003"),
        payload=AuthorizePayment(amount_minor=12_500, currency="RUB"),
    )

    restored = MessageEnvelope[AuthorizePayment].model_validate_json(envelope.model_dump_json())

    assert restored == envelope
    assert restored.occurred_at.tzinfo is not None
    assert restored.schema_version == 1


def test_envelope_rejects_naive_occurred_at() -> None:
    with pytest.raises(ValidationError):
        MessageEnvelope[AuthorizePayment](
            message_id=UUID("00000000-0000-0000-0000-000000000001"),
            message_type="AuthorizePayment",
            schema_version=1,
            occurred_at=datetime(2026, 7, 30, 8, 0),
            correlation_id=UUID("00000000-0000-0000-0000-000000000002"),
            causation_id=None,
            order_id=UUID("00000000-0000-0000-0000-000000000003"),
            payload=AuthorizePayment(amount_minor=12_500, currency="RUB"),
        )


@pytest.mark.parametrize("invalid_amount", [0, -1, 1.5])
def test_payment_amount_must_be_positive_integer_minor_units(
    invalid_amount: int | float,
) -> None:
    with pytest.raises(ValidationError):
        AuthorizePayment(amount_minor=invalid_amount, currency="RUB")  # type: ignore[arg-type]


@pytest.mark.parametrize("currency", ["rub", "RUBLE", "RU", "12A"])
def test_currency_is_three_uppercase_letters(currency: str) -> None:
    with pytest.raises(ValidationError):
        AuthorizePayment(amount_minor=100, currency=currency)


def test_schema_version_cannot_change_without_new_contract() -> None:
    with pytest.raises(ValidationError):
        MessageEnvelope[PaymentAuthorized](
            message_id=UUID("00000000-0000-0000-0000-000000000001"),
            message_type="PaymentAuthorized",
            schema_version=2,
            occurred_at=datetime(2026, 7, 30, 8, 0, tzinfo=UTC),
            correlation_id=UUID("00000000-0000-0000-0000-000000000002"),
            causation_id=None,
            order_id=UUID("00000000-0000-0000-0000-000000000003"),
            payload=PaymentAuthorized(amount_minor=100, currency="RUB"),
        )


def test_topic_routing_covers_commands_events_and_rejects_unknown() -> None:
    assert topic_for("AuthorizePayment") == "payments.commands.v1"
    assert topic_for("PaymentAuthorized") == "payments.events.v1"
    assert topic_for("InventoryRejected") == "inventory.events.v1"
    assert InventoryRejected(sku="OUT-OF-STOCK", quantity=1, reason="insufficient_stock")

    with pytest.raises(ValueError, match="Unknown message type"):
        topic_for("UnknownMessage")
