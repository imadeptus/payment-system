from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pytest

from libs.contracts import AuthorizePayment, MessageEnvelope
from libs.messaging.retry import (
    BusinessMessageError,
    TransientMessageError,
    consume_with_retry,
)


def envelope() -> MessageEnvelope[AuthorizePayment]:
    return MessageEnvelope[AuthorizePayment](
        message_id=UUID("00000000-0000-0000-0000-000000000301"),
        message_type="AuthorizePayment",
        occurred_at=datetime(2026, 7, 30, 8, 0, tzinfo=UTC),
        correlation_id=UUID("00000000-0000-0000-0000-000000000302"),
        causation_id=UUID("00000000-0000-0000-0000-000000000303"),
        order_id=UUID("00000000-0000-0000-0000-000000000304"),
        payload=AuthorizePayment(amount_minor=500, currency="RUB"),
    )


@pytest.mark.asyncio
async def test_transient_failures_exhaust_backoff_and_preserve_dlq_metadata() -> None:
    delays: list[float] = []
    dlq_records: list[dict[str, Any]] = []

    async def failing_handler(_: MessageEnvelope[Any]) -> None:
        raise TransientMessageError("database password=do-not-leak")

    async def record_dlq(record: dict[str, Any]) -> None:
        dlq_records.append(record)

    async def no_sleep(delay: float) -> None:
        delays.append(delay)

    await consume_with_retry(
        envelope(),
        failing_handler,
        record_dlq,
        attempts=3,
        delays=(1.0, 2.0, 4.0),
        sleep=no_sleep,
        random_fraction=lambda: 0.0,
    )

    assert delays == [1.0, 2.0, 4.0]
    assert len(dlq_records) == 1
    record = dlq_records[0]
    assert record["attempts"] == 3
    assert record["correlation_id"] == "00000000-0000-0000-0000-000000000302"
    assert record["causation_id"] == "00000000-0000-0000-0000-000000000303"
    assert record["reason"] == "TransientMessageError"
    assert "password" not in str(record)
    assert "Traceback" not in str(record)


@pytest.mark.asyncio
async def test_retry_adds_bounded_injectable_jitter() -> None:
    delays: list[float] = []

    async def failing_handler(_: MessageEnvelope[Any]) -> None:
        raise TransientMessageError("temporary")

    async def no_sleep(delay: float) -> None:
        delays.append(delay)

    async def ignore_dlq(_: dict[str, Any]) -> None:
        return None

    await consume_with_retry(
        envelope(),
        failing_handler,
        ignore_dlq,
        attempts=3,
        delays=(1.0, 2.0, 10.0),
        sleep=no_sleep,
        random_fraction=lambda: 0.5,
    )

    assert delays == [1.1, 2.2, 10.5]


@pytest.mark.asyncio
async def test_transient_dlq_keeps_only_validated_payload_fields() -> None:
    dlq_records: list[dict[str, Any]] = []
    raw = envelope().model_dump(mode="json")
    raw["payload"]["password"] = "do-not-leak"
    unsafe_envelope = MessageEnvelope[Any].model_validate(raw)

    async def failing_handler(_: MessageEnvelope[Any]) -> None:
        raise TransientMessageError("temporary")

    async def record_dlq(record: dict[str, Any]) -> None:
        dlq_records.append(record)

    async def no_sleep(_: float) -> None:
        return None

    await consume_with_retry(
        unsafe_envelope,
        failing_handler,
        record_dlq,
        attempts=1,
        delays=(0.0,),
        sleep=no_sleep,
        random_fraction=lambda: 0.0,
    )

    terminal_record = dlq_records[-1]
    assert terminal_record["original_message"]["payload"] == {
        "amount_minor": 500,
        "currency": "RUB",
    }
    assert "password" not in str(terminal_record)
    assert "do-not-leak" not in str(terminal_record)


@pytest.mark.asyncio
async def test_business_rejection_is_not_retried_or_sent_to_dlq() -> None:
    calls = 0
    dlq_records: list[dict[str, Any]] = []
    delays: list[float] = []

    async def business_rejection(_: MessageEnvelope[Any]) -> None:
        nonlocal calls
        calls += 1
        raise BusinessMessageError("insufficient_stock")

    async def record_dlq(record: dict[str, Any]) -> None:
        dlq_records.append(record)

    async def no_sleep(delay: float) -> None:
        delays.append(delay)

    with pytest.raises(BusinessMessageError):
        await consume_with_retry(
            envelope(),
            business_rejection,
            record_dlq,
            attempts=3,
            delays=(1.0, 2.0, 4.0),
            sleep=no_sleep,
        )

    assert calls == 1
    assert delays == []
    assert dlq_records == []
