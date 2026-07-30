from datetime import UTC, datetime
from uuid import UUID

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from libs.contracts import AuthorizePayment, MessageEnvelope, RefundPayment
from libs.messaging.retry import BusinessMessageError
from services.payment_service.handlers import handle_authorize, handle_refund
from services.payment_service.models import Base, Outbox, Payment
from services.payment_service.provider import PaymentProvider

ORDER_ID = UUID("00000000-0000-0000-0000-000000000101")


def test_provider_authorizes_ordinary_positive_minor_amount() -> None:
    provider = PaymentProvider()

    result = provider.authorize(ORDER_ID, amount_minor=12_500, currency="RUB")

    assert result.approved is True
    assert result.reason is None


def test_provider_rejects_configured_sentinel_order() -> None:
    provider = PaymentProvider(reject_order_ids={ORDER_ID})

    result = provider.authorize(ORDER_ID, amount_minor=12_500, currency="RUB")

    assert result.approved is False
    assert result.reason == "provider_rejected"


@pytest.mark.asyncio
async def test_rejected_authorization_is_persisted_and_emits_domain_event() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    envelope = MessageEnvelope[AuthorizePayment](
        message_id=UUID("00000000-0000-0000-0000-000000000111"),
        message_type="AuthorizePayment",
        occurred_at=datetime(2026, 7, 30, 8, 0, tzinfo=UTC),
        correlation_id=UUID("00000000-0000-0000-0000-000000000112"),
        causation_id=None,
        order_id=ORDER_ID,
        payload=AuthorizePayment(amount_minor=12_500, currency="RUB"),
    )

    async with session_factory() as session:
        await handle_authorize(
            session,
            envelope,
            PaymentProvider(reject_order_ids={ORDER_ID}),
        )

    async with session_factory() as session:
        payment = await session.scalar(select(Payment))
        event_type = await session.scalar(
            select(Outbox.payload_json["message_type"].as_string())
        )

    assert payment is not None
    assert payment.status == "REJECTED"
    assert event_type == "PaymentRejected"
    await engine.dispose()


@pytest.mark.asyncio
async def test_refund_failure_is_explicit_and_emits_manual_review_event() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    provider = PaymentProvider(refund_failure_order_ids={ORDER_ID})
    authorize = MessageEnvelope[AuthorizePayment](
        message_id=UUID("00000000-0000-0000-0000-000000000131"),
        message_type="AuthorizePayment",
        occurred_at=datetime(2026, 7, 30, 8, 0, tzinfo=UTC),
        correlation_id=UUID("00000000-0000-0000-0000-000000000132"),
        causation_id=None,
        order_id=ORDER_ID,
        payload=AuthorizePayment(amount_minor=12_500, currency="RUB"),
    )
    refund = MessageEnvelope[RefundPayment](
        message_id=UUID("00000000-0000-0000-0000-000000000133"),
        message_type="RefundPayment",
        occurred_at=datetime(2026, 7, 30, 8, 1, tzinfo=UTC),
        correlation_id=authorize.correlation_id,
        causation_id=authorize.message_id,
        order_id=ORDER_ID,
        payload=RefundPayment(amount_minor=12_500, currency="RUB"),
    )

    async with session_factory() as session:
        await handle_authorize(session, authorize, provider)
    async with session_factory() as session:
        await handle_refund(session, refund, provider)

    async with session_factory() as session:
        payment = await session.scalar(select(Payment))
        events = list(
            await session.scalars(
                select(Outbox.payload_json["message_type"].as_string()).order_by(
                    Outbox.created_at
                )
            )
        )

    assert payment is not None
    assert payment.status == "REFUND_FAILED"
    assert payment.history == ["AUTHORIZED", "REFUND_FAILED"]
    assert events == ["PaymentAuthorized", "PaymentRefundFailed"]
    await engine.dispose()


@pytest.mark.parametrize(
    "override",
    [
        {"amount_minor": 12_501},
        {"currency": "USD"},
        {
            "correlation_id": UUID(
                "00000000-0000-0000-0000-000000000199"
            )
        },
    ],
)
@pytest.mark.asyncio
async def test_refund_rejects_command_that_does_not_match_payment(
    override: dict[str, object],
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    provider = PaymentProvider()
    authorize = MessageEnvelope[AuthorizePayment](
        message_id=UUID("00000000-0000-0000-0000-000000000151"),
        message_type="AuthorizePayment",
        occurred_at=datetime(2026, 7, 30, 8, 0, tzinfo=UTC),
        correlation_id=UUID("00000000-0000-0000-0000-000000000152"),
        causation_id=None,
        order_id=ORDER_ID,
        payload=AuthorizePayment(amount_minor=12_500, currency="RUB"),
    )
    refund_data: dict[str, object] = {
        "message_id": UUID("00000000-0000-0000-0000-000000000153"),
        "message_type": "RefundPayment",
        "occurred_at": datetime(2026, 7, 30, 8, 1, tzinfo=UTC),
        "correlation_id": authorize.correlation_id,
        "causation_id": authorize.message_id,
        "order_id": ORDER_ID,
        "payload": RefundPayment(
            amount_minor=int(override.get("amount_minor", 12_500)),
            currency=str(override.get("currency", "RUB")),
        ),
    }
    if "correlation_id" in override:
        refund_data["correlation_id"] = override["correlation_id"]
    refund = MessageEnvelope[RefundPayment].model_validate(refund_data)

    async with session_factory() as session:
        await handle_authorize(session, authorize, provider)
    async with session_factory() as session:
        with pytest.raises(BusinessMessageError, match="does not match"):
            await handle_refund(session, refund, provider)

    async with session_factory() as session:
        payment = await session.scalar(select(Payment))

    assert payment is not None
    assert payment.status == "AUTHORIZED"
    assert payment.history == ["AUTHORIZED"]
    await engine.dispose()


@pytest.mark.asyncio
async def test_rejected_payment_cannot_be_refunded() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    authorize = MessageEnvelope[AuthorizePayment](
        message_id=UUID("00000000-0000-0000-0000-000000000161"),
        message_type="AuthorizePayment",
        occurred_at=datetime(2026, 7, 30, 8, 0, tzinfo=UTC),
        correlation_id=UUID("00000000-0000-0000-0000-000000000162"),
        causation_id=None,
        order_id=ORDER_ID,
        payload=AuthorizePayment(amount_minor=12_500, currency="RUB"),
    )
    refund = MessageEnvelope[RefundPayment](
        message_id=UUID("00000000-0000-0000-0000-000000000163"),
        message_type="RefundPayment",
        occurred_at=datetime(2026, 7, 30, 8, 1, tzinfo=UTC),
        correlation_id=authorize.correlation_id,
        causation_id=authorize.message_id,
        order_id=ORDER_ID,
        payload=RefundPayment(amount_minor=12_500, currency="RUB"),
    )
    provider = PaymentProvider(reject_order_ids={ORDER_ID})

    async with session_factory() as session:
        await handle_authorize(session, authorize, provider)
    async with session_factory() as session:
        with pytest.raises(BusinessMessageError, match="not authorized"):
            await handle_refund(session, refund, provider)

    async with session_factory() as session:
        payment = await session.scalar(select(Payment))

    assert payment is not None
    assert payment.status == "REJECTED"
    assert payment.history == ["REJECTED"]
    await engine.dispose()
