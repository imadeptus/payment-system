from datetime import UTC, datetime
from uuid import UUID

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from libs.contracts import (
    InventoryRejected,
    InventoryReserved,
    MessageEnvelope,
    PaymentAuthorized,
    PaymentRefundFailed,
)
from libs.messaging.retry import BusinessMessageError
from services.order_service.handlers import handle_saga_event
from services.order_service.models import Base, Order, Outbox, Saga
from services.order_service.repository import NewOrder, create_order


def event_envelope(
    *,
    message_type: str,
    payload: PaymentAuthorized | InventoryRejected | PaymentRefundFailed,
    suffix: int,
    order_id: UUID,
    correlation_id: UUID,
) -> MessageEnvelope:
    return MessageEnvelope(
        message_id=UUID(f"00000000-0000-0000-0000-{suffix:012d}"),
        message_type=message_type,
        occurred_at=datetime(2026, 7, 30, 8, suffix % 60, tzinfo=UTC),
        correlation_id=correlation_id,
        causation_id=UUID("00000000-0000-0000-0000-000000000320"),
        order_id=order_id,
        payload=payload,
    )


@pytest.mark.asyncio
async def test_compensation_failure_moves_saga_to_manual_review_once() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        order, _ = await create_order(
            session,
            NewOrder(
                sku="OUT-OF-STOCK",
                quantity=1,
                amount_minor=900,
                currency="RUB",
            ),
            "manual-review-path",
        )
        order_id = order.id
    async with session_factory() as session:
        saga = await session.scalar(select(Saga).where(Saga.order_id == order_id))
        assert saga is not None
        correlation_id = saga.id

    events = [
        event_envelope(
            message_type="PaymentAuthorized",
            payload=PaymentAuthorized(amount_minor=900, currency="RUB"),
            suffix=321,
            order_id=order_id,
            correlation_id=correlation_id,
        ),
        event_envelope(
            message_type="InventoryRejected",
            payload=InventoryRejected(
                sku="OUT-OF-STOCK",
                quantity=1,
                reason="insufficient_stock",
            ),
            suffix=322,
            order_id=order_id,
            correlation_id=correlation_id,
        ),
        event_envelope(
            message_type="PaymentRefundFailed",
            payload=PaymentRefundFailed(
                amount_minor=900,
                currency="RUB",
                reason="provider_refund_failed",
            ),
            suffix=323,
            order_id=order_id,
            correlation_id=correlation_id,
        ),
    ]
    for event in events:
        async with session_factory() as session:
            await handle_saga_event(session, event)
    async with session_factory() as session:
        await handle_saga_event(session, events[-1])

    async with session_factory() as session:
        order = await session.get(Order, order_id)
        saga = await session.scalar(select(Saga).where(Saga.order_id == order_id))
        outbox_types = list(
            await session.scalars(
                select(Outbox.payload_json["message_type"].as_string()).order_by(
                    Outbox.created_at
                )
            )
        )

    assert order is not None
    assert saga is not None
    assert order.status == "MANUAL_REVIEW"
    assert saga.state == "MANUAL_REVIEW"
    assert outbox_types == ["AuthorizePayment", "ReserveInventory", "RefundPayment"]
    assert [entry["to"] for entry in saga.history] == [
        "PAYMENT_PENDING",
        "INVENTORY_PENDING",
        "REFUND_PENDING",
        "MANUAL_REVIEW",
    ]
    await engine.dispose()


@pytest.mark.asyncio
async def test_payment_event_must_match_order_amount_and_currency() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        order, _ = await create_order(
            session,
            NewOrder(
                sku="IN-STOCK",
                quantity=1,
                amount_minor=900,
                currency="RUB",
            ),
            "mismatched-payment-event",
        )
        order_id = order.id
    async with session_factory() as session:
        saga = await session.scalar(select(Saga).where(Saga.order_id == order_id))
        assert saga is not None
        correlation_id = saga.id

    mismatched = event_envelope(
        message_type="PaymentAuthorized",
        payload=PaymentAuthorized(amount_minor=901, currency="RUB"),
        suffix=331,
        order_id=order_id,
        correlation_id=correlation_id,
    )
    async with session_factory() as session:
        with pytest.raises(BusinessMessageError, match="does not match Order"):
            await handle_saga_event(session, mismatched)

    async with session_factory() as session:
        order = await session.get(Order, order_id)
        saga = await session.scalar(select(Saga).where(Saga.order_id == order_id))

    assert order is not None
    assert saga is not None
    assert order.status == "PAYMENT_PENDING"
    assert saga.state == "PAYMENT_PENDING"
    await engine.dispose()


@pytest.mark.asyncio
async def test_inventory_event_must_match_order_sku_and_quantity() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        order, _ = await create_order(
            session,
            NewOrder(
                sku="IN-STOCK",
                quantity=1,
                amount_minor=900,
                currency="RUB",
            ),
            "mismatched-inventory-event",
        )
        order_id = order.id
    async with session_factory() as session:
        saga = await session.scalar(select(Saga).where(Saga.order_id == order_id))
        assert saga is not None
        correlation_id = saga.id

    authorized = event_envelope(
        message_type="PaymentAuthorized",
        payload=PaymentAuthorized(amount_minor=900, currency="RUB"),
        suffix=341,
        order_id=order_id,
        correlation_id=correlation_id,
    )
    mismatched = MessageEnvelope[InventoryReserved](
        message_id=UUID("00000000-0000-0000-0000-000000000342"),
        message_type="InventoryReserved",
        occurred_at=datetime(2026, 7, 30, 8, 2, tzinfo=UTC),
        correlation_id=correlation_id,
        causation_id=authorized.message_id,
        order_id=order_id,
        payload=InventoryReserved(sku="WRONG-SKU", quantity=1),
    )
    async with session_factory() as session:
        await handle_saga_event(session, authorized)
    async with session_factory() as session:
        with pytest.raises(BusinessMessageError, match="does not match Order"):
            await handle_saga_event(session, mismatched)

    async with session_factory() as session:
        order = await session.get(Order, order_id)
        saga = await session.scalar(select(Saga).where(Saga.order_id == order_id))

    assert order is not None
    assert saga is not None
    assert order.status == "INVENTORY_PENDING"
    assert saga.state == "INVENTORY_PENDING"
    await engine.dispose()
