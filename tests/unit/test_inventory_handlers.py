from datetime import UTC, datetime
from uuid import UUID

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from libs.contracts import MessageEnvelope, ReleaseInventory, ReserveInventory
from services.inventory_service.handlers import handle_release, handle_reserve
from services.inventory_service.models import Base, Outbox, Reservation, Stock


def reserve_envelope(*, sku: str, message_suffix: int, order_suffix: int) -> MessageEnvelope:
    return MessageEnvelope[ReserveInventory](
        message_id=UUID(f"00000000-0000-0000-0000-{message_suffix:012d}"),
        message_type="ReserveInventory",
        occurred_at=datetime(2026, 7, 30, 8, 0, tzinfo=UTC),
        correlation_id=UUID("00000000-0000-0000-0000-000000000202"),
        causation_id=UUID("00000000-0000-0000-0000-000000000203"),
        order_id=UUID(f"00000000-0000-0000-0000-{order_suffix:012d}"),
        payload=ReserveInventory(sku=sku, quantity=2),
    )


@pytest.mark.asyncio
async def test_in_stock_reservation_decrements_exactly_once_on_duplicate() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory.begin() as session:
        session.add(Stock(sku="IN-STOCK", available=10))
    envelope = reserve_envelope(sku="IN-STOCK", message_suffix=211, order_suffix=213)

    for _ in range(2):
        async with session_factory() as session:
            await handle_reserve(session, envelope)

    async with session_factory() as session:
        available = await session.scalar(
            select(Stock.available).where(Stock.sku == "IN-STOCK")
        )
        reservation_count = await session.scalar(
            select(func.count()).select_from(Reservation)
        )
        outbox_count = await session.scalar(select(func.count()).select_from(Outbox))

    assert available == 8
    assert reservation_count == 1
    assert outbox_count == 1
    await engine.dispose()


@pytest.mark.asyncio
async def test_out_of_stock_rejects_without_negative_inventory() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory.begin() as session:
        session.add(Stock(sku="OUT-OF-STOCK", available=0))

    async with session_factory() as session:
        await handle_reserve(
            session,
            reserve_envelope(
                sku="OUT-OF-STOCK",
                message_suffix=221,
                order_suffix=223,
            ),
        )

    async with session_factory() as session:
        available = await session.scalar(
            select(Stock.available).where(Stock.sku == "OUT-OF-STOCK")
        )
        reservation_count = await session.scalar(
            select(func.count()).select_from(Reservation)
        )
        event_type = await session.scalar(
            select(Outbox.payload_json["message_type"].as_string())
        )

    assert available == 0
    assert reservation_count == 0
    assert event_type == "InventoryRejected"
    await engine.dispose()


@pytest.mark.asyncio
async def test_release_restores_stock_once_and_marks_reservation_released() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory.begin() as session:
        session.add(Stock(sku="IN-STOCK", available=10))
    reserve = reserve_envelope(sku="IN-STOCK", message_suffix=241, order_suffix=243)
    release = MessageEnvelope[ReleaseInventory](
        message_id=UUID("00000000-0000-0000-0000-000000000244"),
        message_type="ReleaseInventory",
        occurred_at=datetime(2026, 7, 30, 8, 1, tzinfo=UTC),
        correlation_id=reserve.correlation_id,
        causation_id=reserve.message_id,
        order_id=reserve.order_id,
        payload=ReleaseInventory(sku="IN-STOCK", quantity=2),
    )

    async with session_factory() as session:
        await handle_reserve(session, reserve)
    for _ in range(2):
        async with session_factory() as session:
            await handle_release(session, release)

    async with session_factory() as session:
        available = await session.scalar(
            select(Stock.available).where(Stock.sku == "IN-STOCK")
        )
        reservation_status = await session.scalar(select(Reservation.status))
        events = list(
            await session.scalars(
                select(Outbox.payload_json["message_type"].as_string()).order_by(
                    Outbox.created_at
                )
            )
        )

    assert available == 10
    assert reservation_status == "RELEASED"
    assert events == ["InventoryReserved", "InventoryReleased"]
    await engine.dispose()
