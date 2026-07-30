from datetime import UTC, datetime
from uuid import UUID

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from libs.contracts import MessageEnvelope, ReserveInventory
from services.inventory_service.handlers import handle_reserve
from services.inventory_service.models import Base, Inbox, Outbox, Reservation, Stock


@pytest.mark.integration
@pytest.mark.asyncio
async def test_duplicate_reserve_delivery_is_idempotent_in_postgresql(
    postgres_engine: AsyncEngine,
) -> None:
    async with postgres_engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(postgres_engine, expire_on_commit=False)
    async with session_factory.begin() as session:
        session.add(Stock(sku="IN-STOCK", available=10))
    envelope = MessageEnvelope[ReserveInventory](
        message_id=UUID("00000000-0000-0000-0000-000000000231"),
        message_type="ReserveInventory",
        occurred_at=datetime(2026, 7, 30, 8, 0, tzinfo=UTC),
        correlation_id=UUID("00000000-0000-0000-0000-000000000232"),
        causation_id=UUID("00000000-0000-0000-0000-000000000233"),
        order_id=UUID("00000000-0000-0000-0000-000000000234"),
        payload=ReserveInventory(sku="IN-STOCK", quantity=3),
    )

    for _ in range(2):
        async with session_factory() as session:
            await handle_reserve(session, envelope)

    async with session_factory() as session:
        counts = {
            "reservations": await session.scalar(
                select(func.count()).select_from(Reservation)
            ),
            "inbox": await session.scalar(select(func.count()).select_from(Inbox)),
            "outbox": await session.scalar(select(func.count()).select_from(Outbox)),
        }
        available = await session.scalar(
            select(Stock.available).where(Stock.sku == "IN-STOCK")
        )

    assert counts == {"reservations": 1, "inbox": 1, "outbox": 1}
    assert available == 7
