from typing import Any

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from services.order_service.api import create_app
from services.order_service.models import Base, IdempotencyRecord, Order, Outbox, Saga


@pytest.mark.integration
@pytest.mark.asyncio
async def test_post_orders_replays_same_result_without_duplicate_side_effects(
    postgres_engine: AsyncEngine,
) -> None:
    async with postgres_engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(postgres_engine, expire_on_commit=False)
    app = create_app(session_factory)
    transport = httpx.ASGITransport(app=app)
    payload = {
        "sku": "IN-STOCK",
        "quantity": 2,
        "amount_minor": 12_500,
        "currency": "RUB",
    }
    headers = {"Idempotency-Key": "order-api-test-001"}

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        created = await client.post("/orders", json=payload, headers=headers)
        replayed = await client.post("/orders", json=payload, headers=headers)
        fetched = await client.get(f"/orders/{created.json()['order_id']}")

    assert created.status_code == 202
    assert replayed.status_code == 200
    assert replayed.json() == created.json()
    assert fetched.status_code == 200
    assert fetched.json() == created.json()

    async with session_factory() as session:
        counts = {
            "orders": await session.scalar(select(func.count()).select_from(Order)),
            "sagas": await session.scalar(select(func.count()).select_from(Saga)),
            "keys": await session.scalar(
                select(func.count()).select_from(IdempotencyRecord)
            ),
            "outbox": await session.scalar(select(func.count()).select_from(Outbox)),
        }
        outbox_payload: dict[str, Any] = await session.scalar(
            select(Outbox.payload_json)
        )

    assert counts == {"orders": 1, "sagas": 1, "keys": 1, "outbox": 1}
    assert outbox_payload["message_type"] == "AuthorizePayment"
