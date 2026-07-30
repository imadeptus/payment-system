from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from services.inventory_service.models import Reservation
from services.payment_service.models import Payment
from tests.e2e.conftest import wait_for_order_status


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_in_stock_order_reaches_confirmed(
    order_client: httpx.AsyncClient,
    payment_database: AsyncEngine,
    inventory_database: AsyncEngine,
) -> None:
    response = await order_client.post(
        "/orders",
        headers={"Idempotency-Key": f"e2e-happy-{uuid4()}"},
        json={
            "sku": "IN-STOCK",
            "quantity": 2,
            "amount_minor": 12_500,
            "currency": "RUB",
        },
    )

    assert response.status_code == 202
    order_id = UUID(response.json()["order_id"])
    order = await wait_for_order_status(order_client, order_id, "CONFIRMED")
    assert order["status"] == "CONFIRMED"

    payment_sessions = async_sessionmaker(payment_database, expire_on_commit=False)
    inventory_sessions = async_sessionmaker(inventory_database, expire_on_commit=False)
    async with payment_sessions() as session:
        payment = await session.scalar(
            select(Payment).where(Payment.order_id == order_id)
        )
    async with inventory_sessions() as session:
        reservation = await session.scalar(
            select(Reservation).where(Reservation.order_id == order_id)
        )

    assert payment is not None
    assert payment.status == "AUTHORIZED"
    assert payment.history == ["AUTHORIZED"]
    assert reservation is not None
    assert reservation.status == "RESERVED"
