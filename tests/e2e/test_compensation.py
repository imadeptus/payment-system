from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from services.inventory_service.models import Reservation
from services.order_service.models import Saga
from services.payment_service.models import Payment
from tests.e2e.conftest import wait_for_order_status


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_out_of_stock_order_is_refunded_then_cancelled(
    order_client: httpx.AsyncClient,
    order_database: AsyncEngine,
    payment_database: AsyncEngine,
    inventory_database: AsyncEngine,
) -> None:
    response = await order_client.post(
        "/orders",
        headers={"Idempotency-Key": f"e2e-compensation-{uuid4()}"},
        json={
            "sku": "OUT-OF-STOCK",
            "quantity": 1,
            "amount_minor": 9_900,
            "currency": "RUB",
        },
    )

    assert response.status_code == 202
    order_id = UUID(response.json()["order_id"])
    order = await wait_for_order_status(order_client, order_id, "CANCELLED")
    assert order["status"] == "CANCELLED"

    order_sessions = async_sessionmaker(order_database, expire_on_commit=False)
    payment_sessions = async_sessionmaker(payment_database, expire_on_commit=False)
    inventory_sessions = async_sessionmaker(inventory_database, expire_on_commit=False)
    async with order_sessions() as session:
        saga = await session.scalar(select(Saga).where(Saga.order_id == order_id))
    async with payment_sessions() as session:
        payment = await session.scalar(
            select(Payment).where(Payment.order_id == order_id)
        )
    async with inventory_sessions() as session:
        reservation = await session.scalar(
            select(Reservation).where(Reservation.order_id == order_id)
        )

    assert saga is not None
    assert payment is not None
    assert [entry["to"] for entry in saga.history] == [
        "PAYMENT_PENDING",
        "INVENTORY_PENDING",
        "REFUND_PENDING",
        "CANCELLED",
    ]
    assert payment.history == ["AUTHORIZED", "REFUNDED"]
    assert reservation is None
