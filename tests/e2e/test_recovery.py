import asyncio
import os
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from services.order_service.models import Outbox
from tests.e2e.conftest import wait_for_order_status


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_kafka_retained_command_resumes_after_inventory_consumer_restart(
    order_client: httpx.AsyncClient,
    order_database: AsyncEngine,
) -> None:
    if os.getenv("E2E_RUN_RECOVERY") != "1":
        pytest.skip("Set E2E_RUN_RECOVERY=1 for service restart scenario")

    stop_process = await asyncio.create_subprocess_exec(
        "docker",
        "compose",
        "stop",
        "inventory-service",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    assert await stop_process.wait() == 0
    try:
        response = await order_client.post(
            "/orders",
            headers={"Idempotency-Key": f"e2e-recovery-{uuid4()}"},
            json={
                "sku": "IN-STOCK",
                "quantity": 1,
                "amount_minor": 700,
                "currency": "RUB",
            },
        )
        assert response.status_code == 202
        order_id = UUID(response.json()["order_id"])
        order_sessions = async_sessionmaker(order_database, expire_on_commit=False)
        deadline = asyncio.get_running_loop().time() + 10.0
        reserve_command = None
        while asyncio.get_running_loop().time() < deadline:
            async with order_sessions() as session:
                reserve_command = await session.scalar(
                    select(Outbox).where(
                        Outbox.payload_json["order_id"].as_string() == str(order_id),
                        Outbox.payload_json["message_type"].as_string()
                        == "ReserveInventory",
                    )
                )
            if reserve_command is not None and reserve_command.published_at is not None:
                break
            await asyncio.sleep(0.25)
        assert reserve_command is not None
        assert reserve_command.topic == "inventory.commands.v1"
        assert reserve_command.published_at is not None
    finally:
        start_process = await asyncio.create_subprocess_exec(
            "docker",
            "compose",
            "start",
            "inventory-service",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        assert await start_process.wait() == 0

    await wait_for_order_status(order_client, order_id, "CONFIRMED")
