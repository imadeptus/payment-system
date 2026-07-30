import asyncio
import os
from collections.abc import AsyncIterator
from uuid import UUID

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


@pytest.fixture
async def order_client() -> AsyncIterator[httpx.AsyncClient]:
    base_url = os.getenv("E2E_ORDER_BASE_URL")
    if not base_url:
        pytest.skip("E2E_ORDER_BASE_URL is required")
    async with httpx.AsyncClient(base_url=base_url, timeout=10.0) as client:
        yield client


async def _database_engine(environment_name: str, default_url: str) -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(os.getenv(environment_name, default_url))
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def order_database() -> AsyncIterator[AsyncEngine]:
    async for engine in _database_engine(
        "E2E_ORDER_DATABASE_URL",
        "postgresql+asyncpg://saga:saga-local-only@127.0.0.1:55439/orders",
    ):
        yield engine


@pytest.fixture
async def payment_database() -> AsyncIterator[AsyncEngine]:
    async for engine in _database_engine(
        "E2E_PAYMENT_DATABASE_URL",
        "postgresql+asyncpg://saga:saga-local-only@127.0.0.1:55439/payments",
    ):
        yield engine


@pytest.fixture
async def inventory_database() -> AsyncIterator[AsyncEngine]:
    async for engine in _database_engine(
        "E2E_INVENTORY_DATABASE_URL",
        "postgresql+asyncpg://saga:saga-local-only@127.0.0.1:55439/inventory",
    ):
        yield engine


async def wait_for_order_status(
    client: httpx.AsyncClient,
    order_id: UUID,
    expected: str,
    timeout_seconds: float = 30.0,
) -> dict[str, object]:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    last: dict[str, object] = {}
    while loop.time() < deadline:
        response = await client.get(f"/orders/{order_id}")
        if response.status_code == 200:
            last = response.json()
            if last.get("status") == expected:
                return last
        await asyncio.sleep(0.25)
    raise AssertionError(f"Order {order_id} did not reach {expected}; last={last}")
