import os
from collections.abc import AsyncIterator

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


@pytest.fixture
async def postgres_engine() -> AsyncIterator[AsyncEngine]:
    database_url = os.getenv("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required for PostgreSQL integration tests")

    engine = create_async_engine(database_url)
    try:
        yield engine
    finally:
        await engine.dispose()
