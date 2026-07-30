from datetime import UTC, datetime
from uuid import UUID

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from libs.contracts import AuthorizePayment, MessageEnvelope
from services.payment_service.handlers import handle_authorize
from services.payment_service.models import Base, Inbox, Outbox, Payment
from services.payment_service.provider import PaymentProvider


@pytest.mark.integration
@pytest.mark.asyncio
async def test_duplicate_authorize_envelope_has_one_effective_side_effect(
    postgres_engine: AsyncEngine,
) -> None:
    async with postgres_engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(postgres_engine, expire_on_commit=False)
    envelope = MessageEnvelope[AuthorizePayment](
        message_id=UUID("00000000-0000-0000-0000-000000000121"),
        message_type="AuthorizePayment",
        occurred_at=datetime(2026, 7, 30, 8, 0, tzinfo=UTC),
        correlation_id=UUID("00000000-0000-0000-0000-000000000122"),
        causation_id=None,
        order_id=UUID("00000000-0000-0000-0000-000000000123"),
        payload=AuthorizePayment(amount_minor=750, currency="RUB"),
    )

    for _ in range(2):
        async with session_factory() as session:
            await handle_authorize(session, envelope, PaymentProvider())

    async with session_factory() as session:
        counts = {
            "payments": await session.scalar(select(func.count()).select_from(Payment)),
            "inbox": await session.scalar(select(func.count()).select_from(Inbox)),
            "outbox": await session.scalar(select(func.count()).select_from(Outbox)),
        }
        event_type = await session.scalar(
            select(Outbox.payload_json["message_type"].as_string())
        )

    assert counts == {"payments": 1, "inbox": 1, "outbox": 1}
    assert event_type == "PaymentAuthorized"
