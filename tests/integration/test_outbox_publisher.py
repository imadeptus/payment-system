from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from libs.contracts import AuthorizePayment, MessageEnvelope
from libs.messaging import claim_inbox, enqueue
from libs.messaging.publisher import OutboxPublisher
from services.payment_service.models import Base, Inbox, Outbox


class CrashAfterBrokerAck(BaseException):
    pass


class RecordingTransport:
    def __init__(self) -> None:
        self.published: list[dict[str, Any]] = []

    async def publish(self, topic: str, key: bytes, value: bytes) -> None:
        import json

        self.published.append(
            {"topic": topic, "key": key.decode(), "value": json.loads(value)}
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_crash_after_ack_republishes_same_message_and_inbox_deduplicates(
    postgres_engine: AsyncEngine,
) -> None:
    async with postgres_engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(postgres_engine, expire_on_commit=False)
    message = MessageEnvelope[AuthorizePayment](
        message_id=UUID("00000000-0000-0000-0000-000000000311"),
        message_type="AuthorizePayment",
        occurred_at=datetime(2026, 7, 30, 8, 0, tzinfo=UTC),
        correlation_id=UUID("00000000-0000-0000-0000-000000000312"),
        causation_id=None,
        order_id=UUID("00000000-0000-0000-0000-000000000313"),
        payload=AuthorizePayment(amount_minor=500, currency="RUB"),
    )
    async with session_factory.begin() as session:
        enqueue(session, message, outbox_model=Outbox)

    transport = RecordingTransport()

    async def crash() -> None:
        raise CrashAfterBrokerAck

    crashing_publisher = OutboxPublisher(
        session_factory,
        Outbox,
        transport,
        after_publish=crash,
    )
    with pytest.raises(CrashAfterBrokerAck):
        await crashing_publisher.publish_batch()

    async with session_factory() as session:
        published_at = await session.scalar(select(Outbox.published_at))
    assert published_at is None

    restarted_publisher = OutboxPublisher(session_factory, Outbox, transport)
    assert await restarted_publisher.publish_batch() == 1
    assert [item["key"] for item in transport.published] == [
        str(message.message_id),
        str(message.message_id),
    ]

    claims: list[bool] = []
    for _ in transport.published:
        async with session_factory.begin() as session:
            claims.append(
                await claim_inbox(
                    session,
                    message.message_id,
                    "publisher-recovery-test",
                    inbox_model=Inbox,
                )
            )
    assert claims == [True, False]
