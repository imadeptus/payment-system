import asyncio
import json
import os
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from aiokafka import AIOKafkaConsumer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from libs.contracts import AuthorizePayment, MessageEnvelope
from libs.messaging import claim_inbox, enqueue
from libs.messaging.kafka import KafkaTransport
from libs.messaging.publisher import OutboxPublisher
from services.payment_service.models import Base, Inbox, Outbox


class CrashAfterBrokerAck(BaseException):
    pass


@pytest.mark.integration
@pytest.mark.asyncio
async def test_crash_after_ack_republishes_same_message_and_inbox_deduplicates(
    postgres_engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bootstrap_servers = os.getenv("TEST_KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap_servers:
        pytest.skip("TEST_KAFKA_BOOTSTRAP_SERVERS is required")
    topic = f"test.outbox.{uuid4().hex}"
    monkeypatch.setenv("PAYMENT_COMMANDS_TOPIC", topic)
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

    transport = KafkaTransport(
        bootstrap_servers,
        client_id=f"outbox-integration-{uuid4().hex}",
    )
    await transport.start()

    async def crash() -> None:
        raise CrashAfterBrokerAck

    crashing_publisher = OutboxPublisher(
        session_factory,
        Outbox,
        transport,
        after_publish=crash,
    )
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=f"outbox-reader-{uuid4().hex}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    try:
        with pytest.raises(CrashAfterBrokerAck):
            await crashing_publisher.publish_batch()

        async with session_factory() as session:
            published_at = await session.scalar(select(Outbox.published_at))
        assert published_at is None

        restarted_publisher = OutboxPublisher(session_factory, Outbox, transport)
        assert await restarted_publisher.publish_batch() == 1

        await consumer.start()
        records = [
            await asyncio.wait_for(consumer.getone(), timeout=15.0)
            for _ in range(2)
        ]
        published = [
            {
                "key": record.key.decode(),
                "value": json.loads(record.value),
            }
            for record in records
        ]
        assert [item["key"] for item in published] == [
            str(message.message_id),
            str(message.message_id),
        ]
        assert [item["value"]["message_id"] for item in published] == [
            str(message.message_id),
            str(message.message_id),
        ]

        claims: list[bool] = []
        for _ in published:
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
    finally:
        await consumer.stop()
        await transport.stop()
