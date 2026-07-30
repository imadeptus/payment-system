import asyncio
import hashlib
import json
import os
from uuid import UUID, uuid4

import httpx
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from services.payment_service.models import Payment
from tests.e2e.conftest import wait_for_order_status


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_payment_consumer_survives_poison_record(
    order_client: httpx.AsyncClient,
    payment_database: AsyncEngine,
) -> None:
    bootstrap_servers = os.getenv(
        "E2E_KAFKA_BOOTSTRAP_SERVERS",
        "127.0.0.1:19092",
    )
    dlq_consumer = AIOKafkaConsumer(
        "saga.dlq.v1",
        bootstrap_servers=bootstrap_servers,
        group_id=f"e2e-dlq-reader-{uuid4()}",
        auto_offset_reset="latest",
        enable_auto_commit=False,
    )
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await dlq_consumer.start()
    await producer.start()
    secret_poison = b'{"password":"do-not-leak"'
    try:
        await dlq_consumer.getmany(timeout_ms=1_000)
        await producer.send_and_wait(
            "payments.commands.v1",
            value=secret_poison,
        )

        loop = asyncio.get_running_loop()
        deadline = loop.time() + 15.0
        poison_dlq: dict[str, object] | None = None
        while loop.time() < deadline:
            records = await dlq_consumer.getmany(timeout_ms=1_000)
            for partition_records in records.values():
                for record in partition_records:
                    candidate = json.loads(record.value)
                    if (
                        candidate.get("reason") == "JSONDecodeError"
                        and candidate.get("original_message")
                        == {
                            "raw_sha256": hashlib.sha256(
                                secret_poison
                            ).hexdigest(),
                            "raw_size_bytes": len(secret_poison),
                        }
                    ):
                        poison_dlq = candidate
                        break
            if poison_dlq is not None:
                break
        assert poison_dlq is not None
        assert "password" not in str(poison_dlq)
        assert "do-not-leak" not in str(poison_dlq)
    finally:
        await producer.stop()
        await dlq_consumer.stop()

    response = await order_client.post(
        "/orders",
        headers={"Idempotency-Key": f"e2e-after-poison-{uuid4()}"},
        json={
            "sku": "IN-STOCK",
            "quantity": 1,
            "amount_minor": 1_337,
            "currency": "RUB",
        },
    )

    assert response.status_code == 202
    order_id = UUID(response.json()["order_id"])
    await wait_for_order_status(order_client, order_id, "CONFIRMED")

    payment_sessions = async_sessionmaker(payment_database, expire_on_commit=False)
    async with payment_sessions() as session:
        payment = await session.scalar(
            select(Payment).where(Payment.order_id == order_id)
        )

    assert payment is not None
    assert payment.status == "AUTHORIZED"
    assert payment.history == ["AUTHORIZED"]
