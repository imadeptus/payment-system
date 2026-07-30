import asyncio
import base64
import json
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from services.order_service.models import Outbox as OrderOutbox
from services.payment_service.models import Inbox as PaymentInbox
from services.payment_service.models import Outbox as PaymentOutbox
from services.payment_service.models import Payment


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_http_idempotency_key_returns_same_order(
    order_client: httpx.AsyncClient,
    order_database: AsyncEngine,
    payment_database: AsyncEngine,
) -> None:
    key = f"e2e-idempotency-{uuid4()}"
    payload = {
        "sku": "IN-STOCK",
        "quantity": 1,
        "amount_minor": 500,
        "currency": "RUB",
    }

    created = await order_client.post(
        "/orders",
        headers={"Idempotency-Key": key},
        json=payload,
    )
    replayed = await order_client.post(
        "/orders",
        headers={"Idempotency-Key": key},
        json=payload,
    )

    assert created.status_code == 202
    assert replayed.status_code == 200
    assert replayed.json()["order_id"] == created.json()["order_id"]

    order_id = UUID(created.json()["order_id"])
    order_sessions = async_sessionmaker(order_database, expire_on_commit=False)
    payment_sessions = async_sessionmaker(payment_database, expire_on_commit=False)
    async with order_sessions() as session:
        command = await session.scalar(
            select(OrderOutbox.payload_json).where(
                OrderOutbox.payload_json["order_id"].as_string() == str(order_id),
                OrderOutbox.payload_json["message_type"].as_string()
                == "AuthorizePayment",
            )
        )
    assert command is not None
    message_id = UUID(command["message_id"])

    encoded = base64.b64encode(
        json.dumps(command, separators=(",", ":"), sort_keys=True).encode()
    ).decode()
    script = (
        "import asyncio\n"
        "import base64\n"
        "from aiokafka import AIOKafkaProducer\n"
        f"payload = base64.b64decode('{encoded}')\n"
        "async def run():\n"
        "    producer = AIOKafkaProducer(bootstrap_servers='kafka:9092')\n"
        "    await producer.start()\n"
        "    try:\n"
        f"        key = b'{message_id}'\n"
        "        await producer.send_and_wait("
        "'payments.commands.v1', value=payload, key=key)\n"
        "        await producer.send_and_wait("
        "'payments.commands.v1', value=payload, key=key)\n"
        "    finally:\n"
        "        await producer.stop()\n"
        "asyncio.run(run())"
    )
    process = await asyncio.create_subprocess_exec(
        "docker",
        "compose",
        "exec",
        "-T",
        "order-service",
        "python",
        "-c",
        script,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await process.communicate()
    assert process.returncode == 0, stderr.decode()
    await asyncio.sleep(1.0)

    async with payment_sessions() as session:
        payment_count = await session.scalar(
            select(func.count()).select_from(Payment).where(Payment.order_id == order_id)
        )
        event_count = await session.scalar(
            select(func.count())
            .select_from(PaymentOutbox)
            .where(
                PaymentOutbox.payload_json["order_id"].as_string() == str(order_id),
                PaymentOutbox.payload_json["message_type"].as_string()
                == "PaymentAuthorized",
            )
        )
        inbox_count = await session.scalar(
            select(func.count())
            .select_from(PaymentInbox)
            .where(PaymentInbox.message_id == message_id)
        )

    assert payment_count == 1
    assert event_count == 1
    assert inbox_count == 1
