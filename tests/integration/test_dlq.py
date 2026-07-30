import asyncio
import json
import os
from typing import Any
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from libs.messaging.kafka import consume_forever


@pytest.mark.integration
@pytest.mark.asyncio
async def test_real_kafka_poison_record_reaches_dlq_and_consumer_continues() -> None:
    bootstrap_servers = os.getenv("TEST_KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap_servers:
        pytest.skip("TEST_KAFKA_BOOTSTRAP_SERVERS is required")

    suffix = uuid4().hex
    input_topic = f"test.commands.{suffix}"
    dlq_topic = f"test.dlq.{suffix}"
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    dlq_consumer = AIOKafkaConsumer(
        dlq_topic,
        bootstrap_servers=bootstrap_servers,
        group_id=f"test-dlq-reader-{suffix}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    handled: list[dict[str, Any]] = []
    handled_event = asyncio.Event()
    consumer_started = asyncio.Event()

    async def handler(record: dict[str, Any]) -> None:
        handled.append(record)
        handled_event.set()

    async def publish_dlq(record: dict[str, Any]) -> None:
        await producer.send_and_wait(
            dlq_topic,
            json.dumps(record, separators=(",", ":"), sort_keys=True).encode(),
        )

    worker = asyncio.create_task(
        consume_forever(
            bootstrap_servers=bootstrap_servers,
            topics=(input_topic,),
            group_id=f"test-worker-{suffix}",
            handler=handler,
            dlq=publish_dlq,
            attempts=1,
            delays=(0.0,),
            started_event=consumer_started,
        )
    )
    await producer.start()
    await dlq_consumer.start()
    try:
        await asyncio.wait_for(consumer_started.wait(), timeout=15.0)
        valid = {
            "message_id": str(uuid4()),
            "message_type": "AuthorizePayment",
            "schema_version": 1,
            "occurred_at": "2026-07-30T08:00:00Z",
            "correlation_id": str(uuid4()),
            "causation_id": None,
            "order_id": str(uuid4()),
            "payload": {"amount_minor": 100, "currency": "RUB"},
        }
        await producer.send_and_wait(input_topic, b"{invalid-json")
        await producer.send_and_wait(input_topic, json.dumps(valid).encode())

        await asyncio.wait_for(handled_event.wait(), timeout=15.0)
        dlq_record = await asyncio.wait_for(dlq_consumer.getone(), timeout=15.0)
        dlq_value = json.loads(dlq_record.value)

        assert handled == [valid]
        assert dlq_value["reason"] == "JSONDecodeError"
        assert dlq_value["original_message"] == {"raw": "{invalid-json"}
        assert worker.done() is False
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
        await dlq_consumer.stop()
        await producer.stop()
