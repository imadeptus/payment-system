"""Async Kafka producer transport and manual-commit consumer loop."""

import json
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from libs.contracts import MessageEnvelope
from libs.messaging.retry import DlqPublisher, consume_with_retry


class KafkaTransport:
    """Small aiokafka producer wrapper used by OutboxPublisher."""

    def __init__(self, bootstrap_servers: str, *, client_id: str) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            acks="all",
            enable_idempotence=True,
        )
        self._started = False

    async def start(self) -> None:
        await self._producer.start()
        self._started = True

    async def stop(self) -> None:
        if self._started:
            await self._producer.stop()
            self._started = False

    async def publish(self, topic: str, key: bytes, value: bytes) -> None:
        await self._producer.send_and_wait(topic, value=value, key=key)


async def consume_forever(
    *,
    bootstrap_servers: str,
    topics: Sequence[str],
    group_id: str,
    handler: Callable[[dict[str, Any]], Awaitable[None]],
    dlq: DlqPublisher,
) -> None:
    """Consume with manual offsets committed after handling or DLQ success."""

    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        async for record in consumer:
            raw: dict[str, Any] = json.loads(record.value)
            envelope = MessageEnvelope[Any].model_validate(raw)

            async def typed_handler(
                _: MessageEnvelope[Any],
                raw_message: dict[str, Any] = raw,
            ) -> None:
                await handler(raw_message)

            await consume_with_retry(envelope, typed_handler, dlq)
            await consumer.commit()
    finally:
        await consumer.stop()
