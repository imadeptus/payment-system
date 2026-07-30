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
        self._bootstrap_servers = bootstrap_servers
        self._client_id = client_id
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        if self._producer is not None:
            return
        producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            client_id=self._client_id,
            acks="all",
            enable_idempotence=True,
        )
        await producer.start()
        self._producer = producer

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    async def publish(self, topic: str, key: bytes, value: bytes) -> None:
        if self._producer is None:
            raise RuntimeError("KafkaTransport is not started")
        await self._producer.send_and_wait(topic, value=value, key=key)


async def consume_forever(
    *,
    bootstrap_servers: str,
    topics: Sequence[str],
    group_id: str,
    handler: Callable[[dict[str, Any]], Awaitable[None]],
    dlq: DlqPublisher,
    attempts: int = 3,
    delays: Sequence[float] = (1.0, 2.0, 4.0),
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

            await consume_with_retry(
                envelope,
                typed_handler,
                dlq,
                attempts=attempts,
                delays=delays,
            )
            await consumer.commit()
    finally:
        await consumer.stop()
