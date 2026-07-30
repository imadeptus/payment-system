"""Async Kafka producer transport and manual-commit consumer loop."""

import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, cast

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from pydantic import ValidationError

from libs.contracts import MessageEnvelope
from libs.messaging.retry import BusinessMessageError, DlqPublisher, consume_with_retry
from libs.messaging.sanitization import sanitize_decoded_message


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
    started_event: asyncio.Event | None = None,
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
    if started_event is not None:
        started_event.set()
    try:
        async for record in consumer:
            raw: dict[str, Any] | None = None
            try:
                decoded: Any = json.loads(record.value)
                envelope = MessageEnvelope[Any].model_validate(decoded)
                raw = cast(dict[str, Any], decoded)
                raw_for_handler = raw

                async def typed_handler(
                    _: MessageEnvelope[Any],
                    raw_message: dict[str, Any] = raw_for_handler,
                ) -> None:
                    await handler(raw_message)

                await consume_with_retry(
                    envelope,
                    typed_handler,
                    dlq,
                    attempts=attempts,
                    delays=delays,
                )
            except (json.JSONDecodeError, ValidationError, BusinessMessageError) as exc:
                await dlq(
                    {
                        "original_message": (
                            sanitize_decoded_message(
                                raw,
                                include_validated_payload=False,
                            )
                            if raw is not None
                            else {
                                "raw_sha256": hashlib.sha256(
                                    record.value
                                ).hexdigest(),
                                "raw_size_bytes": len(record.value),
                            }
                        ),
                        "reason": type(exc).__name__,
                        "attempts": 1,
                        "correlation_id": (
                            str(raw.get("correlation_id"))
                            if raw is not None and raw.get("correlation_id") is not None
                            else None
                        ),
                        "causation_id": (
                            str(raw.get("causation_id"))
                            if raw is not None and raw.get("causation_id") is not None
                            else None
                        ),
                    }
                )
            await consumer.commit(
                {
                    TopicPartition(record.topic, record.partition): record.offset + 1,
                }
            )
    finally:
        await consumer.stop()
