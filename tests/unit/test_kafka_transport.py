import json
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest

from libs.messaging import kafka
from libs.messaging.kafka import KafkaTransport, consume_forever


class FakeProducer:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.started = 0
        self.stopped = 0
        self.sent: list[tuple[str, bytes, bytes]] = []

    async def start(self) -> None:
        self.started += 1

    async def stop(self) -> None:
        self.stopped += 1

    async def send_and_wait(self, topic: str, *, value: bytes, key: bytes) -> None:
        self.sent.append((topic, key, value))


@pytest.mark.asyncio
async def test_kafka_transport_lifecycle_and_publish(monkeypatch: pytest.MonkeyPatch) -> None:
    producers: list[FakeProducer] = []

    def build_producer(**kwargs: Any) -> FakeProducer:
        producer = FakeProducer(**kwargs)
        producers.append(producer)
        return producer

    monkeypatch.setattr(kafka, "AIOKafkaProducer", build_producer)
    transport = KafkaTransport("kafka:9092", client_id="test-client")

    with pytest.raises(RuntimeError, match="not started"):
        await transport.publish("topic", b"key", b"value")

    await transport.start()
    await transport.start()
    await transport.publish("topic", b"key", b"value")
    await transport.stop()
    await transport.stop()

    assert len(producers) == 1
    assert producers[0].kwargs == {
        "bootstrap_servers": "kafka:9092",
        "client_id": "test-client",
        "acks": "all",
        "enable_idempotence": True,
    }
    assert producers[0].started == 1
    assert producers[0].stopped == 1
    assert producers[0].sent == [("topic", b"key", b"value")]


class FakeConsumer:
    def __init__(self, records: list[SimpleNamespace], *topics: str, **kwargs: Any) -> None:
        self.records = iter(records)
        self.topics = topics
        self.kwargs = kwargs
        self.started = 0
        self.stopped = 0
        self.committed = 0

    async def start(self) -> None:
        self.started += 1

    async def stop(self) -> None:
        self.stopped += 1

    async def commit(self) -> None:
        self.committed += 1

    def __aiter__(self) -> "FakeConsumer":
        return self

    async def __anext__(self) -> SimpleNamespace:
        try:
            return next(self.records)
        except StopIteration as exc:
            raise StopAsyncIteration from exc


@pytest.mark.asyncio
async def test_consumer_validates_handles_then_commits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = {
        "message_id": "00000000-0000-0000-0000-000000000701",
        "message_type": "AuthorizePayment",
        "schema_version": 1,
        "occurred_at": "2026-07-30T08:00:00Z",
        "correlation_id": "00000000-0000-0000-0000-000000000702",
        "causation_id": None,
        "order_id": "00000000-0000-0000-0000-000000000703",
        "payload": {"amount_minor": 100, "currency": "RUB"},
    }
    consumer = FakeConsumer([SimpleNamespace(value=json.dumps(raw).encode())])
    monkeypatch.setattr(kafka, "AIOKafkaConsumer", lambda *args, **kwargs: consumer)
    handled: list[dict[str, Any]] = []
    dlq_records: list[dict[str, Any]] = []

    async def handler(record: dict[str, Any]) -> None:
        handled.append(record)

    async def dlq(record: dict[str, Any]) -> None:
        dlq_records.append(record)

    await consume_forever(
        bootstrap_servers="kafka:9092",
        topics=("payments.commands.v1",),
        group_id="test-group",
        handler=handler,
        dlq=dlq,
        attempts=1,
        delays=(0.0,),
    )

    assert handled == [raw]
    assert dlq_records == []
    assert consumer.started == 1
    assert consumer.committed == 1
    assert consumer.stopped == 1
    assert UUID(handled[0]["message_id"]) == UUID(raw["message_id"])
