import asyncio
import hashlib
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
        self.commits: list[object] = []

    async def start(self) -> None:
        self.started += 1

    async def stop(self) -> None:
        self.stopped += 1

    async def commit(self, offsets: object = None) -> None:
        self.commits.append(offsets)

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
    consumer = FakeConsumer(
        [
            SimpleNamespace(
                value=json.dumps(raw).encode(),
                topic="payments.commands.v1",
                partition=0,
                offset=7,
            )
        ]
    )
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
    assert len(consumer.commits) == 1
    assert consumer.stopped == 1
    assert UUID(handled[0]["message_id"]) == UUID(raw["message_id"])


@pytest.mark.asyncio
async def test_consumer_signals_only_after_broker_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumer = FakeConsumer([])
    monkeypatch.setattr(kafka, "AIOKafkaConsumer", lambda *args, **kwargs: consumer)
    started = asyncio.Event()

    async def handler(_: dict[str, Any]) -> None:
        return None

    async def dlq(_: dict[str, Any]) -> None:
        return None

    await consume_forever(
        bootstrap_servers="kafka:9092",
        topics=("payments.commands.v1",),
        group_id="test-group",
        handler=handler,
        dlq=dlq,
        attempts=1,
        delays=(0.0,),
        started_event=started,
    )

    assert started.is_set()
    assert consumer.started == 1


@pytest.mark.asyncio
async def test_poison_record_goes_to_dlq_and_next_record_is_processed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret_poison = b'{"password":"do-not-leak"'
    valid = {
        "message_id": "00000000-0000-0000-0000-000000000711",
        "message_type": "AuthorizePayment",
        "schema_version": 1,
        "occurred_at": "2026-07-30T08:00:00Z",
        "correlation_id": "00000000-0000-0000-0000-000000000712",
        "causation_id": None,
        "order_id": "00000000-0000-0000-0000-000000000713",
        "payload": {"amount_minor": 100, "currency": "RUB"},
    }
    records = [
        SimpleNamespace(
            value=secret_poison,
            topic="payments.commands.v1",
            partition=0,
            offset=8,
        ),
        SimpleNamespace(
            value=json.dumps(valid).encode(),
            topic="payments.commands.v1",
            partition=0,
            offset=9,
        ),
    ]
    consumer = FakeConsumer(records)
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

    assert handled == [valid]
    assert len(dlq_records) == 1
    assert dlq_records[0]["reason"] == "JSONDecodeError"
    assert dlq_records[0]["attempts"] == 1
    assert dlq_records[0]["original_message"] == {
        "raw_sha256": hashlib.sha256(secret_poison).hexdigest(),
        "raw_size_bytes": len(secret_poison),
    }
    assert "password" not in str(dlq_records[0])
    assert "do-not-leak" not in str(dlq_records[0])
    assert len(consumer.commits) == 2


@pytest.mark.asyncio
async def test_business_poison_goes_to_dlq_without_stopping_consumer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    unknown = {
        "message_id": "00000000-0000-0000-0000-000000000721",
        "message_type": "password=do-not-leak",
        "schema_version": 1,
        "occurred_at": "2026-07-30T08:00:00Z",
        "correlation_id": "00000000-0000-0000-0000-000000000722",
        "causation_id": None,
        "order_id": "00000000-0000-0000-0000-000000000723",
        "payload": {"password": "do-not-leak"},
        "authorization": "Bearer do-not-leak",
    }
    valid = {
        "message_id": "00000000-0000-0000-0000-000000000724",
        "message_type": "AuthorizePayment",
        "schema_version": 1,
        "occurred_at": "2026-07-30T08:00:00Z",
        "correlation_id": "00000000-0000-0000-0000-000000000725",
        "causation_id": None,
        "order_id": "00000000-0000-0000-0000-000000000726",
        "payload": {"amount_minor": 100, "currency": "RUB"},
    }
    records = [
        SimpleNamespace(
            value=json.dumps(unknown).encode(),
            topic="payments.commands.v1",
            partition=0,
            offset=10,
        ),
        SimpleNamespace(
            value=json.dumps(valid).encode(),
            topic="payments.commands.v1",
            partition=0,
            offset=11,
        ),
    ]
    consumer = FakeConsumer(records)
    monkeypatch.setattr(kafka, "AIOKafkaConsumer", lambda *args, **kwargs: consumer)
    handled: list[dict[str, Any]] = []
    dlq_records: list[dict[str, Any]] = []

    async def handler(record: dict[str, Any]) -> None:
        if record["message_id"] == unknown["message_id"]:
            from libs.messaging.retry import BusinessMessageError

            raise BusinessMessageError("unsupported")
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

    assert handled == [valid]
    assert dlq_records[0]["reason"] == "BusinessMessageError"
    assert dlq_records[0]["correlation_id"] == unknown["correlation_id"]
    assert dlq_records[0]["original_message"] == {
        "message_id": unknown["message_id"],
        "message_type": "unknown",
        "schema_version": 1,
        "occurred_at": "2026-07-30T08:00:00Z",
        "correlation_id": unknown["correlation_id"],
        "causation_id": None,
        "order_id": unknown["order_id"],
    }
    assert "password" not in str(dlq_records[0])
    assert "authorization" not in str(dlq_records[0])
    assert "do-not-leak" not in str(dlq_records[0])
    assert len(consumer.commits) == 2
