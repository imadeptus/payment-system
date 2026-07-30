import asyncio
import importlib
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from sqlalchemy.exc import SQLAlchemyError

from libs.messaging.retry import BusinessMessageError, TransientMessageError


@pytest.fixture
def entrypoints(monkeypatch: pytest.MonkeyPatch) -> dict[str, ModuleType]:
    monkeypatch.setenv("ORDER_DATABASE_URL", "sqlite+aiosqlite:///:memory:")
    monkeypatch.setenv("PAYMENT_DATABASE_URL", "sqlite+aiosqlite:///:memory:")
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "sqlite+aiosqlite:///:memory:")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    return {
        name: importlib.import_module(f"services.{name}_service.main")
        for name in ("order", "payment", "inventory")
    }


def envelope(message_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "message_id": "00000000-0000-0000-0000-000000000801",
        "message_type": message_type,
        "schema_version": 1,
        "occurred_at": "2026-07-30T08:00:00Z",
        "correlation_id": "00000000-0000-0000-0000-000000000802",
        "causation_id": None,
        "order_id": "00000000-0000-0000-0000-000000000803",
        "payload": payload,
    }


ORDER_EVENTS = {
    "PaymentAuthorized": {"amount_minor": 100, "currency": "RUB"},
    "PaymentRejected": {
        "amount_minor": 100,
        "currency": "RUB",
        "reason": "rejected",
    },
    "PaymentRefunded": {"amount_minor": 100, "currency": "RUB"},
    "PaymentRefundFailed": {
        "amount_minor": 100,
        "currency": "RUB",
        "reason": "failed",
    },
    "InventoryReserved": {"sku": "IN-STOCK", "quantity": 1},
    "InventoryRejected": {
        "sku": "OUT-OF-STOCK",
        "quantity": 1,
        "reason": "out_of_stock",
    },
    "InventoryReleased": {"sku": "IN-STOCK", "quantity": 1},
    "InventoryReleaseFailed": {
        "sku": "IN-STOCK",
        "quantity": 1,
        "reason": "failed",
    },
}


@pytest.mark.parametrize(("message_type", "payload"), ORDER_EVENTS.items())
def test_order_parser_accepts_all_supported_events(
    entrypoints: dict[str, ModuleType],
    message_type: str,
    payload: dict[str, Any],
) -> None:
    parsed = entrypoints["order"].parse_event(envelope(message_type, payload))

    assert parsed.message_type == message_type


def test_order_parser_rejects_unsupported_event(
    entrypoints: dict[str, ModuleType],
) -> None:
    with pytest.raises(BusinessMessageError, match="Unsupported Order event"):
        entrypoints["order"].parse_event(envelope("Unknown", {}))


@asynccontextmanager
async def fake_session_factory() -> AsyncIterator[object]:
    yield object()


@pytest.mark.asyncio
async def test_order_processes_event_and_translates_database_error(
    entrypoints: dict[str, ModuleType],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = entrypoints["order"]
    handler = AsyncMock()
    monkeypatch.setattr(module, "session_factory", fake_session_factory)
    monkeypatch.setattr(module, "handle_saga_event", handler)
    raw = envelope("PaymentAuthorized", ORDER_EVENTS["PaymentAuthorized"])

    await module.process_event(raw)
    handler.assert_awaited_once()

    handler.side_effect = SQLAlchemyError("database unavailable")
    with pytest.raises(TransientMessageError, match="order_database_error"):
        await module.process_event(raw)


@pytest.mark.parametrize(
    ("service", "message_type", "payload", "handler_name"),
    [
        (
            "payment",
            "AuthorizePayment",
            {"amount_minor": 100, "currency": "RUB"},
            "handle_authorize",
        ),
        (
            "payment",
            "RefundPayment",
            {"amount_minor": 100, "currency": "RUB"},
            "handle_refund",
        ),
        (
            "inventory",
            "ReserveInventory",
            {"sku": "IN-STOCK", "quantity": 1},
            "handle_reserve",
        ),
        (
            "inventory",
            "ReleaseInventory",
            {"sku": "IN-STOCK", "quantity": 1},
            "handle_release",
        ),
    ],
)
@pytest.mark.asyncio
async def test_command_entrypoint_dispatches_supported_commands(
    entrypoints: dict[str, ModuleType],
    monkeypatch: pytest.MonkeyPatch,
    service: str,
    message_type: str,
    payload: dict[str, Any],
    handler_name: str,
) -> None:
    module = entrypoints[service]
    handler = AsyncMock()
    monkeypatch.setattr(module, "session_factory", fake_session_factory)
    monkeypatch.setattr(module, handler_name, handler)

    await module.process_command(envelope(message_type, payload))

    handler.assert_awaited_once()


@pytest.mark.parametrize("service", ["payment", "inventory"])
@pytest.mark.asyncio
async def test_command_entrypoint_rejects_unknown_command(
    entrypoints: dict[str, ModuleType],
    monkeypatch: pytest.MonkeyPatch,
    service: str,
) -> None:
    module = entrypoints[service]
    monkeypatch.setattr(module, "session_factory", fake_session_factory)

    with pytest.raises(BusinessMessageError, match="Unsupported"):
        await module.process_command(envelope("Unknown", {}))


@pytest.mark.parametrize(
    ("service", "message_type", "payload", "handler_name", "expected_error"),
    [
        (
            "payment",
            "AuthorizePayment",
            {"amount_minor": 100, "currency": "RUB"},
            "handle_authorize",
            "payment_database_error",
        ),
        (
            "inventory",
            "ReserveInventory",
            {"sku": "IN-STOCK", "quantity": 1},
            "handle_reserve",
            "inventory_database_error",
        ),
    ],
)
@pytest.mark.asyncio
async def test_command_entrypoint_translates_database_error(
    entrypoints: dict[str, ModuleType],
    monkeypatch: pytest.MonkeyPatch,
    service: str,
    message_type: str,
    payload: dict[str, Any],
    handler_name: str,
    expected_error: str,
) -> None:
    module = entrypoints[service]
    handler = AsyncMock(side_effect=SQLAlchemyError("database unavailable"))
    monkeypatch.setattr(module, "session_factory", fake_session_factory)
    monkeypatch.setattr(module, handler_name, handler)

    with pytest.raises(TransientMessageError, match=expected_error):
        await module.process_command(envelope(message_type, payload))


@pytest.mark.parametrize("service", ["order", "payment", "inventory"])
@pytest.mark.asyncio
async def test_dlq_publisher_uses_correlation_id_as_key(
    entrypoints: dict[str, ModuleType],
    monkeypatch: pytest.MonkeyPatch,
    service: str,
) -> None:
    module = entrypoints[service]
    publish = AsyncMock()
    monkeypatch.setattr(module.transport, "publish", publish)
    raw = envelope("Unknown", {})

    await module.publish_dlq(raw)

    topic, key, value = publish.await_args.args
    assert topic == "saga.dlq.v1"
    assert key == b"00000000-0000-0000-0000-000000000802"
    assert b'"message_type":"Unknown"' in value


@pytest.mark.asyncio
async def test_payment_dlq_publishes_deterministic_refund_failure_event(
    entrypoints: dict[str, ModuleType],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = entrypoints["payment"]
    publish = AsyncMock()
    monkeypatch.setattr(module.transport, "publish", publish)
    refund = envelope(
        "RefundPayment",
        {"amount_minor": 100, "currency": "RUB"},
    )

    await module.publish_dlq(
        {
            "original_message": refund,
            "reason": "TransientMessageError",
            "attempts": 3,
            "correlation_id": refund["correlation_id"],
            "causation_id": refund["causation_id"],
        }
    )

    assert publish.await_count == 2
    failure_call = publish.await_args_list[1]
    topic, key, value = failure_call.args
    failure = json.loads(value)
    assert topic == "payments.events.v1"
    assert key == failure["message_id"].encode()
    assert failure["message_type"] == "PaymentRefundFailed"
    assert failure["correlation_id"] == refund["correlation_id"]
    assert failure["causation_id"] == refund["message_id"]
    assert failure["order_id"] == refund["order_id"]
    assert failure["payload"] == {
        "amount_minor": 100,
        "currency": "RUB",
        "reason": "technical_retry_exhausted",
    }


class FakeConnection:
    async def __aenter__(self) -> "FakeConnection":
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def execute(self, _: object) -> None:
        return None


class FakeEngine:
    def __init__(self) -> None:
        self.disposed = 0

    def connect(self) -> FakeConnection:
        return FakeConnection()

    async def dispose(self) -> None:
        self.disposed += 1


@pytest.mark.parametrize("service", ["order", "payment", "inventory"])
@pytest.mark.asyncio
async def test_service_lifespan_sets_readiness_and_closes_resources(
    entrypoints: dict[str, ModuleType],
    monkeypatch: pytest.MonkeyPatch,
    service: str,
) -> None:
    module = entrypoints[service]
    engine = FakeEngine()
    transport = SimpleNamespace(start=AsyncMock(), stop=AsyncMock())
    worker_blocker = asyncio.Event()

    async def run_publisher(_: asyncio.Event) -> None:
        await worker_blocker.wait()

    async def run_consumer(**kwargs: Any) -> None:
        kwargs["started_event"].set()
        await worker_blocker.wait()

    async def run_supervisor(*_: object, **__: object) -> None:
        await worker_blocker.wait()

    publisher = SimpleNamespace(run=AsyncMock(side_effect=run_publisher))
    consume = AsyncMock(side_effect=run_consumer)
    supervisor = AsyncMock(side_effect=run_supervisor)
    monkeypatch.setattr(module, "engine", engine)
    monkeypatch.setattr(module, "transport", transport)
    monkeypatch.setattr(module, "publisher", publisher)
    monkeypatch.setattr(module, "consume_forever", consume)
    monkeypatch.setattr(module, "supervise_tasks", supervisor, raising=False)

    async with module.lifespan(FastAPI()):
        await asyncio.sleep(0)
        assert module.readiness.database is True
        assert module.readiness.kafka is True
        assert isinstance(consume.await_args.kwargs["started_event"], asyncio.Event)

    assert module.readiness.database is False
    assert module.readiness.kafka is False
    transport.start.assert_awaited_once()
    transport.stop.assert_awaited_once()
    supervisor.assert_awaited_once()
    assert engine.disposed == 1
