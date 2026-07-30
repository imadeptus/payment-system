"""Order Service ASGI entrypoint and runtime lifecycle."""

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import structlog
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from libs.contracts import (
    InventoryRejected,
    InventoryReleased,
    InventoryReleaseFailed,
    InventoryReserved,
    MessageEnvelope,
    PaymentAuthorized,
    PaymentRefunded,
    PaymentRefundFailed,
    PaymentRejected,
)
from libs.messaging.kafka import KafkaTransport, consume_forever
from libs.messaging.publisher import OutboxPublisher
from libs.messaging.retry import BusinessMessageError, TransientMessageError
from libs.observability import configure_logging
from libs.runtime import Readiness, supervise_tasks, wait_for_worker_start
from services.order_service.api import create_app
from services.order_service.config import OrderSettings
from services.order_service.db import build_database
from services.order_service.handlers import handle_saga_event
from services.order_service.models import Outbox

settings = OrderSettings()
configure_logging("order-service", level=settings.log_level)
logger = structlog.get_logger()
engine, session_factory = build_database(settings.order_database_url.get_secret_value())
readiness = Readiness()
transport = KafkaTransport(
    settings.kafka_bootstrap_servers,
    client_id="order-service",
)
publisher = OutboxPublisher(
    session_factory,
    Outbox,
    transport,
    batch_size=settings.outbox_batch_size,
    poll_seconds=settings.outbox_poll_seconds,
)


def parse_event(raw: dict[str, Any]) -> MessageEnvelope[Any]:
    message_type = raw.get("message_type")
    if message_type == "PaymentAuthorized":
        return MessageEnvelope[PaymentAuthorized].model_validate(raw)
    if message_type == "PaymentRejected":
        return MessageEnvelope[PaymentRejected].model_validate(raw)
    if message_type == "PaymentRefunded":
        return MessageEnvelope[PaymentRefunded].model_validate(raw)
    if message_type == "PaymentRefundFailed":
        return MessageEnvelope[PaymentRefundFailed].model_validate(raw)
    if message_type == "InventoryReserved":
        return MessageEnvelope[InventoryReserved].model_validate(raw)
    if message_type == "InventoryRejected":
        return MessageEnvelope[InventoryRejected].model_validate(raw)
    if message_type == "InventoryReleased":
        return MessageEnvelope[InventoryReleased].model_validate(raw)
    if message_type == "InventoryReleaseFailed":
        return MessageEnvelope[InventoryReleaseFailed].model_validate(raw)
    raise BusinessMessageError(f"Unsupported Order event: {message_type}")


async def process_event(raw: dict[str, Any]) -> None:
    envelope = parse_event(raw)
    try:
        async with session_factory() as session:
            await handle_saga_event(session, envelope)
    except SQLAlchemyError as exc:
        raise TransientMessageError("order_database_error") from exc


async def publish_dlq(record: dict[str, Any]) -> None:
    key = str(record.get("correlation_id", "unknown")).encode()
    await transport.publish(
        settings.dlq_topic,
        key,
        json.dumps(record, separators=(",", ":"), sort_keys=True).encode(),
    )


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    stop_event = asyncio.Event()
    worker_tasks: list[asyncio.Task[None]] = []
    supervisor_task: asyncio.Task[None] | None = None
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
        readiness.database = True
        await transport.start()
        consumer_started = asyncio.Event()
        worker_tasks = [
            asyncio.create_task(publisher.run(stop_event), name="order-outbox"),
            asyncio.create_task(
                consume_forever(
                    bootstrap_servers=settings.kafka_bootstrap_servers,
                    topics=(
                        settings.payment_events_topic,
                        settings.inventory_events_topic,
                    ),
                    group_id="order-service-v1",
                    handler=process_event,
                    dlq=publish_dlq,
                    attempts=settings.message_retry_attempts,
                    delays=settings.backoff_delays,
                    started_event=consumer_started,
                ),
                name="order-consumer",
            ),
        ]
        await wait_for_worker_start(consumer_started, worker_tasks)
        readiness.kafka = True
        supervisor_task = asyncio.create_task(
            supervise_tasks(worker_tasks, readiness),
            name="order-supervisor",
        )
        logger.info("service_started")
        yield
    finally:
        readiness.kafka = False
        readiness.database = False
        stop_event.set()
        if supervisor_task is not None:
            supervisor_task.cancel()
        for task in worker_tasks:
            task.cancel()
        await asyncio.gather(
            *worker_tasks,
            *([supervisor_task] if supervisor_task is not None else []),
            return_exceptions=True,
        )
        await transport.stop()
        await engine.dispose()
        logger.info("service_stopped")


app = create_app(session_factory, lifespan=lifespan, readiness=readiness)
