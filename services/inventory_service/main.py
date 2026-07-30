"""Inventory Service ASGI entrypoint and runtime lifecycle."""

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import structlog
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from libs.contracts import MessageEnvelope, ReleaseInventory, ReserveInventory
from libs.messaging.kafka import KafkaTransport, consume_forever
from libs.messaging.publisher import OutboxPublisher
from libs.messaging.retry import BusinessMessageError, TransientMessageError
from libs.observability import configure_logging
from libs.runtime import (
    Readiness,
    add_health_routes,
    supervise_tasks,
    wait_for_worker_start,
)
from services.inventory_service.config import InventorySettings
from services.inventory_service.db import build_database
from services.inventory_service.handlers import handle_release, handle_reserve
from services.inventory_service.models import Outbox

settings = InventorySettings()
configure_logging("inventory-service", level=settings.log_level)
logger = structlog.get_logger()
engine, session_factory = build_database(
    settings.inventory_database_url.get_secret_value()
)
readiness = Readiness()
transport = KafkaTransport(
    settings.kafka_bootstrap_servers,
    client_id="inventory-service",
)
publisher = OutboxPublisher(
    session_factory,
    Outbox,
    transport,
    batch_size=settings.outbox_batch_size,
    poll_seconds=settings.outbox_poll_seconds,
)


async def process_command(raw: dict[str, Any]) -> None:
    message_type = raw.get("message_type")
    try:
        async with session_factory() as session:
            if message_type == "ReserveInventory":
                await handle_reserve(
                    session,
                    MessageEnvelope[ReserveInventory].model_validate(raw),
                )
            elif message_type == "ReleaseInventory":
                await handle_release(
                    session,
                    MessageEnvelope[ReleaseInventory].model_validate(raw),
                )
            else:
                raise BusinessMessageError(
                    f"Unsupported Inventory command: {message_type}"
                )
    except SQLAlchemyError as exc:
        raise TransientMessageError("inventory_database_error") from exc


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
            asyncio.create_task(publisher.run(stop_event), name="inventory-outbox"),
            asyncio.create_task(
                consume_forever(
                    bootstrap_servers=settings.kafka_bootstrap_servers,
                    topics=(settings.inventory_commands_topic,),
                    group_id="inventory-service-v1",
                    handler=process_command,
                    dlq=publish_dlq,
                    attempts=settings.message_retry_attempts,
                    delays=settings.backoff_delays,
                    started_event=consumer_started,
                ),
                name="inventory-consumer",
            ),
        ]
        await wait_for_worker_start(consumer_started, worker_tasks)
        readiness.kafka = True
        supervisor_task = asyncio.create_task(
            supervise_tasks(worker_tasks, readiness),
            name="inventory-supervisor",
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


app = FastAPI(title="Inventory Service", version="1.0.0", lifespan=lifespan)
add_health_routes(app, readiness)
