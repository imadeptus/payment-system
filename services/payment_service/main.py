"""Payment Service ASGI entrypoint and runtime lifecycle."""

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID

import structlog
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from libs.contracts import AuthorizePayment, MessageEnvelope, RefundPayment
from libs.messaging.kafka import KafkaTransport, consume_forever
from libs.messaging.publisher import OutboxPublisher
from libs.messaging.retry import BusinessMessageError, TransientMessageError
from libs.observability import configure_logging
from libs.runtime import Readiness, add_health_routes
from services.payment_service.config import PaymentSettings
from services.payment_service.db import build_database
from services.payment_service.handlers import handle_authorize, handle_refund
from services.payment_service.models import Outbox
from services.payment_service.provider import PaymentProvider

settings = PaymentSettings()
configure_logging("payment-service", level=settings.log_level)
logger = structlog.get_logger()
engine, session_factory = build_database(settings.payment_database_url.get_secret_value())
readiness = Readiness()
transport = KafkaTransport(
    settings.kafka_bootstrap_servers,
    client_id="payment-service",
)
publisher = OutboxPublisher(
    session_factory,
    Outbox,
    transport,
    batch_size=settings.outbox_batch_size,
    poll_seconds=settings.outbox_poll_seconds,
)
provider = PaymentProvider(
    reject_order_ids={
        UUID(value) for value in settings.reject_order_ids.split(",") if value
    },
    refund_failure_order_ids={
        UUID(value) for value in settings.refund_failure_order_ids.split(",") if value
    },
)


async def process_command(raw: dict[str, Any]) -> None:
    message_type = raw.get("message_type")
    try:
        async with session_factory() as session:
            if message_type == "AuthorizePayment":
                await handle_authorize(
                    session,
                    MessageEnvelope[AuthorizePayment].model_validate(raw),
                    provider,
                )
            elif message_type == "RefundPayment":
                await handle_refund(
                    session,
                    MessageEnvelope[RefundPayment].model_validate(raw),
                    provider,
                )
            else:
                raise BusinessMessageError(
                    f"Unsupported Payment command: {message_type}"
                )
    except SQLAlchemyError as exc:
        raise TransientMessageError("payment_database_error") from exc


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
    tasks: list[asyncio.Task[None]] = []
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
        readiness.database = True
        await transport.start()
        readiness.kafka = True
        tasks = [
            asyncio.create_task(publisher.run(stop_event), name="payment-outbox"),
            asyncio.create_task(
                consume_forever(
                    bootstrap_servers=settings.kafka_bootstrap_servers,
                    topics=(settings.payment_commands_topic,),
                    group_id="payment-service-v1",
                    handler=process_command,
                    dlq=publish_dlq,
                    attempts=settings.message_retry_attempts,
                    delays=tuple(
                        float(value)
                        for value in settings.message_backoff_seconds.split(",")
                    ),
                ),
                name="payment-consumer",
            ),
        ]
        logger.info("service_started")
        yield
    finally:
        readiness.kafka = False
        readiness.database = False
        stop_event.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await transport.stop()
        await engine.dispose()
        logger.info("service_stopped")


app = FastAPI(title="Payment Service", version="1.0.0", lifespan=lifespan)
add_health_routes(app, readiness)
