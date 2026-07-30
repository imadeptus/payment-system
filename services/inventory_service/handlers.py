"""Transactional inventory command handlers."""

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from libs.contracts import (
    InventoryRejected,
    InventoryReleased,
    InventoryReleaseFailed,
    InventoryReserved,
    MessageEnvelope,
    ReleaseInventory,
    ReserveInventory,
)
from libs.messaging import claim_inbox, enqueue
from services.inventory_service.models import Inbox, Outbox, Reservation, Stock


async def handle_reserve(
    session: AsyncSession,
    envelope: MessageEnvelope[ReserveInventory],
) -> None:
    """Reserve available stock exactly once with a row-level lock."""

    if envelope.message_type != "ReserveInventory":
        raise ValueError(f"Unexpected message type: {envelope.message_type}")

    async with session.begin():
        claimed = await claim_inbox(
            session,
            envelope.message_id,
            "inventory-reserve",
            inbox_model=Inbox,
        )
        if not claimed:
            return
        existing = await session.scalar(
            select(Reservation)
            .where(Reservation.order_id == envelope.order_id)
            .with_for_update()
        )
        if existing is not None:
            return

        stock = await session.scalar(
            select(Stock).where(Stock.sku == envelope.payload.sku).with_for_update()
        )
        payload: InventoryReserved | InventoryRejected
        if stock is None or stock.available < envelope.payload.quantity:
            event_type = "InventoryRejected"
            payload = InventoryRejected(
                sku=envelope.payload.sku,
                quantity=envelope.payload.quantity,
                reason="insufficient_stock",
            )
        else:
            stock.available -= envelope.payload.quantity
            session.add(
                Reservation(
                    order_id=envelope.order_id,
                    sku=envelope.payload.sku,
                    quantity=envelope.payload.quantity,
                    status="RESERVED",
                )
            )
            event_type = "InventoryReserved"
            payload = InventoryReserved(
                sku=envelope.payload.sku,
                quantity=envelope.payload.quantity,
            )

        enqueue(
            session,
            MessageEnvelope(
                message_id=uuid4(),
                message_type=event_type,
                occurred_at=datetime.now(UTC),
                correlation_id=envelope.correlation_id,
                causation_id=envelope.message_id,
                order_id=envelope.order_id,
                payload=payload,
            ),
            outbox_model=Outbox,
        )


async def handle_release(
    session: AsyncSession,
    envelope: MessageEnvelope[ReleaseInventory],
) -> None:
    """Release a prior reservation once and restore its stock."""

    if envelope.message_type != "ReleaseInventory":
        raise ValueError(f"Unexpected message type: {envelope.message_type}")

    async with session.begin():
        claimed = await claim_inbox(
            session,
            envelope.message_id,
            "inventory-release",
            inbox_model=Inbox,
        )
        if not claimed:
            return
        reservation = await session.scalar(
            select(Reservation)
            .where(Reservation.order_id == envelope.order_id)
            .with_for_update()
        )

        payload: InventoryReleased | InventoryReleaseFailed
        if (
            reservation is None
            or reservation.status != "RESERVED"
            or reservation.sku != envelope.payload.sku
            or reservation.quantity != envelope.payload.quantity
        ):
            event_type = "InventoryReleaseFailed"
            payload = InventoryReleaseFailed(
                sku=envelope.payload.sku,
                quantity=envelope.payload.quantity,
                reason="reservation_not_releasable",
            )
        else:
            stock = await session.scalar(
                select(Stock).where(Stock.sku == reservation.sku).with_for_update()
            )
            if stock is None:
                raise ValueError(f"Stock not found for reserved SKU {reservation.sku}")
            stock.available += reservation.quantity
            reservation.status = "RELEASED"
            event_type = "InventoryReleased"
            payload = InventoryReleased(
                sku=reservation.sku,
                quantity=reservation.quantity,
            )

        enqueue(
            session,
            MessageEnvelope(
                message_id=uuid4(),
                message_type=event_type,
                occurred_at=datetime.now(UTC),
                correlation_id=envelope.correlation_id,
                causation_id=envelope.message_id,
                order_id=envelope.order_id,
                payload=payload,
            ),
            outbox_model=Outbox,
        )
