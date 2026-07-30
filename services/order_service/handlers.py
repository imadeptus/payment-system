"""Transactional handlers for payment and inventory events."""

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from libs.contracts import (
    MessageEnvelope,
    RefundPayment,
    ReserveInventory,
)
from libs.messaging import claim_inbox, enqueue
from services.order_service.models import Inbox, Order, Outbox, Saga
from services.order_service.state_machine import SagaState, transition


async def handle_saga_event(
    session: AsyncSession,
    envelope: MessageEnvelope[Any],
) -> None:
    """Advance one Saga event and atomically enqueue its next command."""

    async with session.begin():
        claimed = await claim_inbox(
            session,
            envelope.message_id,
            "order-saga-events",
            inbox_model=Inbox,
        )
        if not claimed:
            return
        saga = await session.scalar(
            select(Saga).where(Saga.order_id == envelope.order_id).with_for_update()
        )
        order = await session.scalar(
            select(Order).where(Order.id == envelope.order_id).with_for_update()
        )
        if saga is None or order is None:
            raise ValueError(f"Order or Saga not found: {envelope.order_id}")
        if saga.id != envelope.correlation_id:
            raise ValueError("Event correlation_id does not match Saga")

        current = SagaState(saga.state)
        result = transition(current, envelope.message_type)
        saga.state = result.next_state.value
        saga.history = [
            *saga.history,
            {
                "event_type": envelope.message_type,
                "from": current.value,
                "to": result.next_state.value,
            },
        ]
        order.status = result.next_state.value

        if result.command_type is None:
            return
        if result.command_type == "ReserveInventory":
            payload: ReserveInventory | RefundPayment = ReserveInventory(
                sku=order.sku,
                quantity=order.quantity,
            )
        elif result.command_type == "RefundPayment":
            payload = RefundPayment(
                amount_minor=order.amount_minor,
                currency=order.currency,
            )
        else:
            raise ValueError(f"Unsupported Saga command: {result.command_type}")

        enqueue(
            session,
            MessageEnvelope(
                message_id=uuid4(),
                message_type=result.command_type,
                occurred_at=datetime.now(UTC),
                correlation_id=saga.id,
                causation_id=envelope.message_id,
                order_id=order.id,
                payload=payload,
            ),
            outbox_model=Outbox,
        )
