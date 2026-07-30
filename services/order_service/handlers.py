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
from libs.messaging.retry import BusinessMessageError
from services.order_service.models import Inbox, Order, Outbox, Saga
from services.order_service.state_machine import (
    InvalidSagaTransition,
    SagaState,
    transition,
)

PAYMENT_EVENTS = {
    "PaymentAuthorized",
    "PaymentRejected",
    "PaymentRefunded",
    "PaymentRefundFailed",
}
INVENTORY_EVENTS = {
    "InventoryReserved",
    "InventoryRejected",
    "InventoryReleased",
    "InventoryReleaseFailed",
}


def validate_event_matches_order(
    order: Order,
    envelope: MessageEnvelope[Any],
) -> None:
    """Reject a validly shaped event that belongs to different business data."""

    payload = envelope.payload
    if envelope.message_type in PAYMENT_EVENTS and (
        getattr(payload, "amount_minor", None) != order.amount_minor
        or getattr(payload, "currency", None) != order.currency
    ):
        raise BusinessMessageError("Payment event does not match Order")
    if envelope.message_type in INVENTORY_EVENTS and (
        getattr(payload, "sku", None) != order.sku
        or getattr(payload, "quantity", None) != order.quantity
    ):
        raise BusinessMessageError("Inventory event does not match Order")


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
            raise BusinessMessageError(
                f"Order or Saga not found: {envelope.order_id}"
            )
        if saga.id != envelope.correlation_id:
            raise BusinessMessageError("Event correlation_id does not match Saga")
        validate_event_matches_order(order, envelope)

        current = SagaState(saga.state)
        try:
            result = transition(current, envelope.message_type)
        except InvalidSagaTransition as exc:
            raise BusinessMessageError(str(exc)) from exc
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
