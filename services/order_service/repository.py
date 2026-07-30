"""Transactional repository operations for Order Service."""

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from libs.contracts import AuthorizePayment, MessageEnvelope
from libs.messaging import enqueue
from services.order_service.models import IdempotencyRecord, Order, Outbox, Saga
from services.order_service.state_machine import SagaState, transition


@dataclass(frozen=True, slots=True)
class NewOrder:
    sku: str
    quantity: int
    amount_minor: int
    currency: str


class IdempotencyConflict(ValueError):
    """An idempotency key was reused for a different request."""


def request_hash(order: NewOrder) -> str:
    canonical = json.dumps(
        {
            "amount_minor": order.amount_minor,
            "currency": order.currency,
            "quantity": order.quantity,
            "sku": order.sku,
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


async def create_order(
    session: AsyncSession,
    order_data: NewOrder,
    idempotency_key: str,
) -> tuple[Order, bool]:
    """Create order, Saga and first command atomically, or return the replay."""

    digest = request_hash(order_data)
    async with session.begin():
        existing = await session.get(IdempotencyRecord, idempotency_key)
        if existing is not None:
            if existing.request_hash != digest:
                raise IdempotencyConflict("Idempotency key payload does not match")
            order = await session.get(Order, existing.order_id)
            if order is None:
                raise RuntimeError("Idempotency record references a missing order")
            return order, False

        order_id = uuid4()
        saga_id = uuid4()
        initial = transition(SagaState.PENDING, "OrderCreated")
        order = Order(
            id=order_id,
            sku=order_data.sku,
            quantity=order_data.quantity,
            amount_minor=order_data.amount_minor,
            currency=order_data.currency,
            status=initial.next_state.value,
        )
        saga = Saga(
            id=saga_id,
            order=order,
            state=initial.next_state.value,
            history=[
                {
                    "event_type": "OrderCreated",
                    "from": SagaState.PENDING.value,
                    "to": initial.next_state.value,
                }
            ],
        )
        session.add_all(
            [
                order,
                saga,
                IdempotencyRecord(
                    key=idempotency_key,
                    request_hash=digest,
                    order=order,
                ),
            ]
        )
        enqueue(
            session,
            MessageEnvelope[AuthorizePayment](
                message_id=uuid4(),
                message_type="AuthorizePayment",
                occurred_at=datetime.now(UTC),
                correlation_id=saga_id,
                causation_id=None,
                order_id=order_id,
                payload=AuthorizePayment(
                    amount_minor=order_data.amount_minor,
                    currency=order_data.currency,
                ),
            ),
            outbox_model=Outbox,
        )
    return order, True


async def get_order(session: AsyncSession, order_id: UUID) -> Order | None:
    return await session.get(Order, order_id)
