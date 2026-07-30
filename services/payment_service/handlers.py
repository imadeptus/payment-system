"""Transactional payment command handlers."""

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from libs.contracts import (
    AuthorizePayment,
    MessageEnvelope,
    PaymentAuthorized,
    PaymentRefunded,
    PaymentRefundFailed,
    PaymentRejected,
    RefundPayment,
)
from libs.messaging import claim_inbox, enqueue
from services.payment_service.models import Inbox, Outbox, Payment
from services.payment_service.provider import PaymentProvider


async def handle_authorize(
    session: AsyncSession,
    envelope: MessageEnvelope[AuthorizePayment],
    provider: PaymentProvider,
) -> None:
    """Authorize once and atomically persist Inbox, Payment and Outbox."""

    if envelope.message_type != "AuthorizePayment":
        raise ValueError(f"Unexpected message type: {envelope.message_type}")

    async with session.begin():
        claimed = await claim_inbox(
            session,
            envelope.message_id,
            "payment-authorize",
            inbox_model=Inbox,
        )
        if not claimed:
            return
        existing = await session.scalar(
            select(Payment).where(Payment.order_id == envelope.order_id).with_for_update()
        )
        if existing is not None:
            return

        result = provider.authorize(
            envelope.order_id,
            envelope.payload.amount_minor,
            envelope.payload.currency,
        )
        status = "AUTHORIZED" if result.approved else "REJECTED"
        session.add(
            Payment(
                order_id=envelope.order_id,
                amount_minor=envelope.payload.amount_minor,
                currency=envelope.payload.currency,
                status=status,
                provider_reference=(
                    f"sim-{envelope.order_id}" if result.approved else None
                ),
                history=[status],
            )
        )
        payload: PaymentAuthorized | PaymentRejected
        if result.approved:
            event_type = "PaymentAuthorized"
            payload = PaymentAuthorized(
                amount_minor=envelope.payload.amount_minor,
                currency=envelope.payload.currency,
            )
        else:
            event_type = "PaymentRejected"
            payload = PaymentRejected(
                amount_minor=envelope.payload.amount_minor,
                currency=envelope.payload.currency,
                reason=result.reason or "provider_rejected",
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


async def handle_refund(
    session: AsyncSession,
    envelope: MessageEnvelope[RefundPayment],
    provider: PaymentProvider,
) -> None:
    """Refund an authorized payment once inside the Inbox transaction."""

    if envelope.message_type != "RefundPayment":
        raise ValueError(f"Unexpected message type: {envelope.message_type}")

    async with session.begin():
        claimed = await claim_inbox(
            session,
            envelope.message_id,
            "payment-refund",
            inbox_model=Inbox,
        )
        if not claimed:
            return
        payment = await session.scalar(
            select(Payment).where(Payment.order_id == envelope.order_id).with_for_update()
        )
        if payment is None:
            raise ValueError(f"Payment not found for order {envelope.order_id}")
        if payment.status in {"REFUNDED", "REFUND_FAILED"}:
            return

        result = provider.refund(envelope.order_id, envelope.payload.amount_minor)
        payment.status = "REFUNDED" if result.approved else "REFUND_FAILED"
        payment.history = [*payment.history, payment.status]
        payload: PaymentRefunded | PaymentRefundFailed
        if result.approved:
            event_type = "PaymentRefunded"
            payload = PaymentRefunded(
                amount_minor=payment.amount_minor,
                currency=payment.currency,
            )
        else:
            event_type = "PaymentRefundFailed"
            payload = PaymentRefundFailed(
                amount_minor=payment.amount_minor,
                currency=payment.currency,
                reason=result.reason or "provider_refund_failed",
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
