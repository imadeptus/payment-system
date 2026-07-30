"""Pure orchestration-based Saga state machine."""

from dataclasses import dataclass
from enum import StrEnum


class SagaState(StrEnum):
    PENDING = "PENDING"
    PAYMENT_PENDING = "PAYMENT_PENDING"
    INVENTORY_PENDING = "INVENTORY_PENDING"
    CONFIRMED = "CONFIRMED"
    REFUND_PENDING = "REFUND_PENDING"
    CANCELLED = "CANCELLED"
    MANUAL_REVIEW = "MANUAL_REVIEW"


@dataclass(frozen=True, slots=True)
class Transition:
    next_state: SagaState
    command_type: str | None = None


class InvalidSagaTransition(ValueError):
    """Raised when an event cannot be applied to the current Saga state."""


_TRANSITIONS = {
    (SagaState.PENDING, "OrderCreated"): Transition(
        SagaState.PAYMENT_PENDING,
        "AuthorizePayment",
    ),
    (SagaState.PAYMENT_PENDING, "PaymentAuthorized"): Transition(
        SagaState.INVENTORY_PENDING,
        "ReserveInventory",
    ),
    (SagaState.PAYMENT_PENDING, "PaymentRejected"): Transition(SagaState.CANCELLED),
    (SagaState.INVENTORY_PENDING, "InventoryReserved"): Transition(SagaState.CONFIRMED),
    (SagaState.INVENTORY_PENDING, "InventoryRejected"): Transition(
        SagaState.REFUND_PENDING,
        "RefundPayment",
    ),
    (SagaState.REFUND_PENDING, "PaymentRefunded"): Transition(SagaState.CANCELLED),
    (SagaState.REFUND_PENDING, "PaymentRefundFailed"): Transition(SagaState.MANUAL_REVIEW),
}


def transition(state: SagaState, event_type: str) -> Transition:
    """Apply one domain event without performing I/O."""

    try:
        return _TRANSITIONS[(state, event_type)]
    except KeyError as exc:
        raise InvalidSagaTransition(
            f"Invalid Saga transition: {state.value} + {event_type}"
        ) from exc
