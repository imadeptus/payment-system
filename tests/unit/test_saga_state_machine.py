import itertools

import pytest

from services.order_service.state_machine import (
    InvalidSagaTransition,
    SagaState,
    transition,
)

EVENT_TYPES = (
    "OrderCreated",
    "PaymentAuthorized",
    "PaymentRejected",
    "InventoryReserved",
    "InventoryRejected",
    "PaymentRefunded",
    "PaymentRefundFailed",
)

ALLOWED = {
    (SagaState.PENDING, "OrderCreated"): (SagaState.PAYMENT_PENDING, "AuthorizePayment"),
    (SagaState.PAYMENT_PENDING, "PaymentAuthorized"): (
        SagaState.INVENTORY_PENDING,
        "ReserveInventory",
    ),
    (SagaState.PAYMENT_PENDING, "PaymentRejected"): (SagaState.CANCELLED, None),
    (SagaState.INVENTORY_PENDING, "InventoryReserved"): (SagaState.CONFIRMED, None),
    (SagaState.INVENTORY_PENDING, "InventoryRejected"): (
        SagaState.REFUND_PENDING,
        "RefundPayment",
    ),
    (SagaState.REFUND_PENDING, "PaymentRefunded"): (SagaState.CANCELLED, None),
    (SagaState.REFUND_PENDING, "PaymentRefundFailed"): (SagaState.MANUAL_REVIEW, None),
}


@pytest.mark.parametrize(
    ("current", "event_type", "next_state", "command_type"),
    [
        (current, event_type, expected[0], expected[1])
        for (current, event_type), expected in ALLOWED.items()
    ],
)
def test_allowed_transition_returns_next_state_and_command(
    current: SagaState,
    event_type: str,
    next_state: SagaState,
    command_type: str | None,
) -> None:
    result = transition(current, event_type)

    assert result.next_state is next_state
    assert result.command_type == command_type


@pytest.mark.parametrize(
    ("current", "event_type"),
    [
        pair
        for pair in itertools.product(SagaState, EVENT_TYPES)
        if pair not in ALLOWED
    ],
)
def test_every_unlisted_transition_is_rejected(
    current: SagaState,
    event_type: str,
) -> None:
    with pytest.raises(InvalidSagaTransition, match=f"{current.value}.*{event_type}"):
        transition(current, event_type)
