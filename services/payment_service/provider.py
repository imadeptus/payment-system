"""Deterministic payment-provider simulator with no card data."""

from dataclasses import dataclass
from uuid import UUID


@dataclass(frozen=True, slots=True)
class AuthorizationResult:
    approved: bool
    reason: str | None


@dataclass(frozen=True, slots=True)
class RefundResult:
    approved: bool
    reason: str | None


class PaymentProvider:
    """Pure simulator controlled by explicit order-id sentinels."""

    def __init__(
        self,
        *,
        reject_order_ids: set[UUID] | None = None,
        refund_failure_order_ids: set[UUID] | None = None,
    ) -> None:
        self._reject_order_ids = reject_order_ids or set()
        self._refund_failure_order_ids = refund_failure_order_ids or set()

    def authorize(
        self,
        order_id: UUID,
        amount_minor: int,
        currency: str,
    ) -> AuthorizationResult:
        del amount_minor, currency
        if order_id in self._reject_order_ids:
            return AuthorizationResult(approved=False, reason="provider_rejected")
        return AuthorizationResult(approved=True, reason=None)

    def refund(self, order_id: UUID, amount_minor: int) -> RefundResult:
        del amount_minor
        if order_id in self._refund_failure_order_ids:
            return RefundResult(approved=False, reason="provider_refund_failed")
        return RefundResult(approved=True, reason=None)
