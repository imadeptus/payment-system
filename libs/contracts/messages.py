"""Immutable command and event payload contracts."""

from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

PositiveInt = Annotated[int, Field(strict=True, gt=0)]
Currency = Annotated[str, Field(pattern=r"^[A-Z]{3}$")]
NonEmptyText = Annotated[str, Field(min_length=1, max_length=200)]


class ContractPayload(BaseModel):
    """Base payload that prevents consumer-side mutation."""

    model_config = ConfigDict(frozen=True)


class AuthorizePayment(ContractPayload):
    amount_minor: PositiveInt
    currency: Currency


class RefundPayment(ContractPayload):
    amount_minor: PositiveInt
    currency: Currency


class ReserveInventory(ContractPayload):
    sku: NonEmptyText
    quantity: PositiveInt


class ReleaseInventory(ContractPayload):
    sku: NonEmptyText
    quantity: PositiveInt


class PaymentAuthorized(ContractPayload):
    amount_minor: PositiveInt
    currency: Currency


class PaymentRejected(ContractPayload):
    amount_minor: PositiveInt
    currency: Currency
    reason: NonEmptyText


class PaymentRefunded(ContractPayload):
    amount_minor: PositiveInt
    currency: Currency


class PaymentRefundFailed(ContractPayload):
    amount_minor: PositiveInt
    currency: Currency
    reason: NonEmptyText


class InventoryReserved(ContractPayload):
    sku: NonEmptyText
    quantity: PositiveInt


class InventoryRejected(ContractPayload):
    sku: NonEmptyText
    quantity: PositiveInt
    reason: NonEmptyText


class InventoryReleased(ContractPayload):
    sku: NonEmptyText
    quantity: PositiveInt


class InventoryReleaseFailed(ContractPayload):
    sku: NonEmptyText
    quantity: PositiveInt
    reason: NonEmptyText
