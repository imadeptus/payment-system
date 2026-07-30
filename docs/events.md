# Event contracts

Status: realised

## Envelope

All Kafka values are UTF-8 JSON and validate against an immutable version-one
envelope:

```json
{
  "message_id": "8e7d2077-ffdd-4af2-8fa1-0a2ce0d553ee",
  "message_type": "AuthorizePayment",
  "schema_version": 1,
  "occurred_at": "2026-07-30T08:00:00Z",
  "correlation_id": "8f2561cb-9224-4d31-a692-c1514e9fa5e7",
  "causation_id": null,
  "order_id": "c48ab694-f199-4e3b-b6a6-e0377bc1632e",
  "payload": {
    "amount_minor": 12500,
    "currency": "RUB"
  }
}
```

- `message_id` is the Kafka key and Inbox deduplication identifier.
- `correlation_id` is the Saga identifier.
- `causation_id` references the command/event that caused this message.
- `order_id` is the business aggregate identifier.
- `occurred_at` must include a timezone.
- `schema_version` is currently the literal integer `1`.

Required fields and constraints are checked by Pydantic contract validation.
Money is a positive integer in minor units, and currency is a three-letter
uppercase code.

## Topics and message types

| Topic | Producers | Consumers | Types |
|---|---|---|---|
| `payments.commands.v1` | Order | Payment | `AuthorizePayment`, `RefundPayment` |
| `payments.events.v1` | Payment | Order | `PaymentAuthorized`, `PaymentRejected`, `PaymentRefunded`, `PaymentRefundFailed` |
| `inventory.commands.v1` | Order | Inventory | `ReserveInventory`, `ReleaseInventory` |
| `inventory.events.v1` | Inventory | Order | `InventoryReserved`, `InventoryRejected`, `InventoryReleased`, `InventoryReleaseFailed` |
| `saga.dlq.v1` | all consumers | operator tooling (not included) | sanitized terminal failure records |

The physical topic names can be overridden with environment variables while
message routing remains stable by `message_type`.

## Payloads

| Message type | Payload |
|---|---|
| `AuthorizePayment` | `amount_minor: int > 0`, `currency: string` |
| `RefundPayment` | `amount_minor: int > 0`, `currency: string` |
| `PaymentAuthorized` | `amount_minor`, `currency` |
| `PaymentRejected` | `amount_minor`, `currency`, `reason` |
| `PaymentRefunded` | `amount_minor`, `currency` |
| `PaymentRefundFailed` | `amount_minor`, `currency`, `reason` |
| `ReserveInventory` | `sku: non-empty string`, `quantity: int > 0` |
| `ReleaseInventory` | `sku`, `quantity` |
| `InventoryReserved` | `sku`, `quantity` |
| `InventoryRejected` | `sku`, `quantity`, `reason` |
| `InventoryReleased` | `sku`, `quantity` |
| `InventoryReleaseFailed` | `sku`, `quantity`, `reason` |

Domain rejections use explicit events and are not sent to the technical DLQ.
Messages whose envelope or aggregate fields fail validation are poison records
and are sent to the DLQ without stopping the consumer.

## Compatibility rules

- Topic and schema major versions are explicit (`.v1`, `schema_version=1`).
- Producers must not mutate an already-persisted Outbox envelope.
- Consumers must reject unsupported `message_type` values.
- Breaking field changes require a new schema/topic version or a compatibility
  migration.
- Redelivery of the same `message_id` is valid and expected.

## DLQ record

After configured transient retries are exhausted, or after a poison record is
detected, the consumer publishes:

```json
{
  "original_message": {},
  "reason": "TransientMessageError",
  "attempts": 3,
  "correlation_id": "8f2561cb-9224-4d31-a692-c1514e9fa5e7",
  "causation_id": "8e7d2077-ffdd-4af2-8fa1-0a2ce0d553ee"
}
```

`original_message` is the validated envelope. `reason` is a sanitized exception
class rather than a stack trace or secret-bearing exception string. The
consumer commits its Kafka offset only after this DLQ publish succeeds.

For an exhausted `RefundPayment`, Payment additionally publishes a
deterministic `PaymentRefundFailed` event with the same Saga identifiers. Its
broker acknowledgement is required before the command offset is committed, so
Order can durably transition the Saga to `MANUAL_REVIEW`.
