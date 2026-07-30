# Payment System Saga

Event-driven payment workflow with an orchestration-based Saga, compensating
refund, Transactional Outbox, idempotent Inbox, Kafka retry/DLQ and
failure-path tests.

The repository is a reproducible engineering demonstration of a distributed
order workflow. An Order Service orchestrates a simulated Payment Service and
Inventory Service through versioned Kafka commands and events. Every service
owns a PostgreSQL database.

## What it demonstrates

- an explicit Saga state machine with terminal states `CONFIRMED`, `CANCELLED`
  and `MANUAL_REVIEW`;
- atomic domain changes and message creation through Transactional Outbox;
- safe at-least-once consumption through an Inbox unique on `message_id`;
- bounded technical retries with limited jitter, sanitized DLQ records and
  manual Kafka offset commits;
- concurrency-safe HTTP idempotency for order creation;
- recovery after poison records, a consumer restart and duplicate Kafka
  delivery;
- async Python services, Alembic migrations, JSON stdout logs, readiness
  checks and graceful shutdown;
- multi-stage, non-root Docker images.

## Quick start

Requirements: Docker with the Compose plugin. Host ports `18000`–`18002`,
`19092` and `55439` must be available.

```bash
docker compose up -d --build --wait
curl --fail http://localhost:18000/health/ready
```

Create an in-stock order:

```bash
curl --request POST http://localhost:18000/orders \
  --header 'Content-Type: application/json' \
  --header 'Idempotency-Key: demo-in-stock-1' \
  --data '{
    "sku": "IN-STOCK",
    "quantity": 2,
    "amount_minor": 12500,
    "currency": "RUB"
  }'
```

The response contains `order_id`. Poll it until `status` becomes `CONFIRMED`:

```bash
curl http://localhost:18000/orders/<order_id>
```

Create the deterministic compensation scenario:

```bash
curl --request POST http://localhost:18000/orders \
  --header 'Content-Type: application/json' \
  --header 'Idempotency-Key: demo-out-of-stock-1' \
  --data '{
    "sku": "OUT-OF-STOCK",
    "quantity": 1,
    "amount_minor": 9900,
    "currency": "RUB"
  }'
```

This order progresses through `REFUND_PENDING` to `CANCELLED`; its simulated
payment history is `AUTHORIZED -> REFUNDED`.

Reusing an idempotency key with the same body returns the same order with HTTP
`200`. Reusing it with a different body returns HTTP `409`.

Stop the local stack and remove only this Compose project's data:

```bash
docker compose down -v
```

## Workflow

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant O as Order Service
    participant K as Kafka
    participant P as Payment Service
    participant I as Inventory Service

    Client->>O: POST /orders + Idempotency-Key
    O->>O: order + saga + AuthorizePayment outbox
    O->>K: AuthorizePayment
    K->>P: AuthorizePayment
    P->>P: payment + PaymentAuthorized outbox
    P->>K: PaymentAuthorized
    K->>O: PaymentAuthorized
    O->>O: INVENTORY_PENDING + ReserveInventory outbox
    O->>K: ReserveInventory
    K->>I: ReserveInventory
    alt stock is available
        I->>K: InventoryReserved
        K->>O: InventoryReserved
        O->>O: CONFIRMED
    else stock is unavailable
        I->>K: InventoryRejected
        K->>O: InventoryRejected
        O->>O: REFUND_PENDING + RefundPayment outbox
        O->>K: RefundPayment
        K->>P: RefundPayment
        P->>K: PaymentRefunded or PaymentRefundFailed
        K->>O: refund result
        O->>O: CANCELLED or MANUAL_REVIEW
    end
```

More detail is in [Architecture](docs/architecture.md) and
[Event contracts](docs/events.md).

## Expected scenarios

| Scenario | Input or action | Expected result |
|---|---|---|
| Happy path | `sku=IN-STOCK` | one authorization, one reservation, `CONFIRMED` |
| Compensation | `sku=OUT-OF-STOCK` | authorization, rejection, refund, `CANCELLED` |
| Technical refund failure | `sku=OUT-OF-STOCK`, `amount_minor=7777` | retries and DLQ, durable `PaymentRefundFailed`, `MANUAL_REVIEW` |
| Recovery/idempotency | poison record, duplicate HTTP/Kafka delivery or inventory consumer restart | consumer continues; one effective side effect; Kafka-retained command resumes processing |

The E2E suite checks both API status and the three service databases.

## Local endpoints

| Component | Host endpoint |
|---|---|
| Order API and OpenAPI | `http://localhost:18000`, `/docs` |
| Payment health | `http://localhost:18001/health/ready` |
| Inventory health | `http://localhost:18002/health/ready` |
| PostgreSQL | `localhost:55439` |
| Kafka external listener | `localhost:19092` |

Services use Kafka's internal listener at `kafka:9092`; host-side tests use
the external listener at `localhost:19092`. Service container ports remain
`8000`, `8001` and `8002`.

## Validation

Create a Python 3.11+ environment:

```bash
python3.11 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install --editable '.[dev]'
```

Run static checks:

```bash
.venv/bin/ruff check .
.venv/bin/mypy libs services
```

Run unit and PostgreSQL integration tests with branch coverage:

```bash
docker compose up -d --wait postgres kafka
TEST_DATABASE_URL=postgresql+asyncpg://saga:saga-local-only@127.0.0.1:55439/postgres \
TEST_KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19092 \
  .venv/bin/pytest tests/unit tests/integration \
  --cov=libs --cov=services --cov-report=term-missing --cov-fail-under=80
```

Run all containerized failure-path tests, including consumer restart:

```bash
docker compose up -d --build --wait
E2E_ORDER_BASE_URL=http://127.0.0.1:18000 \
E2E_KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19092 \
E2E_RUN_RECOVERY=1 \
  .venv/bin/pytest tests/e2e -q
docker compose down -v
```

GitHub Actions runs the same lint, typing, coverage, PostgreSQL and Kafka E2E
checks on Python 3.11.

## Delivery and failure semantics

Delivery is **at least once**, not exactly once. A publisher can crash after a
Kafka broker acknowledgement but before setting `published_at`, so the same
`message_id` may be published again. Inbox claims and business uniqueness
constraints make that replay safe.

Offsets are committed only after a handler transaction succeeds or after a
terminal technical failure is published to the DLQ. Malformed envelopes and
business-message validation failures also go to the DLQ without terminating
the consumer loop. Domain failures are explicit events (`PaymentRejected`,
`InventoryRejected`, `PaymentRefundFailed`) rather than retryable exceptions.
Retry attempts, positive delays and Outbox timing/batch limits are validated
before a service starts. Each configured delay receives up to 20% additive
jitter, capped at one second.

When technical retries for `RefundPayment` are exhausted, Payment first
publishes the sanitized DLQ record and then a deterministic, broker-acknowledged
`PaymentRefundFailed` event before committing the command offset. The event
moves the Saga to `MANUAL_REVIEW`; replay is safe because it uses a stable
`message_id`. Other exhausted technical failures remain in `saga.dlq.v1`.
This demonstration does not include a DLQ operator or automatic replay.

## Scope and limitations

- Payment authorization/refund is a deterministic in-process simulation.
- Monetary values are integer minor units; no card numbers or card tokens are
  accepted or stored.
- There are no external acquirer credentials.
- This project does not claim PCI DSS compliance, exactly-once delivery or
  production use.
- Local credentials in `.env.example` and Compose are development-only.
- The historical, non-runtime implementation is isolated in
  `attic/legacy-v1`.

Licensed under the MIT License.
