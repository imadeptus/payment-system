"""Bounded technical retries and sanitized DLQ records."""

import asyncio
import random
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from libs.contracts import MessageEnvelope

MessageHandler = Callable[[MessageEnvelope[Any]], Awaitable[None]]
DlqPublisher = Callable[[dict[str, Any]], Awaitable[None]]
Sleep = Callable[[float], Awaitable[None]]
RandomFraction = Callable[[], float]

JITTER_RATIO = 0.2
JITTER_CAP_SECONDS = 1.0


class TransientMessageError(RuntimeError):
    """Potentially recoverable technical processing failure."""


class BusinessMessageError(RuntimeError):
    """Expected domain rejection that must not be retried."""


async def consume_with_retry(
    envelope: MessageEnvelope[Any],
    handler: MessageHandler,
    dlq: DlqPublisher,
    *,
    attempts: int = 3,
    delays: Sequence[float] = (1.0, 2.0, 4.0),
    sleep: Sleep = asyncio.sleep,
    random_fraction: RandomFraction = random.random,
) -> None:
    """Retry transient failures and publish a sanitized terminal DLQ record."""

    if attempts <= 0:
        raise ValueError("attempts must be positive")
    if len(delays) < attempts:
        raise ValueError("one delay is required for every attempt")

    for attempt_index in range(attempts):
        try:
            await handler(envelope)
        except BusinessMessageError:
            raise
        except TransientMessageError as exc:
            base_delay = delays[attempt_index]
            fraction = min(max(random_fraction(), 0.0), 1.0)
            jitter_limit = min(base_delay * JITTER_RATIO, JITTER_CAP_SECONDS)
            await sleep(base_delay + jitter_limit * fraction)
            terminal_error = exc
        else:
            return

    await dlq(
        {
            "original_message": envelope.model_dump(mode="json"),
            "reason": type(terminal_error).__name__,
            "attempts": attempts,
            "correlation_id": str(envelope.correlation_id),
            "causation_id": (
                str(envelope.causation_id) if envelope.causation_id is not None else None
            ),
        }
    )
