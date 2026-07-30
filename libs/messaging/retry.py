"""Bounded technical retries and sanitized DLQ records."""

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from libs.contracts import MessageEnvelope

MessageHandler = Callable[[MessageEnvelope[Any]], Awaitable[None]]
DlqPublisher = Callable[[dict[str, Any]], Awaitable[None]]
Sleep = Callable[[float], Awaitable[None]]


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
            await sleep(delays[attempt_index])
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
