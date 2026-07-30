"""Transactional Outbox publisher."""

import asyncio
import json
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any, Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


class PublishTransport(Protocol):
    async def publish(self, topic: str, key: bytes, value: bytes) -> None: ...


class OutboxPublisher:
    """Publish pending rows with locks safe for concurrent publisher instances."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        outbox_model: type[Any],
        transport: PublishTransport,
        *,
        batch_size: int = 100,
        poll_seconds: float = 0.5,
        after_publish: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._outbox_model = outbox_model
        self._transport = transport
        self._batch_size = batch_size
        self._poll_seconds = poll_seconds
        self._after_publish = after_publish

    async def publish_batch(self) -> int:
        """Publish one locked batch and mark only broker-acknowledged rows."""

        published = 0
        async with self._session_factory() as session, session.begin():
            rows = list(
                await session.scalars(
                    select(self._outbox_model)
                    .where(self._outbox_model.published_at.is_(None))
                    .order_by(self._outbox_model.created_at)
                    .limit(self._batch_size)
                    .with_for_update(skip_locked=True)
                )
            )
            for row in rows:
                try:
                    await self._transport.publish(
                        row.topic,
                        str(row.message_id).encode(),
                        json.dumps(
                            row.payload_json,
                            separators=(",", ":"),
                            sort_keys=True,
                        ).encode(),
                    )
                except Exception as exc:
                    row.attempts += 1
                    row.last_error = f"{type(exc).__name__}: publish_failed"[:500]
                    continue

                if self._after_publish is not None:
                    await self._after_publish()
                row.published_at = datetime.now(UTC)
                row.last_error = None
                published += 1
        return published

    async def run(self, stop_event: asyncio.Event) -> None:
        """Poll until shutdown without delaying SIGTERM handling."""

        while not stop_event.is_set():
            await self.publish_batch()
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self._poll_seconds)
            except TimeoutError:
                continue
