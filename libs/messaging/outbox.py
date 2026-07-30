"""Transactional Outbox write helper."""

from typing import Any

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from libs.contracts import MessageEnvelope, topic_for


def enqueue(
    session: AsyncSession,
    envelope: MessageEnvelope[BaseModel],
    *,
    outbox_model: type[Any] | None = None,
) -> None:
    """Stage an envelope in the caller's current domain transaction."""

    model = outbox_model or session.info.get("outbox_model")
    if model is None:
        raise RuntimeError("AsyncSession.info['outbox_model'] is not configured")

    session.add(
        model(
            message_id=envelope.message_id,
            topic=topic_for(envelope.message_type),
            payload_json=envelope.model_dump(mode="json"),
        )
    )
