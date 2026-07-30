"""Idempotent Inbox claim helper."""

from typing import Any
from uuid import UUID, uuid4

from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession


async def claim_inbox(
    session: AsyncSession,
    message_id: UUID,
    consumer: str,
    *,
    inbox_model: type[Any] | None = None,
) -> bool:
    """Claim a message once within the caller's current transaction."""

    model = inbox_model or session.info.get("inbox_model")
    if model is None:
        raise RuntimeError("AsyncSession.info['inbox_model'] is not configured")

    dialect = session.get_bind().dialect.name
    values = {
        "id": uuid4(),
        "message_id": message_id,
        "consumer": consumer,
    }
    if dialect == "postgresql":
        statement = (
            postgres_insert(model)
            .values(**values)
            .on_conflict_do_nothing(index_elements=["message_id"])
            .returning(model.id)
        )
    elif dialect == "sqlite":
        statement = (
            sqlite_insert(model)
            .values(**values)
            .on_conflict_do_nothing(index_elements=["message_id"])
            .returning(model.id)
        )
    else:
        raise RuntimeError(f"Unsupported inbox dialect: {dialect}")

    result = await session.execute(statement)
    return result.scalar_one_or_none() is not None
