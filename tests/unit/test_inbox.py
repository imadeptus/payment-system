from uuid import UUID

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from libs.messaging import claim_inbox, make_inbox_model


class Base(DeclarativeBase):
    pass


Inbox = make_inbox_model(Base, service="unit")


@pytest.mark.asyncio
async def test_duplicate_message_is_claimed_only_once() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    message_id = UUID("00000000-0000-0000-0000-000000000001")
    async with session_factory() as session:
        session.info["inbox_model"] = Inbox
        first = await claim_inbox(session, message_id, consumer="payment-authorize")
        await session.commit()
        second = await claim_inbox(session, message_id, consumer="payment-authorize")
        await session.commit()

    assert first is True
    assert second is False
    await engine.dispose()
