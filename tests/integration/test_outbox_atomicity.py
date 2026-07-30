from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from sqlalchemy import String, func, select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from libs.contracts import AuthorizePayment, MessageEnvelope
from libs.messaging import enqueue, make_inbox_model, make_outbox_model


class Base(DeclarativeBase):
    pass


class DomainRow(Base):
    __tablename__ = "atomicity_domain_rows"

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    value: Mapped[str] = mapped_column(String(50))


Inbox = make_inbox_model(Base, service="atomicity")
Outbox = make_outbox_model(Base, service="atomicity")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_domain_change_and_outbox_roll_back_together(
    postgres_engine: AsyncEngine,
) -> None:
    async with postgres_engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(postgres_engine, expire_on_commit=False)
    envelope = MessageEnvelope[AuthorizePayment](
        message_id=UUID("00000000-0000-0000-0000-000000000011"),
        message_type="AuthorizePayment",
        occurred_at=datetime(2026, 7, 30, 8, 0, tzinfo=UTC),
        correlation_id=UUID("00000000-0000-0000-0000-000000000012"),
        causation_id=None,
        order_id=UUID("00000000-0000-0000-0000-000000000013"),
        payload=AuthorizePayment(amount_minor=500, currency="RUB"),
    )

    with pytest.raises(RuntimeError, match="sentinel"):
        async with session_factory() as session, session.begin():
            session.info["outbox_model"] = Outbox
            session.add(DomainRow(value="must-rollback"))
            enqueue(session, envelope)
            raise RuntimeError("sentinel")

    async with session_factory() as session:
        domain_count = await session.scalar(select(func.count()).select_from(DomainRow))
        outbox_count = await session.scalar(select(func.count()).select_from(Outbox))

    assert domain_count == 0
    assert outbox_count == 0
