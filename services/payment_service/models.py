"""Payment Service persistence models."""

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import JSON, DateTime, Index, Integer, String, Uuid
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from libs.messaging import make_inbox_model, make_outbox_model
from libs.messaging.models import utc_now


class Base(DeclarativeBase):
    pass


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (Index("ix_payments_status_updated_at", "status", "updated_at"),)

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    order_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        nullable=False,
        unique=True,
        index=True,
    )
    amount_minor: Mapped[int] = mapped_column(Integer, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    provider_reference: Mapped[str | None] = mapped_column(String(100), nullable=True)
    history: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        onupdate=utc_now,
    )


Inbox = make_inbox_model(Base, service="payment")
Outbox = make_outbox_model(Base, service="payment")
