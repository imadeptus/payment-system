"""Inventory Service persistence models."""

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, Uuid
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from libs.messaging import make_inbox_model, make_outbox_model
from libs.messaging.models import utc_now


class Base(DeclarativeBase):
    pass


class Stock(Base):
    __tablename__ = "stock"

    sku: Mapped[str] = mapped_column(String(200), primary_key=True)
    available: Mapped[int] = mapped_column(Integer, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        onupdate=utc_now,
    )


class Reservation(Base):
    __tablename__ = "reservations"
    __table_args__ = (Index("ix_reservations_status_updated_at", "status", "updated_at"),)

    id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4)
    order_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        nullable=False,
        unique=True,
        index=True,
    )
    sku: Mapped[str] = mapped_column(
        String(200),
        ForeignKey("stock.sku"),
        nullable=False,
        index=True,
    )
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
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


Inbox = make_inbox_model(Base, service="inventory")
Outbox = make_outbox_model(Base, service="inventory")
