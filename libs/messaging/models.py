"""Declarative table factories for service-owned inbox and outbox tables."""

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import JSON, DateTime, Integer, String, Text, UniqueConstraint, Uuid
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, mapped_column


def utc_now() -> datetime:
    return datetime.now(UTC)


def make_outbox_model(base: type[DeclarativeBase], *, service: str) -> type[Any]:
    """Create an Outbox ORM model bound to a service-specific metadata."""

    return type(
        f"{service.title().replace('_', '')}Outbox",
        (base,),
        {
            "__tablename__": "outbox",
            "__table_args__": (
                UniqueConstraint(
                    "message_id",
                    name=f"uq_{service}_outbox_message_id",
                ),
            ),
            "id": mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4),
            "message_id": mapped_column(Uuid(as_uuid=True), nullable=False),
            "topic": mapped_column(String(200), nullable=False),
            "payload_json": mapped_column(
                JSONB().with_variant(JSON(), "sqlite"),
                nullable=False,
            ),
            "created_at": mapped_column(
                DateTime(timezone=True),
                nullable=False,
                default=utc_now,
                index=True,
            ),
            "published_at": mapped_column(DateTime(timezone=True), nullable=True, index=True),
            "attempts": mapped_column(Integer, nullable=False, default=0),
            "last_error": mapped_column(Text, nullable=True),
        },
    )


def make_inbox_model(base: type[DeclarativeBase], *, service: str) -> type[Any]:
    """Create an Inbox ORM model bound to a service-specific metadata."""

    return type(
        f"{service.title().replace('_', '')}Inbox",
        (base,),
        {
            "__tablename__": "inbox",
            "__table_args__": (
                UniqueConstraint(
                    "message_id",
                    name=f"uq_{service}_inbox_message_id",
                ),
            ),
            "id": mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid4),
            "message_id": mapped_column(Uuid(as_uuid=True), nullable=False),
            "consumer": mapped_column(String(200), nullable=False),
            "processed_at": mapped_column(
                DateTime(timezone=True),
                nullable=False,
                default=utc_now,
            ),
        },
    )
