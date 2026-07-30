"""Create and seed Inventory Service schema.

Revision ID: 0001_inventory_init
Revises:
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_inventory_init"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    stock = op.create_table(
        "stock",
        sa.Column("sku", sa.String(length=200), nullable=False),
        sa.Column("available", sa.Integer(), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("sku"),
        sa.CheckConstraint("available >= 0", name="ck_stock_available_non_negative"),
    )
    op.bulk_insert(
        stock,
        [
            {"sku": "IN-STOCK", "available": 100},
            {"sku": "OUT-OF-STOCK", "available": 0},
        ],
    )

    op.create_table(
        "reservations",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("order_id", sa.Uuid(), nullable=False),
        sa.Column("sku", sa.String(length=200), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["sku"], ["stock.sku"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("order_id"),
        sa.CheckConstraint("quantity > 0", name="ck_reservations_quantity_positive"),
    )
    op.create_index("ix_reservations_order_id", "reservations", ["order_id"])
    op.create_index("ix_reservations_sku", "reservations", ["sku"])
    op.create_index("ix_reservations_status", "reservations", ["status"])
    op.create_index(
        "ix_reservations_status_updated_at",
        "reservations",
        ["status", "updated_at"],
    )

    op.create_table(
        "inbox",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("message_id", sa.Uuid(), nullable=False),
        sa.Column("consumer", sa.String(length=200), nullable=False),
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("message_id", name="uq_inventory_inbox_message_id"),
    )

    op.create_table(
        "outbox",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("message_id", sa.Uuid(), nullable=False),
        sa.Column("topic", sa.String(length=200), nullable=False),
        sa.Column("payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("message_id", name="uq_inventory_outbox_message_id"),
    )
    op.create_index("ix_outbox_created_at", "outbox", ["created_at"])
    op.create_index("ix_outbox_published_at", "outbox", ["published_at"])


def downgrade() -> None:
    op.drop_table("outbox")
    op.drop_table("inbox")
    op.drop_table("reservations")
    op.drop_table("stock")
