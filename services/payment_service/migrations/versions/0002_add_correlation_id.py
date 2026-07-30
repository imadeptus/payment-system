"""Persist Saga correlation on payments.

Revision ID: 0002_payment_correlation
Revises: 0001_payment_init
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002_payment_correlation"
down_revision: str | None = "0001_payment_init"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Existing demonstration rows cannot be backfilled safely because the
    # previous schema did not retain Saga correlation. New writes always set it.
    op.add_column(
        "payments",
        sa.Column("correlation_id", sa.Uuid(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("payments", "correlation_id")
