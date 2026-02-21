"""Add source_version columns for version-based LWW.

Revision ID: 20260205_0002
Revises: 20260205_0001
Create Date: 2026-02-05 10:05:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "20260205_0002"
down_revision = "20260205_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "ledger_entries",
        sa.Column("source_version", sa.BigInteger()),
        schema="bo",
    )
    op.add_column(
        "payment_orders",
        sa.Column("source_version", sa.BigInteger()),
        schema="bo",
    )


def downgrade() -> None:
    op.drop_column("payment_orders", "source_version", schema="bo")
    op.drop_column("ledger_entries", "source_version", schema="bo")
