"""Add result_count column to admin_audit_logs.

Revision ID: 20260212_0005
Revises: 20260209_0004
Create Date: 2026-02-12 18:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "20260212_0005"
down_revision = "20260209_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "admin_audit_logs",
        sa.Column("result_count", sa.Integer(), nullable=True),
        schema="bo",
    )


def downgrade() -> None:
    op.drop_column("admin_audit_logs", "result_count", schema="bo")
