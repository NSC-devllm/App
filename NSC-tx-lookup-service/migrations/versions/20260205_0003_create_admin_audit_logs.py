"""Create admin audit logs table.

Revision ID: 20260205_0003
Revises: 20260205_0002
Create Date: 2026-02-05 10:20:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "20260205_0003"
down_revision = "20260205_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "admin_audit_logs",
        sa.Column(
            "audit_id", sa.BigInteger(), sa.Identity(always=True), primary_key=True
        ),
        sa.Column("actor_id", sa.Text()),
        sa.Column("actor_roles", sa.Text()),
        sa.Column("action", sa.Text(), nullable=False),
        sa.Column("resource_type", sa.Text(), nullable=False),
        sa.Column("resource_id", sa.Text()),
        sa.Column("result", sa.Text(), nullable=False),
        sa.Column("status_code", sa.Integer(), nullable=False),
        sa.Column("ip", sa.Text()),
        sa.Column("user_agent", sa.Text()),
        sa.Column("request_method", sa.Text(), nullable=False),
        sa.Column("request_path", sa.Text(), nullable=False),
        sa.Column("request_query", sa.Text()),
        sa.Column(
            "requested_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("duration_ms", sa.Integer()),
        schema="bo",
    )

    op.create_index(
        "ix_bo_admin_audit_logs_requested_at",
        "admin_audit_logs",
        ["requested_at"],
        schema="bo",
    )
    op.create_index(
        "ix_bo_admin_audit_logs_resource_id",
        "admin_audit_logs",
        ["resource_id"],
        schema="bo",
    )
    op.create_index(
        "ix_bo_admin_audit_logs_actor_id",
        "admin_audit_logs",
        ["actor_id"],
        schema="bo",
    )
    op.create_index(
        "ix_bo_admin_audit_logs_resource_time",
        "admin_audit_logs",
        ["resource_id", "requested_at"],
        schema="bo",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_bo_admin_audit_logs_resource_time",
        table_name="admin_audit_logs",
        schema="bo",
    )
    op.drop_index(
        "ix_bo_admin_audit_logs_actor_id",
        table_name="admin_audit_logs",
        schema="bo",
    )
    op.drop_index(
        "ix_bo_admin_audit_logs_resource_id",
        table_name="admin_audit_logs",
        schema="bo",
    )
    op.drop_index(
        "ix_bo_admin_audit_logs_requested_at",
        table_name="admin_audit_logs",
        schema="bo",
    )
    op.drop_table("admin_audit_logs", schema="bo")
