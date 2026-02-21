"""Create consumer DLQ events table.

Revision ID: 20260209_0004
Revises: 20260205_0003
Create Date: 2026-02-09 16:20:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "20260209_0004"
down_revision = "20260205_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "consumer_dlq_events",
        sa.Column(
            "dlq_id", sa.BigInteger(), sa.Identity(always=True), primary_key=True
        ),
        sa.Column("topic", sa.Text(), nullable=False),
        sa.Column("partition", sa.Integer()),
        sa.Column("offset", sa.BigInteger()),
        sa.Column("key", sa.Text()),
        sa.Column("payload", sa.Text()),
        sa.Column("error", sa.Text(), nullable=False),
        sa.Column("correlation_id", sa.Text()),
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        schema="bo",
    )

    op.create_index(
        "ix_bo_consumer_dlq_events_ingested_at",
        "consumer_dlq_events",
        ["ingested_at"],
        schema="bo",
    )
    op.create_index(
        "ix_bo_consumer_dlq_events_topic",
        "consumer_dlq_events",
        ["topic"],
        schema="bo",
    )
    op.create_index(
        "ix_bo_consumer_dlq_events_topic_ingested_at",
        "consumer_dlq_events",
        ["topic", "ingested_at"],
        schema="bo",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_bo_consumer_dlq_events_topic_ingested_at",
        table_name="consumer_dlq_events",
        schema="bo",
    )
    op.drop_index(
        "ix_bo_consumer_dlq_events_topic",
        table_name="consumer_dlq_events",
        schema="bo",
    )
    op.drop_index(
        "ix_bo_consumer_dlq_events_ingested_at",
        table_name="consumer_dlq_events",
        schema="bo",
    )
    op.drop_table("consumer_dlq_events", schema="bo")
