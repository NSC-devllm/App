"""Create backoffice schema and core tables.

Revision ID: 20260205_0001
Revises:
Create Date: 2026-02-05 09:30:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "20260205_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS bo")

    op.create_table(
        "ledger_entries",
        sa.Column("tx_id", sa.Text(), primary_key=True),
        sa.Column("wallet_id", sa.Text(), nullable=False),
        sa.Column("entry_type", sa.Text(), nullable=False),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("amount_signed", sa.Numeric(18, 2)),
        sa.Column("related_id", sa.Text()),
        sa.Column("related_type", sa.Text()),
        sa.Column("event_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True)),
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        schema="bo",
    )

    op.create_index(
        "ix_bo_ledger_entries_related_id",
        "ledger_entries",
        ["related_id"],
        schema="bo",
    )
    op.create_index(
        "ix_bo_ledger_entries_wallet_event_time",
        "ledger_entries",
        ["wallet_id", "event_time"],
        schema="bo",
    )
    op.create_index(
        "ix_bo_ledger_entries_event_time",
        "ledger_entries",
        ["event_time"],
        schema="bo",
    )

    op.create_table(
        "payment_orders",
        sa.Column("order_id", sa.Text(), primary_key=True),
        sa.Column("user_id", sa.Text()),
        sa.Column("merchant_name", sa.Text()),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True)),
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        schema="bo",
    )

    op.create_index(
        "ix_bo_payment_orders_user_id",
        "payment_orders",
        ["user_id"],
        schema="bo",
    )
    op.create_index(
        "ix_bo_payment_orders_merchant_name",
        "payment_orders",
        ["merchant_name"],
        schema="bo",
    )
    op.create_index(
        "ix_bo_payment_orders_status",
        "payment_orders",
        ["status"],
        schema="bo",
    )
    op.create_index(
        "ix_bo_payment_orders_created_at",
        "payment_orders",
        ["created_at"],
        schema="bo",
    )

    op.create_table(
        "payment_ledger_pairs",
        sa.Column("payment_order_id", sa.Text(), primary_key=True),
        sa.Column("payment_tx_id", sa.Text()),
        sa.Column("receive_tx_id", sa.Text()),
        sa.Column("payer_wallet_id", sa.Text()),
        sa.Column("payee_wallet_id", sa.Text()),
        sa.Column("amount", sa.Numeric(18, 2)),
        sa.Column("status", sa.Text()),
        sa.Column("event_time", sa.DateTime(timezone=True)),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        schema="bo",
    )

    op.create_index(
        "ix_bo_payment_ledger_pairs_payment_tx_id",
        "payment_ledger_pairs",
        ["payment_tx_id"],
        schema="bo",
    )
    op.create_index(
        "ix_bo_payment_ledger_pairs_receive_tx_id",
        "payment_ledger_pairs",
        ["receive_tx_id"],
        schema="bo",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_bo_payment_ledger_pairs_receive_tx_id",
        table_name="payment_ledger_pairs",
        schema="bo",
    )
    op.drop_index(
        "ix_bo_payment_ledger_pairs_payment_tx_id",
        table_name="payment_ledger_pairs",
        schema="bo",
    )
    op.drop_table("payment_ledger_pairs", schema="bo")

    op.drop_index(
        "ix_bo_payment_orders_created_at", table_name="payment_orders", schema="bo"
    )
    op.drop_index(
        "ix_bo_payment_orders_status", table_name="payment_orders", schema="bo"
    )
    op.drop_index(
        "ix_bo_payment_orders_merchant_name",
        table_name="payment_orders",
        schema="bo",
    )
    op.drop_index(
        "ix_bo_payment_orders_user_id", table_name="payment_orders", schema="bo"
    )
    op.drop_table("payment_orders", schema="bo")

    op.drop_index(
        "ix_bo_ledger_entries_event_time", table_name="ledger_entries", schema="bo"
    )
    op.drop_index(
        "ix_bo_ledger_entries_wallet_event_time",
        table_name="ledger_entries",
        schema="bo",
    )
    op.drop_index(
        "ix_bo_ledger_entries_related_id", table_name="ledger_entries", schema="bo"
    )
    op.drop_table("ledger_entries", schema="bo")

    op.execute("DROP SCHEMA IF EXISTS bo")
