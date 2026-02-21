from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class LedgerEntry(Base):
    __tablename__ = "ledger_entries"
    __table_args__ = (
        sa.Index("ix_bo_ledger_entries_related_id", "related_id"),
        sa.Index("ix_bo_ledger_entries_wallet_event_time", "wallet_id", "event_time"),
        sa.Index("ix_bo_ledger_entries_event_time", "event_time"),
        {"schema": "bo"},
    )

    tx_id: Mapped[str] = mapped_column(sa.Text, primary_key=True)
    wallet_id: Mapped[str] = mapped_column(sa.Text, nullable=False)
    entry_type: Mapped[str] = mapped_column(sa.Text, nullable=False)
    amount: Mapped[Decimal] = mapped_column(sa.Numeric(18, 2), nullable=False)
    amount_signed: Mapped[Decimal | None] = mapped_column(sa.Numeric(18, 2))
    related_id: Mapped[str | None] = mapped_column(sa.Text)
    related_type: Mapped[str | None] = mapped_column(sa.Text)
    event_time: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False
    )
    updated_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))
    source_version: Mapped[int | None] = mapped_column(sa.BigInteger)
    ingested_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
    )


class PaymentOrder(Base):
    __tablename__ = "payment_orders"
    __table_args__ = (
        sa.Index("ix_bo_payment_orders_user_id", "user_id"),
        sa.Index("ix_bo_payment_orders_merchant_name", "merchant_name"),
        sa.Index("ix_bo_payment_orders_status", "status"),
        sa.Index("ix_bo_payment_orders_created_at", "created_at"),
        {"schema": "bo"},
    )

    order_id: Mapped[str] = mapped_column(sa.Text, primary_key=True)
    user_id: Mapped[str | None] = mapped_column(sa.Text)
    merchant_name: Mapped[str | None] = mapped_column(sa.Text)
    amount: Mapped[Decimal] = mapped_column(sa.Numeric(18, 2), nullable=False)
    status: Mapped[str] = mapped_column(sa.Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False
    )
    updated_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))
    source_version: Mapped[int | None] = mapped_column(sa.BigInteger)
    ingested_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
    )


class PaymentLedgerPair(Base):
    __tablename__ = "payment_ledger_pairs"
    __table_args__ = (
        sa.Index("ix_bo_payment_ledger_pairs_payment_tx_id", "payment_tx_id"),
        sa.Index("ix_bo_payment_ledger_pairs_receive_tx_id", "receive_tx_id"),
        {"schema": "bo"},
    )

    payment_order_id: Mapped[str] = mapped_column(sa.Text, primary_key=True)
    payment_tx_id: Mapped[str | None] = mapped_column(sa.Text)
    receive_tx_id: Mapped[str | None] = mapped_column(sa.Text)
    payer_wallet_id: Mapped[str | None] = mapped_column(sa.Text)
    payee_wallet_id: Mapped[str | None] = mapped_column(sa.Text)
    amount: Mapped[Decimal | None] = mapped_column(sa.Numeric(18, 2))
    status: Mapped[str | None] = mapped_column(sa.Text)
    event_time: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
    )
    ingested_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
    )


class AdminAuditLog(Base):
    __tablename__ = "admin_audit_logs"
    __table_args__ = (
        sa.Index("ix_bo_admin_audit_logs_requested_at", "requested_at"),
        sa.Index("ix_bo_admin_audit_logs_resource_id", "resource_id"),
        sa.Index("ix_bo_admin_audit_logs_actor_id", "actor_id"),
        sa.Index("ix_bo_admin_audit_logs_resource_time", "resource_id", "requested_at"),
        {"schema": "bo"},
    )

    audit_id: Mapped[int] = mapped_column(
        sa.BigInteger, sa.Identity(always=True), primary_key=True
    )
    actor_id: Mapped[str | None] = mapped_column(sa.Text)
    actor_roles: Mapped[str | None] = mapped_column(sa.Text)
    action: Mapped[str] = mapped_column(sa.Text, nullable=False)
    resource_type: Mapped[str] = mapped_column(sa.Text, nullable=False)
    resource_id: Mapped[str | None] = mapped_column(sa.Text)
    result: Mapped[str] = mapped_column(sa.Text, nullable=False)
    status_code: Mapped[int] = mapped_column(sa.Integer, nullable=False)
    ip: Mapped[str | None] = mapped_column(sa.Text)
    user_agent: Mapped[str | None] = mapped_column(sa.Text)
    request_method: Mapped[str] = mapped_column(sa.Text, nullable=False)
    request_path: Mapped[str] = mapped_column(sa.Text, nullable=False)
    request_query: Mapped[str | None] = mapped_column(sa.Text)
    requested_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
    )
    duration_ms: Mapped[int | None] = mapped_column(sa.Integer)
    result_count: Mapped[int | None] = mapped_column(sa.Integer)


class ConsumerDlqEvent(Base):
    __tablename__ = "consumer_dlq_events"
    __table_args__ = (
        sa.Index("ix_bo_consumer_dlq_events_ingested_at", "ingested_at"),
        sa.Index("ix_bo_consumer_dlq_events_topic", "topic"),
        sa.Index(
            "ix_bo_consumer_dlq_events_topic_ingested_at",
            "topic",
            "ingested_at",
        ),
        {"schema": "bo"},
    )

    dlq_id: Mapped[int] = mapped_column(
        sa.BigInteger, sa.Identity(always=True), primary_key=True
    )
    topic: Mapped[str] = mapped_column(sa.Text, nullable=False)
    partition: Mapped[int | None] = mapped_column(sa.Integer)
    offset: Mapped[int | None] = mapped_column(sa.BigInteger)
    key: Mapped[str | None] = mapped_column(sa.Text)
    payload: Mapped[str | None] = mapped_column(sa.Text)
    error: Mapped[str] = mapped_column(sa.Text, nullable=False)
    correlation_id: Mapped[str | None] = mapped_column(sa.Text)
    ingested_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")
    )
