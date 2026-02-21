from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

pytestmark = pytest.mark.integration

from sqlalchemy import select

from src.db.admin_tx import (
    fetch_admin_order_context,
    fetch_admin_tx_context,
    fetch_admin_wallet_tx,
)
from src.db.audit import record_admin_audit
from src.db.models import AdminAuditLog, LedgerEntry, PaymentOrder


def _dt(minutes: int = 0) -> datetime:
    return datetime.now(timezone.utc) + timedelta(minutes=minutes)


def test_fetch_admin_tx_context_returns_peer(db_session, cleanup_test_rows):
    related_id = f"{cleanup_test_rows}-order"
    payment_tx_id = f"{cleanup_test_rows}-tx-pay"
    receive_tx_id = f"{cleanup_test_rows}-tx-rec"

    order = PaymentOrder(
        order_id=related_id,
        user_id="user-1",
        merchant_name="Shop",
        amount=Decimal("10.00"),
        status="SETTLED",
        created_at=_dt(-10),
        updated_at=_dt(-5),
        source_version=1,
        ingested_at=_dt(-5),
    )
    payment_entry = LedgerEntry(
        tx_id=payment_tx_id,
        wallet_id="wallet-pay",
        entry_type="PAYMENT",
        amount=Decimal("10.00"),
        amount_signed=Decimal("-10.00"),
        related_id=related_id,
        related_type="PAYMENT_ORDER",
        event_time=_dt(-6),
        created_at=_dt(-6),
        updated_at=_dt(-5),
        source_version=1,
        ingested_at=_dt(-5),
    )
    receive_entry = LedgerEntry(
        tx_id=receive_tx_id,
        wallet_id="wallet-rec",
        entry_type="RECEIVE",
        amount=Decimal("10.00"),
        amount_signed=Decimal("10.00"),
        related_id=related_id,
        related_type="PAYMENT_ORDER",
        event_time=_dt(-5),
        created_at=_dt(-5),
        updated_at=_dt(-4),
        source_version=1,
        ingested_at=_dt(-4),
    )

    db_session.add(order)
    db_session.add(payment_entry)
    db_session.add(receive_entry)
    db_session.commit()

    context = fetch_admin_tx_context(db_session, payment_tx_id)

    assert context is not None
    assert context.ledger_entry.tx_id == payment_tx_id
    assert context.payment_order is not None
    assert context.payment_pair is None
    assert context.peer_entry is not None
    assert context.peer_entry.tx_id == receive_tx_id


def test_record_admin_audit_persists_row(db_session, cleanup_test_rows):
    resource_id = f"{cleanup_test_rows}-tx-audit"
    fields = {
        "actor_id": "actor-1",
        "actor_roles": "ADMIN_READ",
        "action": "ADMIN_TX_LOOKUP",
        "resource_type": "TX",
        "resource_id": resource_id,
        "result": "FOUND",
        "status_code": 200,
        "ip": "127.0.0.1",
        "user_agent": "pytest",
        "request_method": "GET",
        "request_path": "/admin/tx/" + resource_id,
        "request_query": None,
        "requested_at": _dt(0),
        "duration_ms": 12,
    }

    record_admin_audit(db_session, fields)
    db_session.commit()

    row = db_session.execute(
        select(AdminAuditLog).where(AdminAuditLog.resource_id == resource_id)
    ).scalar_one_or_none()

    assert row is not None
    assert row.actor_id == "actor-1"
    assert row.status_code == 200


def test_record_admin_audit_persists_result_count(db_session, cleanup_test_rows):
    """DEC-208: result_count 컬럼 영속성 검증."""
    resource_id = f"{cleanup_test_rows}-tx-audit-count"
    fields = {
        "actor_id": "actor-1",
        "actor_roles": "ADMIN_READ",
        "action": "ADMIN_TX_LOOKUP",
        "resource_type": "TX",
        "resource_id": resource_id,
        "result": "FOUND",
        "status_code": 200,
        "ip": "127.0.0.1",
        "user_agent": "pytest",
        "request_method": "GET",
        "request_path": "/admin/tx/" + resource_id,
        "request_query": None,
        "requested_at": _dt(0),
        "duration_ms": 12,
        "result_count": 3,
    }

    record_admin_audit(db_session, fields)
    db_session.commit()

    row = db_session.execute(
        select(AdminAuditLog).where(AdminAuditLog.resource_id == resource_id)
    ).scalar_one_or_none()

    assert row is not None
    assert row.result_count == 3


def test_record_admin_audit_result_count_null(db_session, cleanup_test_rows):
    """DEC-208: result_count=None 역호환 검증."""
    resource_id = f"{cleanup_test_rows}-tx-audit-null"
    fields = {
        "actor_id": "actor-1",
        "actor_roles": "ADMIN_READ",
        "action": "ADMIN_TX_LOOKUP",
        "resource_type": "TX",
        "resource_id": resource_id,
        "result": "FOUND",
        "status_code": 200,
        "ip": "127.0.0.1",
        "user_agent": "pytest",
        "request_method": "GET",
        "request_path": "/admin/tx/" + resource_id,
        "request_query": None,
        "requested_at": _dt(0),
        "duration_ms": 10,
        "result_count": None,
    }

    record_admin_audit(db_session, fields)
    db_session.commit()

    row = db_session.execute(
        select(AdminAuditLog).where(AdminAuditLog.resource_id == resource_id)
    ).scalar_one_or_none()

    assert row is not None
    assert row.result_count is None


# ========== DEC-207: fetch_admin_order_context tests ==========


def test_fetch_admin_order_context_returns_entries(db_session, cleanup_test_rows):
    """DEC-207: order context에 연관 원장 엔트리가 포함되는지 검증."""
    order_id = f"{cleanup_test_rows}-order-ctx"
    pay_tx = f"{cleanup_test_rows}-tx-pay-ctx"
    rec_tx = f"{cleanup_test_rows}-tx-rec-ctx"

    db_session.add(
        PaymentOrder(
            order_id=order_id,
            user_id="user-1",
            merchant_name="Shop",
            amount=Decimal("10.00"),
            status="SETTLED",
            created_at=_dt(-10),
            updated_at=_dt(-5),
            source_version=1,
            ingested_at=_dt(-5),
        )
    )
    db_session.add(
        LedgerEntry(
            tx_id=pay_tx,
            wallet_id="wallet-pay",
            entry_type="PAYMENT",
            amount=Decimal("10.00"),
            amount_signed=Decimal("-10.00"),
            related_id=order_id,
            related_type="PAYMENT_ORDER",
            event_time=_dt(-6),
            created_at=_dt(-6),
            updated_at=_dt(-5),
            source_version=1,
            ingested_at=_dt(-5),
        )
    )
    db_session.add(
        LedgerEntry(
            tx_id=rec_tx,
            wallet_id="wallet-rec",
            entry_type="RECEIVE",
            amount=Decimal("10.00"),
            amount_signed=Decimal("10.00"),
            related_id=order_id,
            related_type="PAYMENT_ORDER",
            event_time=_dt(-5),
            created_at=_dt(-5),
            updated_at=_dt(-4),
            source_version=1,
            ingested_at=_dt(-4),
        )
    )
    db_session.commit()

    context = fetch_admin_order_context(db_session, order_id)

    assert context is not None
    assert context.payment_order.order_id == order_id
    assert len(context.ledger_entries) == 2
    # event_time DESC → receive (-5) comes before payment (-6)
    assert context.ledger_entries[0].tx_id == rec_tx
    assert context.ledger_entries[1].tx_id == pay_tx


def test_fetch_admin_order_context_not_found(db_session, cleanup_test_rows):
    """DEC-207: 존재하지 않는 order_id는 None 반환."""
    ctx = fetch_admin_order_context(db_session, "nonexistent-order")
    assert ctx is None


# ========== DEC-207: fetch_admin_wallet_tx tests ==========


def test_fetch_admin_wallet_tx_returns_entries(db_session, cleanup_test_rows):
    """DEC-207: wallet_id 기준 거래 목록 반환 검증."""
    wallet_id = f"{cleanup_test_rows}-wallet-query"

    for i in range(3):
        db_session.add(
            LedgerEntry(
                tx_id=f"{cleanup_test_rows}-tx-w{i}",
                wallet_id=wallet_id,
                entry_type="PAYMENT",
                amount=Decimal("10.00"),
                amount_signed=Decimal("-10.00"),
                related_id=None,
                related_type=None,
                event_time=_dt(-10 + i),
                created_at=_dt(-10 + i),
                updated_at=None,
                source_version=None,
                ingested_at=_dt(-10 + i),
            )
        )
    db_session.commit()

    entries = fetch_admin_wallet_tx(db_session, wallet_id, limit=10)

    assert len(entries) == 3
    # event_time DESC → most recent first
    assert entries[0].tx_id == f"{cleanup_test_rows}-tx-w2"


def test_fetch_admin_wallet_tx_time_range(db_session, cleanup_test_rows):
    """DEC-207: from/to 시간 범위 필터링 검증."""
    wallet_id = f"{cleanup_test_rows}-wallet-range"

    for i, offset in enumerate([-10, -5, -1]):
        db_session.add(
            LedgerEntry(
                tx_id=f"{cleanup_test_rows}-tx-r{i}",
                wallet_id=wallet_id,
                entry_type="PAYMENT",
                amount=Decimal("1.00"),
                amount_signed=Decimal("-1.00"),
                related_id=None,
                related_type=None,
                event_time=_dt(offset),
                created_at=_dt(offset),
                updated_at=None,
                source_version=None,
                ingested_at=_dt(offset),
            )
        )
    db_session.commit()

    entries = fetch_admin_wallet_tx(
        db_session,
        wallet_id,
        from_time=_dt(-7),
        to_time=_dt(-3),
    )

    assert len(entries) == 1
    assert entries[0].tx_id == f"{cleanup_test_rows}-tx-r1"


def test_fetch_admin_wallet_tx_limit(db_session, cleanup_test_rows):
    """DEC-207: limit 제한 검증."""
    wallet_id = f"{cleanup_test_rows}-wallet-limit"

    for i in range(5):
        db_session.add(
            LedgerEntry(
                tx_id=f"{cleanup_test_rows}-tx-l{i}",
                wallet_id=wallet_id,
                entry_type="RECEIVE",
                amount=Decimal("1.00"),
                amount_signed=Decimal("1.00"),
                related_id=None,
                related_type=None,
                event_time=_dt(-10 + i),
                created_at=_dt(-10 + i),
                updated_at=None,
                source_version=None,
                ingested_at=_dt(-10 + i),
            )
        )
    db_session.commit()

    entries = fetch_admin_wallet_tx(db_session, wallet_id, limit=2)

    assert len(entries) == 2


def test_fetch_admin_wallet_tx_empty(db_session, cleanup_test_rows):
    """DEC-207: 거래 없는 지갑은 빈 리스트 반환."""
    entries = fetch_admin_wallet_tx(db_session, "nonexistent-wallet")
    assert entries == []
