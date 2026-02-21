from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from conftest import make_ledger_rule

from src.common.time_utils import UTC, date_kst
from src.transforms.ledger_controls import (
    EXCEPTION_RECON,
    EXCEPTION_SUPPLY,
    TAG_SOURCE_STALE,
    build_admin_tx_search,
    build_ops_ledger_pairing_quality_daily,
    build_ops_payment_failure_daily,
    build_ops_payment_refund_daily,
    build_recon_snapshot_flow,
    build_supply_balance_daily,
)


def test_build_recon_snapshot_flow_and_gating() -> None:
    target_date = date_kst(datetime(2026, 2, 1, 0, 0, tzinfo=UTC))
    wallet_snapshots = [
        {
            "snapshot_ts": datetime(2026, 2, 1, 0, 0, tzinfo=UTC),
            "user_id": "u1",
            "balance_total": Decimal("100"),
        },
        {
            "snapshot_ts": datetime(2026, 2, 1, 12, 0, tzinfo=UTC),
            "user_id": "u1",
            "balance_total": Decimal("150"),
        },
        {
            "snapshot_ts": datetime(2026, 2, 1, 1, 0, tzinfo=UTC),
            "user_id": "u2",
            "balance_total": Decimal("200"),
        },
        {
            "snapshot_ts": datetime(2026, 2, 1, 23, 0, tzinfo=UTC),
            "user_id": "u2",
            "balance_total": Decimal("200"),
        },
    ]
    ledger_entries = [
        {
            "wallet_id": "u1",
            "event_time": datetime(2026, 2, 1, 2, 0, tzinfo=UTC),
            "amount_signed": Decimal("50"),
        },
        {
            "wallet_id": "u2",
            "event_time": datetime(2026, 2, 1, 3, 0, tzinfo=UTC),
            "amount_signed": Decimal("10"),
        },
    ]

    output = build_recon_snapshot_flow(
        wallet_snapshots,
        ledger_entries,
        target_date=target_date,
        run_id="run-1",
        rules=[make_ledger_rule("drift_abs")],
    )

    assert len(output.rows) == 2
    exception = output.exceptions[0]
    assert exception["exception_type"] == EXCEPTION_RECON
    assert exception["severity"] == "CRITICAL"

    output_gated = build_recon_snapshot_flow(
        wallet_snapshots,
        ledger_entries,
        target_date=target_date,
        run_id="run-1",
        rules=[make_ledger_rule("drift_abs")],
        dq_tags=[TAG_SOURCE_STALE],
    )
    exception_gated = output_gated.exceptions[0]
    assert exception_gated["severity"] == "CRITICAL"


def test_build_supply_balance_daily() -> None:
    target_date = date_kst(datetime(2026, 2, 1, 0, 0, tzinfo=UTC))
    wallet_snapshots = [
        {
            "snapshot_ts": datetime(2026, 2, 1, 2, 0, tzinfo=UTC),
            "user_id": "u1",
            "balance_total": Decimal("150"),
        },
        {
            "snapshot_ts": datetime(2026, 2, 1, 3, 0, tzinfo=UTC),
            "user_id": "u2",
            "balance_total": Decimal("200"),
        },
    ]
    ledger_entries = [
        {
            "entry_type": "MINT",
            "amount": Decimal("300"),
            "event_time": datetime(2026, 2, 1, 1, 0, tzinfo=UTC),
        },
        {
            "entry_type": "BURN",
            "amount": Decimal("10"),
            "event_time": datetime(2026, 2, 1, 1, 30, tzinfo=UTC),
        },
    ]

    output = build_supply_balance_daily(
        wallet_snapshots,
        ledger_entries,
        target_date=target_date,
        run_id="run-2",
        rules=[make_ledger_rule("supply_diff_abs")],
    )

    assert output.row["issued_supply"] == Decimal("290")
    assert output.row["wallet_total_balance"] == Decimal("350")
    assert output.row["is_ok"] is False
    assert output.exceptions[0]["exception_type"] == EXCEPTION_SUPPLY


def test_build_ops_payment_failure_daily() -> None:
    target_date = date_kst(datetime(2026, 2, 1, 0, 0, tzinfo=UTC))
    payment_orders = [
        {
            "order_id": "o1",
            "merchant_name": "M1",
            "status": "FAILED",
            "created_at": datetime(2026, 2, 1, 1, 0, tzinfo=UTC),
        },
        {
            "order_id": "o2",
            "merchant_name": "M1",
            "status": "PAID",
            "created_at": datetime(2026, 2, 1, 2, 0, tzinfo=UTC),
        },
        {
            "order_id": "o3",
            "status": "CANCELLED",
            "created_at": datetime(2026, 2, 1, 3, 0, tzinfo=UTC),
        },
    ]

    rows = build_ops_payment_failure_daily(
        payment_orders,
        target_date=target_date,
        run_id="run-3",
    )
    rows_by_merchant = {row["merchant_name"]: row for row in rows}

    assert rows_by_merchant["M1"]["total_cnt"] == 2
    assert rows_by_merchant["M1"]["failed_cnt"] == 1
    assert rows_by_merchant[None]["failed_cnt"] == 1


def test_build_ops_payment_refund_daily() -> None:
    target_date = date_kst(datetime(2026, 2, 1, 0, 0, tzinfo=UTC))
    payment_orders = [
        {
            "order_id": "o1",
            "merchant_name": "M1",
            "status": "REFUNDED",
            "created_at": datetime(2026, 2, 1, 1, 0, tzinfo=UTC),
        },
        {
            "order_id": "o2",
            "merchant_name": "M1",
            "status": "PAID",
            "created_at": datetime(2026, 2, 1, 2, 0, tzinfo=UTC),
        },
        {
            "order_id": "o3",
            "status": "REFUNDED",
            "created_at": datetime(2026, 2, 1, 3, 0, tzinfo=UTC),
        },
    ]

    rows = build_ops_payment_refund_daily(
        payment_orders,
        target_date=target_date,
        run_id="run-3",
    )
    rows_by_merchant = {row["merchant_name"]: row for row in rows}

    assert rows_by_merchant["M1"]["total_cnt"] == 2
    assert rows_by_merchant["M1"]["refunded_cnt"] == 1
    assert rows_by_merchant["M1"]["refund_rate"] == Decimal("0.5")
    assert rows_by_merchant[None]["refunded_cnt"] == 1


def test_build_ops_ledger_pairing_quality_daily_and_admin_tx_search() -> None:
    target_date = date_kst(datetime(2026, 2, 1, 0, 0, tzinfo=UTC))
    ledger_entries = [
        {
            "tx_id": "t1",
            "wallet_id": "w1",
            "event_time": datetime(2026, 2, 1, 1, 0, tzinfo=UTC),
            "related_id": "o1",
        },
        {
            "tx_id": "t2",
            "wallet_id": "w2",
            "event_time": datetime(2026, 2, 1, 1, 1, tzinfo=UTC),
            "related_id": "o1",
        },
        {
            "tx_id": "t3",
            "wallet_id": "w3",
            "event_time": datetime(2026, 2, 1, 2, 0, tzinfo=UTC),
            "related_id": None,
        },
    ]
    payment_orders = [
        {"order_id": "o1"},
    ]

    quality = build_ops_ledger_pairing_quality_daily(
        ledger_entries,
        target_date=target_date,
        run_id="run-4",
        payment_orders=payment_orders,
    )

    assert quality["entry_cnt"] == 3
    assert quality["join_payment_orders_rate"] == Decimal("1")

    admin_rows = build_admin_tx_search(
        ledger_entries,
        target_date=target_date,
        run_id="run-4",
    )
    paired = {row["tx_id"]: row["paired_tx_id"] for row in admin_rows}
    assert paired["t1"] == "t2"
    assert paired["t2"] == "t1"
