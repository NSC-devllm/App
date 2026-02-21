from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from conftest import merge_rows, normalize_run_id, sorted_rows

from src.common.time_utils import UTC
from src.io.rule_loader import load_default_rule_seed
from src.transforms.analytics import build_fact_payment_anonymized
from src.transforms.ledger_controls import (
    EXCEPTION_RECON,
    build_recon_snapshot_flow,
    build_supply_balance_daily,
)


def test_pipeline_b_two_day_backfill_idempotency() -> None:
    wallet_snapshots = [
        {
            "snapshot_ts": datetime(2026, 2, 1, 0, 0, tzinfo=UTC),
            "user_id": "u1",
            "balance_total": Decimal("100"),
        },
        {
            "snapshot_ts": datetime(2026, 2, 1, 12, 0, tzinfo=UTC),
            "user_id": "u1",
            "balance_total": Decimal("120"),
        },
        {
            "snapshot_ts": datetime(2026, 2, 2, 0, 0, tzinfo=UTC),
            "user_id": "u1",
            "balance_total": Decimal("120"),
        },
        {
            "snapshot_ts": datetime(2026, 2, 2, 12, 0, tzinfo=UTC),
            "user_id": "u1",
            "balance_total": Decimal("140"),
        },
    ]
    ledger_entries = [
        {
            "wallet_id": "u1",
            "amount_signed": Decimal("20"),
            "entry_type": "MINT",
            "amount": Decimal("20"),
            "event_time": datetime(2026, 2, 1, 2, 0, tzinfo=UTC),
        },
        {
            "wallet_id": "u1",
            "amount_signed": Decimal("20"),
            "entry_type": "MINT",
            "amount": Decimal("20"),
            "event_time": datetime(2026, 2, 2, 2, 0, tzinfo=UTC),
        },
    ]
    rules = load_default_rule_seed()
    target_dates = [date(2026, 2, 1), date(2026, 2, 2)]

    recon_run1 = []
    recon_run2 = []
    supply_run1 = []
    supply_run2 = []

    for target_date in target_dates:
        recon_output_1 = build_recon_snapshot_flow(
            wallet_snapshots,
            ledger_entries,
            target_date=target_date,
            run_id="run-backfill-1",
            rules=rules,
        )
        recon_output_2 = build_recon_snapshot_flow(
            wallet_snapshots,
            ledger_entries,
            target_date=target_date,
            run_id="run-backfill-2",
            rules=rules,
        )
        recon_run1 = merge_rows(
            recon_run1, recon_output_1.rows, ("date_kst", "user_id")
        )
        recon_run2 = merge_rows(
            recon_run2, recon_output_2.rows, ("date_kst", "user_id")
        )

        supply_output_1 = build_supply_balance_daily(
            wallet_snapshots,
            ledger_entries,
            target_date=target_date,
            run_id="run-backfill-1",
            rules=rules,
        )
        supply_output_2 = build_supply_balance_daily(
            wallet_snapshots,
            ledger_entries,
            target_date=target_date,
            run_id="run-backfill-2",
            rules=rules,
        )
        supply_run1 = merge_rows(supply_run1, [supply_output_1.row], ("date_kst",))
        supply_run2 = merge_rows(supply_run2, [supply_output_2.row], ("date_kst",))

    assert sorted_rows(
        normalize_run_id(recon_run1), ("date_kst", "user_id")
    ) == sorted_rows(normalize_run_id(recon_run2), ("date_kst", "user_id"))
    assert sorted_rows(normalize_run_id(supply_run1), ("date_kst",)) == sorted_rows(
        normalize_run_id(supply_run2), ("date_kst",)
    )


def test_pipeline_c_backfill_partition_result_converges() -> None:
    order_events = [
        {
            "order_ref": "o1",
            "order_source": "PAYMENT_ORDERS",
            "user_id": "u1",
            "merchant_name": "M1",
            "amount": "100.00",
            "status": "PAID",
            "event_date_kst": date(2026, 2, 1),
        },
        {
            "order_ref": "o2",
            "order_source": "PAYMENT_ORDERS",
            "user_id": "u1",
            "merchant_name": "M1",
            "amount": "200.00",
            "status": "PAID",
            "event_date_kst": date(2026, 2, 2),
        },
    ]
    order_items = [
        {
            "item_id": 1,
            "order_ref": "o1",
            "product_id": 10,
            "quantity": 1,
            "price_at_purchase": "100.00",
        },
        {
            "item_id": 2,
            "order_ref": "o2",
            "product_id": 20,
            "quantity": 1,
            "price_at_purchase": "200.00",
        },
    ]
    products = [
        {"product_id": 10, "category": "A"},
        {"product_id": 20, "category": "B"},
    ]

    rows_run1 = build_fact_payment_anonymized(
        order_events,
        order_items,
        products,
        run_id="run-c-1",
        salt="salt",
    )
    rows_run2 = build_fact_payment_anonymized(
        order_events,
        order_items,
        products,
        run_id="run-c-2",
        salt="salt",
    )

    assert sorted_rows(
        normalize_run_id(rows_run1),
        ("date_kst", "user_key", "merchant_name", "amount", "status", "category"),
    ) == sorted_rows(
        normalize_run_id(rows_run2),
        ("date_kst", "user_key", "merchant_name", "amount", "status", "category"),
    )


def _strip_generated_at(exceptions: list[dict]) -> list[dict]:
    """Remove generated_at (timing-dependent) for idempotency comparison."""
    return [{k: v for k, v in exc.items() if k != "generated_at"} for exc in exceptions]


def test_exception_ledger_idempotency() -> None:
    """Recon exceptions merge identically when keyed by
    (date_kst, domain, exception_type, run_id, metric, message)."""
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
            "snapshot_ts": datetime(2026, 2, 1, 0, 30, tzinfo=UTC),
            "user_id": "u2",
            "balance_total": Decimal("200"),
        },
        {
            "snapshot_ts": datetime(2026, 2, 1, 12, 30, tzinfo=UTC),
            "user_id": "u2",
            "balance_total": Decimal("260"),
        },
    ]
    ledger_entries = [
        {
            "wallet_id": "u1",
            "amount_signed": Decimal("40"),
            "entry_type": "MINT",
            "amount": Decimal("40"),
            "event_time": datetime(2026, 2, 1, 2, 0, tzinfo=UTC),
        },
        {
            "wallet_id": "u2",
            "amount_signed": Decimal("50"),
            "entry_type": "MINT",
            "amount": Decimal("50"),
            "event_time": datetime(2026, 2, 1, 2, 30, tzinfo=UTC),
        },
    ]
    rules = load_default_rule_seed()
    target_date = date(2026, 2, 1)

    output_1 = build_recon_snapshot_flow(
        wallet_snapshots,
        ledger_entries,
        target_date=target_date,
        run_id="run-exc-1",
        rules=rules,
    )
    output_2 = build_recon_snapshot_flow(
        wallet_snapshots,
        ledger_entries,
        target_date=target_date,
        run_id="run-exc-1",
        rules=rules,
    )

    assert len(output_1.exceptions) >= 2
    assert all(exc["exception_type"] == EXCEPTION_RECON for exc in output_1.exceptions)
    assert len({exc["message"] for exc in output_1.exceptions}) >= 2

    exception_keys = (
        "date_kst",
        "domain",
        "exception_type",
        "run_id",
        "metric",
        "message",
    )
    merged_once = merge_rows(
        [], _strip_generated_at(output_1.exceptions), exception_keys
    )
    merged_twice = merge_rows(
        merged_once, _strip_generated_at(output_2.exceptions), exception_keys
    )
    assert sorted_rows(merged_once, exception_keys) == sorted_rows(
        merged_twice, exception_keys
    )


def test_fact_payment_partition_overwrite_isolation() -> None:
    """Simulates overwrite_partitions: re-running for date 2026-02-01 does
    not affect date 2026-02-02 rows."""
    order_events = [
        {
            "order_ref": "o1",
            "order_source": "PAYMENT_ORDERS",
            "user_id": "u1",
            "merchant_name": "M1",
            "amount": "100.00",
            "status": "PAID",
            "event_date_kst": date(2026, 2, 1),
        },
        {
            "order_ref": "o2",
            "order_source": "PAYMENT_ORDERS",
            "user_id": "u2",
            "merchant_name": "M2",
            "amount": "200.00",
            "status": "PAID",
            "event_date_kst": date(2026, 2, 2),
        },
    ]
    order_items = [
        {
            "item_id": 1,
            "order_ref": "o1",
            "product_id": 10,
            "quantity": 1,
            "price_at_purchase": "100.00",
        },
        {
            "item_id": 2,
            "order_ref": "o2",
            "product_id": 20,
            "quantity": 1,
            "price_at_purchase": "200.00",
        },
    ]
    products = [
        {"product_id": 10, "category": "A"},
        {"product_id": 20, "category": "B"},
    ]

    # Initial run: rows for both dates
    all_rows = build_fact_payment_anonymized(
        order_events, order_items, products, run_id="run-initial", salt="salt"
    )
    date1_initial = [r for r in all_rows if r["date_kst"] == date(2026, 2, 1)]
    date2_initial = [r for r in all_rows if r["date_kst"] == date(2026, 2, 2)]
    assert len(date1_initial) > 0
    assert len(date2_initial) > 0

    # Partition overwrite: re-run only date 2026-02-01 events
    date1_events = [e for e in order_events if e["event_date_kst"] == date(2026, 2, 1)]
    rerun_rows = build_fact_payment_anonymized(
        date1_events, order_items, products, run_id="run-rewrite", salt="salt"
    )

    # Simulate overwrite_partitions: replace date1 rows, keep date2 untouched
    final_store = {
        date(2026, 2, 1): normalize_run_id(rerun_rows),
        date(2026, 2, 2): normalize_run_id(date2_initial),
    }

    # Date 2026-02-02 rows unchanged
    sort_key = ("date_kst", "user_key", "merchant_name", "amount", "status", "category")
    assert sorted_rows(final_store[date(2026, 2, 2)], sort_key) == sorted_rows(
        normalize_run_id(date2_initial), sort_key
    )

    # Date 2026-02-01 rows converged (same as initial, ignoring run_id)
    assert sorted_rows(final_store[date(2026, 2, 1)], sort_key) == sorted_rows(
        normalize_run_id(date1_initial), sort_key
    )
