from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from conftest import merge_rows, normalize_run_id, sorted_rows

from src.common.time_utils import UTC, date_kst
from src.io.rule_loader import load_default_rule_seed
from src.transforms.ledger_controls import (
    EXCEPTION_RECON,
    EXCEPTION_SUPPLY,
    build_recon_snapshot_flow,
    build_supply_balance_daily,
)


def test_recon_supply_results_and_idempotency() -> None:
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
            "snapshot_ts": datetime(2026, 2, 1, 13, 0, tzinfo=UTC),
            "user_id": "u2",
            "balance_total": Decimal("200"),
        },
    ]
    ledger_entries = [
        {
            "wallet_id": "u1",
            "amount_signed": Decimal("40"),
            "entry_type": "MINT",
            "amount": Decimal("300"),
            "event_time": datetime(2026, 2, 1, 1, 0, tzinfo=UTC),
        },
        {
            "wallet_id": "u2",
            "amount_signed": Decimal("0"),
            "entry_type": "BURN",
            "amount": Decimal("10"),
            "event_time": datetime(2026, 2, 1, 2, 0, tzinfo=UTC),
        },
    ]
    rules = load_default_rule_seed()

    recon_output_1 = build_recon_snapshot_flow(
        wallet_snapshots,
        ledger_entries,
        target_date=target_date,
        run_id="run-recon-1",
        rules=rules,
    )
    recon_output_2 = build_recon_snapshot_flow(
        wallet_snapshots,
        ledger_entries,
        target_date=target_date,
        run_id="run-recon-2",
        rules=rules,
    )

    u1_row = next(row for row in recon_output_1.rows if row["user_id"] == "u1")
    assert u1_row["delta_balance_total"] == Decimal("50")
    assert u1_row["net_flow_total"] == Decimal("40")
    assert u1_row["drift_abs"] == Decimal("10")
    assert recon_output_1.exceptions[0]["exception_type"] == EXCEPTION_RECON

    merged_once = merge_rows([], recon_output_1.rows, ("date_kst", "user_id"))
    merged_twice = merge_rows(merged_once, recon_output_2.rows, ("date_kst", "user_id"))
    assert sorted_rows(
        normalize_run_id(merged_once), ("date_kst", "user_id")
    ) == sorted_rows(normalize_run_id(merged_twice), ("date_kst", "user_id"))

    supply_output_1 = build_supply_balance_daily(
        wallet_snapshots,
        ledger_entries,
        target_date=target_date,
        run_id="run-supply-1",
        rules=rules,
    )
    supply_output_2 = build_supply_balance_daily(
        wallet_snapshots,
        ledger_entries,
        target_date=target_date,
        run_id="run-supply-2",
        rules=rules,
    )

    assert supply_output_1.row["issued_supply"] == Decimal("290")
    assert supply_output_1.row["wallet_total_balance"] == Decimal("350")
    assert supply_output_1.exceptions[0]["exception_type"] == EXCEPTION_SUPPLY

    merged_supply_once = merge_rows([], [supply_output_1.row], ("date_kst",))
    merged_supply_twice = merge_rows(
        merged_supply_once, [supply_output_2.row], ("date_kst",)
    )
    assert sorted_rows(
        normalize_run_id(merged_supply_once), ("date_kst",)
    ) == sorted_rows(normalize_run_id(merged_supply_twice), ("date_kst",))
