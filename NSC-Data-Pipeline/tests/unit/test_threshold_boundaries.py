from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from conftest import make_ledger_rule, make_rule

from src.transforms.dq_guardrail import (
    SEVERITY_CRITICAL,
    SEVERITY_WARN,
    TAG_SOURCE_STALE,
    DQTableConfig,
    build_dq_status,
)
from src.transforms.ledger_controls import (
    build_recon_snapshot_flow,
    build_supply_balance_daily,
)

BASE_TS = datetime(2026, 2, 1, 0, 10, tzinfo=timezone.utc)
WINDOW_START = datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc)
WINDOW_END = BASE_TS

DQ_CONFIG = DQTableConfig(
    source_table="bronze.transaction_ledger_raw",
    window_start_ts=WINDOW_START,
    window_end_ts=WINDOW_END,
    dup_key_fields=("tx_id",),
    dup_rule_metric="dup_rate_tx_id",
)


def _ledger_record(ingested_at: datetime) -> dict:
    return {
        "tx_id": "tx-boundary-1",
        "wallet_id": "user-1",
        "type": "PAYMENT",
        "amount": "10.00",
        "ingested_at": ingested_at.isoformat(),
    }


# ---- DQ Guardrail: freshness boundary (uses >= comparison) ----


@pytest.mark.parametrize(
    "freshness_sec,expected_severity",
    [
        (299, None),
        (300, SEVERITY_WARN),
        (599, SEVERITY_WARN),
        (600, SEVERITY_CRITICAL),
        (601, SEVERITY_CRITICAL),
    ],
    ids=["below-warn", "at-warn", "between", "at-crit", "above-crit"],
)
def test_dq_freshness_boundary(
    freshness_sec: int, expected_severity: str | None
) -> None:
    rules = [
        make_rule(
            {
                "rule_id": "dq_freshness_boundary_test",
                "domain": "dq",
                "metric": "freshness_sec",
                "severity_map": {"warn": 300, "crit": 600},
            }
        )
    ]
    ingested_at = BASE_TS - timedelta(seconds=freshness_sec)
    records = [_ledger_record(ingested_at)]

    output = build_dq_status(
        records,
        config=DQ_CONFIG,
        run_id="run-boundary",
        rules=rules,
        now_ts=BASE_TS,
    )

    assert output.dq_status["severity"] == expected_severity
    if expected_severity is not None:
        assert output.dq_status["dq_tag"] == TAG_SOURCE_STALE
        assert len(output.exceptions) >= 1
    else:
        assert output.exceptions == []


# ---- Ledger controls: recon drift boundary (uses > comparison) ----

TARGET_DATE = date(2026, 2, 1)


@pytest.mark.parametrize(
    "drift_value,expected_exception_count",
    [
        (Decimal("0"), 0),
        (Decimal("0.01"), 1),
        (Decimal("100"), 1),
    ],
    ids=["at-threshold-no-exception", "just-above", "well-above"],
)
def test_recon_drift_boundary(
    drift_value: Decimal, expected_exception_count: int
) -> None:
    wallet_snapshots = [
        {
            "snapshot_ts": datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc),
            "user_id": "u1",
            "balance_total": Decimal("100"),
        },
        {
            "snapshot_ts": datetime(2026, 2, 1, 12, 0, tzinfo=timezone.utc),
            "user_id": "u1",
            "balance_total": Decimal("100") + drift_value,
        },
    ]

    output = build_recon_snapshot_flow(
        wallet_snapshots,
        [],
        target_date=TARGET_DATE,
        run_id="run-boundary",
        rules=[make_ledger_rule("drift_abs")],
    )

    assert len(output.exceptions) == expected_exception_count
    assert output.rows[0]["drift_abs"] == drift_value


# ---- Ledger controls: supply diff boundary (uses > for exception, <= for is_ok) ----


@pytest.mark.parametrize(
    "diff_value,expected_is_ok,expected_exception_count",
    [
        (Decimal("0"), True, 0),
        (Decimal("0.01"), False, 1),
    ],
    ids=["at-zero-ok", "just-above-not-ok"],
)
def test_supply_diff_boundary(
    diff_value: Decimal,
    expected_is_ok: bool,
    expected_exception_count: int,
) -> None:
    wallet_total = Decimal("100")
    issued_supply = wallet_total + diff_value

    wallet_snapshots = [
        {
            "snapshot_ts": datetime(2026, 2, 1, 12, 0, tzinfo=timezone.utc),
            "user_id": "u1",
            "balance_total": wallet_total,
        },
    ]
    ledger_entries = [
        {
            "entry_type": "MINT",
            "amount": issued_supply,
            "event_time": datetime(2026, 2, 1, 1, 0, tzinfo=timezone.utc),
        },
    ]

    output = build_supply_balance_daily(
        wallet_snapshots,
        ledger_entries,
        target_date=TARGET_DATE,
        run_id="run-boundary",
        rules=[make_ledger_rule("supply_diff_abs")],
    )

    assert output.row["is_ok"] is expected_is_ok
    assert len(output.exceptions) == expected_exception_count
