from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.io.bronze_io import prepare_bronze_records
from src.io.rule_loader import load_default_rule_seed
from src.transforms import ledger_controls, silver_controls
from src.transforms.dq_guardrail import (
    SEVERITY_CRITICAL,
    TAG_CONTRACT,
    TAG_DUP,
    TAG_EVENT_DROP,
    TAG_SOURCE_STALE,
    DQTableConfig,
    build_dq_status,
)

BASE_TS = datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc)
WINDOW_START = BASE_TS
WINDOW_END = BASE_TS + timedelta(minutes=10)


def _load_bronze_scenario(
    table: str,
    *,
    scenario: str,
    ingested_at: datetime,
    source_extracted_at: datetime,
) -> list[dict]:
    return prepare_bronze_records(
        table,
        file_glob=f"{scenario}/*.jsonl",
        ingested_at=ingested_at,
        source_extracted_at=source_extracted_at,
        batch_id="batch-test",
        source_system="mock",
    )


@pytest.mark.parametrize(
    "scenario,ingested_at,source_extracted_at,expected_tag",
    [
        (
            "stale_source",
            BASE_TS - timedelta(days=7),
            BASE_TS - timedelta(days=7),
            TAG_SOURCE_STALE,
        ),
        ("dup_tx_id", BASE_TS, BASE_TS, TAG_DUP),
        ("invalid_amount", BASE_TS, BASE_TS, TAG_CONTRACT),
        ("unknown_entry_type", BASE_TS, BASE_TS, TAG_CONTRACT),
        ("bad_records_rate_exceed", BASE_TS, BASE_TS, TAG_CONTRACT),
    ],
)
def test_dq_transaction_ledger_scenarios_produce_expected_tag(
    scenario: str,
    ingested_at: datetime,
    source_extracted_at: datetime,
    expected_tag: str,
) -> None:
    rules = load_default_rule_seed()
    records = _load_bronze_scenario(
        "transaction_ledger_raw",
        scenario=scenario,
        ingested_at=ingested_at,
        source_extracted_at=source_extracted_at,
    )
    config = DQTableConfig(
        source_table="bronze.transaction_ledger_raw",
        window_start_ts=WINDOW_START,
        window_end_ts=WINDOW_END,
        dup_key_fields=("tx_id",),
        dup_rule_metric="dup_rate_tx_id",
        freshness_fields=("ingested_at", "created_at"),
    )
    output = build_dq_status(
        records,
        config=config,
        run_id="run-test",
        rules=rules,
        now_ts=BASE_TS,
    )
    assert output.dq_status["dq_tag"] == expected_tag
    assert output.dq_status["severity"] == SEVERITY_CRITICAL


def test_dq_zero_window_scenario_flags_event_drop_after_consecutive_windows() -> None:
    rules = load_default_rule_seed()
    records = _load_bronze_scenario(
        "transaction_ledger_raw",
        scenario="zero_window",
        ingested_at=BASE_TS,
        source_extracted_at=BASE_TS,
    )
    assert records == []
    config = DQTableConfig(
        source_table="bronze.transaction_ledger_raw",
        window_start_ts=WINDOW_START,
        window_end_ts=WINDOW_END,
        dup_key_fields=("tx_id",),
        dup_rule_metric="dup_rate_tx_id",
        freshness_fields=("ingested_at", "created_at"),
    )
    output = build_dq_status(
        records,
        config=config,
        run_id="run-test",
        rules=rules,
        previous_zero_windows=2,
        now_ts=BASE_TS,
    )
    assert output.zero_window_count == 3
    assert output.dq_status["dq_tag"] == TAG_EVENT_DROP
    assert output.dq_status["severity"] == SEVERITY_CRITICAL


def test_supply_mismatch_scenario_generates_supply_exception() -> None:
    rules = load_default_rule_seed()
    user_wallets = _load_bronze_scenario(
        "user_wallets_raw",
        scenario="supply_mismatch",
        ingested_at=BASE_TS,
        source_extracted_at=BASE_TS,
    )
    ledger_raw = _load_bronze_scenario(
        "transaction_ledger_raw",
        scenario="supply_mismatch",
        ingested_at=BASE_TS,
        source_extracted_at=BASE_TS,
    )

    wallet_result, _ = silver_controls.transform_wallet_snapshot_with_rules(
        user_wallets,
        run_id="run-test",
        rules=rules,
    )
    ledger_result, _ = silver_controls.transform_ledger_entries_with_rules(
        ledger_raw,
        run_id="run-test",
        rules=rules,
        status_lookup={},
    )

    target_date = wallet_result.records[0]["snapshot_date_kst"]
    supply_output = ledger_controls.build_supply_balance_daily(
        wallet_result.records,
        ledger_result.records,
        target_date=target_date,
        run_id="run-test",
        rules=rules,
    )
    assert supply_output.row["is_ok"] is False
    assert supply_output.exceptions
    assert (
        supply_output.exceptions[0]["exception_type"]
        == ledger_controls.EXCEPTION_SUPPLY
    )


def test_hold_release_zero_flow_does_not_trigger_recon_exception() -> None:
    rules = load_default_rule_seed()
    user_wallets = _load_bronze_scenario(
        "user_wallets_raw",
        scenario="hold_release_zero_flow",
        ingested_at=BASE_TS,
        source_extracted_at=BASE_TS,
    )
    ledger_raw = _load_bronze_scenario(
        "transaction_ledger_raw",
        scenario="hold_release_zero_flow",
        ingested_at=BASE_TS,
        source_extracted_at=BASE_TS,
    )

    wallet_result, _ = silver_controls.transform_wallet_snapshot_with_rules(
        user_wallets,
        run_id="run-test",
        rules=rules,
    )
    ledger_result, _ = silver_controls.transform_ledger_entries_with_rules(
        ledger_raw,
        run_id="run-test",
        rules=rules,
        status_lookup={},
    )

    target_date = wallet_result.records[0]["snapshot_date_kst"]
    recon_output = ledger_controls.build_recon_snapshot_flow(
        wallet_result.records,
        ledger_result.records,
        target_date=target_date,
        run_id="run-test",
        rules=rules,
    )
    assert recon_output.exceptions == []


def test_one_day_normal_scenario_produces_no_exceptions() -> None:
    """Normal data should not trigger any DQ exceptions."""
    rules = load_default_rule_seed()
    records = _load_bronze_scenario(
        "transaction_ledger_raw",
        scenario="one_day_normal",
        ingested_at=BASE_TS,
        source_extracted_at=BASE_TS,
    )
    config = DQTableConfig(
        source_table="bronze.transaction_ledger_raw",
        window_start_ts=WINDOW_START,
        window_end_ts=WINDOW_END,
        dup_key_fields=("tx_id",),
        dup_rule_metric="dup_rate_tx_id",
        freshness_fields=("ingested_at", "created_at"),
    )
    output = build_dq_status(
        records,
        config=config,
        run_id="run-test",
        rules=rules,
        now_ts=BASE_TS,
    )
    assert output.dq_status["severity"] is None
    assert output.exceptions == []


def test_drift_mismatch_scenario_generates_recon_exception() -> None:
    """drift_mismatch scenario: wallet balance changes without matching ledger flow."""
    rules = load_default_rule_seed()
    user_wallets = _load_bronze_scenario(
        "user_wallets_raw",
        scenario="drift_mismatch",
        ingested_at=BASE_TS,
        source_extracted_at=BASE_TS,
    )
    ledger_raw = _load_bronze_scenario(
        "transaction_ledger_raw",
        scenario="drift_mismatch",
        ingested_at=BASE_TS,
        source_extracted_at=BASE_TS,
    )

    wallet_result, _ = silver_controls.transform_wallet_snapshot_with_rules(
        user_wallets,
        run_id="run-test",
        rules=rules,
    )
    ledger_result, _ = silver_controls.transform_ledger_entries_with_rules(
        ledger_raw,
        run_id="run-test",
        rules=rules,
        status_lookup={},
    )

    target_date = wallet_result.records[0]["snapshot_date_kst"]
    recon_output = ledger_controls.build_recon_snapshot_flow(
        wallet_result.records,
        ledger_result.records,
        target_date=target_date,
        run_id="run-test",
        rules=rules,
    )
    assert recon_output.exceptions
    assert (
        recon_output.exceptions[0]["exception_type"] == ledger_controls.EXCEPTION_RECON
    )
