from __future__ import annotations

from datetime import datetime

from src.common.rules import select_rule
from src.common.time_utils import UTC
from src.io.bronze_io import prepare_bronze_records
from src.io.pipeline_state_io import STATE_SUCCESS, apply_pipeline_state
from src.io.rule_loader import load_default_rule_seed
from src.transforms import analytics, silver_controls

BASE_TS = datetime(2026, 2, 11, 0, 0, tzinfo=UTC)
SCENARIO_GLOB = "one_day_normal/*.jsonl"


def _load(table_name: str) -> list[dict]:
    return prepare_bronze_records(
        table_name,
        file_glob=SCENARIO_GLOB,
        ingested_at=BASE_TS,
        source_extracted_at=BASE_TS,
        batch_id="batch-pipeline-silver-int",
        source_system="mock",
    )


def test_pipeline_silver_runtime_transforms_from_bronze() -> None:
    rules = load_default_rule_seed()
    bad_rate_rule = select_rule(rules, domain="silver", metric="bad_records_rate")
    entry_type_rule = select_rule(rules, domain="silver", metric="entry_type_allowed")
    status_rule = select_rule(rules, domain="silver", metric="payment_status_allowed")

    wallet_raw = _load("user_wallets_raw")
    ledger_raw = _load("transaction_ledger_raw")
    payment_orders_raw = _load("payment_orders_raw")
    orders_raw = _load("orders_raw")
    order_items_raw = _load("order_items_raw")
    products_raw = _load("products_raw")

    status_lookup = silver_controls.build_status_lookup(payment_orders_raw)
    wallet_result = silver_controls.transform_wallet_snapshot_records(
        wallet_raw,
        run_id="run-pipeline-silver-int",
        rule_id=bad_rate_rule.rule_id if bad_rate_rule else None,
    )
    ledger_result = silver_controls.transform_ledger_entries_records(
        ledger_raw,
        run_id="run-pipeline-silver-int",
        rule_id=entry_type_rule.rule_id if entry_type_rule else None,
        allowed_entry_types=silver_controls.resolve_allowed_values(entry_type_rule),
        allowed_statuses=silver_controls.resolve_allowed_values(status_rule),
        status_lookup=status_lookup,
    )
    order_events_result = analytics.transform_order_events_records(
        orders_raw,
        payment_orders_raw,
        run_id="run-pipeline-silver-int",
    )
    order_items_result = analytics.transform_order_items_records(
        order_items_raw,
        run_id="run-pipeline-silver-int",
    )
    products_result = analytics.transform_products_records(
        products_raw,
        run_id="run-pipeline-silver-int",
    )

    assert len(wallet_result.records) > 0
    assert len(ledger_result.records) > 0
    assert len(order_events_result.records) > 0
    assert len(order_items_result.records) > 0
    assert len(products_result.records) > 0
    assert all(
        row["run_id"] == "run-pipeline-silver-int" for row in wallet_result.records
    )
    assert all(
        row["run_id"] == "run-pipeline-silver-int" for row in ledger_result.records
    )

    silver_controls.enforce_bad_records_rate(
        valid_count=len(wallet_result.records),
        bad_count=len(wallet_result.bad_records),
        rule=bad_rate_rule,
    )
    silver_controls.enforce_bad_records_rate(
        valid_count=len(ledger_result.records),
        bad_count=len(ledger_result.bad_records),
        rule=bad_rate_rule,
    )


def test_pipeline_silver_pipeline_state_success_update() -> None:
    processed_end = datetime(2026, 2, 11, 15, 0, tzinfo=UTC)
    state = apply_pipeline_state(
        pipeline_name="pipeline_silver",
        run_id="run-pipeline-silver-int",
        status=STATE_SUCCESS,
        last_processed_end=processed_end,
    )
    assert state.pipeline_name == "pipeline_silver"
    assert state.last_run_id == "run-pipeline-silver-int"
    assert state.last_processed_end == processed_end
