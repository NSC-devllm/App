from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.io.bronze_io import prepare_bronze_records
from src.io.rule_loader import load_default_rule_seed
from src.transforms import silver_controls
from src.transforms.dq_guardrail import (
    TAG_EVENT_DROP,
    DQTableConfig,
    build_dq_status,
)
from src.transforms.ledger_controls import build_recon_snapshot_flow

BASE_TS = datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc)


def _load_gating_bronze(table: str, ingested_at: datetime) -> list[dict]:
    return prepare_bronze_records(
        table,
        file_glob="gating_effect/*.jsonl",
        ingested_at=ingested_at,
        source_extracted_at=ingested_at,
        batch_id="batch-gating-test",
        source_system="mock",
    )


def test_pipeline_a_event_drop_propagates_to_pipeline_b_recon() -> None:
    """When Pipeline A detects EVENT_DROP (consecutive zero-windows),
    the dq_tag propagates into Pipeline B recon rows via dq_tags parameter."""
    rules = load_default_rule_seed()

    # Step 1: Pipeline A — empty ledger triggers EVENT_DROP after 3 consecutive windows
    ledger_records = _load_gating_bronze("transaction_ledger_raw", ingested_at=BASE_TS)
    assert ledger_records == []

    dq_config = DQTableConfig(
        source_table="bronze.transaction_ledger_raw",
        window_start_ts=BASE_TS,
        window_end_ts=BASE_TS + timedelta(minutes=10),
        dup_key_fields=("tx_id",),
        dup_rule_metric="dup_rate_tx_id",
        freshness_fields=("ingested_at", "created_at"),
    )
    dq_output = build_dq_status(
        ledger_records,
        config=dq_config,
        run_id="run-gating-test",
        rules=rules,
        previous_zero_windows=2,
        now_ts=BASE_TS,
    )
    assert dq_output.dq_status["dq_tag"] == TAG_EVENT_DROP
    assert dq_output.zero_window_count == 3

    # Step 2: Pipeline B — wallet snapshots + empty ledger with dq_tags from Pipeline A
    wallet_records = _load_gating_bronze("user_wallets_raw", ingested_at=BASE_TS)
    assert len(wallet_records) > 0

    wallet_result, _ = silver_controls.transform_wallet_snapshot_with_rules(
        wallet_records,
        run_id="run-gating-test",
        rules=rules,
    )
    assert len(wallet_result.records) > 0

    target_date = wallet_result.records[0]["snapshot_date_kst"]
    recon_output = build_recon_snapshot_flow(
        wallet_result.records,
        [],
        target_date=target_date,
        run_id="run-gating-test",
        rules=rules,
        dq_tags=[TAG_EVENT_DROP],
    )

    # Step 3: Assert — dq_tag propagated to every recon row
    assert len(recon_output.rows) > 0
    for row in recon_output.rows:
        assert row["dq_tag"] == TAG_EVENT_DROP
