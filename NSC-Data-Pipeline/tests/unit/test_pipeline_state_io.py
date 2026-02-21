from __future__ import annotations

from datetime import datetime

import pytest

from src.io.pipeline_state_io import (
    PipelineStateRecord,
    apply_pipeline_state,
    parse_pipeline_state_record,
    parse_zero_window_counts,
    serialize_zero_window_counts,
)


def test_apply_pipeline_state_success_sets_checkpoint_fields() -> None:
    updated = apply_pipeline_state(
        pipeline_name="pipeline_a",
        run_id="run-1",
        status="success",
        last_processed_end=datetime(2026, 2, 6, 0, 10, 0),
        event_ts=datetime(2026, 2, 6, 0, 11, 0),
        dq_zero_window_counts='{"bronze.transaction_ledger_raw": 2}',
    )
    assert updated.pipeline_name == "pipeline_a"
    assert updated.last_run_id == "run-1"
    assert updated.last_processed_end is not None
    assert updated.last_success_ts is not None


def test_apply_pipeline_state_failure_keeps_last_success_checkpoint() -> None:
    current = PipelineStateRecord(
        pipeline_name="pipeline_a",
        last_success_ts=datetime(2026, 2, 6, 0, 11, 0),
        last_processed_end=datetime(2026, 2, 6, 0, 10, 0),
        last_run_id="run-1",
        dq_zero_window_counts='{"bronze.transaction_ledger_raw": 1}',
        updated_at=datetime(2026, 2, 6, 0, 11, 0),
    )
    failed = apply_pipeline_state(
        pipeline_name="pipeline_a",
        run_id="run-2",
        status="failure",
        current_state=current,
        event_ts=datetime(2026, 2, 6, 0, 20, 0),
    )
    assert failed.last_success_ts == current.last_success_ts
    assert failed.last_processed_end == current.last_processed_end
    assert failed.last_run_id == "run-2"
    assert failed.dq_zero_window_counts == current.dq_zero_window_counts


def test_parse_pipeline_state_record_requires_pipeline_name() -> None:
    with pytest.raises(ValueError):
        parse_pipeline_state_record({})


def test_zero_window_count_helpers_roundtrip_and_sanitize() -> None:
    counts = {"bronze.transaction_ledger_raw": 2, "bronze.user_wallets_raw": -1}
    encoded = serialize_zero_window_counts(counts)
    decoded = parse_zero_window_counts(encoded)
    assert decoded["bronze.transaction_ledger_raw"] == 2
    assert decoded["bronze.user_wallets_raw"] == 0
