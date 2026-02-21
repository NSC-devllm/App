from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.common.window_defaults import (
    inject_default_daily_backfill,
    inject_pipeline_a_default_incremental_window,
)


def test_inject_default_daily_backfill_when_window_and_mode_blank() -> None:
    payload = {
        "run_mode": "",
        "start_ts": "",
        "end_ts": "",
        "date_kst_start": "",
        "date_kst_end": "",
    }
    resolved = inject_default_daily_backfill(
        payload,
        now_ts=datetime(2026, 2, 12, 1, 0, tzinfo=timezone.utc),
    )
    assert resolved["run_mode"] == "backfill"
    assert resolved["date_kst_start"] == "2026-02-11"
    assert resolved["date_kst_end"] == "2026-02-11"


def test_inject_default_daily_backfill_keeps_explicit_run_mode() -> None:
    payload = {
        "run_mode": "incremental",
        "start_ts": None,
        "end_ts": None,
        "date_kst_start": "",
        "date_kst_end": "",
    }
    resolved = inject_default_daily_backfill(payload)
    assert resolved == payload


def test_inject_default_daily_backfill_keeps_explicit_window() -> None:
    payload = {
        "run_mode": "incremental",
        "start_ts": "2026-02-11T00:00:00Z",
        "end_ts": "2026-02-11T00:10:00Z",
        "date_kst_start": "",
        "date_kst_end": "",
    }
    resolved = inject_default_daily_backfill(payload)
    assert resolved == payload


def test_inject_default_daily_backfill_keeps_invalid_mode_for_validation() -> None:
    payload = {
        "run_mode": "nightly",
        "start_ts": "",
        "end_ts": "",
        "date_kst_start": "",
        "date_kst_end": "",
    }
    resolved = inject_default_daily_backfill(payload)
    assert resolved == payload


def test_pipeline_a_default_window_uses_last_processed_end() -> None:
    payload = {
        "run_mode": "incremental",
        "start_ts": "",
        "end_ts": "",
        "date_kst_start": "",
        "date_kst_end": "",
    }
    last_processed_end = datetime(2026, 2, 12, 0, 10, tzinfo=timezone.utc)
    now_ts = datetime(2026, 2, 12, 0, 20, tzinfo=timezone.utc)

    resolved = inject_pipeline_a_default_incremental_window(
        payload,
        last_processed_end=last_processed_end,
        now_ts=now_ts,
    )
    assert resolved["run_mode"] == "incremental"
    assert resolved["start_ts"] == last_processed_end
    assert resolved["end_ts"] == now_ts


def test_pipeline_a_default_window_without_state_uses_recent_window() -> None:
    payload = {
        "run_mode": "incremental",
        "start_ts": "",
        "end_ts": "",
        "date_kst_start": "",
        "date_kst_end": "",
    }
    now_ts = datetime(2026, 2, 12, 0, 20, tzinfo=timezone.utc)

    resolved = inject_pipeline_a_default_incremental_window(payload, now_ts=now_ts)
    assert resolved["run_mode"] == "incremental"
    assert resolved["start_ts"] == datetime(2026, 2, 12, 0, 10, tzinfo=timezone.utc)
    assert resolved["end_ts"] == now_ts


def test_pipeline_a_default_window_keeps_non_incremental_run_mode() -> None:
    payload = {
        "run_mode": "backfill",
        "start_ts": "",
        "end_ts": "",
        "date_kst_start": "",
        "date_kst_end": "",
    }
    resolved = inject_pipeline_a_default_incremental_window(payload)
    assert resolved == payload


def test_pipeline_a_default_window_keeps_partial_window_for_validation() -> None:
    payload = {
        "run_mode": "incremental",
        "start_ts": "2026-02-12T00:10:00Z",
        "end_ts": "",
        "date_kst_start": "",
        "date_kst_end": "",
    }
    resolved = inject_pipeline_a_default_incremental_window(payload)
    assert resolved == payload


def test_pipeline_a_default_window_rejects_non_positive_window_minutes() -> None:
    with pytest.raises(ValueError, match="default_window_minutes must be > 0"):
        inject_pipeline_a_default_incremental_window(
            {"run_mode": "incremental"},
            default_window_minutes=0,
        )
