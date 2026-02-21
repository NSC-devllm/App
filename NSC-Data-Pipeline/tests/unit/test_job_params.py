from __future__ import annotations

from datetime import date

import pytest

from src.common.job_params import JobParams


def test_incremental_params_require_window_and_generate_run_id() -> None:
    params = JobParams.from_mapping(
        {
            "run_mode": "incremental",
            "start_ts": "2026-02-06T00:00:00Z",
            "end_ts": "2026-02-06T00:10:00Z",
        },
        pipeline_name="pipeline_a",
    )
    assert params.run_mode == "incremental"
    assert params.run_id.startswith("pipeline_a_")
    assert params.target_dates() == [date(2026, 2, 6)]


def test_backfill_params_accept_date_range() -> None:
    params = JobParams.from_mapping(
        {
            "run_mode": "backfill",
            "date_kst_start": "2026-02-03",
            "date_kst_end": "2026-02-05",
            "run_id": "manual-run",
        },
        pipeline_name="pipeline_b",
    )
    assert params.run_id == "manual-run"
    assert params.target_dates() == [
        date(2026, 2, 3),
        date(2026, 2, 4),
        date(2026, 2, 5),
    ]
    windows = params.windows_utc()
    assert len(windows) == 3
    assert windows[0][0].isoformat() == "2026-02-02T15:00:00+00:00"
    assert windows[-1][1].isoformat() == "2026-02-05T15:00:00+00:00"


def test_backfill_derives_dates_from_window() -> None:
    params = JobParams.from_mapping(
        {
            "run_mode": "backfill",
            "start_ts": "2026-02-03T00:00:00Z",
            "end_ts": "2026-02-05T00:00:00Z",
        },
        pipeline_name="pipeline_b",
    )
    assert params.date_kst_start == date(2026, 2, 3)
    assert params.date_kst_end == date(2026, 2, 5)


def test_incremental_without_window_raises() -> None:
    with pytest.raises(ValueError):
        JobParams.from_mapping(
            {"run_mode": "incremental"},
            pipeline_name="pipeline_a",
        )


def test_invalid_run_mode_raises() -> None:
    with pytest.raises(ValueError):
        JobParams.from_mapping(
            {
                "run_mode": "nightly",
                "start_ts": "2026-02-06T00:00:00Z",
                "end_ts": "2026-02-06T00:10:00Z",
            },
            pipeline_name="pipeline_a",
        )
