from __future__ import annotations

from datetime import datetime

import pytest

from src.common.job_params import JobParams
from src.common.time_utils import UTC
from src.io.pipeline_state_io import PipelineStateRecord
from src.io.upstream_readiness import (
    UpstreamReadinessError,
    validate_upstream_state_record,
)


def _pipeline_silver_state(*, processed_end: datetime) -> PipelineStateRecord:
    return PipelineStateRecord(
        pipeline_name="pipeline_silver",
        last_success_ts=processed_end,
        last_processed_end=processed_end,
        last_run_id="run-silver-ready",
        dq_zero_window_counts=None,
        updated_at=processed_end,
    )


def test_pipeline_c_readiness_fails_when_upstream_missing_window() -> None:
    params = JobParams.from_mapping(
        {
            "run_mode": "backfill",
            "date_kst_start": "2026-02-11",
            "date_kst_end": "2026-02-11",
            "run_id": "run-c",
        },
        pipeline_name="pipeline_c",
    )
    stale_state = _pipeline_silver_state(
        processed_end=datetime(2026, 2, 11, 14, 59, tzinfo=UTC)
    )
    with pytest.raises(UpstreamReadinessError):
        validate_upstream_state_record(
            stale_state,
            upstream_pipeline_name="pipeline_silver",
            required_processed_end=params.processed_end_utc(),
        )


def test_pipeline_c_readiness_passes_when_upstream_ready() -> None:
    params = JobParams.from_mapping(
        {
            "run_mode": "backfill",
            "date_kst_start": "2026-02-11",
            "date_kst_end": "2026-02-11",
            "run_id": "run-c",
        },
        pipeline_name="pipeline_c",
    )
    fresh_state = _pipeline_silver_state(
        processed_end=datetime(2026, 2, 11, 15, 0, tzinfo=UTC)
    )
    validate_upstream_state_record(
        fresh_state,
        upstream_pipeline_name="pipeline_silver",
        required_processed_end=params.processed_end_utc(),
    )
