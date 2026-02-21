from __future__ import annotations

from datetime import datetime

import pytest

from src.common.time_utils import UTC
from src.io.pipeline_state_io import PipelineStateRecord
from src.io.upstream_readiness import (
    UpstreamReadinessError,
    validate_upstream_state_record,
)


def _state(
    *,
    last_success_ts: datetime | None,
    last_processed_end: datetime | None,
) -> PipelineStateRecord:
    return PipelineStateRecord(
        pipeline_name="pipeline_silver",
        last_success_ts=last_success_ts,
        last_processed_end=last_processed_end,
        last_run_id="run-1",
        dq_zero_window_counts=None,
        updated_at=datetime(2026, 2, 11, 0, 0, tzinfo=UTC),
    )


def test_validate_upstream_state_record_requires_last_success() -> None:
    with pytest.raises(UpstreamReadinessError):
        validate_upstream_state_record(
            _state(
                last_success_ts=None,
                last_processed_end=datetime(2026, 2, 11, 15, 0, tzinfo=UTC),
            ),
            upstream_pipeline_name="pipeline_silver",
            required_processed_end=datetime(2026, 2, 11, 15, 0, tzinfo=UTC),
        )


def test_validate_upstream_state_record_requires_fresh_processed_end() -> None:
    with pytest.raises(UpstreamReadinessError):
        validate_upstream_state_record(
            _state(
                last_success_ts=datetime(2026, 2, 11, 15, 0, tzinfo=UTC),
                last_processed_end=datetime(2026, 2, 11, 14, 59, tzinfo=UTC),
            ),
            upstream_pipeline_name="pipeline_silver",
            required_processed_end=datetime(2026, 2, 11, 15, 0, tzinfo=UTC),
        )


def test_validate_upstream_state_record_passes_when_ready() -> None:
    validate_upstream_state_record(
        _state(
            last_success_ts=datetime(2026, 2, 11, 15, 0, tzinfo=UTC),
            last_processed_end=datetime(2026, 2, 11, 15, 0, tzinfo=UTC),
        ),
        upstream_pipeline_name="pipeline_silver",
        required_processed_end=datetime(2026, 2, 11, 15, 0, tzinfo=UTC),
    )
