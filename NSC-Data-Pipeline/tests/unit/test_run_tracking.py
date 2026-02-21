from __future__ import annotations

from datetime import datetime

from src.common.run_tracking import PipelineStateUpdate, generate_run_id


def test_generate_run_id_prefix() -> None:
    run_id = generate_run_id(prefix="pipeline_a")
    assert run_id.startswith("pipeline_a_")


def test_pipeline_state_update_defaults() -> None:
    processed_end = datetime(2026, 2, 4, 1, 0, 0)
    update = PipelineStateUpdate.for_success(
        pipeline_name="pipeline_a",
        run_id="run_123",
        last_processed_end=processed_end,
    )
    assert update.pipeline_name == "pipeline_a"
    assert update.last_run_id == "run_123"
    assert update.last_processed_end.tzinfo is not None
    assert update.last_success_ts.tzinfo is not None
    assert update.updated_at.tzinfo is not None
