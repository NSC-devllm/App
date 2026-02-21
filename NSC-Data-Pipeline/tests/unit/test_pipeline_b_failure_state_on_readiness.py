from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("pyspark")

import scripts.run_pipeline_b as run_pipeline_b
from src.io.pipeline_state_io import STATE_FAILURE
from src.io.upstream_readiness import UpstreamReadinessError


def _args(task: str) -> SimpleNamespace:
    return SimpleNamespace(
        catalog="hive_metastore",
        task=task,
        run_mode="backfill",
        start_ts="",
        end_ts="",
        date_kst_start="2026-02-11",
        date_kst_end="2026-02-11",
        run_id="run-readiness-fail",
        rule_load_mode="fallback",
        rule_table="gold.dim_rule_scd2",
        rule_seed_path="mock_data/fixtures/dim_rule_scd2.json",
        repo_root=".",
        skip_upstream_readiness_check=False,
    )


def _setup_readiness_failure(monkeypatch: pytest.MonkeyPatch, *, task: str):
    calls: list[dict] = []

    monkeypatch.setattr(run_pipeline_b, "parse_args", lambda: _args(task))
    monkeypatch.setattr(
        run_pipeline_b.SparkSession.builder,
        "getOrCreate",
        lambda: object(),
    )

    def _raise_readiness(*_args, **_kwargs):
        raise UpstreamReadinessError("pipeline_silver checkpoint stale")

    monkeypatch.setattr(
        "src.io.upstream_readiness.assert_pipeline_ready",
        _raise_readiness,
    )

    def _capture_write_pipeline_state(*_args, **kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(
        run_pipeline_b, "_write_pipeline_state", _capture_write_pipeline_state
    )
    return calls


def test_pipeline_b_task_all_writes_failure_state_when_readiness_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _setup_readiness_failure(monkeypatch, task=run_pipeline_b.TASK_ALL)

    with pytest.raises(UpstreamReadinessError):
        run_pipeline_b.main()

    assert len(calls) == 1
    assert calls[0]["pipeline_name"] == "pipeline_b"
    assert calls[0]["run_id"] == "run-readiness-fail"
    assert calls[0]["status"] == STATE_FAILURE
    assert calls[0]["last_processed_end"] is None


def test_pipeline_b_task_recon_does_not_write_failure_state_on_readiness_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _setup_readiness_failure(monkeypatch, task=run_pipeline_b.TASK_RECON)

    with pytest.raises(UpstreamReadinessError):
        run_pipeline_b.main()

    assert calls == []
