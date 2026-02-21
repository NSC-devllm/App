from __future__ import annotations

from datetime import datetime

from src.common.time_utils import to_utc
from src.io.pipeline_state_io import PipelineStateRecord, parse_pipeline_state_record
from src.io.spark_safety import safe_collect


class UpstreamReadinessError(RuntimeError):
    """Raised when an upstream pipeline checkpoint is not ready."""


def validate_upstream_state_record(
    state: PipelineStateRecord,
    *,
    upstream_pipeline_name: str,
    required_processed_end: datetime,
) -> None:
    required_end_utc = to_utc(required_processed_end)
    if state.last_success_ts is None:
        raise UpstreamReadinessError(
            f"upstream pipeline has no last_success_ts: {upstream_pipeline_name}"
        )
    if state.last_processed_end is None:
        raise UpstreamReadinessError(
            f"upstream pipeline has no last_processed_end: {upstream_pipeline_name}"
        )
    if state.last_processed_end < required_end_utc:
        raise UpstreamReadinessError(
            "upstream pipeline is stale: "
            f"{upstream_pipeline_name} last_processed_end={state.last_processed_end.isoformat()} "
            f"< required={required_end_utc.isoformat()}"
        )


def assert_pipeline_ready(
    spark,
    *,
    catalog: str,
    upstream_pipeline_name: str,
    required_processed_end: datetime,
) -> None:
    from pyspark.sql import functions as F

    table_fqn = f"{catalog}.gold.pipeline_state"
    if not spark.catalog.tableExists(table_fqn):
        raise UpstreamReadinessError(
            f"pipeline_state table not found: {table_fqn} "
            f"(required upstream={upstream_pipeline_name})"
        )

    rows = safe_collect(
        spark.table(table_fqn).filter(
            F.col("pipeline_name") == F.lit(upstream_pipeline_name)
        ),
        max_rows=1,
        context=f"upstream_readiness:{upstream_pipeline_name}",
    )
    if not rows:
        raise UpstreamReadinessError(
            "upstream pipeline_state row not found: "
            f"{upstream_pipeline_name} in {table_fqn}"
        )

    state = parse_pipeline_state_record(rows[0].asDict(recursive=True))
    validate_upstream_state_record(
        state,
        upstream_pipeline_name=upstream_pipeline_name,
        required_processed_end=required_processed_end,
    )
