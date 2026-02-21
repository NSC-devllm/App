from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

from src.common.time_utils import now_utc, to_utc
from src.io.merge_utils import merge_delta_table

STATE_SUCCESS = "success"
STATE_FAILURE = "failure"


@dataclass(frozen=True)
class PipelineStateRecord:
    pipeline_name: str
    last_success_ts: datetime | None
    last_processed_end: datetime | None
    last_run_id: str | None
    dq_zero_window_counts: str | None
    updated_at: datetime

    def as_dict(self) -> dict[str, Any]:
        return {
            "pipeline_name": self.pipeline_name,
            "last_success_ts": self.last_success_ts,
            "last_processed_end": self.last_processed_end,
            "last_run_id": self.last_run_id,
            "dq_zero_window_counts": self.dq_zero_window_counts,
            "updated_at": self.updated_at,
        }


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return to_utc(value)
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        return to_utc(datetime.fromisoformat(normalized))
    raise TypeError(f"Unsupported datetime value: {value!r}")


def parse_pipeline_state_record(payload: Mapping[str, Any]) -> PipelineStateRecord:
    pipeline_name = payload.get("pipeline_name")
    if not pipeline_name:
        raise ValueError("pipeline_name is required")
    updated_at = _parse_datetime(payload.get("updated_at")) or now_utc()
    return PipelineStateRecord(
        pipeline_name=str(pipeline_name),
        last_success_ts=_parse_datetime(payload.get("last_success_ts")),
        last_processed_end=_parse_datetime(payload.get("last_processed_end")),
        last_run_id=payload.get("last_run_id"),
        dq_zero_window_counts=payload.get("dq_zero_window_counts"),
        updated_at=updated_at,
    )


def parse_zero_window_counts(value: str | None) -> dict[str, int]:
    if not value:
        return {}
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    counts: dict[str, int] = {}
    for key, raw_value in payload.items():
        if raw_value is None:
            continue
        try:
            counts[str(key)] = max(int(raw_value), 0)
        except (TypeError, ValueError):
            continue
    return counts


def serialize_zero_window_counts(counts: Mapping[str, int]) -> str:
    sanitized = {str(key): max(int(value), 0) for key, value in counts.items()}
    return json.dumps(sanitized, ensure_ascii=True, sort_keys=True)


def apply_pipeline_state(
    *,
    pipeline_name: str,
    run_id: str,
    status: str,
    current_state: PipelineStateRecord | None = None,
    last_processed_end: datetime | None = None,
    event_ts: datetime | None = None,
    dq_zero_window_counts: str | None = None,
) -> PipelineStateRecord:
    status_normalized = status.strip().lower()
    if status_normalized not in {STATE_SUCCESS, STATE_FAILURE}:
        raise ValueError(f"invalid pipeline state status: {status}")

    ts = to_utc(event_ts) if event_ts is not None else now_utc()
    current = current_state or PipelineStateRecord(
        pipeline_name=pipeline_name,
        last_success_ts=None,
        last_processed_end=None,
        last_run_id=None,
        dq_zero_window_counts=None,
        updated_at=ts,
    )

    if status_normalized == STATE_SUCCESS:
        if last_processed_end is None:
            raise ValueError("last_processed_end is required for success updates")
        return PipelineStateRecord(
            pipeline_name=pipeline_name,
            last_success_ts=ts,
            last_processed_end=to_utc(last_processed_end),
            last_run_id=run_id,
            dq_zero_window_counts=dq_zero_window_counts,
            updated_at=ts,
        )

    return PipelineStateRecord(
        pipeline_name=pipeline_name,
        last_success_ts=current.last_success_ts,
        last_processed_end=current.last_processed_end,
        last_run_id=run_id,
        dq_zero_window_counts=current.dq_zero_window_counts,
        updated_at=ts,
    )


def write_pipeline_state_delta(  # pragma: no cover
    df,
    table_fqn: str,
    *,
    mode: str = "overwrite",
) -> None:
    spark = df.sparkSession
    if spark.catalog.tableExists(table_fqn):
        merge_delta_table(df, table_fqn, ("pipeline_name",))
        return
    # Avoid atomic replace semantics on initial CREATE TABLE (Unity Catalog + ADLS).
    df.write.format("delta").mode("append").saveAsTable(table_fqn)
