from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import uuid4

from src.common.time_utils import now_utc, to_utc


def generate_run_id(prefix: str | None = None) -> str:
    run_id = str(uuid4())
    if prefix:
        return f"{prefix}_{run_id}"
    return run_id


@dataclass(frozen=True)
class PipelineStateUpdate:
    pipeline_name: str
    last_success_ts: datetime
    last_processed_end: datetime
    last_run_id: str
    updated_at: datetime

    @classmethod
    def for_success(
        cls,
        pipeline_name: str,
        run_id: str,
        last_processed_end: datetime,
        last_success_ts: datetime | None = None,
        updated_at: datetime | None = None,
    ) -> "PipelineStateUpdate":
        success_ts = to_utc(last_success_ts) if last_success_ts else now_utc()
        processed_end = to_utc(last_processed_end)
        updated = to_utc(updated_at) if updated_at else now_utc()
        return cls(
            pipeline_name=pipeline_name,
            last_success_ts=success_ts,
            last_processed_end=processed_end,
            last_run_id=run_id,
            updated_at=updated,
        )
