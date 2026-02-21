from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Mapping

from src.common.time_utils import date_kst, now_utc, to_utc

WINDOW_PARAM_KEYS = ("start_ts", "end_ts", "date_kst_start", "date_kst_end")
PIPELINE_A_DEFAULT_WINDOW_MINUTES = 10


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return False


def inject_default_daily_backfill(
    payload: Mapping[str, Any],
    *,
    now_ts: datetime | None = None,
) -> dict[str, Any]:
    """Default empty window parameters to one-day KST backfill.

    If `run_mode/start_ts/end_ts/date_kst_start/date_kst_end` are all blank,
    this helper sets:
    - run_mode=backfill
    - date_kst_start=date_kst_end=yesterday(KST)
    """

    resolved = dict(payload)
    should_inject = _is_blank(resolved.get("run_mode")) and all(
        _is_blank(resolved.get(key)) for key in WINDOW_PARAM_KEYS
    )
    if not should_inject:
        return resolved

    baseline = now_ts or now_utc()
    target_date = date_kst(baseline) - timedelta(days=1)
    target_iso = target_date.isoformat()
    resolved["run_mode"] = "backfill"
    resolved["date_kst_start"] = target_iso
    resolved["date_kst_end"] = target_iso
    return resolved


def inject_pipeline_a_default_incremental_window(
    payload: Mapping[str, Any],
    *,
    last_processed_end: datetime | None = None,
    now_ts: datetime | None = None,
    default_window_minutes: int = PIPELINE_A_DEFAULT_WINDOW_MINUTES,
) -> dict[str, Any]:
    """Default Pipeline A empty parameters to an incremental UTC window.

    Injection applies only when all window parameters are blank and run_mode is
    blank/incremental. In that case:
    - run_mode=incremental
    - end_ts=now_utc()
    - start_ts=last_processed_end (if available) else now_utc()-10m
    """

    if default_window_minutes <= 0:
        raise ValueError("default_window_minutes must be > 0")

    resolved = dict(payload)
    run_mode_raw = resolved.get("run_mode")
    run_mode = str(run_mode_raw).strip().lower() if not _is_blank(run_mode_raw) else ""
    if run_mode not in {"", "incremental"}:
        return resolved

    window_all_blank = all(_is_blank(resolved.get(key)) for key in WINDOW_PARAM_KEYS)
    if not window_all_blank:
        return resolved

    baseline_end = to_utc(now_ts) if now_ts is not None else now_utc()
    baseline_start = (
        to_utc(last_processed_end)
        if last_processed_end is not None
        else baseline_end - timedelta(minutes=default_window_minutes)
    )
    if baseline_start >= baseline_end:
        baseline_start = baseline_end - timedelta(minutes=default_window_minutes)

    resolved["run_mode"] = "incremental"
    resolved["start_ts"] = baseline_start
    resolved["end_ts"] = baseline_end
    return resolved
