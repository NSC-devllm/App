from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any, Mapping

from src.common.run_tracking import generate_run_id
from src.common.time_utils import KST, date_kst, normalize_window, now_utc, to_utc

RUN_MODE_INCREMENTAL = "incremental"
RUN_MODE_BACKFILL = "backfill"
VALID_RUN_MODES = {RUN_MODE_INCREMENTAL, RUN_MODE_BACKFILL}


def _parse_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return to_utc(value)
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        return to_utc(datetime.fromisoformat(normalized))
    raise TypeError(f"Unsupported datetime parameter: {value!r}")


def _parse_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return date_kst(value)
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise TypeError(f"Unsupported date parameter: {value!r}")


@dataclass(frozen=True)
class JobParams:
    pipeline_name: str
    run_mode: str
    run_id: str
    start_ts: datetime | None
    end_ts: datetime | None
    date_kst_start: date | None
    date_kst_end: date | None

    @classmethod
    def from_mapping(
        cls,
        payload: Mapping[str, Any],
        *,
        pipeline_name: str,
    ) -> "JobParams":
        run_mode_raw = payload.get("run_mode", RUN_MODE_INCREMENTAL)
        run_mode = str(run_mode_raw).strip().lower()
        if run_mode not in VALID_RUN_MODES:
            raise ValueError(f"invalid run_mode: {run_mode_raw}")

        run_id_raw = payload.get("run_id")
        run_id = str(run_id_raw) if run_id_raw else generate_run_id(pipeline_name)

        start_ts = _parse_datetime(payload.get("start_ts"))
        end_ts = _parse_datetime(payload.get("end_ts"))
        if start_ts and end_ts:
            start_ts, end_ts = normalize_window(start_ts, end_ts)

        date_start = _parse_date(payload.get("date_kst_start"))
        date_end = _parse_date(payload.get("date_kst_end"))

        if run_mode == RUN_MODE_BACKFILL:
            if date_start is None and start_ts is not None:
                date_start = date_kst(start_ts)
            if date_end is None and end_ts is not None:
                date_end = date_kst(end_ts - timedelta(microseconds=1))
            if date_start is None or date_end is None:
                raise ValueError(
                    "backfill mode requires date_kst_start/date_kst_end or start_ts/end_ts"
                )
            if date_start > date_end:
                raise ValueError("date_kst_start must be <= date_kst_end")

        if run_mode == RUN_MODE_INCREMENTAL:
            if start_ts is None or end_ts is None:
                raise ValueError("incremental mode requires start_ts and end_ts")

        return cls(
            pipeline_name=pipeline_name,
            run_mode=run_mode,
            run_id=run_id,
            start_ts=start_ts,
            end_ts=end_ts,
            date_kst_start=date_start,
            date_kst_end=date_end,
        )

    def target_dates(self) -> list[date]:
        if self.date_kst_start and self.date_kst_end:
            days = (self.date_kst_end - self.date_kst_start).days
            return [
                self.date_kst_start + timedelta(days=offset)
                for offset in range(days + 1)
            ]

        if self.start_ts and self.end_ts:
            start_day = date_kst(self.start_ts)
            end_probe = self.end_ts - timedelta(microseconds=1)
            end_day = date_kst(end_probe)
            days = (end_day - start_day).days
            return [start_day + timedelta(days=offset) for offset in range(days + 1)]

        return []

    def processed_end_utc(self) -> datetime:
        if self.end_ts is not None:
            return self.end_ts
        if self.date_kst_end is not None:
            next_day = self.date_kst_end + timedelta(days=1)
            next_day_kst = datetime.combine(next_day, time.min, tzinfo=KST)
            return to_utc(next_day_kst)
        return now_utc()

    def windows_utc(self) -> list[tuple[datetime, datetime]]:
        if self.run_mode == RUN_MODE_INCREMENTAL:
            if self.start_ts is None or self.end_ts is None:
                raise ValueError("incremental mode requires start_ts/end_ts window")
            return [(self.start_ts, self.end_ts)]

        windows: list[tuple[datetime, datetime]] = []
        for day in self.target_dates():
            start_kst = datetime.combine(day, time.min, tzinfo=KST)
            end_kst = start_kst + timedelta(days=1)
            windows.append((to_utc(start_kst), to_utc(end_kst)))
        return windows
