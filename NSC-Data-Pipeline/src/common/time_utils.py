from __future__ import annotations

from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

UTC = timezone.utc
KST = ZoneInfo("Asia/Seoul")


def ensure_tzaware(value: datetime, default_tz: timezone = UTC) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=default_tz)
    return value


def to_utc(value: datetime) -> datetime:
    return ensure_tzaware(value).astimezone(UTC)


def to_kst(value: datetime) -> datetime:
    return ensure_tzaware(value).astimezone(KST)


def date_kst(value: datetime) -> date:
    return to_kst(value).date()


def normalize_window(start_ts: datetime, end_ts: datetime) -> tuple[datetime, datetime]:
    start_utc = to_utc(start_ts)
    end_utc = to_utc(end_ts)
    if start_utc >= end_utc:
        raise ValueError("window start must be earlier than end")
    return start_utc, end_utc


def now_utc() -> datetime:
    return datetime.now(tz=UTC)
