from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.common.time_utils import KST, UTC, date_kst, normalize_window, to_kst, to_utc


def test_to_kst_from_naive_utc() -> None:
    naive_utc = datetime(2026, 2, 4, 0, 0, 0)
    kst_value = to_kst(naive_utc)
    assert kst_value.tzinfo == KST
    assert kst_value.hour == 9
    assert kst_value.date().isoformat() == "2026-02-04"


def test_date_kst_rollover() -> None:
    dt_utc = datetime(2026, 2, 3, 16, 0, 0, tzinfo=timezone.utc)
    assert date_kst(dt_utc).isoformat() == "2026-02-04"


def test_normalize_window() -> None:
    start = datetime(2026, 2, 4, 0, 0, 0)
    end = datetime(2026, 2, 4, 1, 0, 0)
    start_utc, end_utc = normalize_window(start, end)
    assert start_utc.tzinfo == UTC
    assert end_utc.tzinfo == UTC
    assert start_utc < end_utc


def test_normalize_window_invalid() -> None:
    start = datetime(2026, 2, 4, 1, 0, 0)
    end = datetime(2026, 2, 4, 1, 0, 0)
    with pytest.raises(ValueError):
        normalize_window(start, end)


def test_to_utc_from_kst() -> None:
    kst_time = datetime(2026, 2, 4, 9, 0, 0, tzinfo=KST)
    utc_value = to_utc(kst_time)
    assert utc_value.tzinfo == UTC
    assert utc_value.hour == 0
