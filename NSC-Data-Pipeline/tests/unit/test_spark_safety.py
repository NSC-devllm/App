from __future__ import annotations

import pytest

from src.io.spark_safety import CollectLimitExceededError, safe_collect


class _DummyDataFrame:
    def __init__(self, rows: list[dict]):
        self._rows = rows

    def limit(self, size: int):
        return _DummyDataFrame(self._rows[:size])

    def collect(self) -> list[dict]:
        return list(self._rows)


def test_safe_collect_returns_rows_within_cap() -> None:
    df = _DummyDataFrame(rows=[{"id": 1}, {"id": 2}])
    rows = safe_collect(df, max_rows=2, context="unit-test")
    assert rows == [{"id": 1}, {"id": 2}]


def test_safe_collect_raises_when_rows_exceed_cap() -> None:
    df = _DummyDataFrame(rows=[{"id": 1}, {"id": 2}, {"id": 3}])
    with pytest.raises(CollectLimitExceededError):
        safe_collect(df, max_rows=2, context="unit-test")


def test_safe_collect_requires_positive_cap() -> None:
    df = _DummyDataFrame(rows=[{"id": 1}])
    with pytest.raises(ValueError):
        safe_collect(df, max_rows=0, context="unit-test")
