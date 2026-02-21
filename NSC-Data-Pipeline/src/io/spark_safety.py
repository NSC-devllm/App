from __future__ import annotations

from collections.abc import Sequence
from typing import Any


class CollectLimitExceededError(RuntimeError):
    """Raised when a DataFrame collect exceeds the configured row cap."""


def assert_max_rows(df: Any, *, max_rows: int, context: str) -> Sequence[Any]:
    """Collect at most max_rows rows from a Spark DataFrame-like object."""
    if max_rows <= 0:
        raise ValueError(f"max_rows must be positive (context={context})")

    if hasattr(df, "limit"):
        rows = df.limit(max_rows + 1).collect()
    else:
        rows = df.collect()

    if len(rows) > max_rows:
        raise CollectLimitExceededError(
            f"safe_collect exceeded row cap (context={context}, max_rows={max_rows})"
        )
    return rows


def safe_collect(df: Any, *, max_rows: int, context: str) -> Sequence[Any]:
    return assert_max_rows(df, max_rows=max_rows, context=context)
