from __future__ import annotations

import shutil
from datetime import datetime

import pytest

pyspark = pytest.importorskip("pyspark")

# PySpark requires a JVM. Keep this test opt-in for local development
# environments that have both pyspark and Java installed.
if shutil.which("java") is None:
    pytest.skip("Java is required to run local PySpark tests (install OpenJDK).")
from pyspark.sql import SparkSession  # noqa: E402

from scripts.run_pipeline_silver import _filter_to_windows  # noqa: E402
from src.common.time_utils import UTC  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pipeline-silver-window-filter-integration")
        .getOrCreate()
    )
    yield session
    session.stop()


def _dt(year: int, month: int, day: int, hour: int = 0) -> datetime:
    return datetime(year, month, day, hour, 0, tzinfo=UTC)


def test_filter_to_windows_keeps_rows_when_any_timestamp_field_matches(spark) -> None:
    rows = [
        {
            "id": "updated_null_source_in",
            "updated_at": None,
            "created_at": None,
            "source_extracted_at": _dt(2026, 2, 11, 1),
            "ingested_at": None,
        },
        {
            "id": "created_null_ingested_in",
            "updated_at": None,
            "created_at": None,
            "source_extracted_at": None,
            "ingested_at": _dt(2026, 2, 11, 2),
        },
        {
            "id": "primary_out_fallback_in",
            "updated_at": _dt(2026, 2, 10, 23),
            "created_at": None,
            "source_extracted_at": _dt(2026, 2, 11, 3),
            "ingested_at": None,
        },
        {
            "id": "all_null",
            "updated_at": None,
            "created_at": None,
            "source_extracted_at": None,
            "ingested_at": None,
        },
        {
            "id": "all_out_window",
            "updated_at": _dt(2026, 2, 10, 23),
            "created_at": _dt(2026, 2, 10, 23),
            "source_extracted_at": _dt(2026, 2, 10, 23),
            "ingested_at": _dt(2026, 2, 12, 1),
        },
    ]
    df = spark.createDataFrame(rows)
    windows = [(_dt(2026, 2, 11, 0), _dt(2026, 2, 12, 0))]

    filtered = _filter_to_windows(
        df,
        windows,
        ("updated_at", "created_at", "source_extracted_at", "ingested_at"),
    )
    actual_ids = {row["id"] for row in filtered.select("id").collect()}

    assert actual_ids == {
        "updated_null_source_in",
        "created_null_ingested_in",
        "primary_out_fallback_in",
    }


def test_filter_to_windows_supports_or_across_multiple_windows(spark) -> None:
    rows = [
        {
            "id": "in_first_window",
            "updated_at": _dt(2026, 2, 11, 6),
            "ingested_at": None,
        },
        {
            "id": "in_second_window_by_fallback",
            "updated_at": _dt(2026, 2, 12, 12),
            "ingested_at": _dt(2026, 2, 13, 1),
        },
        {
            "id": "outside_all",
            "updated_at": _dt(2026, 2, 14, 2),
            "ingested_at": None,
        },
    ]
    df = spark.createDataFrame(rows)
    windows = [
        (_dt(2026, 2, 11, 0), _dt(2026, 2, 12, 0)),
        (_dt(2026, 2, 13, 0), _dt(2026, 2, 14, 0)),
    ]

    filtered = _filter_to_windows(df, windows, ("updated_at", "ingested_at"))
    actual_ids = {row["id"] for row in filtered.select("id").collect()}

    assert actual_ids == {"in_first_window", "in_second_window_by_fallback"}
