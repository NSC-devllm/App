from __future__ import annotations

import pytest

import scripts.ops.cleanup_bad_records as cleanup


class _DummyRow:
    def __init__(self, payload: dict):
        self._payload = payload

    def asDict(self, recursive: bool = False) -> dict:
        return dict(self._payload)


class _DummyDataFrame:
    def __init__(self, rows: list[dict]):
        self._rows = [_DummyRow(row) for row in rows]


class _DummyCatalog:
    def __init__(self, table_exists: bool):
        self._table_exists = table_exists

    def tableExists(self, table_fqn: str) -> bool:  # noqa: N802
        return self._table_exists


class _DummyConf:
    def __init__(self):
        self.settings: list[tuple[str, str]] = []

    def set(self, key: str, value: str) -> None:
        self.settings.append((key, value))


class _DummySpark:
    def __init__(self, *, table_exists: bool = True, candidate_count: int = 0):
        self.catalog = _DummyCatalog(table_exists=table_exists)
        self.conf = _DummyConf()
        self._candidate_count = candidate_count
        self.queries: list[str] = []

    def sql(self, query: str):
        self.queries.append(query)
        if query.strip().upper().startswith("SELECT COUNT(*)"):
            return _DummyDataFrame([{"candidate_count": self._candidate_count}])
        return _DummyDataFrame([])


@pytest.fixture(autouse=True)
def _patch_safe_collect(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(cleanup, "safe_collect", lambda df, **_kwargs: list(df._rows))


def test_parse_args_supports_dry_run_flag_and_value() -> None:
    args = cleanup.parse_args(["--catalog", "cat"])
    assert args.dry_run is False

    args = cleanup.parse_args(["--catalog", "cat", "--dry-run"])
    assert args.dry_run is True

    args = cleanup.parse_args(["--catalog", "cat", "--dry-run", "true"])
    assert args.dry_run is True

    args = cleanup.parse_args(["--catalog", "cat", "--dry-run", "false"])
    assert args.dry_run is False


def test_run_cleanup_dry_run_executes_count_only() -> None:
    spark = _DummySpark(table_exists=True, candidate_count=7)
    count = cleanup.run_cleanup(
        spark=spark,
        catalog="hive_metastore",
        retention_days=180,
        dry_run=True,
    )
    assert count == 7
    assert len(spark.queries) == 1
    assert "SELECT COUNT(*) AS candidate_count" in spark.queries[0]
    assert "DELETE FROM" not in spark.queries[0]
    assert ("spark.sql.session.timeZone", "Asia/Seoul") in spark.conf.settings


def test_run_cleanup_execute_runs_delete() -> None:
    spark = _DummySpark(table_exists=True, candidate_count=3)
    count = cleanup.run_cleanup(
        spark=spark,
        catalog="hive_metastore",
        retention_days=180,
        dry_run=False,
    )
    assert count == 3
    assert len(spark.queries) == 2
    assert spark.queries[1].startswith("DELETE FROM")


def test_run_cleanup_rejects_non_positive_retention_days() -> None:
    spark = _DummySpark(table_exists=True, candidate_count=1)
    with pytest.raises(ValueError, match="retention_days must be > 0"):
        cleanup.run_cleanup(
            spark=spark,
            catalog="hive_metastore",
            retention_days=0,
            dry_run=True,
        )


def test_run_cleanup_raises_when_table_not_found() -> None:
    spark = _DummySpark(table_exists=False, candidate_count=1)
    with pytest.raises(RuntimeError, match="Target table not found"):
        cleanup.run_cleanup(
            spark=spark,
            catalog="hive_metastore",
            retention_days=180,
            dry_run=True,
        )
    assert spark.queries == []
