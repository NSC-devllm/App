from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.io.rule_loader import load_rule_table, load_runtime_rules


class _DummyRow:
    def __init__(self, payload: dict):
        self._payload = payload

    def asDict(self, recursive: bool = False) -> dict:
        return dict(self._payload)


class _DummyTable:
    def __init__(self, rows: list[dict]):
        self._rows = rows

    def collect(self) -> list[_DummyRow]:
        return [_DummyRow(row) for row in self._rows]


class _DummyCatalog:
    def __init__(self, table_exists: bool):
        self._table_exists = table_exists

    def tableExists(self, table_fqn: str) -> bool:  # noqa: N802
        return self._table_exists


class _DummySpark:
    def __init__(self, *, table_exists: bool, rows: list[dict] | None = None):
        self.catalog = _DummyCatalog(table_exists=table_exists)
        self._rows = rows or []

    def table(self, table_fqn: str) -> _DummyTable:
        return _DummyTable(self._rows)


def test_load_rule_table_parses_rows() -> None:
    spark = _DummySpark(
        table_exists=True,
        rows=[
            {
                "rule_id": "ledger_recon_table_v1",
                "domain": "ledger",
                "metric": "drift_abs",
                "threshold": 0.0,
                "severity_map": {"crit": 0.0},
                "allowed_values": None,
                "comment": "table loaded",
                "effective_start_ts": "2026-02-01T00:00:00Z",
                "effective_end_ts": None,
                "is_current": True,
            }
        ],
    )
    rules = load_rule_table(spark, "cat.gold.dim_rule_scd2")
    assert len(rules) == 1
    assert rules[0].rule_id == "ledger_recon_table_v1"
    assert rules[0].metric == "drift_abs"


def test_load_runtime_rules_strict_raises_when_table_missing(tmp_path: Path) -> None:
    seed_path = tmp_path / "rules.json"
    seed_path.write_text(
        json.dumps(
            [
                {
                    "rule_id": "seed_rule",
                    "domain": "dq",
                    "metric": "freshness_sec",
                    "threshold": 300,
                }
            ]
        ),
        encoding="utf-8",
    )
    spark = _DummySpark(table_exists=False)

    with pytest.raises(RuntimeError):
        load_runtime_rules(
            spark,
            catalog="cat",
            mode="strict",
            seed_path=seed_path,
            table_name="gold.dim_rule_scd2",
        )


def test_load_runtime_rules_fallback_uses_seed(tmp_path: Path) -> None:
    seed_path = tmp_path / "rules.json"
    seed_path.write_text(
        json.dumps(
            [
                {
                    "rule_id": "seed_rule",
                    "domain": "dq",
                    "metric": "freshness_sec",
                    "threshold": 300,
                }
            ]
        ),
        encoding="utf-8",
    )
    spark = _DummySpark(table_exists=False)

    rules = load_runtime_rules(
        spark,
        catalog="cat",
        mode="fallback",
        seed_path=seed_path,
        table_name="gold.dim_rule_scd2",
    )

    assert len(rules) == 1
    assert rules[0].rule_id == "seed_rule"


def test_load_rule_table_rejects_duplicate_current_rules() -> None:
    spark = _DummySpark(
        table_exists=True,
        rows=[
            {
                "rule_id": "rule_v1",
                "domain": "ledger",
                "metric": "drift_abs",
                "threshold": 0.0,
                "effective_start_ts": "2026-01-01T00:00:00Z",
                "effective_end_ts": None,
                "is_current": True,
            },
            {
                "rule_id": "rule_v2",
                "domain": "ledger",
                "metric": "drift_abs",
                "threshold": 0.1,
                "effective_start_ts": "2026-02-01T00:00:00Z",
                "effective_end_ts": None,
                "is_current": True,
            },
        ],
    )
    with pytest.raises(ValueError, match="Multiple is_current=true"):
        load_rule_table(spark, "cat.gold.dim_rule_scd2")


def test_load_runtime_rules_fallback_does_not_mask_invalid_table_payload(
    tmp_path: Path,
) -> None:
    seed_path = tmp_path / "rules.json"
    seed_path.write_text(
        json.dumps(
            [
                {
                    "rule_id": "seed_rule",
                    "domain": "dq",
                    "metric": "freshness_sec",
                    "threshold": 300,
                }
            ]
        ),
        encoding="utf-8",
    )
    spark = _DummySpark(
        table_exists=True,
        rows=[
            {
                # Invalid runtime row: missing rule_id
                "domain": "dq",
                "metric": "freshness_sec",
                "threshold": 300,
            }
        ],
    )

    with pytest.raises(RuntimeError, match="Failed to load runtime rules from table"):
        load_runtime_rules(
            spark,
            catalog="cat",
            mode="fallback",
            seed_path=seed_path,
            table_name="gold.dim_rule_scd2",
        )
