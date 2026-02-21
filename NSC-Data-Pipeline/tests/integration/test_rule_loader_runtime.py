from __future__ import annotations

# ruff: noqa: E402
import json
import shutil
from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import uuid4

import pytest

pyspark = pytest.importorskip("pyspark")

if shutil.which("java") is None:
    pytest.skip("Java is required to run local PySpark tests (install OpenJDK).")

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.io.rule_loader import load_rule_table, load_runtime_rules
from src.transforms.ledger_controls import build_recon_snapshot_flow


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("rule-loader-runtime-integration")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_load_rule_table_and_apply_to_recon(spark) -> None:
    db = f"test_rule_loader_runtime_{uuid4().hex[:8]}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    table_name = f"{db}.dim_rule_scd2"

    rows = [
        {
            "rule_id": "ledger_recon_table_v1",
            "domain": "ledger",
            "metric": "drift_abs",
            "threshold": 0.0,
            "severity_map": {"crit": 0.0},
            "allowed_values": ["DRIFT"],
            "comment": "strict table rule",
            "effective_start_ts": datetime(2026, 2, 1, tzinfo=timezone.utc),
            "effective_end_ts": None,
            "is_current": True,
        },
        {
            "rule_id": "ledger_supply_table_v1",
            "domain": "ledger",
            "metric": "supply_diff_abs",
            "threshold": 0.0,
            "severity_map": {"crit": 0.0},
            "allowed_values": ["SUPPLY"],
            "comment": "strict table rule",
            "effective_start_ts": datetime(2026, 2, 1, tzinfo=timezone.utc),
            "effective_end_ts": datetime(2026, 2, 2, tzinfo=timezone.utc),
            "is_current": True,
        },
    ]
    schema = StructType(
        [
            StructField("rule_id", StringType(), nullable=False),
            StructField("domain", StringType(), nullable=False),
            StructField("metric", StringType(), nullable=False),
            StructField("threshold", DoubleType(), nullable=True),
            StructField(
                "severity_map",
                MapType(StringType(), DoubleType(), valueContainsNull=True),
                nullable=True,
            ),
            StructField(
                "allowed_values",
                ArrayType(StringType(), containsNull=False),
                nullable=True,
            ),
            StructField("comment", StringType(), nullable=True),
            StructField("effective_start_ts", TimestampType(), nullable=False),
            StructField("effective_end_ts", TimestampType(), nullable=True),
            StructField("is_current", BooleanType(), nullable=False),
        ]
    )
    spark.createDataFrame(rows, schema=schema).write.mode("overwrite").saveAsTable(
        table_name
    )

    runtime_rules = load_runtime_rules(
        spark,
        catalog="ignored_catalog",
        mode="strict",
        seed_path="mock_data/fixtures/dim_rule_scd2.json",
        table_name=f"spark_catalog.{table_name}",
    )
    assert {r.rule_id for r in runtime_rules} == {
        "ledger_recon_table_v1",
        "ledger_supply_table_v1",
    }

    loaded = load_rule_table(spark, table_name)
    assert len(loaded) == 2

    wallet_snapshots = [
        {
            "snapshot_ts": datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc),
            "user_id": "u1",
            "balance_total": Decimal("100"),
        },
        {
            "snapshot_ts": datetime(2026, 2, 1, 12, 0, tzinfo=timezone.utc),
            "user_id": "u1",
            "balance_total": Decimal("140"),
        },
    ]
    ledger_entries = [
        {
            "wallet_id": "u1",
            "amount_signed": Decimal("40"),
            "entry_type": "MINT",
            "amount": Decimal("40"),
            "event_time": datetime(2026, 2, 1, 2, 0, tzinfo=timezone.utc),
        }
    ]
    output = build_recon_snapshot_flow(
        wallet_snapshots,
        ledger_entries,
        target_date=date(2026, 2, 1),
        run_id="run-rule-loader-int",
        rules=runtime_rules,
    )
    assert len(output.rows) == 1
    assert output.rows[0]["rule_id"] == "ledger_recon_table_v1"


def test_load_runtime_rules_fallback_when_table_missing(spark, tmp_path) -> None:
    seed_path = tmp_path / "rules.json"
    seed_path.write_text(
        json.dumps(
            [
                {
                    "rule_id": "seed_rule_fallback",
                    "domain": "dq",
                    "metric": "freshness_sec",
                    "threshold": 300,
                }
            ]
        ),
        encoding="utf-8",
    )

    rules = load_runtime_rules(
        spark,
        catalog="fallback_catalog",
        mode="fallback",
        seed_path=seed_path,
        table_name="gold.dim_rule_missing",
    )
    assert len(rules) == 1
    assert rules[0].rule_id == "seed_rule_fallback"
