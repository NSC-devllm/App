from __future__ import annotations

import shutil
from collections import Counter
from datetime import datetime
from decimal import Decimal

import pytest

pyspark = pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("Java is required to run local PySpark tests (install OpenJDK).")

from pyspark.sql import SparkSession  # noqa: E402

from src.common.rules import select_rule  # noqa: E402
from src.common.time_utils import UTC  # noqa: E402
from src.io.bronze_io import prepare_bronze_records  # noqa: E402
from src.io.rule_loader import load_default_rule_seed  # noqa: E402
from src.jobs.pipeline_silver_spark import transform_silver_tables_spark  # noqa: E402
from src.transforms import silver_controls  # noqa: E402

BASE_TS = datetime(2026, 2, 11, 0, 0, tzinfo=UTC)
SCENARIO_GLOB = "one_day_normal/*.jsonl"
RUN_ID = "run-pipeline-silver-spark"


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pipeline-silver-spark")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session
    session.stop()


def _load(table_name: str) -> list[dict]:
    return prepare_bronze_records(
        table_name,
        file_glob=SCENARIO_GLOB,
        ingested_at=BASE_TS,
        source_extracted_at=BASE_TS,
        batch_id="batch-silver-spark",
        source_system="mock",
    )


def _run_spark(
    spark,
    *,
    wallet_raw: list[dict],
    ledger_raw: list[dict],
    payment_orders_raw: list[dict],
    orders_raw: list[dict],
    order_items_raw: list[dict],
    products_raw: list[dict],
):
    rules = load_default_rule_seed()
    bad_rate_rule = select_rule(rules, domain="silver", metric="bad_records_rate")
    entry_type_rule = select_rule(rules, domain="silver", metric="entry_type_allowed")
    status_rule = select_rule(rules, domain="silver", metric="payment_status_allowed")

    return transform_silver_tables_spark(
        wallet_df=spark.createDataFrame(wallet_raw),
        ledger_df=spark.createDataFrame(ledger_raw),
        payment_orders_df=spark.createDataFrame(payment_orders_raw),
        orders_df=spark.createDataFrame(orders_raw),
        order_items_df=spark.createDataFrame(order_items_raw),
        products_df=spark.createDataFrame(products_raw),
        run_id=RUN_ID,
        bad_rate_rule_id=bad_rate_rule.rule_id if bad_rate_rule else None,
        entry_type_rule_id=entry_type_rule.rule_id if entry_type_rule else None,
        allowed_entry_types=silver_controls.resolve_allowed_values(entry_type_rule),
        allowed_statuses=silver_controls.resolve_allowed_values(status_rule),
    )


def test_pipeline_silver_spark_expected_outputs_one_day_normal(spark) -> None:
    output = _run_spark(
        spark,
        wallet_raw=_load("user_wallets_raw"),
        ledger_raw=_load("transaction_ledger_raw"),
        payment_orders_raw=_load("payment_orders_raw"),
        orders_raw=_load("orders_raw"),
        order_items_raw=_load("order_items_raw"),
        products_raw=_load("products_raw"),
    )

    assert output.wallet_valid_count == 3
    assert output.wallet_bad_count == 0
    assert output.ledger_valid_count == 10
    assert output.ledger_bad_count == 0

    wallet_rows = [
        row.asDict(recursive=True) for row in output.wallet_snapshot_df.collect()
    ]
    ledger_rows = [
        row.asDict(recursive=True) for row in output.ledger_entries_df.collect()
    ]
    order_event_rows = [
        row.asDict(recursive=True) for row in output.order_events_df.collect()
    ]
    order_item_rows = [
        row.asDict(recursive=True) for row in output.order_items_df.collect()
    ]
    product_rows = [row.asDict(recursive=True) for row in output.products_df.collect()]
    bad_rows = [row.asDict(recursive=True) for row in output.bad_records_df.collect()]

    assert len(wallet_rows) == 3
    assert len(ledger_rows) == 10
    assert len(order_event_rows) == 10
    assert len(order_item_rows) == 5
    assert len(product_rows) == 4
    assert bad_rows == []

    wallet_by_user = {row["user_id"]: row for row in wallet_rows}
    assert wallet_by_user["user_1"]["balance_total"] == Decimal("1000.00")
    assert wallet_by_user["user_1"]["rule_id"] == "silver_bad_records_default"

    ledger_by_tx = {row["tx_id"]: row for row in ledger_rows}
    assert ledger_by_tx["tx_pay_1004"]["amount_signed"] == Decimal("-130.00")
    assert ledger_by_tx["tx_pay_1004"]["status"] == "PAID"
    assert ledger_by_tx["tx_pay_1004"]["rule_id"] == "silver_entry_type_allowed_v1"


def test_pipeline_silver_spark_expected_bad_reason_counts(spark) -> None:
    wallet_raw = _load("user_wallets_raw")
    ledger_raw = _load("transaction_ledger_raw")
    payment_orders_raw = _load("payment_orders_raw")
    orders_raw = _load("orders_raw")
    order_items_raw = _load("order_items_raw")
    products_raw = _load("products_raw")

    wallet_raw.append(
        {
            "user_id": "",
            "balance": "1000",
            "frozen_amount": "0",
            "updated_at": BASE_TS,
            "ingested_at": BASE_TS,
            "source_extracted_at": BASE_TS,
            "batch_id": "batch-bad-wallet",
            "source_system": "mock",
        }
    )
    ledger_raw.append(
        {
            "tx_id": "bad-ledger-entry-1",
            "wallet_id": "u_bad",
            "type": "UNKNOWN_TYPE",
            "amount": "100",
            "related_id": None,
            "created_at": BASE_TS,
            "ingested_at": BASE_TS,
            "source_extracted_at": BASE_TS,
            "batch_id": "batch-bad-ledger",
            "source_system": "mock",
        }
    )

    output = _run_spark(
        spark,
        wallet_raw=wallet_raw,
        ledger_raw=ledger_raw,
        payment_orders_raw=payment_orders_raw,
        orders_raw=orders_raw,
        order_items_raw=order_items_raw,
        products_raw=products_raw,
    )

    assert output.wallet_valid_count == 3
    assert output.wallet_bad_count == 1
    assert output.ledger_valid_count == 10
    assert output.ledger_bad_count == 1

    counter = Counter(
        (row["source_table"], row["reason"])
        for row in output.bad_records_df.select("source_table", "reason").collect()
    )
    assert counter == Counter(
        {
            ("silver.wallet_snapshot", "missing_user_id"): 1,
            ("silver.ledger_entries", "invalid_entry_type"): 1,
        }
    )
