from __future__ import annotations

import shutil
from datetime import datetime
from decimal import Decimal

import pytest

pyspark = pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("Java is required to run local PySpark tests (install OpenJDK).")

from pyspark.sql import SparkSession  # noqa: E402

from src.common.time_utils import UTC  # noqa: E402
from src.io.rule_loader import load_default_rule_seed  # noqa: E402
from src.jobs.pipeline_a_spark import build_dq_status_spark  # noqa: E402
from src.transforms.dq_guardrail import DQTableConfig  # noqa: E402

RUN_ID = "run-pipeline-a-spark"
NOW_TS = datetime(2026, 2, 1, 0, 10, tzinfo=UTC)
WINDOW_START = datetime(2026, 2, 1, 0, 0, tzinfo=UTC)
WINDOW_END = datetime(2026, 2, 1, 0, 10, tzinfo=UTC)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pipeline-a-spark")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session
    session.stop()


def _run_dq(
    spark, *, records: list[dict], config: DQTableConfig, previous_zero_windows: int
):
    if records:
        source_df = spark.createDataFrame(records)
    else:
        source_df = spark.createDataFrame(
            [],
            schema=(
                "tx_id string, wallet_id string, type string, amount string, "
                "related_id string, created_at timestamp, ingested_at timestamp"
            ),
        )

    return build_dq_status_spark(
        source_df,
        config=config,
        run_id=RUN_ID,
        rules=load_default_rule_seed(),
        previous_zero_windows=previous_zero_windows,
        now_ts=NOW_TS,
    )


def test_pipeline_a_spark_expected_metrics_three_sources(spark) -> None:
    ledger_records = [
        {
            "tx_id": "tx-1",
            "wallet_id": "user-1",
            "type": "PAYMENT",
            "amount": "10.00",
            "created_at": "2026-02-01T00:08:00Z",
            "ingested_at": "2026-02-01T00:08:30Z",
        },
        {
            "tx_id": "tx-1",
            "wallet_id": "user-1",
            "type": "PAYMENT",
            "amount": "10.00",
            "created_at": "2026-02-01T00:08:30Z",
            "ingested_at": "2026-02-01T00:08:45Z",
        },
        {
            "tx_id": "tx-2",
            "wallet_id": "user-2",
            "type": "UNKNOWN_TYPE",
            "amount": "3.00",
            "created_at": "2026-02-01T00:09:00Z",
            "ingested_at": "2026-02-01T00:09:15Z",
        },
    ]
    wallet_records = [
        {
            "user_id": "user-1",
            "balance": "100.00",
            "frozen_amount": "0.00",
            "updated_at": "2026-02-01T00:08:00Z",
            "ingested_at": "2026-02-01T00:08:30Z",
        },
        {
            "user_id": "user-1",
            "balance": "100.00",
            "frozen_amount": "0.00",
            "updated_at": "2026-02-01T00:08:10Z",
            "ingested_at": "2026-02-01T00:08:40Z",
        },
        {
            "user_id": "user-2",
            "balance": "-1.00",
            "frozen_amount": "0.00",
            "updated_at": "2026-02-01T00:08:20Z",
            "ingested_at": "2026-02-01T00:08:50Z",
        },
    ]
    payment_records = [
        {
            "order_id": "order-1",
            "amount": "5.00",
            "status": "PAID",
            "created_at": "2026-02-01T00:08:00Z",
            "ingested_at": "2026-02-01T00:08:15Z",
        },
        {
            "order_id": "order-1",
            "amount": "5.00",
            "status": "PAID",
            "created_at": "2026-02-01T00:08:20Z",
            "ingested_at": "2026-02-01T00:08:35Z",
        },
        {
            "order_id": "order-2",
            "amount": "0.00",
            "status": "INVALID",
            "created_at": "2026-02-01T00:08:30Z",
            "ingested_at": "2026-02-01T00:08:45Z",
        },
    ]

    ledger_output = _run_dq(
        spark,
        records=ledger_records,
        config=DQTableConfig(
            source_table="bronze.transaction_ledger_raw",
            window_start_ts=WINDOW_START,
            window_end_ts=WINDOW_END,
            dup_key_fields=("tx_id",),
            dup_rule_metric="dup_rate_tx_id",
            freshness_fields=("ingested_at", "created_at"),
        ),
        previous_zero_windows=0,
    )
    wallet_output = _run_dq(
        spark,
        records=wallet_records,
        config=DQTableConfig(
            source_table="bronze.user_wallets_raw",
            window_start_ts=WINDOW_START,
            window_end_ts=WINDOW_END,
            dup_key_fields=("user_id",),
            dup_rule_metric="dup_rate_user_id",
            freshness_fields=("ingested_at", "updated_at"),
        ),
        previous_zero_windows=0,
    )
    payment_output = _run_dq(
        spark,
        records=payment_records,
        config=DQTableConfig(
            source_table="bronze.payment_orders_raw",
            window_start_ts=WINDOW_START,
            window_end_ts=WINDOW_END,
            dup_key_fields=("order_id",),
            dup_rule_metric="dup_rate_order_id",
            freshness_fields=("ingested_at", "created_at"),
        ),
        previous_zero_windows=0,
    )

    assert ledger_output.zero_window_count == 0
    assert ledger_output.dq_status["event_count"] == 3
    assert ledger_output.dq_status["dup_rate"] == Decimal("0.3333333333333333")
    assert ledger_output.dq_status["bad_records_rate"] == Decimal("0.3333333333333333")
    assert ledger_output.dq_status["dq_tag"] == "DUP_SUSPECTED"
    assert {row["exception_type"] for row in ledger_output.exceptions} == {
        "DUP_SUSPECTED",
        "CONTRACT_VIOLATION",
    }

    assert wallet_output.zero_window_count == 0
    assert wallet_output.dq_status["event_count"] == 3
    assert wallet_output.dq_status["dup_rate"] == Decimal("0.3333333333333333")
    assert wallet_output.dq_status["bad_records_rate"] == Decimal("0.3333333333333333")
    assert wallet_output.dq_status["dq_tag"] == "CONTRACT_VIOLATION"
    assert {row["exception_type"] for row in wallet_output.exceptions} == {
        "CONTRACT_VIOLATION"
    }

    assert payment_output.zero_window_count == 0
    assert payment_output.dq_status["event_count"] == 3
    assert payment_output.dq_status["dup_rate"] == Decimal("0.3333333333333333")
    assert payment_output.dq_status["bad_records_rate"] == Decimal("0.3333333333333333")
    assert payment_output.dq_status["dq_tag"] == "CONTRACT_VIOLATION"
    assert {row["exception_type"] for row in payment_output.exceptions} == {
        "CONTRACT_VIOLATION"
    }


def test_pipeline_a_spark_zero_window_progression(spark) -> None:
    output = _run_dq(
        spark,
        records=[],
        config=DQTableConfig(
            source_table="bronze.transaction_ledger_raw",
            window_start_ts=WINDOW_START,
            window_end_ts=WINDOW_END,
            dup_key_fields=("tx_id",),
            dup_rule_metric="dup_rate_tx_id",
            freshness_fields=("ingested_at", "created_at"),
        ),
        previous_zero_windows=2,
    )

    assert output.zero_window_count == 3
    assert output.dq_status["event_count"] == 0
    assert output.dq_status["dq_tag"] == "EVENT_DROP_SUSPECTED"
    assert len(output.exceptions) == 1
    assert output.exceptions[0]["exception_type"] == "EVENT_DROP_SUSPECTED"
    assert output.exceptions[0]["metric"] == "completeness_zero_windows"
