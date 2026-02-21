from __future__ import annotations

import shutil
from datetime import date, datetime
from decimal import Decimal

import pytest

pyspark = pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("Java is required to run local PySpark tests (install OpenJDK).")

from pyspark.sql import SparkSession  # noqa: E402

from src.common.time_utils import UTC  # noqa: E402
from src.io.rule_loader import load_default_rule_seed  # noqa: E402
from src.jobs.pipeline_b_spark import transform_pipeline_b_tables_spark  # noqa: E402

RUN_ID = "run-pipeline-b-spark"
TARGET_DATES = [date(2026, 2, 1), date(2026, 2, 2)]


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pipeline-b-spark")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session
    session.stop()


def _scenario():
    wallet_snapshots = [
        {
            "snapshot_ts": datetime(2026, 2, 1, 0, 0, tzinfo=UTC),
            "snapshot_date_kst": date(2026, 2, 1),
            "user_id": "u1",
            "balance_total": "100.00",
        },
        {
            "snapshot_ts": datetime(2026, 2, 1, 12, 0, tzinfo=UTC),
            "snapshot_date_kst": date(2026, 2, 1),
            "user_id": "u1",
            "balance_total": "150.00",
        },
        {
            "snapshot_ts": datetime(2026, 2, 1, 0, 30, tzinfo=UTC),
            "snapshot_date_kst": date(2026, 2, 1),
            "user_id": "u2",
            "balance_total": "200.00",
        },
        {
            "snapshot_ts": datetime(2026, 2, 1, 12, 30, tzinfo=UTC),
            "snapshot_date_kst": date(2026, 2, 1),
            "user_id": "u2",
            "balance_total": "220.00",
        },
        {
            "snapshot_ts": datetime(2026, 2, 2, 0, 0, tzinfo=UTC),
            "snapshot_date_kst": date(2026, 2, 2),
            "user_id": "u1",
            "balance_total": "150.00",
        },
        {
            "snapshot_ts": datetime(2026, 2, 2, 12, 0, tzinfo=UTC),
            "snapshot_date_kst": date(2026, 2, 2),
            "user_id": "u1",
            "balance_total": "160.00",
        },
        {
            "snapshot_ts": datetime(2026, 2, 2, 0, 30, tzinfo=UTC),
            "snapshot_date_kst": date(2026, 2, 2),
            "user_id": "u2",
            "balance_total": "220.00",
        },
        {
            "snapshot_ts": datetime(2026, 2, 2, 12, 30, tzinfo=UTC),
            "snapshot_date_kst": date(2026, 2, 2),
            "user_id": "u2",
            "balance_total": "210.00",
        },
    ]

    ledger_entries = [
        {
            "tx_id": "tx_u1_d1",
            "wallet_id": "u1",
            "entry_type": "PAYMENT",
            "amount": "40.00",
            "amount_signed": "40.00",
            "related_id": "po_1",
            "related_type": "ORDER",
            "status": "PAID",
            "event_time": datetime(2026, 2, 1, 1, 0, tzinfo=UTC),
            "event_date_kst": date(2026, 2, 1),
        },
        {
            "tx_id": "tx_u2_d1",
            "wallet_id": "u2",
            "entry_type": "RECEIVE",
            "amount": "10.00",
            "amount_signed": "10.00",
            "related_id": "po_1",
            "related_type": "ORDER",
            "status": "PAID",
            "event_time": datetime(2026, 2, 1, 2, 0, tzinfo=UTC),
            "event_date_kst": date(2026, 2, 1),
        },
        {
            "tx_id": "tx_mint_d1",
            "wallet_id": "treasury",
            "entry_type": "MINT",
            "amount": "300.00",
            "amount_signed": "300.00",
            "related_id": None,
            "related_type": None,
            "status": None,
            "event_time": datetime(2026, 2, 1, 0, 10, tzinfo=UTC),
            "event_date_kst": date(2026, 2, 1),
        },
        {
            "tx_id": "tx_burn_d1",
            "wallet_id": "treasury",
            "entry_type": "BURN",
            "amount": "10.00",
            "amount_signed": "-10.00",
            "related_id": None,
            "related_type": None,
            "status": None,
            "event_time": datetime(2026, 2, 1, 0, 20, tzinfo=UTC),
            "event_date_kst": date(2026, 2, 1),
        },
        {
            "tx_id": "tx_u1_d2",
            "wallet_id": "u1",
            "entry_type": "PAYMENT",
            "amount": "20.00",
            "amount_signed": "20.00",
            "related_id": "po_2",
            "related_type": "ORDER",
            "status": "FAILED",
            "event_time": datetime(2026, 2, 2, 1, 0, tzinfo=UTC),
            "event_date_kst": date(2026, 2, 2),
        },
        {
            "tx_id": "tx_u2_d2",
            "wallet_id": "u2",
            "entry_type": "PAYMENT",
            "amount": "5.00",
            "amount_signed": "-5.00",
            "related_id": "po_2",
            "related_type": "ORDER",
            "status": "PAID",
            "event_time": datetime(2026, 2, 2, 1, 30, tzinfo=UTC),
            "event_date_kst": date(2026, 2, 2),
        },
        {
            "tx_id": "tx_pair_left",
            "wallet_id": "u1",
            "entry_type": "PAYMENT",
            "amount": "15.00",
            "amount_signed": "-15.00",
            "related_id": "po_pair",
            "related_type": "ORDER",
            "status": "PAID",
            "event_time": datetime(2026, 2, 2, 2, 0, tzinfo=UTC),
            "event_date_kst": date(2026, 2, 2),
        },
        {
            "tx_id": "tx_pair_right",
            "wallet_id": "u2",
            "entry_type": "RECEIVE",
            "amount": "15.00",
            "amount_signed": "15.00",
            "related_id": "po_pair",
            "related_type": "ORDER",
            "status": "PAID",
            "event_time": datetime(2026, 2, 2, 2, 0, tzinfo=UTC),
            "event_date_kst": date(2026, 2, 2),
        },
        {
            "tx_id": "tx_charge_d2",
            "wallet_id": "treasury",
            "entry_type": "CHARGE",
            "amount": "50.00",
            "amount_signed": "50.00",
            "related_id": None,
            "related_type": None,
            "status": None,
            "event_time": datetime(2026, 2, 2, 0, 5, tzinfo=UTC),
            "event_date_kst": date(2026, 2, 2),
        },
    ]

    payment_orders = [
        {
            "order_id": "po_1",
            "merchant_name": "Shop A",
            "status": "FAILED",
            "created_at": datetime(2026, 2, 1, 4, 0, tzinfo=UTC),
        },
        {
            "order_id": "po_2",
            "merchant_name": "Shop A",
            "status": "PAID",
            "created_at": datetime(2026, 2, 2, 4, 0, tzinfo=UTC),
        },
        {
            "order_id": "po_3",
            "merchant_name": "Shop B",
            "status": "REFUNDED",
            "created_at": datetime(2026, 2, 2, 5, 0, tzinfo=UTC),
        },
        {
            "order_id": None,
            "merchant_name": None,
            "status": "CANCELLED",
            "created_at": datetime(2026, 2, 1, 6, 0, tzinfo=UTC),
        },
    ]

    dq_status = [
        {"date_kst": date(2026, 2, 1), "dq_tag": "EVENT_DROP_SUSPECTED"},
        {"date_kst": date(2026, 2, 1), "dq_tag": "SOURCE_STALE"},
        {"date_kst": date(2026, 2, 2), "dq_tag": "EVENT_DROP_SUSPECTED"},
    ]

    return wallet_snapshots, ledger_entries, payment_orders, dq_status


def test_pipeline_b_spark_expected_outputs_two_days(spark) -> None:
    wallet_snapshots, ledger_entries, payment_orders, dq_status = _scenario()

    output = transform_pipeline_b_tables_spark(
        wallet_snapshot_df=spark.createDataFrame(wallet_snapshots),
        ledger_entries_df=spark.createDataFrame(ledger_entries),
        payment_orders_df=spark.createDataFrame(payment_orders),
        dq_status_df=spark.createDataFrame(dq_status),
        target_dates=TARGET_DATES,
        run_id=RUN_ID,
        rules=load_default_rule_seed(),
        run_recon=True,
        run_supply_ops=True,
    )

    assert output.recon_df is not None
    assert output.supply_df is not None
    assert output.ops_failure_df is not None
    assert output.ops_refund_df is not None
    assert output.pairing_quality_df is not None
    assert output.admin_tx_search_df is not None
    assert output.exception_df is not None

    recon_rows = [row.asDict(recursive=True) for row in output.recon_df.collect()]
    assert len(recon_rows) == 4
    recon_by_key = {(row["date_kst"], row["user_id"]): row for row in recon_rows}
    assert recon_by_key[(date(2026, 2, 1), "u1")]["drift_abs"] == Decimal("10.00")
    assert recon_by_key[(date(2026, 2, 1), "u1")]["dq_tag"] == "SOURCE_STALE"
    assert recon_by_key[(date(2026, 2, 2), "u2")]["drift_abs"] == Decimal("20.00")

    supply_rows = [row.asDict(recursive=True) for row in output.supply_df.collect()]
    assert len(supply_rows) == 2
    supply_by_date = {row["date_kst"]: row for row in supply_rows}
    assert supply_by_date[date(2026, 2, 1)]["diff_amount"] == Decimal("-80.00")
    assert supply_by_date[date(2026, 2, 2)]["diff_amount"] == Decimal("-30.00")
    assert supply_by_date[date(2026, 2, 1)]["is_ok"] is False

    failure_rows = [
        row.asDict(recursive=True) for row in output.ops_failure_df.collect()
    ]
    assert len(failure_rows) == 4
    failure_by_key = {
        (row["date_kst"], row["merchant_name"]): row for row in failure_rows
    }
    assert failure_by_key[(date(2026, 2, 1), "Shop A")]["failed_cnt"] == 1
    assert failure_by_key[(date(2026, 2, 2), "Shop B")]["failed_cnt"] == 0

    refund_rows = [row.asDict(recursive=True) for row in output.ops_refund_df.collect()]
    assert len(refund_rows) == 4
    refund_by_key = {
        (row["date_kst"], row["merchant_name"]): row for row in refund_rows
    }
    assert refund_by_key[(date(2026, 2, 2), "Shop B")]["refunded_cnt"] == 1
    assert refund_by_key[(date(2026, 2, 2), "Shop B")]["refund_rate"] == Decimal(
        "1.000000"
    )

    pairing_rows = [
        row.asDict(recursive=True) for row in output.pairing_quality_df.collect()
    ]
    assert len(pairing_rows) == 2
    pairing_by_date = {row["date_kst"]: row for row in pairing_rows}
    assert pairing_by_date[date(2026, 2, 2)]["pair_candidate_rate"] == Decimal(
        "1.000000"
    )
    assert pairing_by_date[date(2026, 2, 2)]["join_payment_orders_rate"] == Decimal(
        "0.500000"
    )

    admin_rows = [
        row.asDict(recursive=True) for row in output.admin_tx_search_df.collect()
    ]
    assert len(admin_rows) == 9
    admin_by_tx_id = {row["tx_id"]: row for row in admin_rows}
    assert admin_by_tx_id["tx_pair_left"]["paired_tx_id"] == "tx_pair_right"
    assert admin_by_tx_id["tx_u1_d2"]["payment_status"] == "FAILED"

    exception_rows = [
        row.asDict(recursive=True) for row in output.exception_df.collect()
    ]
    assert len(exception_rows) == 6
    merge_key_set = {
        (
            row["date_kst"],
            row["domain"],
            row["exception_type"],
            row["run_id"],
            row["metric"],
            row["message"],
        )
        for row in exception_rows
    }
    assert len(merge_key_set) == len(exception_rows)
