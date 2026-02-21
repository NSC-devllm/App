from __future__ import annotations

import hashlib
import shutil
from datetime import date, datetime
from decimal import Decimal

import pytest

pyspark = pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("Java is required to run local PySpark tests (install OpenJDK).")

from pyspark.sql import SparkSession  # noqa: E402

from src.common.time_utils import UTC  # noqa: E402
from src.jobs.pipeline_c_spark import transform_pipeline_c_fact_spark  # noqa: E402

RUN_ID = "run-pipeline-c-spark"
SALT = "pipeline-c-spark-salt"


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pipeline-c-spark")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session
    session.stop()


def _collect_rows(df) -> list[dict]:
    rows = [row.asDict(recursive=True) for row in df.collect()]
    return sorted(
        rows, key=lambda row: (str(row.get("date_kst")), str(row.get("user_key")))
    )


def test_pipeline_c_spark_happy_path_and_filtering(spark) -> None:
    order_events = [
        {
            "order_ref": "101",
            "order_source": "PAYMENT_ORDERS",
            "user_id": "user-1",
            "merchant_name": "CoffeeLab",
            "amount": "120.00",
            "status": "PAID",
            "event_time": datetime(2026, 2, 1, 1, 0, tzinfo=UTC),
            "event_date_kst": date(2026, 2, 1),
        },
        {
            "order_ref": "102",
            "order_source": "PAYMENT_ORDERS",
            "user_id": "user-2",
            "merchant_name": "TeaLab",
            "amount": "10.00",
            "status": "PAID",
            "event_time": datetime(2026, 2, 1, 15, 30, tzinfo=UTC),
            "event_date_kst": None,
        },
        {
            "order_ref": "103",
            "order_source": "ORDERS",
            "user_id": "user-3",
            "merchant_name": "LegacyOnly",
            "amount": "33.00",
            "status": "PAID",
            "event_time": datetime(2026, 2, 1, 2, 0, tzinfo=UTC),
            "event_date_kst": date(2026, 2, 1),
        },
        {
            "order_ref": "",
            "order_source": "PAYMENT_ORDERS",
            "user_id": "user-4",
            "merchant_name": "InvalidOrderRef",
            "amount": "44.00",
            "status": "PAID",
            "event_time": datetime(2026, 2, 1, 3, 0, tzinfo=UTC),
            "event_date_kst": date(2026, 2, 1),
        },
        {
            "order_ref": "105",
            "order_source": "PAYMENT_ORDERS",
            "user_id": "",
            "merchant_name": "InvalidUser",
            "amount": "55.00",
            "status": "PAID",
            "event_time": datetime(2026, 2, 1, 4, 0, tzinfo=UTC),
            "event_date_kst": date(2026, 2, 1),
        },
    ]
    order_items = [
        {
            "item_id": 1,
            "order_ref": "101",
            "product_id": 10,
            "quantity": 1,
            "price_at_purchase": "120.00",
        },
        {
            "item_id": 2,
            "order_ref": "102",
            "product_id": 20,
            "quantity": 1,
            "price_at_purchase": "10.00",
        },
        {
            "item_id": 3,
            "order_ref": "102",
            "product_id": 30,
            "quantity": 1,
            "price_at_purchase": "9.00",
        },
    ]
    products = [
        {"product_id": 10, "category": "BEANS"},
        {"product_id": 20, "category": "TEA"},
        {"product_id": 30, "category": "SNACK"},
    ]

    output_df = transform_pipeline_c_fact_spark(
        spark.createDataFrame(order_events),
        spark.createDataFrame(order_items),
        spark.createDataFrame(products),
        run_id=RUN_ID,
        salt=SALT,
    )
    rows = _collect_rows(output_df)

    assert len(rows) == 2
    expected_user1_key = hashlib.sha256(f"user-1{SALT}".encode("utf-8")).hexdigest()
    expected_user2_key = hashlib.sha256(f"user-2{SALT}".encode("utf-8")).hexdigest()

    row_by_user = {row["user_key"]: row for row in rows}
    assert row_by_user[expected_user1_key]["category"] == "BEANS"
    assert row_by_user[expected_user1_key]["amount"] == Decimal("120.00")
    assert row_by_user[expected_user1_key]["date_kst"] == date(2026, 2, 1)

    assert row_by_user[expected_user2_key]["category"] == "TEA"
    assert row_by_user[expected_user2_key]["amount"] == Decimal("10.00")
    # event_date_kst missing -> event_time UTC to KST date fallback
    assert row_by_user[expected_user2_key]["date_kst"] == date(2026, 2, 2)


def test_pipeline_c_spark_tie_break_and_no_order_id_fallback(spark) -> None:
    order_events = [
        {
            "order_ref": "200",
            "order_source": "PAYMENT_ORDERS",
            "user_id": "user-200",
            "merchant_name": "TieShop",
            "amount": "100.00",
            "status": "PAID",
            "event_date_kst": date(2026, 2, 3),
        },
        {
            "order_ref": "201",
            "order_source": "PAYMENT_ORDERS",
            "user_id": "user-201",
            "merchant_name": "NoFallbackShop",
            "amount": "77.00",
            "status": "PAID",
            "event_date_kst": date(2026, 2, 3),
        },
        {
            "order_ref": "202",
            "order_source": "PAYMENT_ORDERS",
            "user_id": "user-202",
            "merchant_name": "DeterministicShop",
            "amount": "88.00",
            "status": "PAID",
            "event_date_kst": date(2026, 2, 3),
        },
    ]
    order_items = [
        {
            "item_id": 9,
            "order_ref": "200",
            "product_id": 901,
            "quantity": "true",
            "price_at_purchase": "100.00",
        },
        {
            "item_id": 7,
            "order_ref": "200",
            "product_id": 902,
            "quantity": 2,
            "price_at_purchase": "50.00",
        },
        {
            "item_id": 1,
            "order_ref": None,
            "order_id": "201",
            "product_id": 1001,
            "quantity": 1,
            "price_at_purchase": "77.00",
        },
        {
            "item_id": 2,
            "order_ref": "non-target-order",
            "order_id": "non-target-order",
            "product_id": 1001,
            "quantity": 1,
            "price_at_purchase": "1.00",
        },
        {
            "item_id": None,
            "order_ref": "202",
            "product_id": 2002,
            "quantity": 1,
            "price_at_purchase": "88.00",
        },
        {
            "item_id": None,
            "order_ref": "202",
            "product_id": 2001,
            "quantity": 1,
            "price_at_purchase": "88.00",
        },
        {
            "item_id": 99,
            "order_ref": "non-target-order-2",
            "product_id": 2003,
            "quantity": 1,
            "price_at_purchase": "1.00",
        },
    ]
    products = [
        {"product_id": 901, "category": "CATEGORY_A"},
        {"product_id": 902, "category": "CATEGORY_B"},
        {"product_id": 1001, "category": "SHOULD_NOT_JOIN"},
        {"product_id": 2001, "category": "CATEGORY_A"},
        {"product_id": 2002, "category": "CATEGORY_B"},
        {"product_id": 2003, "category": "CATEGORY_OTHER"},
    ]

    output_df = transform_pipeline_c_fact_spark(
        spark.createDataFrame(order_events),
        spark.createDataFrame(order_items),
        spark.createDataFrame(products),
        run_id=RUN_ID,
        salt=SALT,
    )
    rows = _collect_rows(output_df)

    assert len(rows) == 3
    by_merchant = {row["merchant_name"]: row for row in rows}

    # line_amount tie: smallest item_id wins
    assert by_merchant["TieShop"]["category"] == "CATEGORY_B"
    # order_ref-only join: no order_id fallback
    assert by_merchant["NoFallbackShop"]["category"] is None
    # dirty tie(item_id both null): deterministic product/category tie-break
    assert by_merchant["DeterministicShop"]["category"] == "CATEGORY_A"


def test_pipeline_c_spark_handles_empty_result(spark) -> None:
    order_events = [
        {
            "order_ref": "300",
            "order_source": "ORDERS",
            "user_id": "user-300",
            "merchant_name": "ShouldBeFiltered",
            "amount": "10.00",
            "status": "PAID",
            "event_date_kst": date(2026, 2, 4),
        }
    ]
    order_items = [
        {
            "item_id": 1,
            "order_ref": "300",
            "product_id": 1,
            "quantity": 1,
            "price_at_purchase": "10.00",
        }
    ]
    products = [{"product_id": 1, "category": "ANY"}]

    output_df = transform_pipeline_c_fact_spark(
        spark.createDataFrame(order_events),
        spark.createDataFrame(order_items),
        spark.createDataFrame(products),
        run_id=RUN_ID,
        salt=SALT,
    )
    rows = _collect_rows(output_df)

    assert output_df.columns == [
        "date_kst",
        "user_key",
        "merchant_name",
        "amount",
        "status",
        "category",
        "run_id",
    ]
    assert rows == []
