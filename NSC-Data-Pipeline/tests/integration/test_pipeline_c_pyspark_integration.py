from __future__ import annotations

import hashlib
import shutil
from datetime import date

import pytest

pyspark = pytest.importorskip("pyspark")

# PySpark requires a JVM. Keep this test opt-in for local development
# environments that have both pyspark and Java installed.
if shutil.which("java") is None:
    pytest.skip("Java is required to run local PySpark tests (install OpenJDK).")
from pyspark.sql import SparkSession

from src.jobs.pipeline_c_spark import transform_pipeline_c_fact_spark


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pipeline-c-integration")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_pipeline_c_smoke_with_local_pyspark(spark) -> None:
    order_events_df = spark.createDataFrame(
        [
            {
                "order_ref": "101",
                "order_source": "PAYMENT_ORDERS",
                "user_id": "user-1",
                "merchant_name": "CoffeeLab",
                "amount": "120.00",
                "status": "PAID",
                "event_date_kst": date(2026, 2, 1),
            }
        ]
    )
    order_items_df = spark.createDataFrame(
        [
            {
                "item_id": 1,
                "order_ref": "101",
                "product_id": 10,
                "quantity": 1,
                "price_at_purchase": "120.00",
            }
        ]
    )
    products_df = spark.createDataFrame([{"product_id": 10, "category": "BEANS"}])

    result_df = transform_pipeline_c_fact_spark(
        order_events_df,
        order_items_df,
        products_df,
        run_id="run-integration",
        salt="integration-salt",
    )
    result = result_df.collect()[0].asDict(recursive=True)
    expected_key = hashlib.sha256("user-1integration-salt".encode("utf-8")).hexdigest()
    assert result["user_key"] == expected_key
    assert result["category"] == "BEANS"
