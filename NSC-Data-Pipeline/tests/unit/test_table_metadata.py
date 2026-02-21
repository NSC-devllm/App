from __future__ import annotations

from src.common.table_metadata import (
    GOLD_MERGE_KEYS,
    GOLD_PARTITION_COLUMNS,
    GOLD_WRITE_STRATEGY,
    SILVER_MERGE_KEYS,
    SILVER_PARTITION_COLUMNS,
)


def test_silver_merge_keys_contains_expected_tables() -> None:
    assert SILVER_MERGE_KEYS["silver.wallet_snapshot"] == (
        "snapshot_ts",
        "user_id",
    )
    assert SILVER_MERGE_KEYS["silver.ledger_entries"] == ("tx_id", "wallet_id")
    assert SILVER_MERGE_KEYS["silver.order_events"] == (
        "order_ref",
        "order_source",
    )
    assert SILVER_MERGE_KEYS["silver.order_items"] == ("item_id",)
    assert SILVER_MERGE_KEYS["silver.products"] == ("product_id",)


def test_silver_partition_columns() -> None:
    assert SILVER_PARTITION_COLUMNS["silver.wallet_snapshot"] == ("snapshot_date_kst",)
    assert SILVER_PARTITION_COLUMNS["silver.ledger_entries"] == ("event_date_kst",)
    assert SILVER_PARTITION_COLUMNS["silver.order_events"] == ("event_date_kst",)
    assert SILVER_PARTITION_COLUMNS["silver.order_items"] == ()
    assert SILVER_PARTITION_COLUMNS["silver.products"] == ()
    assert SILVER_PARTITION_COLUMNS["silver.bad_records"] == ("detected_date_kst",)
    assert SILVER_PARTITION_COLUMNS["silver.dq_status"] == ("date_kst",)


def test_gold_merge_keys_and_partitions() -> None:
    assert GOLD_MERGE_KEYS["gold.recon_daily_snapshot_flow"] == (
        "date_kst",
        "user_id",
    )
    assert GOLD_MERGE_KEYS["gold.exception_ledger"] == (
        "date_kst",
        "domain",
        "exception_type",
        "run_id",
        "metric",
        "message",
    )
    assert GOLD_MERGE_KEYS["gold.pipeline_state"] == ("pipeline_name",)
    assert GOLD_MERGE_KEYS["gold.dim_rule_scd2"] == ("rule_id",)
    assert GOLD_MERGE_KEYS["gold.fact_payment_anonymized"] == ()
    assert GOLD_PARTITION_COLUMNS["gold.fact_payment_anonymized"] == ("date_kst",)
    assert GOLD_PARTITION_COLUMNS["gold.pipeline_state"] == ()
    assert GOLD_PARTITION_COLUMNS["gold.dim_rule_scd2"] == ()
    assert GOLD_PARTITION_COLUMNS["gold.admin_tx_search"] == ("event_date_kst",)


def test_gold_write_strategy() -> None:
    assert GOLD_WRITE_STRATEGY["gold.recon_daily_snapshot_flow"] == "merge"
    assert GOLD_WRITE_STRATEGY["gold.dim_rule_scd2"] == "merge"
    assert GOLD_WRITE_STRATEGY["gold.fact_payment_anonymized"] == "overwrite_partitions"
