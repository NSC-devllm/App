from __future__ import annotations

import pytest

from src.io.gold_io import (
    build_gold_write_config,
    gold_merge_keys,
    gold_partition_columns,
    gold_write_strategy,
    normalize_table_name,
)


def test_normalize_table_name_accepts_short_name() -> None:
    assert (
        normalize_table_name("fact_payment_anonymized")
        == "gold.fact_payment_anonymized"
    )


def test_gold_partition_columns() -> None:
    assert gold_partition_columns("recon_daily_snapshot_flow") == ("date_kst",)
    assert gold_partition_columns("fact_payment_anonymized") == ("date_kst",)
    assert gold_partition_columns("admin_tx_search") == ("event_date_kst",)


def test_gold_merge_keys() -> None:
    assert gold_merge_keys("ledger_supply_balance_daily") == ("date_kst",)
    assert gold_merge_keys("fact_payment_anonymized") == ()


def test_gold_write_strategy() -> None:
    assert gold_write_strategy("recon_daily_snapshot_flow") == "merge"
    assert gold_write_strategy("fact_payment_anonymized") == "overwrite_partitions"


def test_gold_write_config() -> None:
    config = build_gold_write_config("gold.fact_payment_anonymized")
    assert config.partition_columns == ("date_kst",)
    assert config.merge_keys == ()
    assert config.write_strategy == "overwrite_partitions"


def test_gold_partition_columns_unknown_table() -> None:
    with pytest.raises(KeyError):
        gold_partition_columns("gold.unknown")
