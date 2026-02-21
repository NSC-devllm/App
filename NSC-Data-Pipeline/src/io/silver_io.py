from __future__ import annotations

from dataclasses import dataclass

from src.common.contracts import SILVER_CONTRACTS
from src.common.table_metadata import SILVER_MERGE_KEYS, SILVER_PARTITION_COLUMNS
from src.io.merge_utils import merge_delta_table


@dataclass(frozen=True)
class SilverWriteConfig:
    table_name: str
    partition_columns: tuple[str, ...]
    merge_keys: tuple[str, ...]


def normalize_table_name(table_name: str) -> str:
    if table_name in SILVER_CONTRACTS:
        return table_name
    candidate = f"silver.{table_name}"
    if candidate in SILVER_CONTRACTS:
        return candidate
    raise KeyError(f"Unknown silver table: {table_name}")


def silver_partition_columns(table_name: str) -> tuple[str, ...]:
    normalized = normalize_table_name(table_name)
    try:
        return SILVER_PARTITION_COLUMNS[normalized]
    except KeyError as exc:
        raise KeyError(f"Missing partition columns for {normalized}") from exc


def silver_merge_keys(table_name: str) -> tuple[str, ...]:
    normalized = normalize_table_name(table_name)
    try:
        return SILVER_MERGE_KEYS[normalized]
    except KeyError as exc:
        raise KeyError(f"Missing merge keys for {normalized}") from exc


def build_silver_write_config(table_name: str) -> SilverWriteConfig:
    normalized = normalize_table_name(table_name)
    return SilverWriteConfig(
        table_name=normalized,
        partition_columns=silver_partition_columns(normalized),
        merge_keys=silver_merge_keys(normalized),
    )


def write_silver_delta(  # pragma: no cover
    df,
    table_fqn: str,
    *,
    table_name: str,
    mode: str = "overwrite",
) -> None:
    config = build_silver_write_config(table_name)
    spark = df.sparkSession
    if spark.catalog.tableExists(table_fqn):
        merge_delta_table(df, table_fqn, config.merge_keys)
        return

    # See src/io/bronze_io.py for rationale: avoid atomic replace on first create.
    writer = df.write.format("delta").mode("append")
    if config.partition_columns:
        writer = writer.partitionBy(*config.partition_columns)
    writer.saveAsTable(table_fqn)
