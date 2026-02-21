from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Iterable

from src.common.contracts import GOLD_CONTRACTS
from src.common.table_metadata import (
    GOLD_MERGE_KEYS,
    GOLD_PARTITION_COLUMNS,
    GOLD_WRITE_STRATEGY,
)
from src.io.merge_utils import merge_delta_table
from src.io.spark_safety import safe_collect

MAX_PARTITION_VALUES_FOR_REPLACE_WHERE = 366


@dataclass(frozen=True)
class GoldWriteConfig:
    table_name: str
    partition_columns: tuple[str, ...]
    merge_keys: tuple[str, ...]
    write_strategy: str


def normalize_table_name(table_name: str) -> str:
    if table_name in GOLD_CONTRACTS:
        return table_name
    candidate = f"gold.{table_name}"
    if candidate in GOLD_CONTRACTS:
        return candidate
    raise KeyError(f"Unknown gold table: {table_name}")


def gold_partition_columns(table_name: str) -> tuple[str, ...]:
    normalized = normalize_table_name(table_name)
    try:
        return GOLD_PARTITION_COLUMNS[normalized]
    except KeyError as exc:
        raise KeyError(f"Missing partition columns for {normalized}") from exc


def gold_merge_keys(table_name: str) -> tuple[str, ...]:
    normalized = normalize_table_name(table_name)
    try:
        return GOLD_MERGE_KEYS[normalized]
    except KeyError as exc:
        raise KeyError(f"Missing merge keys for {normalized}") from exc


def gold_write_strategy(table_name: str) -> str:
    normalized = normalize_table_name(table_name)
    try:
        return GOLD_WRITE_STRATEGY[normalized]
    except KeyError as exc:
        raise KeyError(f"Missing write strategy for {normalized}") from exc


def build_gold_write_config(table_name: str) -> GoldWriteConfig:
    normalized = normalize_table_name(table_name)
    return GoldWriteConfig(
        table_name=normalized,
        partition_columns=gold_partition_columns(normalized),
        merge_keys=gold_merge_keys(normalized),
        write_strategy=gold_write_strategy(normalized),
    )


def _render_partition_literal(value: object) -> str:
    if isinstance(value, (date, datetime)):
        return f"'{value.isoformat()}'"
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    return str(value)


def _build_replace_where(
    df, partition_columns: Iterable[str]
) -> str | None:  # pragma: no cover
    columns = tuple(partition_columns)
    if len(columns) != 1:
        raise ValueError("overwrite_partitions supports exactly one partition column")
    column = columns[0]
    values = [
        row[column]
        for row in safe_collect(
            df.select(column).distinct(),
            max_rows=MAX_PARTITION_VALUES_FOR_REPLACE_WHERE,
            context=f"replace_where:{column}",
        )
        if row[column] is not None
    ]
    if not values:
        return None
    literals = sorted(_render_partition_literal(value) for value in values)
    return f"{column} IN ({', '.join(literals)})"


def write_gold_delta(  # pragma: no cover
    df,
    table_fqn: str,
    *,
    table_name: str,
    mode: str = "overwrite",
) -> None:
    config = build_gold_write_config(table_name)
    spark = df.sparkSession
    table_exists = spark.catalog.tableExists(table_fqn)

    if table_exists and config.write_strategy == "merge":
        if not config.merge_keys:
            raise ValueError(f"merge strategy requires merge keys: {config.table_name}")
        merge_delta_table(df, table_fqn, config.merge_keys)
        return

    # Avoid atomic replace semantics on initial CREATE TABLE (Unity Catalog + ADLS).
    effective_mode = mode if table_exists else "append"
    writer = df.write.format("delta").mode(effective_mode)
    if config.partition_columns:
        writer = writer.partitionBy(*config.partition_columns)

    if table_exists and config.write_strategy == "overwrite_partitions":
        replace_where = _build_replace_where(df, config.partition_columns)
        if not replace_where:
            # Safety: prevent accidental full table overwrites when we expected
            # partition-scoped replacement.
            raise ValueError(
                "overwrite_partitions strategy requires non-empty partition values "
                f"to build replaceWhere (table={table_fqn}, "
                f"partition_columns={config.partition_columns})"
            )
        writer = writer.option("replaceWhere", replace_where)
    writer.saveAsTable(table_fqn)
