"""Catalog bootstrap utilities for schema and table creation.

Shared by the production bootstrap script and E2E setup.
This module handles ONLY structural creation (DDL) -- no data loading.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from src.common.contracts import (
    BRONZE_CONTRACTS,
    GOLD_CONTRACTS,
    SILVER_CONTRACTS,
    TableContract,
)
from src.common.table_metadata import (
    GOLD_PARTITION_COLUMNS,
    SILVER_PARTITION_COLUMNS,
)

logger = logging.getLogger(__name__)

SCHEMAS = ("bronze", "silver", "gold")


# ── SQL quoting helpers ──────────────────────────────────────────────────


def quote_sql_ident(value: str) -> str:
    """Backtick-quote a SQL identifier, escaping embedded backticks."""
    escaped = value.replace("`", "``")
    return f"`{escaped}`"


def quote_table_fqn(table_fqn: str) -> str:
    """Backtick-quote each part of a catalog.schema.table FQN."""
    return ".".join(quote_sql_ident(part) for part in table_fqn.split(".") if part)


# ── DDL generation ───────────────────────────────────────────────────────


def build_create_schema_ddl(catalog: str, schema: str) -> str:
    """Return a CREATE SCHEMA IF NOT EXISTS DDL string."""
    return f"CREATE SCHEMA IF NOT EXISTS {quote_sql_ident(catalog)}.{quote_sql_ident(schema)}"


def build_create_table_ddl(
    *,
    table_fqn: str,
    contract: TableContract,
    partition_columns: tuple[str, ...] = (),
) -> str:
    """Return a CREATE TABLE IF NOT EXISTS ... USING DELTA DDL string."""
    quoted_table = quote_table_fqn(table_fqn)
    column_defs = ", ".join(
        f"{quote_sql_ident(col.name)} {col.data_type}" for col in contract.columns
    )
    ddl = f"CREATE TABLE IF NOT EXISTS {quoted_table} ({column_defs}) USING DELTA"
    if partition_columns:
        parts = ", ".join(quote_sql_ident(c) for c in partition_columns)
        ddl += f" PARTITIONED BY ({parts})"
    return ddl


# ── Table inventory ──────────────────────────────────────────────────────


@dataclass(frozen=True)
class TableSpec:
    """Fully resolved table specification for DDL generation."""

    contract_key: str  # e.g. "bronze.user_wallets_raw"
    table_fqn: str  # e.g. "prod_catalog.bronze.user_wallets_raw"
    contract: TableContract
    partition_columns: tuple[str, ...]


def resolve_all_table_specs(catalog: str) -> list[TableSpec]:
    """Build the full list of TableSpecs for all contract tables."""
    specs: list[TableSpec] = []

    for contract_key, contract in BRONZE_CONTRACTS.items():
        short_name = contract_key.split(".", maxsplit=1)[1]
        specs.append(
            TableSpec(
                contract_key=contract_key,
                table_fqn=f"{catalog}.bronze.{short_name}",
                contract=contract,
                partition_columns=(),
            )
        )

    for contract_key, contract in SILVER_CONTRACTS.items():
        short_name = contract_key.split(".", maxsplit=1)[1]
        partition_columns = SILVER_PARTITION_COLUMNS.get(contract_key, ())
        specs.append(
            TableSpec(
                contract_key=contract_key,
                table_fqn=f"{catalog}.silver.{short_name}",
                contract=contract,
                partition_columns=partition_columns,
            )
        )

    for contract_key, contract in GOLD_CONTRACTS.items():
        short_name = contract_key.split(".", maxsplit=1)[1]
        partition_columns = GOLD_PARTITION_COLUMNS.get(contract_key, ())
        specs.append(
            TableSpec(
                contract_key=contract_key,
                table_fqn=f"{catalog}.gold.{short_name}",
                contract=contract,
                partition_columns=partition_columns,
            )
        )

    return specs


# ── Execution ────────────────────────────────────────────────────────────


def create_schemas(spark, catalog: str, *, dry_run: bool = False) -> list[str]:
    """Create bronze/silver/gold schemas. Returns executed DDL statements."""
    statements: list[str] = []
    for schema in SCHEMAS:
        ddl = build_create_schema_ddl(catalog, schema)
        statements.append(ddl)
        if dry_run:
            logger.info("[DRY RUN] %s", ddl)
        else:
            spark.sql(ddl)
            logger.info("Executed: %s", ddl)
    return statements


def create_all_tables(
    spark,
    catalog: str,
    *,
    dry_run: bool = False,
) -> list[str]:
    """Create all contract tables. Returns executed DDL statements."""
    specs = resolve_all_table_specs(catalog)
    statements: list[str] = []
    for spec in specs:
        ddl = build_create_table_ddl(
            table_fqn=spec.table_fqn,
            contract=spec.contract,
            partition_columns=spec.partition_columns,
        )
        statements.append(ddl)
        if dry_run:
            logger.info("[DRY RUN] %s", ddl)
        else:
            spark.sql(ddl)
            logger.info("Created: %s", spec.table_fqn)
    return statements


# ── Validation ───────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ValidationResult:
    table_fqn: str
    exists: bool


def validate_tables_exist(spark, catalog: str) -> list[ValidationResult]:
    """Check that all expected tables exist. Returns per-table results."""
    specs = resolve_all_table_specs(catalog)
    results: list[ValidationResult] = []
    for spec in specs:
        exists = spark.catalog.tableExists(spec.table_fqn)
        results.append(ValidationResult(table_fqn=spec.table_fqn, exists=exists))
        if not exists:
            logger.warning("MISSING: %s", spec.table_fqn)
        else:
            logger.info("OK: %s", spec.table_fqn)
    return results
