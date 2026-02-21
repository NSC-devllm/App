"""Unit tests for src.io.catalog_bootstrap."""

from __future__ import annotations

from src.common.contracts import (
    BRONZE_CONTRACTS,
    GOLD_CONTRACTS,
    SILVER_CONTRACTS,
    get_contract,
)
from src.common.table_metadata import SILVER_PARTITION_COLUMNS
from src.io.catalog_bootstrap import (
    build_create_schema_ddl,
    build_create_table_ddl,
    quote_sql_ident,
    quote_table_fqn,
    resolve_all_table_specs,
)

# ── SQL quoting ──────────────────────────────────────────────────────────


def test_quote_sql_ident_basic() -> None:
    assert quote_sql_ident("bronze") == "`bronze`"


def test_quote_sql_ident_backtick_escape() -> None:
    assert quote_sql_ident("my`table") == "`my``table`"


def test_quote_table_fqn() -> None:
    result = quote_table_fqn("catalog.bronze.user_wallets_raw")
    assert result == "`catalog`.`bronze`.`user_wallets_raw`"


# ── DDL generation ───────────────────────────────────────────────────────


def test_build_create_schema_ddl() -> None:
    ddl = build_create_schema_ddl("prod_cat", "bronze")
    assert ddl == "CREATE SCHEMA IF NOT EXISTS `prod_cat`.`bronze`"


def test_build_create_table_ddl_no_partitions() -> None:
    contract = get_contract("bronze.user_wallets_raw")
    ddl = build_create_table_ddl(
        table_fqn="cat.bronze.user_wallets_raw",
        contract=contract,
    )
    assert "CREATE TABLE IF NOT EXISTS" in ddl
    assert "USING DELTA" in ddl
    assert "PARTITIONED BY" not in ddl
    assert "`user_id` string" in ddl


def test_build_create_table_ddl_with_partitions() -> None:
    contract = get_contract("silver.wallet_snapshot")
    ddl = build_create_table_ddl(
        table_fqn="cat.silver.wallet_snapshot",
        contract=contract,
        partition_columns=("snapshot_date_kst",),
    )
    assert "PARTITIONED BY (`snapshot_date_kst`)" in ddl


# ── Table specs ──────────────────────────────────────────────────────────


def test_resolve_all_table_specs_count() -> None:
    specs = resolve_all_table_specs("test_catalog")
    expected_count = len(BRONZE_CONTRACTS) + len(SILVER_CONTRACTS) + len(GOLD_CONTRACTS)
    assert len(specs) == expected_count


def test_resolve_all_table_specs_fqn_format() -> None:
    specs = resolve_all_table_specs("my_catalog")
    for spec in specs:
        parts = spec.table_fqn.split(".")
        assert len(parts) == 3
        assert parts[0] == "my_catalog"
        assert parts[1] in ("bronze", "silver", "gold")


def test_all_contracts_covered_by_specs() -> None:
    """Every contract key must appear in the resolved specs."""
    specs = resolve_all_table_specs("cat")
    spec_keys = {s.contract_key for s in specs}
    all_contract_keys = (
        set(BRONZE_CONTRACTS.keys())
        | set(SILVER_CONTRACTS.keys())
        | set(GOLD_CONTRACTS.keys())
    )
    assert spec_keys == all_contract_keys


def test_bronze_tables_have_no_partitions() -> None:
    specs = resolve_all_table_specs("cat")
    bronze_specs = [s for s in specs if s.table_fqn.split(".")[1] == "bronze"]
    for spec in bronze_specs:
        assert spec.partition_columns == (), (
            f"{spec.contract_key} should have no partitions"
        )


# ── dq_status SSOT fix ──────────────────────────────────────────────────


def test_dq_status_partition_in_table_metadata() -> None:
    """Partition must come from table_metadata, not a hardcoded override."""
    assert "silver.dq_status" in SILVER_PARTITION_COLUMNS
    assert SILVER_PARTITION_COLUMNS["silver.dq_status"] == ("date_kst",)


def test_dq_status_partition_in_specs() -> None:
    """resolve_all_table_specs must pick up dq_status partition from SSOT."""
    specs = resolve_all_table_specs("cat")
    dq_specs = [s for s in specs if s.contract_key == "silver.dq_status"]
    assert len(dq_specs) == 1
    assert dq_specs[0].partition_columns == ("date_kst",)
    assert dq_specs[0].table_fqn == "cat.silver.dq_status"


def test_dim_rule_table_in_specs() -> None:
    specs = resolve_all_table_specs("cat")
    dim_rule_specs = [s for s in specs if s.contract_key == "gold.dim_rule_scd2"]
    assert len(dim_rule_specs) == 1
    assert dim_rule_specs[0].partition_columns == ()
    assert dim_rule_specs[0].table_fqn == "cat.gold.dim_rule_scd2"


# ── dry-run path (spark=None) ───────────────────────────────────────────


def test_create_schemas_dry_run_no_spark() -> None:
    """dry_run=True must not touch spark (None is safe)."""
    from src.io.catalog_bootstrap import create_schemas

    stmts = create_schemas(spark=None, catalog="test_cat", dry_run=True)
    assert len(stmts) == 3
    assert all("CREATE SCHEMA IF NOT EXISTS" in s for s in stmts)


def test_create_all_tables_dry_run_no_spark() -> None:
    """dry_run=True must not touch spark (None is safe)."""
    from src.io.catalog_bootstrap import create_all_tables

    stmts = create_all_tables(spark=None, catalog="test_cat", dry_run=True)
    assert len(stmts) > 0
    assert all("CREATE TABLE IF NOT EXISTS" in s for s in stmts)
