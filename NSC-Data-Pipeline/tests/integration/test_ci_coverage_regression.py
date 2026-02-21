from __future__ import annotations

import builtins
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from src.common.contracts import get_contract
from src.common.table_metadata import GOLD_WRITE_STRATEGY, SILVER_PARTITION_COLUMNS
from src.common.time_utils import UTC
from src.common.window_defaults import inject_default_daily_backfill
from src.io.catalog_bootstrap import (
    build_create_schema_ddl,
    build_create_table_ddl,
    create_all_tables,
    create_schemas,
    quote_sql_ident,
    quote_table_fqn,
    resolve_all_table_specs,
    validate_tables_exist,
)
from src.io.gold_io import (
    _render_partition_literal,
    build_gold_write_config,
    gold_merge_keys,
    gold_partition_columns,
    gold_write_strategy,
)
from src.io.gold_io import (
    normalize_table_name as normalize_gold_table_name,
)
from src.io.merge_utils import build_merge_condition
from src.io.pipeline_state_io import (
    STATE_FAILURE,
    STATE_SUCCESS,
    PipelineStateRecord,
    apply_pipeline_state,
    parse_pipeline_state_record,
    parse_zero_window_counts,
    serialize_zero_window_counts,
)
from src.io.secret_loader import LOCAL_DUMMY_SALT, resolve_user_key_salt
from src.io.silver_io import (
    build_silver_write_config,
    silver_merge_keys,
    silver_partition_columns,
)
from src.io.silver_io import (
    normalize_table_name as normalize_silver_table_name,
)


class _FakeCatalog:
    def __init__(self, existing_tables: set[str] | None = None) -> None:
        self._existing_tables = existing_tables or set()

    def tableExists(self, table_fqn: str) -> bool:  # noqa: N802
        return table_fqn in self._existing_tables


class _FakeSpark:
    def __init__(self, existing_tables: set[str] | None = None) -> None:
        self.sql_calls: list[str] = []
        self.catalog = _FakeCatalog(existing_tables)

    def sql(self, ddl: str) -> None:
        self.sql_calls.append(ddl)


def test_catalog_bootstrap_dry_run_and_ddl_generation() -> None:
    assert quote_sql_ident("my`schema") == "`my``schema`"
    assert quote_table_fqn("cat.silver.wallet_snapshot") == (
        "`cat`.`silver`.`wallet_snapshot`"
    )
    assert build_create_schema_ddl("prod_cat", "gold") == (
        "CREATE SCHEMA IF NOT EXISTS `prod_cat`.`gold`"
    )

    bronze_contract = get_contract("bronze.user_wallets_raw")
    bronze_ddl = build_create_table_ddl(
        table_fqn="cat.bronze.user_wallets_raw",
        contract=bronze_contract,
    )
    assert "CREATE TABLE IF NOT EXISTS" in bronze_ddl
    assert "USING DELTA" in bronze_ddl
    assert "PARTITIONED BY" not in bronze_ddl

    silver_contract = get_contract("silver.wallet_snapshot")
    silver_ddl = build_create_table_ddl(
        table_fqn="cat.silver.wallet_snapshot",
        contract=silver_contract,
        partition_columns=("snapshot_date_kst",),
    )
    assert "PARTITIONED BY (`snapshot_date_kst`)" in silver_ddl

    specs = resolve_all_table_specs("cat")
    assert any(spec.contract_key == "silver.dq_status" for spec in specs)

    schema_stmts = create_schemas(spark=None, catalog="cat", dry_run=True)
    table_stmts = create_all_tables(spark=None, catalog="cat", dry_run=True)
    assert len(schema_stmts) == 3
    assert len(table_stmts) == len(specs)


def test_catalog_bootstrap_execute_and_validate_paths() -> None:
    spark_schema = _FakeSpark()
    schema_stmts = create_schemas(
        spark=spark_schema, catalog="test_catalog", dry_run=False
    )
    assert spark_schema.sql_calls == schema_stmts

    spark_tables = _FakeSpark()
    table_stmts = create_all_tables(
        spark=spark_tables, catalog="test_catalog", dry_run=False
    )
    assert spark_tables.sql_calls == table_stmts

    specs = resolve_all_table_specs("test_catalog")
    existing = {spec.table_fqn for index, spec in enumerate(specs) if index % 2 == 0}
    spark_validate = _FakeSpark(existing_tables=existing)
    results = validate_tables_exist(spark_validate, "test_catalog")
    assert len(results) == len(specs)
    assert any(result.exists for result in results)
    assert any(not result.exists for result in results)


def test_window_defaults_and_merge_condition() -> None:
    payload = {
        "run_mode": "",
        "start_ts": "",
        "end_ts": "",
        "date_kst_start": "",
        "date_kst_end": "",
    }
    resolved = inject_default_daily_backfill(
        payload,
        now_ts=datetime(2026, 2, 12, 1, 0, tzinfo=timezone.utc),
    )
    assert resolved["run_mode"] == "backfill"
    assert resolved["date_kst_start"] == "2026-02-11"
    assert resolved["date_kst_end"] == "2026-02-11"

    explicit = {
        "run_mode": "incremental",
        "start_ts": "2026-02-11T00:00:00Z",
        "end_ts": "2026-02-11T00:10:00Z",
    }
    assert inject_default_daily_backfill(explicit) == explicit

    assert build_merge_condition(("date_kst", "user_id")) == (
        "target.date_kst = source.date_kst AND target.user_id = source.user_id"
    )
    with pytest.raises(ValueError):
        build_merge_condition(())


def test_silver_and_gold_io_write_configs() -> None:
    assert SILVER_PARTITION_COLUMNS["silver.dq_status"] == ("date_kst",)
    assert GOLD_WRITE_STRATEGY["gold.fact_payment_anonymized"] == "overwrite_partitions"

    assert normalize_silver_table_name("wallet_snapshot") == "silver.wallet_snapshot"
    assert silver_partition_columns("ledger_entries") == ("event_date_kst",)
    assert silver_merge_keys("order_events") == ("order_ref", "order_source")
    silver_config = build_silver_write_config("silver.wallet_snapshot")
    assert silver_config.merge_keys == ("snapshot_ts", "user_id")

    assert normalize_gold_table_name("fact_payment_anonymized") == (
        "gold.fact_payment_anonymized"
    )
    assert gold_partition_columns("fact_payment_anonymized") == ("date_kst",)
    assert gold_merge_keys("ledger_supply_balance_daily") == ("date_kst",)
    assert gold_write_strategy("fact_payment_anonymized") == "overwrite_partitions"
    gold_config = build_gold_write_config("gold.fact_payment_anonymized")
    assert gold_config.partition_columns == ("date_kst",)
    assert gold_config.write_strategy == "overwrite_partitions"

    assert _render_partition_literal("o'hara") == "'o''hara'"
    assert _render_partition_literal(Decimal("10.5")) == "10.5"

    with pytest.raises(KeyError):
        silver_partition_columns("silver.unknown_table")
    with pytest.raises(KeyError):
        gold_partition_columns("gold.unknown")


def test_pipeline_state_helpers_and_failure_transition() -> None:
    parsed = parse_pipeline_state_record(
        {
            "pipeline_name": "pipeline_b",
            "last_success_ts": "2026-02-11T15:00:00Z",
            "last_processed_end": "2026-02-11T15:00:00+00:00",
            "last_run_id": "run-1",
            "dq_zero_window_counts": '{"transaction_ledger": 2}',
            "updated_at": "2026-02-11T15:01:00Z",
        }
    )
    assert parsed.pipeline_name == "pipeline_b"
    assert parsed.last_run_id == "run-1"
    assert parsed.updated_at.tzinfo is not None

    counts = parse_zero_window_counts(
        '{"transaction_ledger": 2, "wallet_snapshot": -1, "bad": "x"}'
    )
    assert counts == {"transaction_ledger": 2, "wallet_snapshot": 0}
    assert parse_zero_window_counts("[1,2,3]") == {}
    assert serialize_zero_window_counts(counts) == (
        '{"transaction_ledger": 2, "wallet_snapshot": 0}'
    )

    current = PipelineStateRecord(
        pipeline_name="pipeline_b",
        last_success_ts=datetime(2026, 2, 11, 15, 0, tzinfo=UTC),
        last_processed_end=datetime(2026, 2, 11, 15, 0, tzinfo=UTC),
        last_run_id="run-1",
        dq_zero_window_counts='{"transaction_ledger": 2}',
        updated_at=datetime(2026, 2, 11, 15, 0, tzinfo=UTC),
    )
    failed = apply_pipeline_state(
        pipeline_name="pipeline_b",
        run_id="run-2",
        status=STATE_FAILURE,
        current_state=current,
        event_ts=datetime(2026, 2, 11, 15, 30, tzinfo=UTC),
    )
    assert failed.last_success_ts == current.last_success_ts
    assert failed.last_processed_end == current.last_processed_end
    assert failed.last_run_id == "run-2"

    with pytest.raises(ValueError):
        apply_pipeline_state(
            pipeline_name="pipeline_b",
            run_id="run-3",
            status=STATE_SUCCESS,
            current_state=current,
            last_processed_end=None,
        )


def test_secret_loader_env_dbutils_and_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert (
        resolve_user_key_salt(
            env={"ANON_USER_KEY_SALT": "env-salt"},
            dbutils=None,
            allow_local_fallback=False,
        )
        == "env-salt"
    )

    class _Secrets:
        def __init__(
            self, value: str | None = None, should_raise: bool = False
        ) -> None:
            self._value = value
            self._should_raise = should_raise

        def get(self, *, scope: str, key: str) -> str | None:
            if self._should_raise:
                raise RuntimeError("secret read failed")
            return self._value

    class _Dbutils:
        def __init__(
            self, value: str | None = None, should_raise: bool = False
        ) -> None:
            self.secrets = _Secrets(value=value, should_raise=should_raise)

    monkeypatch.setattr(
        builtins, "dbutils", _Dbutils(value="dbutils-salt"), raising=False
    )
    assert (
        resolve_user_key_salt(
            env={},
            dbutils=None,
            allow_local_fallback=False,
        )
        == "dbutils-salt"
    )

    monkeypatch.delattr(builtins, "dbutils", raising=False)
    assert (
        resolve_user_key_salt(
            env={},
            dbutils=_Dbutils(should_raise=True),
            allow_local_fallback=True,
        )
        == LOCAL_DUMMY_SALT
    )

    with pytest.raises(RuntimeError):
        resolve_user_key_salt(
            env={"DATABRICKS_RUNTIME_VERSION": "15.4"},
            dbutils=None,
            allow_local_fallback=True,
        )
