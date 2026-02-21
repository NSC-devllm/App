from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence


def _repo_root() -> Path:
    """Best-effort repo root resolution for Databricks Jobs/Bundles.

    Databricks may execute this file via `exec(compile(..., filename, "exec"))`,
    which means `__file__` may be missing even though tracebacks show a filename.
    In that case, fall back to this function's code object filename.
    """

    candidates: list[Path] = []

    file_name = globals().get("__file__") or _repo_root.__code__.co_filename
    if file_name:
        candidates.append(Path(file_name))

    if sys.argv and sys.argv[0]:
        candidates.append(Path(sys.argv[0]))

    candidates.append(Path.cwd())

    env_root = os.environ.get("PIPELINE_ROOT")
    if env_root:
        candidates.append(Path(env_root))

    for base in candidates:
        for probe in (base, base.parent, *base.parents):
            if (probe / "src").is_dir() and (probe / "mock_data").is_dir():
                return probe

    return Path(env_root or "/dbfs/tmp/data-pipeline")


def _resolve_mock_data_root(repo_root: Path, *, catalog: str) -> Path:
    """Pick a mock_data root that works reliably in Databricks Jobs.

    Priority:
    1) Explicit override via env var (filesystem path)
    2) UC Volume mount (preferred for UC-enabled workspaces)
    3) DBFS staging area (/dbfs/...) when available
    4) Repo-relative mock_data (may be a workspace path in Jobs)
    """

    env_root = os.environ.get("E2E_MOCK_DATA_ROOT")
    if env_root:
        return Path(env_root)

    # Unity Catalog Volumes are mounted at /Volumes/<catalog>/<schema>/<volume>.
    # We keep mock data in an external volume for predictable access.
    volume_root = Path(f"/Volumes/{catalog}/bronze/mock_data")
    if (volume_root / "bronze").is_dir():
        return volume_root

    dbfs_root = Path("/dbfs/tmp/data-pipeline/mock_data")
    if (dbfs_root / "bronze").is_dir():
        return dbfs_root

    return repo_root / "mock_data"


def _default_run_id(prefix: str = "e2e_setup") -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}_{timestamp}"


def _parse_base_ts(value: str | None) -> datetime:
    """Parse an ISO8601 UTC timestamp (or default to now UTC).

    We use this timestamp to stamp ingestion metadata and, by default, align
    source event timestamps for realistic incremental E2E runs (freshness, window
    filters, etc).
    """

    if value is None:
        value = ""
    value = value.strip()
    if not value:
        return datetime.now(timezone.utc).replace(microsecond=0)

    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(microsecond=0)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="E2E setup: create UC schemas, load mock bronze, materialize required silver tables."
    )
    parser.add_argument("--catalog", required=True, help="Target Unity Catalog name.")
    parser.add_argument(
        "--scenario",
        default="",
        help=(
            "Optional mock_data scenario directory name (e.g. one_day_normal). "
            "When empty, uses mock_data/bronze/*/data.jsonl."
        ),
    )
    parser.add_argument(
        "--run-id",
        default="",
        help="run_id to stamp into generated silver rows (default: auto).",
    )
    parser.add_argument(
        "--base-ts",
        default="",
        help=(
            "Base timestamp (UTC, ISO8601) used for ingested_at/source_extracted_at. "
            "When empty, defaults to now (UTC)."
        ),
    )
    parser.add_argument(
        "--preserve-source-timestamps",
        action="store_true",
        help=(
            "Do not override created_at/updated_at in mock data. "
            "By default, this script aligns timestamps to base_ts to make incremental E2E runs realistic."
        ),
    )
    parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="Drop existing bronze/silver/gold tables before writing.",
    )
    return parser.parse_args()


def _align_to_contract(df, contract):
    from pyspark.sql import functions as F

    for column, data_type in contract.cast_map.items():
        if column in df.columns:
            df = df.withColumn(column, F.col(column).cast(data_type))
        else:
            df = df.withColumn(column, F.lit(None).cast(data_type))
    return df.select(*contract.column_names)


def _contract_schema(contract):
    """Build a Spark StructType from a TableContract."""
    from pyspark.sql.types import ArrayType, DoubleType, MapType, StringType, StructType

    # PySpark StructType.add() expects 'long'/'integer' not SQL DDL 'bigint'/'int'
    _type_map = {"bigint": "long", "int": "integer"}

    def _resolve_type(data_type: str):
        normalized = data_type.strip().lower()
        if normalized in _type_map:
            return _type_map[normalized]
        if normalized == "map<string,double>":
            return MapType(StringType(), DoubleType(), valueContainsNull=True)
        if normalized == "array<string>":
            return ArrayType(StringType(), containsNull=True)
        return data_type

    schema = StructType()
    for col in contract.columns:
        spark_type = _resolve_type(col.data_type)
        schema.add(col.name, spark_type, nullable=True)
    return schema


def _empty_df_for_contract(spark, contract):
    return spark.createDataFrame([], schema=_contract_schema(contract))


def _collect_table(spark, table_fqn: str) -> list[dict]:
    if not spark.catalog.tableExists(table_fqn):
        return []
    return [row.asDict(recursive=True) for row in spark.table(table_fqn).collect()]


@dataclass(frozen=True)
class FailFastCheck:
    table_name: str
    valid_count: int
    bad_count: int


def persist_bad_records_and_enforce_fail_fast(
    *,
    bad_records: Sequence[dict[str, Any]],
    persist_bad_records: Callable[[Sequence[dict[str, Any]]], None],
    fail_fast_checks: Sequence[FailFastCheck],
    rule,
) -> dict[str, float]:
    """Persist bad records first, then enforce bad-rate fail-fast checks."""

    from src.transforms import silver_controls

    persist_bad_records(bad_records)
    bad_rates: dict[str, float] = {}
    for check in fail_fast_checks:
        bad_rates[check.table_name] = silver_controls.enforce_bad_records_rate(
            valid_count=check.valid_count,
            bad_count=check.bad_count,
            rule=rule,
        )
    return bad_rates


def _write_dbfs_status(payload: dict) -> None:
    """Write a small status blob for out-of-band debugging in Jobs runs.

    Databricks spark_python_task logs can be hard to stream while running; this
    gives us a way to see which step the setup is currently executing.
    """

    try:
        status_dir = Path("/dbfs/tmp/data-pipeline/e2e")
        status_dir.mkdir(parents=True, exist_ok=True)
        tmp_path = status_dir / "setup_status.json.tmp"
        final_path = status_dir / "setup_status.json"
        tmp_path.write_text(
            json.dumps(payload, ensure_ascii=True, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        tmp_path.replace(final_path)
    except Exception as exc:  # pragma: no cover
        print(f"WARNING: failed to write DBFS status: {exc}")


def _set_status(stage: str, **extra: object) -> None:
    payload = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "stage": stage,
        **{k: v for k, v in extra.items() if v is not None},
    }
    _write_dbfs_status(payload)


def _create_schemas(spark, catalog: str) -> None:
    from src.io.catalog_bootstrap import create_schemas

    create_schemas(spark, catalog)


def _ensure_known_tables(spark, catalog: str) -> None:
    from src.io.catalog_bootstrap import create_all_tables

    create_all_tables(spark, catalog)


def _drop_known_tables(spark, catalog: str) -> None:
    tables_by_schema: dict[str, tuple[str, ...]] = {
        "bronze": (
            "user_wallets_raw",
            "transaction_ledger_raw",
            "payment_orders_raw",
            "orders_raw",
            "order_items_raw",
            "products_raw",
        ),
        "silver": (
            "wallet_snapshot",
            "ledger_entries",
            "order_events",
            "order_items",
            "products",
            "bad_records",
            "dq_status",
        ),
        "gold": (
            "recon_daily_snapshot_flow",
            "ledger_supply_balance_daily",
            "ops_payment_failure_daily",
            "ops_payment_refund_daily",
            "ops_ledger_pairing_quality_daily",
            "admin_tx_search",
            "fact_payment_anonymized",
            "exception_ledger",
            "pipeline_state",
            "dim_rule_scd2",
        ),
    }
    for schema, table_names in tables_by_schema.items():
        for table_name in table_names:
            spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{table_name}")


def _write_dim_rule_table(spark, *, catalog: str, rules) -> None:
    from src.common.contracts import get_contract, validate_required_columns
    from src.io.gold_io import write_gold_delta

    rows = [rule.as_storage_dict() for rule in rules]
    if not rows:
        raise ValueError("Cannot sync empty rule set into gold.dim_rule_scd2")

    contract = get_contract("gold.dim_rule_scd2")
    validate_required_columns(rows[0].keys(), contract)
    df = _align_to_contract(
        spark.createDataFrame(rows, schema=_contract_schema(contract)),
        contract,
    )
    write_gold_delta(
        df,
        f"{catalog}.gold.dim_rule_scd2",
        table_name="gold.dim_rule_scd2",
        mode="overwrite",
    )


def main() -> None:
    args = parse_args()
    repo_root = _repo_root()
    if repo_root.as_posix() not in sys.path:
        sys.path.insert(0, repo_root.as_posix())

    from pyspark.sql import SparkSession

    from src.common.contracts import BRONZE_CONTRACTS, get_contract
    from src.common.rules import select_rule
    from src.io.bronze_io import (
        bronze_short_name,
        default_batch_id,
        prepare_bronze_records,
    )
    from src.io.rule_loader import load_rule_seed, load_runtime_rules
    from src.transforms import analytics, silver_controls

    spark = SparkSession.builder.getOrCreate()

    catalog = args.catalog
    scenario = args.scenario.strip()
    run_id = args.run_id.strip() or _default_run_id()
    mock_root = _resolve_mock_data_root(repo_root, catalog=catalog)
    _set_status(
        "start",
        catalog=catalog,
        scenario=scenario or "default",
        run_id=run_id,
        drop_existing=args.drop_existing,
        repo_root=str(repo_root),
        mock_root=str(mock_root),
    )

    _set_status("create_schemas", catalog=catalog)
    _create_schemas(spark, catalog)
    if args.drop_existing:
        _set_status("drop_tables", catalog=catalog)
        _drop_known_tables(spark, catalog)

    _set_status("ensure_tables", catalog=catalog)
    _ensure_known_tables(spark, catalog)

    file_glob = "*.jsonl" if not scenario else f"{scenario}/*.jsonl"
    base_dir = mock_root / "bronze"
    _set_status(
        "load_bronze_begin",
        catalog=catalog,
        scenario=scenario or "default",
        base_dir=str(base_dir),
    )

    batch_id = default_batch_id(prefix="e2e")
    base_ts = _parse_base_ts(args.base_ts)
    ingested_at = base_ts

    for table_name in BRONZE_CONTRACTS.keys():
        from pyspark.sql import functions as F

        table_short = bronze_short_name(table_name)
        table_fqn = f"{catalog}.bronze.{table_short}"

        source_glob = str(base_dir / table_short / file_glob)
        _set_status("bronze_prepare", table=table_fqn, source_glob=source_glob)

        # Use driver-side JSONL parsing (small mock data) to avoid Spark JSON reads
        # hanging on some UC job clusters for certain DBFS paths.
        records = prepare_bronze_records(
            table_name,
            base_dir=base_dir,
            ingested_at=ingested_at,
            source_extracted_at=ingested_at,
            batch_id=batch_id,
            source_system="mock_data",
            file_glob=file_glob,
        )
        _set_status("bronze_loaded", table=table_fqn, rows=len(records))

        contract = get_contract(table_name)
        if records:
            df_source = spark.createDataFrame(records)
            if not args.preserve_source_timestamps:
                if "created_at" in df_source.columns:
                    df_source = df_source.withColumn(
                        "created_at", F.lit(base_ts).cast("timestamp")
                    )
                if "updated_at" in df_source.columns:
                    df_source = df_source.withColumn(
                        "updated_at", F.lit(base_ts).cast("timestamp")
                    )
            df = _align_to_contract(df_source, contract)
        else:
            df = _empty_df_for_contract(spark, contract)
        _set_status("bronze_write", table=table_fqn)
        view_name = f"_tmp_{table_short}"
        df.createOrReplaceTempView(view_name)
        spark.sql(f"INSERT INTO {table_fqn} SELECT * FROM {view_name}")
        print(f"bronze: loaded -> {table_fqn}")
        _set_status("bronze_written", table=table_fqn)

    rules_path = mock_root / "fixtures" / "dim_rule_scd2.json"
    seed_rules = load_rule_seed(rules_path)
    _set_status(
        "sync_rule_table_begin",
        table=f"{catalog}.gold.dim_rule_scd2",
        rows=len(seed_rules),
    )
    _write_dim_rule_table(spark, catalog=catalog, rules=seed_rules)
    _set_status(
        "sync_rule_table_done",
        table=f"{catalog}.gold.dim_rule_scd2",
        rows=len(seed_rules),
    )
    rules = load_runtime_rules(
        spark,
        catalog=catalog,
        mode="strict",
        seed_path=rules_path,
        table_name="gold.dim_rule_scd2",
    )
    _set_status("materialize_silver_begin", run_id=run_id)

    wallet_raw = _collect_table(spark, f"{catalog}.bronze.user_wallets_raw")
    ledger_raw = _collect_table(spark, f"{catalog}.bronze.transaction_ledger_raw")
    payment_orders_raw = _collect_table(spark, f"{catalog}.bronze.payment_orders_raw")
    orders_raw = _collect_table(spark, f"{catalog}.bronze.orders_raw")
    order_items_raw = _collect_table(spark, f"{catalog}.bronze.order_items_raw")
    products_raw = _collect_table(spark, f"{catalog}.bronze.products_raw")

    status_lookup = silver_controls.build_status_lookup(payment_orders_raw)
    bad_rate_rule = select_rule(rules, domain="silver", metric="bad_records_rate")
    entry_type_rule = select_rule(rules, domain="silver", metric="entry_type_allowed")
    status_rule = select_rule(rules, domain="silver", metric="payment_status_allowed")

    wallet_result = silver_controls.transform_wallet_snapshot_records(
        wallet_raw,
        run_id=run_id,
        rule_id=bad_rate_rule.rule_id if bad_rate_rule else None,
    )
    ledger_result = silver_controls.transform_ledger_entries_records(
        ledger_raw,
        run_id=run_id,
        rule_id=entry_type_rule.rule_id if entry_type_rule else None,
        allowed_entry_types=silver_controls.resolve_allowed_values(entry_type_rule),
        allowed_statuses=silver_controls.resolve_allowed_values(status_rule),
        status_lookup=status_lookup,
    )
    order_events_result = analytics.transform_order_events_records(
        orders_raw,
        payment_orders_raw,
        run_id=run_id,
    )
    order_items_result = analytics.transform_order_items_records(
        order_items_raw,
        run_id=run_id,
    )
    products_result = analytics.transform_products_records(
        products_raw,
        run_id=run_id,
    )

    silver_outputs = (
        ("silver.wallet_snapshot", wallet_result),
        ("silver.ledger_entries", ledger_result),
        ("silver.order_events", order_events_result),
        ("silver.order_items", order_items_result),
        ("silver.products", products_result),
    )

    all_bad_records: list[dict[str, Any]] = []
    for _, result in silver_outputs:
        all_bad_records.extend(result.bad_records)

    bad_contract = get_contract("silver.bad_records")
    bad_table_fqn = f"{catalog}.silver.bad_records"

    def _persist_bad_records(records: Sequence[dict[str, Any]]) -> None:
        _set_status("silver_bad_records_write", table=bad_table_fqn, rows=len(records))
        if not records:
            _set_status("silver_bad_records_written", table=bad_table_fqn, rows=0)
            return
        bad_df = _align_to_contract(
            spark.createDataFrame(records, schema=_contract_schema(bad_contract)),
            bad_contract,
        )
        bad_df.createOrReplaceTempView("_tmp_bad_records")
        spark.sql(f"INSERT INTO {bad_table_fqn} SELECT * FROM _tmp_bad_records")
        _set_status(
            "silver_bad_records_written", table=bad_table_fqn, rows=len(records)
        )

    _set_status("silver_bad_records_persist_then_failfast", table=bad_table_fqn)
    bad_rate_by_table = persist_bad_records_and_enforce_fail_fast(
        bad_records=all_bad_records,
        persist_bad_records=_persist_bad_records,
        fail_fast_checks=(
            FailFastCheck(
                table_name="silver.wallet_snapshot",
                valid_count=len(wallet_result.records),
                bad_count=len(wallet_result.bad_records),
            ),
            FailFastCheck(
                table_name="silver.ledger_entries",
                valid_count=len(ledger_result.records),
                bad_count=len(ledger_result.bad_records),
            ),
        ),
        rule=bad_rate_rule,
    )
    _set_status(
        "silver_failfast_passed",
        wallet_bad_rate=bad_rate_by_table.get("silver.wallet_snapshot"),
        ledger_bad_rate=bad_rate_by_table.get("silver.ledger_entries"),
    )

    for table_name, result in silver_outputs:
        contract = get_contract(table_name)
        if result.records:
            df = _align_to_contract(
                spark.createDataFrame(
                    result.records, schema=_contract_schema(contract)
                ),
                contract,
            )
        else:
            df = _empty_df_for_contract(spark, contract)
        short_name = table_name.split(".", maxsplit=1)[1]
        table_fqn = f"{catalog}.silver.{short_name}"
        _set_status("silver_write", table=table_fqn)
        if result.records:
            view_name = f"_tmp_{short_name}"
            df.createOrReplaceTempView(view_name)
            spark.sql(f"INSERT INTO {table_fqn} SELECT * FROM {view_name}")
        bad_rate = bad_rate_by_table.get(table_name)
        extra = f", bad_rate={bad_rate:.4f}" if bad_rate is not None else ""
        print(
            f"silver: wrote {len(result.records)} rows"
            f", bad={len(result.bad_records)}{extra} -> {table_fqn}"
        )
        _set_status("silver_written", table=table_fqn, rows=len(result.records))

    print(
        "E2E setup complete "
        f"(catalog={catalog}, scenario={scenario or 'default'}, run_id={run_id}, base_ts={base_ts.isoformat()})"
    )
    _set_status("done", catalog=catalog, run_id=run_id)


if __name__ == "__main__":
    main()
