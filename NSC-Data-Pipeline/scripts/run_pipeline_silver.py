from __future__ import annotations

import argparse
import inspect
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Bootstrap: ensure repo root is on sys.path for src.* imports.
_SCRIPT_PATH = (
    globals().get("__file__") or inspect.getframeinfo(inspect.currentframe()).filename
)
_REPO_ROOT = Path(_SCRIPT_PATH).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.common.config_loader import find_repo_root, get_config_value  # noqa: E402
from src.common.rule_mode_guard import enforce_prod_strict_rule_mode  # noqa: E402
from src.io.spark_safety import safe_collect  # noqa: E402

SPARK_SESSION_TIMEZONE = "UTC"
MAX_PIPELINE_STATE_ROWS = 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Pipeline Silver (Bronze -> Silver materialization) in Databricks."
    )
    parser.add_argument("--catalog", default=get_config_value("databricks.catalog"))
    parser.add_argument("--run-mode", default="")
    parser.add_argument("--start-ts")
    parser.add_argument("--end-ts")
    parser.add_argument("--date-kst-start")
    parser.add_argument("--date-kst-end")
    parser.add_argument("--run-id")
    parser.add_argument(
        "--rule-load-mode",
        default="fallback",
        choices=("strict", "fallback"),
        help="Rule load mode: strict(table only) or fallback(table->seed).",
    )
    parser.add_argument(
        "--rule-table",
        default="gold.dim_rule_scd2",
        help="Rule table name (gold.dim_rule_scd2 or fully-qualified catalog.schema.table).",
    )
    parser.add_argument(
        "--rule-seed-path",
        default="mock_data/fixtures/dim_rule_scd2.json",
        help="Fallback rule seed path.",
    )
    parser.add_argument(
        "--repo-root",
        default=str(find_repo_root()),
    )
    return parser.parse_args()


def _contract_schema(contract):
    """Build a Spark StructType from a TableContract."""
    from pyspark.sql.types import StructType

    _type_map = {"bigint": "long", "int": "integer"}
    schema = StructType()
    for col in contract.columns:
        spark_type = _type_map.get(col.data_type, col.data_type)
        schema.add(col.name, spark_type, nullable=True)
    return schema


def _align_to_contract(df, contract):
    for column, data_type in contract.cast_map.items():
        if column in df.columns:
            df = df.withColumn(column, F.col(column).cast(data_type))
        else:
            df = df.withColumn(column, F.lit(None).cast(data_type))
    return df.select(*contract.column_names)


def _load_current_state(spark, table_fqn: str, pipeline_name: str):
    from src.io.pipeline_state_io import parse_pipeline_state_record

    if not spark.catalog.tableExists(table_fqn):
        return None
    rows = safe_collect(
        spark.table(table_fqn).filter(F.col("pipeline_name") == F.lit(pipeline_name)),
        max_rows=MAX_PIPELINE_STATE_ROWS,
        context=f"pipeline_state:{pipeline_name}",
    )
    if not rows:
        return None
    return parse_pipeline_state_record(rows[0].asDict(recursive=True))


def _resolve_seed_path(repo_root: Path, seed_path: str) -> Path:
    path = Path(seed_path)
    if path.is_absolute():
        return path
    return repo_root / path


def _filter_to_windows(df, windows_utc, timestamp_fields: tuple[str, ...]):
    if not windows_utc:
        return df
    present_fields = [field for field in timestamp_fields if field in df.columns]
    if not present_fields:
        return df

    window_cond = None
    for start_ts, end_ts in windows_utc:
        field_cond = None
        for field in present_fields:
            cond = (F.col(field) >= F.lit(start_ts)) & (F.col(field) < F.lit(end_ts))
            field_cond = cond if field_cond is None else (field_cond | cond)
        window_cond = field_cond if window_cond is None else (window_cond | field_cond)
    return df.filter(window_cond)


def _append_bad_records_df(
    spark,
    *,
    catalog: str,
    bad_records_df,
) -> None:
    from src.common.contracts import get_contract

    contract = get_contract("silver.bad_records")
    table_fqn = f"{catalog}.silver.bad_records"
    aligned_df = _align_to_contract(bad_records_df, contract)
    aligned_df.write.format("delta").mode("append").partitionBy(
        "detected_date_kst"
    ).saveAsTable(table_fqn)


def main() -> None:
    args = parse_args()
    repo_root = Path(args.repo_root)
    if repo_root.as_posix() not in sys.path:
        sys.path.insert(0, repo_root.as_posix())
    enforce_prod_strict_rule_mode(
        pipeline_name="pipeline_silver",
        rule_load_mode=args.rule_load_mode,
        catalog=args.catalog,
        repo_root=repo_root,
    )

    from src.common.contracts import get_contract, validate_required_columns
    from src.common.job_params import JobParams
    from src.common.rules import select_rule
    from src.common.window_defaults import inject_default_daily_backfill
    from src.io.pipeline_state_io import (
        STATE_FAILURE,
        STATE_SUCCESS,
        apply_pipeline_state,
        write_pipeline_state_delta,
    )
    from src.io.rule_loader import load_runtime_rules
    from src.io.silver_io import write_silver_delta
    from src.jobs.pipeline_silver_spark import transform_silver_tables_spark
    from src.transforms import silver_controls

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", SPARK_SESSION_TIMEZONE)

    params_payload = inject_default_daily_backfill(
        {
            "run_mode": args.run_mode,
            "start_ts": args.start_ts,
            "end_ts": args.end_ts,
            "date_kst_start": args.date_kst_start,
            "date_kst_end": args.date_kst_end,
            "run_id": args.run_id,
        }
    )
    params = JobParams.from_mapping(params_payload, pipeline_name="pipeline_silver")
    windows_utc = params.windows_utc()

    rules = load_runtime_rules(
        spark,
        catalog=args.catalog,
        mode=args.rule_load_mode,
        seed_path=_resolve_seed_path(repo_root, args.rule_seed_path),
        table_name=args.rule_table,
    )

    bronze_schema = f"{args.catalog}.bronze"
    pipeline_state_table = f"{args.catalog}.gold.pipeline_state"
    pipeline_contract = get_contract("gold.pipeline_state")

    try:
        wallet_df = _filter_to_windows(
            spark.table(f"{bronze_schema}.user_wallets_raw"),
            windows_utc,
            ("updated_at", "source_extracted_at", "ingested_at"),
        )
        ledger_df = _filter_to_windows(
            spark.table(f"{bronze_schema}.transaction_ledger_raw"),
            windows_utc,
            ("created_at", "ingested_at"),
        )
        payment_orders_df = _filter_to_windows(
            spark.table(f"{bronze_schema}.payment_orders_raw"),
            windows_utc,
            ("created_at", "ingested_at"),
        )
        orders_df = _filter_to_windows(
            spark.table(f"{bronze_schema}.orders_raw"),
            windows_utc,
            ("created_at", "ingested_at"),
        )
        order_items_df = spark.table(f"{bronze_schema}.order_items_raw")
        products_df = spark.table(f"{bronze_schema}.products_raw")

        bad_rate_rule = select_rule(rules, domain="silver", metric="bad_records_rate")
        entry_type_rule = select_rule(
            rules, domain="silver", metric="entry_type_allowed"
        )
        status_rule = select_rule(
            rules, domain="silver", metric="payment_status_allowed"
        )
        allowed_entry_types = silver_controls.resolve_allowed_values(entry_type_rule)
        allowed_statuses = silver_controls.resolve_allowed_values(status_rule)

        spark_outputs = transform_silver_tables_spark(
            wallet_df=wallet_df,
            ledger_df=ledger_df,
            payment_orders_df=payment_orders_df,
            orders_df=orders_df,
            order_items_df=order_items_df,
            products_df=products_df,
            run_id=params.run_id,
            bad_rate_rule_id=bad_rate_rule.rule_id if bad_rate_rule else None,
            entry_type_rule_id=entry_type_rule.rule_id if entry_type_rule else None,
            allowed_entry_types=allowed_entry_types,
            allowed_statuses=allowed_statuses,
        )
        _append_bad_records_df(
            spark,
            catalog=args.catalog,
            bad_records_df=spark_outputs.bad_records_df,
        )

        silver_controls.enforce_bad_records_rate(
            valid_count=spark_outputs.wallet_valid_count,
            bad_count=spark_outputs.wallet_bad_count,
            rule=bad_rate_rule,
        )
        silver_controls.enforce_bad_records_rate(
            valid_count=spark_outputs.ledger_valid_count,
            bad_count=spark_outputs.ledger_bad_count,
            rule=bad_rate_rule,
        )

        silver_output_dfs = (
            ("silver.wallet_snapshot", spark_outputs.wallet_snapshot_df),
            ("silver.ledger_entries", spark_outputs.ledger_entries_df),
            ("silver.order_events", spark_outputs.order_events_df),
            ("silver.order_items", spark_outputs.order_items_df),
            ("silver.products", spark_outputs.products_df),
        )
        for table_name, df in silver_output_dfs:
            contract = get_contract(table_name)
            validate_required_columns(df.columns, contract)
            aligned_df = _align_to_contract(df, contract)
            short_name = table_name.split(".", maxsplit=1)[1]
            write_silver_delta(
                aligned_df,
                f"{args.catalog}.silver.{short_name}",
                table_name=table_name,
                mode="overwrite",
            )

        success_state = apply_pipeline_state(
            pipeline_name="pipeline_silver",
            run_id=params.run_id,
            status=STATE_SUCCESS,
            last_processed_end=params.processed_end_utc(),
        )
        state_df = _align_to_contract(
            spark.createDataFrame(
                [success_state.as_dict()],
                schema=_contract_schema(pipeline_contract),
            ),
            pipeline_contract,
        )
        write_pipeline_state_delta(state_df, pipeline_state_table)
    except Exception:
        failed_state = apply_pipeline_state(
            pipeline_name="pipeline_silver",
            run_id=params.run_id,
            status=STATE_FAILURE,
            current_state=_load_current_state(
                spark,
                pipeline_state_table,
                "pipeline_silver",
            ),
        )
        state_df = _align_to_contract(
            spark.createDataFrame(
                [failed_state.as_dict()],
                schema=_contract_schema(pipeline_contract),
            ),
            pipeline_contract,
        )
        write_pipeline_state_delta(state_df, pipeline_state_table)
        raise


if __name__ == "__main__":
    main()
