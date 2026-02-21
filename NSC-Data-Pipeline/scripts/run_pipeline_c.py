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
from src.io.spark_safety import safe_collect  # noqa: E402

MAX_PIPELINE_STATE_ROWS = 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Pipeline C (Analytics anonymized mart) in Databricks."
    )
    parser.add_argument("--catalog", default=get_config_value("databricks.catalog"))
    parser.add_argument("--run-mode", default="")
    parser.add_argument("--run-id")
    parser.add_argument("--start-ts")
    parser.add_argument("--end-ts")
    parser.add_argument("--date-kst-start")
    parser.add_argument("--date-kst-end")
    parser.add_argument("--salt")
    parser.add_argument(
        "--secret-scope",
        default=get_config_value("analytics.secret_scope"),
    )
    parser.add_argument(
        "--secret-key",
        default=get_config_value("analytics.secret_key"),
    )
    parser.add_argument(
        "--repo-root",
        default=str(find_repo_root()),
    )
    parser.add_argument(
        "--skip-upstream-readiness-check",
        action="store_true",
        help="Skip fail-closed upstream readiness check for emergency/manual runs.",
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


def main() -> None:
    args = parse_args()
    repo_root = Path(args.repo_root)
    if repo_root.as_posix() not in sys.path:
        sys.path.insert(0, repo_root.as_posix())

    from src.common.contracts import get_contract
    from src.common.job_params import JobParams
    from src.common.window_defaults import inject_default_daily_backfill
    from src.io.gold_io import write_gold_delta
    from src.io.pipeline_state_io import (
        STATE_FAILURE,
        STATE_SUCCESS,
        apply_pipeline_state,
        write_pipeline_state_delta,
    )
    from src.io.secret_loader import resolve_user_key_salt
    from src.io.upstream_readiness import assert_pipeline_ready
    from src.jobs.pipeline_c_spark import transform_pipeline_c_fact_spark

    spark = SparkSession.builder.getOrCreate()

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
    params = JobParams.from_mapping(
        {
            "run_mode": params_payload.get("run_mode"),
            "start_ts": params_payload.get("start_ts"),
            "end_ts": params_payload.get("end_ts"),
            "date_kst_start": params_payload.get("date_kst_start"),
            "date_kst_end": params_payload.get("date_kst_end"),
            "run_id": params_payload.get("run_id"),
        },
        pipeline_name="pipeline_c",
    )

    silver_schema = f"{args.catalog}.silver"
    pipeline_state_table = f"{args.catalog}.gold.pipeline_state"
    pipeline_contract = get_contract("gold.pipeline_state")

    try:
        if not args.skip_upstream_readiness_check:
            assert_pipeline_ready(
                spark,
                catalog=args.catalog,
                upstream_pipeline_name="pipeline_silver",
                required_processed_end=params.processed_end_utc(),
            )

        order_events_df = spark.table(f"{silver_schema}.order_events")
        if (
            params.start_ts
            and params.end_ts
            and "event_time" in order_events_df.columns
        ):
            order_events_df = order_events_df.filter(
                (F.col("event_time") >= F.lit(params.start_ts))
                & (F.col("event_time") < F.lit(params.end_ts))
            )
        elif params.target_dates() and "event_date_kst" in order_events_df.columns:
            order_events_df = order_events_df.filter(
                F.col("event_date_kst").isin(*params.target_dates())
            )

        resolved_salt = args.salt or resolve_user_key_salt(
            secret_scope=args.secret_scope,
            secret_key=args.secret_key,
            allow_local_fallback=True,
        )
        result_df = transform_pipeline_c_fact_spark(
            order_events_df,
            spark.table(f"{silver_schema}.order_items"),
            spark.table(f"{silver_schema}.products"),
            run_id=params.run_id,
            salt=resolved_salt,
        )
        result_df = result_df.persist()
        try:
            row_count = result_df.count()
            if row_count > 0:
                contract = get_contract("gold.fact_payment_anonymized")
                df = _align_to_contract(result_df, contract)
                target_table = f"{args.catalog}.gold.fact_payment_anonymized"
                write_gold_delta(
                    df,
                    target_table,
                    table_name="gold.fact_payment_anonymized",
                    mode="overwrite",
                )
                print(
                    f"Upserted {row_count} rows into {target_table} (run_id={params.run_id})"
                )
            else:
                print("No rows generated for gold.fact_payment_anonymized")
        finally:
            result_df.unpersist()

        success_state = apply_pipeline_state(
            pipeline_name="pipeline_c",
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
        current_state = _load_current_state(
            spark,
            pipeline_state_table,
            "pipeline_c",
        )
        failed_state = apply_pipeline_state(
            pipeline_name="pipeline_c",
            run_id=params.run_id,
            status=STATE_FAILURE,
            current_state=current_state,
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
