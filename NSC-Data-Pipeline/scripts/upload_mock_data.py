from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

try:
    ROOT = Path(__file__).resolve().parents[1]
except NameError:  # __file__ may be missing in Databricks job context
    ROOT = Path(os.environ.get("PIPELINE_ROOT", "/dbfs/tmp/data-pipeline"))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.common.contracts import BRONZE_CONTRACTS
from src.io.bronze_io import (
    bronze_short_name,
    build_bronze_dataframe,
    default_batch_id,
    normalize_table_name,
    write_bronze_delta,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload mock bronze data into a Databricks catalog."
    )
    parser.add_argument(
        "--base-dir",
        default=str(Path("mock_data/bronze")),
        help="Base directory containing mock bronze files.",
    )
    parser.add_argument(
        "--catalog",
        default=os.environ.get("DATABRICKS_CATALOG", "dev_catalog"),
        help="Target catalog name.",
    )
    parser.add_argument(
        "--schema",
        default="bronze",
        help="Target schema name.",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        help="Optional list of bronze tables to upload (short or full names).",
    )
    parser.add_argument(
        "--batch-id",
        help="Override batch_id for ingested records.",
    )
    parser.add_argument(
        "--source-system",
        default="mock_data",
        help="Override source_system metadata value.",
    )
    parser.add_argument(
        "--source-extracted-at",
        help="Override source_extracted_at (ISO8601).",
    )
    parser.add_argument(
        "--ingested-at",
        help="Override ingested_at (ISO8601).",
    )
    parser.add_argument(
        "--skip-create-schema",
        action="store_true",
        help="Skip CREATE SCHEMA IF NOT EXISTS.",
    )
    return parser.parse_args()


def get_spark_session():
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("pyspark is required to run this script") from exc
    return SparkSession.builder.getOrCreate()


def resolve_tables(requested: list[str] | None) -> list[str]:
    if not requested:
        return list(BRONZE_CONTRACTS.keys())
    return [normalize_table_name(name) for name in requested]


def main() -> None:
    args = parse_args()
    spark = get_spark_session()

    if not args.skip_create_schema:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.schema}")

    batch_id = args.batch_id or default_batch_id()
    table_names = resolve_tables(args.tables)

    for table_name in table_names:
        df = build_bronze_dataframe(
            spark,
            table_name,
            base_dir=Path(args.base_dir),
            ingested_at=args.ingested_at,
            source_extracted_at=args.source_extracted_at,
            batch_id=batch_id,
            source_system=args.source_system,
        )
        table_fqn = f"{args.catalog}.{args.schema}.{bronze_short_name(table_name)}"
        write_bronze_delta(df, table_fqn)
        print(f"Uploaded {table_name} -> {table_fqn}")


if __name__ == "__main__":
    main()
