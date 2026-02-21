"""Quick verification of Gold table data after E2E Workflow run."""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="hive_metastore")
    parser.add_argument("--run-id", default="")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    catalog = args.catalog

    gold_tables = [
        "recon_daily_snapshot_flow",
        "ledger_supply_balance_daily",
        "fact_payment_anonymized",
        "admin_tx_search",
        "ops_payment_failure_daily",
        "ops_payment_refund_daily",
        "ops_ledger_pairing_quality_daily",
        "exception_ledger",
        "pipeline_state",
    ]

    print("=" * 70)
    print(f"Gold Table Verification (catalog={catalog})")
    if args.run_id:
        print(f"Filtering by run_id={args.run_id}")
    print("=" * 70)

    total_tables = 0
    ok_tables = 0

    for table_name in gold_tables:
        fqn = f"{catalog}.gold.{table_name}"
        total_tables += 1

        if not spark.catalog.tableExists(fqn):
            print(f"\n[MISSING] {fqn}")
            continue

        df = spark.table(fqn)
        count = df.count()

        if args.run_id and "run_id" in df.columns:
            from pyspark.sql import functions as F

            df_filtered = df.filter(F.col("run_id") == args.run_id)
            filtered_count = df_filtered.count()
        else:
            filtered_count = count

        status = "OK" if filtered_count > 0 else "EMPTY"
        if filtered_count > 0:
            ok_tables += 1

        print(f"\n[{status}] {fqn}: {count} total rows, {filtered_count} for run_id")

        # Show schema
        print(f"  Columns: {', '.join(df.columns)}")

        # Show sample (up to 3 rows)
        if filtered_count > 0:
            sample_df = df_filtered if args.run_id and "run_id" in df.columns else df
            rows = sample_df.limit(3).collect()
            for i, row in enumerate(rows):
                d = row.asDict(recursive=True)
                # Truncate long values
                preview = {
                    k: (str(v)[:40] + "..." if len(str(v)) > 40 else v)
                    for k, v in d.items()
                }
                print(f"  Row {i}: {preview}")

    print("\n" + "=" * 70)
    print(f"Summary: {ok_tables}/{total_tables} tables have data")
    print("=" * 70)

    if ok_tables < total_tables:
        print(f"WARNING: {total_tables - ok_tables} tables missing or empty")
        sys.exit(1)


if __name__ == "__main__":
    main()
