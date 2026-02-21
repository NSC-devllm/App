"""Minimal diagnostic script to test Delta write to managed table via UC.

Tests whether ADLS write works through various credential paths.
Run as a one-off Databricks job.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone


def _set_status(stage: str, **extra) -> None:
    payload = {
        "stage": stage,
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        **extra,
    }
    print(json.dumps(payload), flush=True)
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark:
            try:
                from pyspark.dbutils import DBUtils

                DBUtils(spark).fs.put(
                    "dbfs:/tmp/data-pipeline/e2e/diag_status.json",
                    json.dumps(payload),
                    overwrite=True,
                )
            except Exception:
                pass
    except Exception:
        pass


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--sas-token", default="")
    args = parser.parse_args()
    catalog = args.catalog

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    _set_status("start", catalog=catalog)

    storage_acct = "2dtfinalteam4storagetest"

    # ── Test 1: Set SAS config if provided ──────────────────────
    if args.sas_token:
        _set_status("set_sas_config")
        key = f"fs.azure.sas.fixed.token.{storage_acct}.dfs.core.windows.net"
        spark.conf.set(f"spark.hadoop.{key}", args.sas_token)
        spark.conf.set(
            f"spark.hadoop.fs.azure.account.auth.type.{storage_acct}.dfs.core.windows.net",
            "SAS",
        )
        spark.conf.set(
            f"spark.hadoop.fs.azure.sas.token.provider.type.{storage_acct}.dfs.core.windows.net",
            "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider",
        )
        print("  SAS config set", flush=True)

    # ── Test 2: Parquet write (no Delta commit protocol) ────────
    _set_status("parquet_write")
    parquet_path = f"abfss://2dt-final-team4-adls-test@{storage_acct}.dfs.core.windows.net/bronze/_diag_parquet"
    try:
        t0 = time.time()
        spark.range(1).write.format("parquet").mode("overwrite").save(parquet_path)
        elapsed = time.time() - t0
        print(f"  parquet write OK ({elapsed:.1f}s)", flush=True)
        from pyspark.dbutils import DBUtils

        DBUtils(spark).fs.rm(parquet_path, recurse=True)
    except Exception as e:
        print(f"  parquet write FAILED: {e}", flush=True)

    # ── Test 3: Delta write (direct path, no UC table) ──────────
    _set_status("delta_direct_write")
    delta_path = f"abfss://2dt-final-team4-adls-test@{storage_acct}.dfs.core.windows.net/bronze/_diag_delta"
    try:
        t0 = time.time()
        spark.range(1).write.format("delta").mode("overwrite").save(delta_path)
        elapsed = time.time() - t0
        print(f"  delta direct write OK ({elapsed:.1f}s)", flush=True)
        from pyspark.dbutils import DBUtils

        DBUtils(spark).fs.rm(delta_path, recurse=True)
    except Exception as e:
        print(f"  delta direct write FAILED: {e}", flush=True)

    # ── Test 4: UC managed table write ──────────────────────────
    _set_status("managed_table_write")
    test_table = f"{catalog}.bronze._diag_test"
    try:
        spark.sql(f"DROP TABLE IF EXISTS {test_table}")
        spark.sql(f"CREATE TABLE {test_table} (id BIGINT, msg STRING) USING DELTA")
        print(f"  created {test_table}", flush=True)

        t0 = time.time()
        spark.sql(f"INSERT INTO {test_table} VALUES (1, 'hello')")
        elapsed = time.time() - t0
        print(f"  managed table INSERT OK ({elapsed:.1f}s)", flush=True)

        cnt = spark.sql(f"SELECT count(*) FROM {test_table}").collect()[0][0]
        print(f"  count: {cnt}", flush=True)
        spark.sql(f"DROP TABLE IF EXISTS {test_table}")
    except Exception as e:
        print(f"  managed table FAILED: {e}", flush=True)

    _set_status("done")
    print("=== ALL DIAGNOSTICS COMPLETE ===", flush=True)


if __name__ == "__main__":
    main()
