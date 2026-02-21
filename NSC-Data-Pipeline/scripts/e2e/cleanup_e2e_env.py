from __future__ import annotations

import argparse

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="E2E cleanup: drop known UC tables created by mock-data E2E runs."
    )
    parser.add_argument("--catalog", required=True, help="Target Unity Catalog name.")
    parser.add_argument(
        "--drop-schemas",
        default="0",
        help="If '1', also DROP SCHEMA ... CASCADE for bronze/silver/gold (default: 0).",
    )
    return parser.parse_args()


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
        ),
    }
    for schema, table_names in tables_by_schema.items():
        for table_name in table_names:
            spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{table_name}")


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.getOrCreate()

    catalog = args.catalog
    _drop_known_tables(spark, catalog)
    print(f"Dropped known E2E tables in {catalog}.(bronze|silver|gold)")

    if str(args.drop_schemas).strip() == "1":
        for schema in ("bronze", "silver", "gold"):
            spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE")
        print(f"Dropped schemas {catalog}.(bronze|silver|gold) CASCADE")


if __name__ == "__main__":
    main()
