"""Production catalog bootstrap: create UC schemas + all contract tables.

Usage:
    python scripts/bootstrap_catalog.py --catalog prod_catalog
    python scripts/bootstrap_catalog.py --catalog prod_catalog --dry-run

This script is idempotent (IF NOT EXISTS everywhere) and creates NO data.
It must NOT be confused with scripts/e2e/setup_e2e_env.py which loads mock data.
"""

from __future__ import annotations

import argparse
import inspect
import logging
import sys
from pathlib import Path

# Bootstrap: ensure repo root is on sys.path for src.* imports.
_SCRIPT_PATH = (
    globals().get("__file__") or inspect.getframeinfo(inspect.currentframe()).filename
)
_REPO_ROOT = Path(_SCRIPT_PATH).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Production catalog bootstrap: create UC schemas and contract tables.",
    )
    parser.add_argument(
        "--catalog",
        required=True,
        help="Target Unity Catalog name (required, no default for safety).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print DDL without executing.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    log = logging.getLogger("bootstrap_catalog")

    args = parse_args()

    from src.io.catalog_bootstrap import (
        create_all_tables,
        create_schemas,
        resolve_all_table_specs,
        validate_tables_exist,
    )

    catalog = args.catalog
    dry_run = args.dry_run

    log.info(
        "Bootstrap start (catalog=%s, dry_run=%s, tables=%d)",
        catalog,
        dry_run,
        len(resolve_all_table_specs(catalog)),
    )

    # Step 1: Create schemas
    log.info("Step 1/3: Creating schemas")
    schema_ddl = create_schemas(
        spark=None if dry_run else _get_spark(), catalog=catalog, dry_run=dry_run
    )
    for ddl in schema_ddl:
        print(ddl)

    # Step 2: Create tables
    log.info("Step 2/3: Creating tables")
    table_ddl = create_all_tables(
        spark=None if dry_run else _get_spark(), catalog=catalog, dry_run=dry_run
    )
    for ddl in table_ddl:
        print(ddl)

    # Step 3: Validate
    if not dry_run:
        log.info("Step 3/3: Validating all tables exist")
        results = validate_tables_exist(_get_spark(), catalog)
        missing = [r for r in results if not r.exists]
        if missing:
            for r in missing:
                log.error("MISSING after bootstrap: %s", r.table_fqn)
            sys.exit(1)
        log.info("Validation passed: all %d tables exist", len(results))
    else:
        log.info("Step 3/3: Skipping validation (dry-run mode)")

    log.info("Bootstrap complete (catalog=%s)", catalog)


_spark_session = None


def _get_spark():
    global _spark_session
    if _spark_session is None:
        from pyspark.sql import SparkSession

        _spark_session = SparkSession.builder.getOrCreate()
    return _spark_session


if __name__ == "__main__":
    main()
