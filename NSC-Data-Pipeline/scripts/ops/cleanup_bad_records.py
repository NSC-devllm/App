from __future__ import annotations

import argparse
import inspect
import sys
from pathlib import Path

# Bootstrap: ensure repo root is on sys.path for src.* imports.
_SCRIPT_PATH = (
    globals().get("__file__") or inspect.getframeinfo(inspect.currentframe()).filename
)
_REPO_ROOT = Path(_SCRIPT_PATH).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.io.spark_safety import safe_collect  # noqa: E402

DEFAULT_RETENTION_DAYS = 180
SPARK_SESSION_TIMEZONE = "Asia/Seoul"


def _parse_bool(raw_value: str) -> bool:
    normalized = str(raw_value).strip().lower()
    if normalized in {"true", "1", "yes", "y"}:
        return True
    if normalized in {"false", "0", "no", "n"}:
        return False
    raise argparse.ArgumentTypeError(
        f"Invalid boolean value for --dry-run: {raw_value!r} (expected true/false)"
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Delete old rows from silver.bad_records with retention window.",
    )
    parser.add_argument(
        "--catalog",
        required=True,
        help="Target catalog name.",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=DEFAULT_RETENTION_DAYS,
        help=f"Retention window in days (default: {DEFAULT_RETENTION_DAYS}).",
    )
    parser.add_argument(
        "--dry-run",
        nargs="?",
        const="true",
        default="false",
        type=_parse_bool,
        help="Show candidate row count only (no DELETE).",
    )
    return parser.parse_args(argv)


def _quote_identifier(value: str) -> str:
    return f"`{value.replace('`', '``')}`"


def _resolve_bad_records_tables(catalog: str) -> tuple[str, str]:
    normalized_catalog = catalog.strip()
    if not normalized_catalog:
        raise ValueError("catalog must not be empty")
    table_fqn = f"{normalized_catalog}.silver.bad_records"
    quoted_table_fqn = f"{_quote_identifier(normalized_catalog)}.`silver`.`bad_records`"
    return table_fqn, quoted_table_fqn


def build_retention_predicate(retention_days: int) -> str:
    return f"detected_date_kst < date_sub(current_date(), {retention_days})"


def _set_kst_session_timezone(spark) -> None:
    conf = getattr(spark, "conf", None)
    if conf is not None and hasattr(conf, "set"):
        conf.set("spark.sql.session.timeZone", SPARK_SESSION_TIMEZONE)


def _count_candidates(
    spark,
    *,
    quoted_table_fqn: str,
    predicate_sql: str,
) -> int:
    query = (
        "SELECT COUNT(*) AS candidate_count "
        f"FROM {quoted_table_fqn} WHERE {predicate_sql}"
    )
    rows = safe_collect(
        spark.sql(query),
        max_rows=1,
        context="bad_records_cleanup:count",
    )
    if not rows:
        raise RuntimeError("Failed to retrieve candidate_count for cleanup")

    candidate_count = rows[0].asDict(recursive=True).get("candidate_count")
    if candidate_count is None:
        raise RuntimeError("candidate_count is missing from cleanup count query result")
    return int(candidate_count)


def run_cleanup(
    *,
    spark,
    catalog: str,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    dry_run: bool = False,
) -> int:
    if retention_days <= 0:
        raise ValueError(
            f"retention_days must be > 0 (received retention_days={retention_days})"
        )

    _set_kst_session_timezone(spark)

    table_fqn, quoted_table_fqn = _resolve_bad_records_tables(catalog)
    if not spark.catalog.tableExists(table_fqn):
        raise RuntimeError(f"Target table not found: {table_fqn}")

    predicate_sql = build_retention_predicate(retention_days)
    candidate_count = _count_candidates(
        spark,
        quoted_table_fqn=quoted_table_fqn,
        predicate_sql=predicate_sql,
    )
    print(
        "cleanup_bad_records candidates "
        f"(table={table_fqn}, retention_days={retention_days}, count={candidate_count})"
    )

    if dry_run:
        print("cleanup_bad_records dry-run=true (DELETE skipped)")
        return candidate_count

    delete_sql = f"DELETE FROM {quoted_table_fqn} WHERE {predicate_sql}"
    spark.sql(delete_sql)
    print(
        "cleanup_bad_records delete executed "
        f"(table={table_fqn}, deleted_rows={candidate_count})"
    )
    return candidate_count


def main() -> None:
    from pyspark.sql import SparkSession

    args = parse_args()
    spark = SparkSession.builder.getOrCreate()
    run_cleanup(
        spark=spark,
        catalog=args.catalog,
        retention_days=args.retention_days,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
