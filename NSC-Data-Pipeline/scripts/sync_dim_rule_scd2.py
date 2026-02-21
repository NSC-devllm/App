from __future__ import annotations

import argparse
import inspect
import sys
from collections import defaultdict
from pathlib import Path

# Bootstrap: ensure repo root is on sys.path for src.* imports.
_SCRIPT_PATH = (
    globals().get("__file__") or inspect.getframeinfo(inspect.currentframe()).filename
)
_REPO_ROOT = Path(_SCRIPT_PATH).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.common.config_loader import find_repo_root  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync dim_rule seed into gold.dim_rule_scd2",
    )
    parser.add_argument("--catalog", required=True)
    parser.add_argument(
        "--seed-path",
        default="mock_data/fixtures/dim_rule_scd2.json",
        help="Rule seed JSON path.",
    )
    parser.add_argument(
        "--table-name",
        default="gold.dim_rule_scd2",
        help="Rule table name (gold.dim_rule_scd2 or fully-qualified catalog.schema.table).",
    )
    parser.add_argument(
        "--repo-root",
        default=str(find_repo_root()),
    )
    return parser.parse_args()


def _contract_schema(contract):
    from pyspark.sql.types import ArrayType, DoubleType, MapType, StringType, StructType

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


def _align_to_contract(df, contract):
    from pyspark.sql import functions as F

    for column, data_type in contract.cast_map.items():
        if column in df.columns:
            df = df.withColumn(column, F.col(column).cast(data_type))
        else:
            df = df.withColumn(column, F.lit(None).cast(data_type))
    return df.select(*contract.column_names)


def _resolve_seed_path(repo_root: Path, seed_path: str) -> Path:
    path = Path(seed_path)
    if path.is_absolute():
        return path
    return repo_root / path


def _resolve_table_fqn(*, catalog: str, table_name: str) -> str:
    normalized = table_name.strip()
    if not normalized:
        raise ValueError("table_name must not be empty")
    parts = [part for part in normalized.split(".") if part]
    if len(parts) == 3:
        return normalized
    if len(parts) == 2:
        return f"{catalog}.{normalized}"
    if len(parts) == 1:
        return f"{catalog}.gold.{normalized}"
    raise ValueError(f"Unsupported table_name format: {table_name!r}")


def _validate_rules(rules) -> None:
    if not rules:
        raise ValueError("Rule seed is empty")

    seen_rule_ids: set[str] = set()
    duplicates: set[str] = set()
    current_by_metric: defaultdict[tuple[str | None, str | None], int] = defaultdict(
        int
    )

    for rule in rules:
        if rule.rule_id in seen_rule_ids:
            duplicates.add(rule.rule_id)
        seen_rule_ids.add(rule.rule_id)

        if rule.is_current:
            metric_key = (rule.domain, rule.metric)
            current_by_metric[metric_key] += 1

    if duplicates:
        dup_list = ", ".join(sorted(duplicates))
        raise ValueError(f"Duplicate rule_id detected: {dup_list}")

    invalid_currents = sorted(
        key for key, count in current_by_metric.items() if count > 1
    )
    if invalid_currents:
        formatted = ", ".join(
            f"{domain}.{metric}" for domain, metric in invalid_currents
        )
        raise ValueError(
            f"Multiple is_current=true rules for same domain+metric: {formatted}"
        )


def main() -> None:
    args = parse_args()
    repo_root = Path(args.repo_root)
    if repo_root.as_posix() not in sys.path:
        sys.path.insert(0, repo_root.as_posix())

    from pyspark.sql import SparkSession

    from src.common.contracts import get_contract, validate_required_columns
    from src.io.gold_io import write_gold_delta
    from src.io.rule_loader import load_rule_seed

    spark = SparkSession.builder.getOrCreate()
    seed_path = _resolve_seed_path(repo_root, args.seed_path)
    rules = load_rule_seed(seed_path)
    _validate_rules(rules)

    rows = [rule.as_storage_dict() for rule in rules]
    contract = get_contract("gold.dim_rule_scd2")
    validate_required_columns(rows[0].keys(), contract)
    df = _align_to_contract(
        spark.createDataFrame(rows, schema=_contract_schema(contract)),
        contract,
    )

    table_fqn = _resolve_table_fqn(catalog=args.catalog, table_name=args.table_name)
    write_gold_delta(
        df,
        table_fqn,
        table_name="gold.dim_rule_scd2",
        mode="overwrite",
    )

    print(
        "sync_dim_rule_scd2 completed "
        f"(table_fqn={table_fqn}, rows={len(rows)}, seed_path={seed_path})"
    )


if __name__ == "__main__":
    main()
