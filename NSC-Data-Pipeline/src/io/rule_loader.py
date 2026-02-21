from __future__ import annotations

import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any

from src.common.rules import RuleDefinition
from src.io.spark_safety import safe_collect

RULE_LOAD_MODE_STRICT = "strict"
RULE_LOAD_MODE_FALLBACK = "fallback"
VALID_RULE_LOAD_MODES = frozenset((RULE_LOAD_MODE_STRICT, RULE_LOAD_MODE_FALLBACK))
DEFAULT_RULE_TABLE_NAME = "gold.dim_rule_scd2"
DEFAULT_RULE_SEED_PATH = Path("mock_data/fixtures/dim_rule_scd2.json")
DEFAULT_RULE_TABLE_MAX_ROWS = 1000
LOGGER = logging.getLogger(__name__)


class RuleTableAccessError(RuntimeError):
    """Raised when runtime rule table cannot be looked up/read from Spark."""


def _validate_runtime_rule_integrity(
    rules: list[RuleDefinition], *, source: str
) -> None:
    if not rules:
        raise ValueError(f"Rule source is empty: {source}")

    seen_rule_ids: set[str] = set()
    duplicate_rule_ids: set[str] = set()
    current_by_metric: defaultdict[tuple[str | None, str | None], int] = defaultdict(
        int
    )

    for rule in rules:
        if rule.rule_id in seen_rule_ids:
            duplicate_rule_ids.add(rule.rule_id)
        seen_rule_ids.add(rule.rule_id)

        if rule.is_current:
            current_by_metric[(rule.domain, rule.metric)] += 1

    if duplicate_rule_ids:
        duplicates = ", ".join(sorted(duplicate_rule_ids))
        raise ValueError(f"Duplicate rule_id detected in {source}: {duplicates}")

    invalid_currents = sorted(
        key for key, count in current_by_metric.items() if count > 1
    )
    if invalid_currents:
        metrics = ", ".join(f"{domain}.{metric}" for domain, metric in invalid_currents)
        raise ValueError(
            "Multiple is_current=true rules for same domain+metric in "
            f"{source}: {metrics}"
        )


def load_rule_seed(path: str | Path) -> list[RuleDefinition]:
    seed_path = Path(path)
    if not seed_path.exists():
        raise FileNotFoundError(f"Rule seed not found: {seed_path}")

    payload = json.loads(seed_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("Rule seed must be a list of rule definitions")

    return [_parse_rule(item) for item in payload]


def _parse_rule(item: dict[str, Any]) -> RuleDefinition:
    if not isinstance(item, dict):
        raise ValueError("Each rule definition must be a JSON object")
    return RuleDefinition.from_dict(item)


def _resolve_rule_table_fqn(*, catalog: str, table_name: str) -> str:
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


def load_rule_table(spark, table_fqn: str) -> list[RuleDefinition]:
    try:
        table_exists = spark.catalog.tableExists(table_fqn)
    except Exception as exc:
        raise RuleTableAccessError(f"Rule table lookup failed: {table_fqn}") from exc

    if not table_exists:
        raise FileNotFoundError(f"Rule table not found: {table_fqn}")

    try:
        payload = [
            row.asDict(recursive=True)
            for row in safe_collect(
                spark.table(table_fqn),
                max_rows=DEFAULT_RULE_TABLE_MAX_ROWS,
                context=f"rule_loader:{table_fqn}",
            )
        ]
    except Exception as exc:
        raise RuleTableAccessError(f"Rule table read failed: {table_fqn}") from exc
    if not payload:
        raise ValueError(f"Rule table is empty: {table_fqn}")

    rules = [_parse_rule(item) for item in payload]
    _validate_runtime_rule_integrity(rules, source=f"table:{table_fqn}")
    LOGGER.info(
        "rule_source=table rule_count=%d table_fqn=%s",
        len(rules),
        table_fqn,
    )
    return rules


def load_runtime_rules(
    spark,
    *,
    catalog: str,
    mode: str,
    seed_path: str | Path,
    table_name: str = DEFAULT_RULE_TABLE_NAME,
) -> list[RuleDefinition]:
    normalized_mode = mode.strip().lower()
    if normalized_mode not in VALID_RULE_LOAD_MODES:
        raise ValueError(
            f"Unsupported rule load mode: {mode!r}. "
            f"Expected one of {sorted(VALID_RULE_LOAD_MODES)}"
        )

    table_fqn = _resolve_rule_table_fqn(catalog=catalog, table_name=table_name)
    try:
        return load_rule_table(spark, table_fqn)
    except Exception as exc:
        if normalized_mode == RULE_LOAD_MODE_STRICT:
            raise RuntimeError(
                f"Failed to load runtime rules in strict mode (table_fqn={table_fqn})"
            ) from exc

        # In fallback mode, only table-not-found errors use seed fallback.
        # Table corruption/invalid payload should fail fast to avoid masking issues.
        if not isinstance(exc, (FileNotFoundError, RuleTableAccessError)):
            raise RuntimeError(
                "Failed to load runtime rules from table "
                f"(table_fqn={table_fqn}, mode={normalized_mode})"
            ) from exc

        rules = load_rule_seed(seed_path)
        _validate_runtime_rule_integrity(rules, source=f"seed:{seed_path}")
        LOGGER.warning(
            "rule_source=seed_fallback rule_count=%d table_fqn=%s error=%s",
            len(rules),
            table_fqn,
            exc,
        )
        return rules


def load_default_rule_seed() -> list[RuleDefinition]:
    return load_rule_seed(DEFAULT_RULE_SEED_PATH)
