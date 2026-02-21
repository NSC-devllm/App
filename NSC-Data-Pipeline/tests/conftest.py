from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.common.rules import RuleDefinition  # noqa: E402
from src.io.rule_loader import load_default_rule_seed  # noqa: E402

# ---------------------------------------------------------------------------
# Shared test helpers
# ---------------------------------------------------------------------------


def merge_rows(
    rows: list[dict], new_rows: list[dict], keys: tuple[str, ...]
) -> list[dict]:
    """Simulate Delta MERGE upsert: deduplicate by composite key, last-write-wins."""
    index = {tuple(row[key] for key in keys): row for row in rows}
    for row in new_rows:
        index[tuple(row[key] for key in keys)] = row
    return list(index.values())


def normalize_run_id(rows: list[dict], run_id: str = "run") -> list[dict]:
    """Replace run_id in every row for comparison ignoring run identity."""
    normalized = []
    for row in rows:
        payload = dict(row)
        payload["run_id"] = run_id
        normalized.append(payload)
    return normalized


def sorted_rows(rows: list[dict], keys: tuple[str, ...]) -> list[dict]:
    """Deterministic sort by composite key for assertion comparison."""
    return sorted(rows, key=lambda row: tuple(row[key] for key in keys))


def make_rule(payload: dict) -> RuleDefinition:
    """Build a RuleDefinition from a dict."""
    return RuleDefinition.from_dict(payload)


def make_ledger_rule(metric: str) -> RuleDefinition:
    """Build a strict ledger rule (threshold=0, crit=0)."""
    return RuleDefinition.from_dict(
        {
            "rule_id": f"rule_{metric}",
            "domain": "ledger",
            "metric": metric,
            "threshold": 0,
            "severity_map": {"crit": 0},
        }
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def default_rules() -> list[RuleDefinition]:
    """Load dim_rule_scd2.json once per test function."""
    return load_default_rule_seed()
