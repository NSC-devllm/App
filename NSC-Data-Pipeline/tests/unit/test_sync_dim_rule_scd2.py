from __future__ import annotations

from types import SimpleNamespace

import pytest

from scripts.sync_dim_rule_scd2 import _resolve_table_fqn, _validate_rules


def _rule(
    *,
    rule_id: str,
    domain: str,
    metric: str,
    is_current: bool,
):
    return SimpleNamespace(
        rule_id=rule_id,
        domain=domain,
        metric=metric,
        is_current=is_current,
    )


def test_resolve_table_fqn_supports_short_and_fully_qualified_names() -> None:
    assert (
        _resolve_table_fqn(catalog="hive_metastore", table_name="gold.dim_rule_scd2")
        == "hive_metastore.gold.dim_rule_scd2"
    )
    assert (
        _resolve_table_fqn(
            catalog="hive_metastore",
            table_name="dev_catalog.gold.dim_rule_scd2",
        )
        == "dev_catalog.gold.dim_rule_scd2"
    )
    assert (
        _resolve_table_fqn(catalog="hive_metastore", table_name="dim_rule_scd2")
        == "hive_metastore.gold.dim_rule_scd2"
    )


def test_validate_rules_rejects_duplicate_rule_ids() -> None:
    rules = [
        _rule(
            rule_id="dup_rule",
            domain="dq",
            metric="freshness_sec",
            is_current=True,
        ),
        _rule(
            rule_id="dup_rule",
            domain="ledger",
            metric="drift_abs",
            is_current=True,
        ),
    ]

    with pytest.raises(ValueError, match="Duplicate rule_id detected"):
        _validate_rules(rules)


def test_validate_rules_rejects_multiple_current_rules_for_same_metric() -> None:
    rules = [
        _rule(
            rule_id="dq_freshness_v1",
            domain="dq",
            metric="freshness_sec",
            is_current=True,
        ),
        _rule(
            rule_id="dq_freshness_v2",
            domain="dq",
            metric="freshness_sec",
            is_current=True,
        ),
    ]

    with pytest.raises(ValueError, match="Multiple is_current=true rules"):
        _validate_rules(rules)


def test_validate_rules_accepts_single_current_rule_per_domain_metric() -> None:
    rules = [
        _rule(
            rule_id="dq_freshness_v1",
            domain="dq",
            metric="freshness_sec",
            is_current=False,
        ),
        _rule(
            rule_id="dq_freshness_v2",
            domain="dq",
            metric="freshness_sec",
            is_current=True,
        ),
        _rule(
            rule_id="ledger_drift_v1",
            domain="ledger",
            metric="drift_abs",
            is_current=True,
        ),
    ]

    _validate_rules(rules)
