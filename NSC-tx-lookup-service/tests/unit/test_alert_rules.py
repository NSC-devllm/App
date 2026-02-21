"""Validate alert_rules.yml metric names match actual OTel instruments in code."""

from __future__ import annotations

from pathlib import Path

import yaml

ALERT_RULES_PATH = Path(__file__).resolve().parents[2] / "docker" / "observability" / "alert_rules.yml"

# Known metric names from src/common/metrics.py and src/consumer/metrics.py.
# Keep in sync when adding new instruments.
KNOWN_METRICS = {
    # src/common/metrics.py
    "api_request_latency_seconds",
    "api_requests_total",
    "api_requests_inflight",
    "db_query_latency_seconds",
    "db_queries_total",
    "db_pool_size",
    "db_pool_checked_out",
    "db_pool_overflow",
    "db_pool_checked_in",
    "db_pool_checkout_latency_seconds",
    "db_replication_lag_seconds",
    # src/consumer/metrics.py
    "consumer_messages_total",
    "consumer_event_lag_seconds",
    "consumer_dlq_total",
    "consumer_version_missing_total",
    "pairing_total",
    "pairing_incomplete_total",
    "pairing_incomplete_age_seconds",
    "consumer_kafka_lag",
    "consumer_freshness_seconds",
    "pairing_skipped_non_payment_order_total",
}

REQUIRED_FIELDS = {"name", "metric", "condition", "window", "severity"}


def _load_rules() -> list[dict]:
    with open(ALERT_RULES_PATH) as f:
        data = yaml.safe_load(f)
    return data["rules"]


def test_alert_rules_file_exists() -> None:
    assert ALERT_RULES_PATH.exists(), f"Missing {ALERT_RULES_PATH}"


def test_alert_rules_metrics_match_code() -> None:
    rules = _load_rules()
    for rule in rules:
        metric = rule["metric"]
        assert metric in KNOWN_METRICS, (
            f"Rule '{rule['name']}' references unknown metric '{metric}'. "
            f"Update KNOWN_METRICS or fix the rule."
        )


def test_alert_rules_have_required_fields() -> None:
    rules = _load_rules()
    for rule in rules:
        missing = REQUIRED_FIELDS - set(rule.keys())
        assert not missing, (
            f"Rule '{rule.get('name', '?')}' missing fields: {missing}"
        )


def test_alert_rules_severity_values() -> None:
    valid = {"warning", "critical"}
    rules = _load_rules()
    for rule in rules:
        assert rule["severity"] in valid, (
            f"Rule '{rule['name']}' has invalid severity '{rule['severity']}'"
        )


def test_alert_rules_unique_names() -> None:
    rules = _load_rules()
    names = [r["name"] for r in rules]
    assert len(names) == len(set(names)), f"Duplicate rule names: {names}"


def test_data_freshness_is_critical() -> None:
    """DEC-202: DataFreshnessHigh must be critical severity."""
    rules = _load_rules()
    freshness_rules = [r for r in rules if r["name"] == "DataFreshnessHigh"]
    assert len(freshness_rules) == 1
    assert freshness_rules[0]["severity"] == "critical"


def test_db_replication_lag_rule_is_critical() -> None:
    """DEC-217: DbReplicationLagHigh must be critical severity."""
    rules = _load_rules()
    lag_rules = [r for r in rules if r["name"] == "DbReplicationLagHigh"]
    assert len(lag_rules) == 1
    assert lag_rules[0]["severity"] == "critical"
