from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.common.rules import DEFAULT_EFFECTIVE_START, RuleDefinition, select_rule
from src.io.rule_loader import load_rule_seed


def test_rule_from_dict_defaults() -> None:
    payload = {
        "rule_id": "rule_test",
        "domain": "dq",
        "metric": "freshness_sec",
        "threshold": 300,
    }
    rule = RuleDefinition.from_dict(payload)
    assert rule.rule_id == "rule_test"
    assert rule.domain == "dq"
    assert rule.metric == "freshness_sec"
    assert rule.effective_start_ts == DEFAULT_EFFECTIVE_START
    assert rule.is_current is True


def test_rule_from_dict_parses_iso_timestamp() -> None:
    payload = {
        "rule_id": "rule_time",
        "effective_start_ts": "2026-02-04T00:00:00Z",
    }
    rule = RuleDefinition.from_dict(payload)
    assert isinstance(rule.effective_start_ts, datetime)
    assert rule.effective_start_ts.tzinfo is not None


def test_rule_from_dict_requires_rule_id() -> None:
    with pytest.raises(ValueError):
        RuleDefinition.from_dict({})


def test_load_rule_seed() -> None:
    seed_path = Path("mock_data/fixtures/dim_rule_scd2.json")
    rules = load_rule_seed(seed_path)
    rule_ids = {rule.rule_id for rule in rules}
    assert "dq_freshness_default" in rule_ids
    assert "silver_bad_records_default" in rule_ids


def test_rule_from_dict_parses_allowed_values() -> None:
    payload = {
        "rule_id": "rule_allowed",
        "allowed_values": ["A", "B"],
    }
    rule = RuleDefinition.from_dict(payload)
    assert rule.allowed_values == ("A", "B")


def test_select_rule_prefers_current() -> None:
    rule_old = RuleDefinition.from_dict(
        {
            "rule_id": "old",
            "domain": "silver",
            "metric": "bad_records_rate",
            "effective_start_ts": "2026-01-01T00:00:00Z",
            "is_current": False,
        }
    )
    rule_new = RuleDefinition.from_dict(
        {
            "rule_id": "new",
            "domain": "silver",
            "metric": "bad_records_rate",
            "effective_start_ts": "2026-02-01T00:00:00Z",
            "is_current": True,
        }
    )
    selected = select_rule(
        [rule_old, rule_new], domain="silver", metric="bad_records_rate"
    )
    assert selected is rule_new


def test_rule_as_storage_dict_keeps_typed_timestamps() -> None:
    rule = RuleDefinition.from_dict(
        {
            "rule_id": "rule_storage",
            "domain": "dq",
            "metric": "freshness_sec",
            "threshold": 300,
            "severity_map": {"warn": 300, "crit": 600},
            "effective_start_ts": "2026-02-01T00:00:00Z",
            "effective_end_ts": "2026-02-02T00:00:00Z",
            "is_current": False,
        }
    )
    payload = rule.as_storage_dict()
    assert isinstance(payload["effective_start_ts"], datetime)
    assert isinstance(payload["effective_end_ts"], datetime)
    assert payload["effective_start_ts"].tzinfo == timezone.utc
    assert isinstance(payload["threshold"], float)
    assert payload["threshold"] == 300.0
    assert payload["severity_map"] == {"warn": 300.0, "crit": 600.0}
