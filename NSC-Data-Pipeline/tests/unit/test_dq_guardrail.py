from __future__ import annotations

from datetime import datetime, timezone

from conftest import make_rule

from src.transforms.dq_guardrail import (
    SEVERITY_CRITICAL,
    SEVERITY_WARN,
    TAG_CONTRACT,
    TAG_DUP,
    TAG_EVENT_DROP,
    TAG_SOURCE_STALE,
    DQTableConfig,
    build_dq_status,
)


def test_build_dq_status_completeness_critical() -> None:
    rules = [
        make_rule(
            {
                "rule_id": "dq_completeness_zero_windows_v1",
                "domain": "dq",
                "metric": "completeness_zero_windows",
                "severity_map": {"warn": 1, "crit": 3},
            }
        )
    ]
    config = DQTableConfig(
        source_table="bronze.transaction_ledger_raw",
        window_start_ts=datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc),
        window_end_ts=datetime(2026, 2, 1, 0, 10, tzinfo=timezone.utc),
        dup_key_fields=("tx_id",),
        dup_rule_metric="dup_rate_tx_id",
    )
    output = build_dq_status(
        [],
        config=config,
        run_id="run-1",
        rules=rules,
        previous_zero_windows=2,
        now_ts=datetime(2026, 2, 1, 0, 10, tzinfo=timezone.utc),
    )
    assert output.zero_window_count == 3
    assert output.dq_status["dq_tag"] == TAG_EVENT_DROP
    assert output.dq_status["severity"] == SEVERITY_CRITICAL
    assert len(output.exceptions) == 1


def test_build_dq_status_duplicate_and_contract() -> None:
    rules = [
        make_rule(
            {
                "rule_id": "dq_dup_txid_default",
                "domain": "dq",
                "metric": "dup_rate_tx_id",
                "severity_map": {"warn": 0.001, "crit": 0.01},
            }
        ),
        make_rule(
            {
                "rule_id": "dq_contract_bad_records_default",
                "domain": "dq",
                "metric": "contract_bad_records_rate",
                "severity_map": {"warn": 0.001, "crit": 0.01},
            }
        ),
        make_rule(
            {
                "rule_id": "silver_entry_type_allowed_v1",
                "domain": "silver",
                "metric": "entry_type_allowed",
                "allowed_values": ["PAYMENT"],
            }
        ),
    ]
    records = [
        {
            "tx_id": "tx-1",
            "wallet_id": "user-1",
            "type": "PAYMENT",
            "amount": "10.00",
            "ingested_at": "2026-02-01T00:00:00Z",
        },
        {
            "tx_id": "tx-1",
            "wallet_id": "user-1",
            "type": "PAYMENT",
            "amount": "10.00",
            "ingested_at": "2026-02-01T00:00:00Z",
        },
        {
            "tx_id": "tx-2",
            "wallet_id": "user-2",
            "type": "PAYMENT",
            "amount": None,
            "ingested_at": "2026-02-01T00:00:00Z",
        },
    ]
    config = DQTableConfig(
        source_table="bronze.transaction_ledger_raw",
        window_start_ts=datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc),
        window_end_ts=datetime(2026, 2, 1, 0, 10, tzinfo=timezone.utc),
        dup_key_fields=("tx_id",),
        dup_rule_metric="dup_rate_tx_id",
    )
    output = build_dq_status(
        records,
        config=config,
        run_id="run-1",
        rules=rules,
        now_ts=datetime(2026, 2, 1, 0, 10, tzinfo=timezone.utc),
    )
    exception_tags = {exception["exception_type"] for exception in output.exceptions}
    assert TAG_DUP in exception_tags
    assert TAG_CONTRACT in exception_tags
    assert output.dq_status["severity"] == SEVERITY_CRITICAL


def test_build_dq_status_freshness_warn() -> None:
    rules = [
        make_rule(
            {
                "rule_id": "dq_freshness_default",
                "domain": "dq",
                "metric": "freshness_sec",
                "severity_map": {"warn": 300, "crit": 600},
            }
        )
    ]
    records = [
        {
            "tx_id": "tx-1",
            "wallet_id": "user-1",
            "type": "PAYMENT",
            "amount": "10.00",
            "ingested_at": "2026-02-01T00:05:00Z",
        }
    ]
    config = DQTableConfig(
        source_table="bronze.transaction_ledger_raw",
        window_start_ts=datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc),
        window_end_ts=datetime(2026, 2, 1, 0, 10, tzinfo=timezone.utc),
        dup_key_fields=("tx_id",),
        dup_rule_metric="dup_rate_tx_id",
    )
    output = build_dq_status(
        records,
        config=config,
        run_id="run-1",
        rules=rules,
        now_ts=datetime(2026, 2, 1, 0, 10, tzinfo=timezone.utc),
    )
    assert output.dq_status["dq_tag"] == TAG_SOURCE_STALE
    assert output.dq_status["severity"] == SEVERITY_WARN
    assert len(output.exceptions) == 1
