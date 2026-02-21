from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from src.common.rules import RuleDefinition
from src.io.bronze_io import prepare_bronze_records
from src.transforms import silver_controls


def test_derive_amount_signed_mapping() -> None:
    amount = Decimal("10.00")
    assert silver_controls.derive_amount_signed("PAYMENT", amount) == Decimal("-10.00")
    assert silver_controls.derive_amount_signed("RECEIVE", amount) == Decimal("10.00")
    assert silver_controls.derive_amount_signed("HOLD", amount) == Decimal("0")
    assert silver_controls.derive_amount_signed("UNKNOWN", amount) is None


def test_transform_wallet_snapshot_records() -> None:
    records = [
        {
            "user_id": "user_1",
            "balance": "100.00",
            "frozen_amount": "25.00",
            "source_extracted_at": "2026-02-01T00:00:00Z",
            "ingested_at": "2026-02-01T00:10:00Z",
            "updated_at": "2026-02-01T00:00:00Z",
        }
    ]
    result = silver_controls.transform_wallet_snapshot_records(records, run_id="run-1")
    assert len(result.records) == 1
    assert len(result.bad_records) == 0
    snapshot = result.records[0]
    assert snapshot["balance_total"] == Decimal("125.00")
    assert snapshot["snapshot_ts"].tzinfo is not None


def test_transform_wallet_snapshot_records_invalid_balance() -> None:
    records = [
        {
            "user_id": "user_1",
            "frozen_amount": "25.00",
            "ingested_at": "2026-02-01T00:10:00Z",
        }
    ]
    result = silver_controls.transform_wallet_snapshot_records(records, run_id="run-1")
    assert len(result.records) == 0
    assert len(result.bad_records) == 1
    bad_record = result.bad_records[0]
    assert "detected_date_kst" in bad_record
    assert isinstance(bad_record["detected_date_kst"], date)
    assert bad_record["detected_at"].tzinfo is not None


def test_transform_ledger_entries_records_invalid_entry_type() -> None:
    records = [
        {
            "tx_id": "tx-1",
            "wallet_id": "user_1",
            "type": "UNKNOWN",
            "amount": "10.00",
            "created_at": "2026-02-01T00:00:00Z",
        }
    ]
    result = silver_controls.transform_ledger_entries_records(
        records,
        run_id="run-1",
        allowed_entry_types=set(silver_controls.ALLOWED_ENTRY_TYPES),
    )
    assert len(result.records) == 0
    assert len(result.bad_records) == 1


def test_transform_ledger_entries_records_with_status_lookup() -> None:
    records = [
        {
            "tx_id": "tx-1",
            "wallet_id": "user_1",
            "type": "PAYMENT",
            "amount": "10.00",
            "related_id": "po_1",
            "created_at": "2026-02-01T00:00:00Z",
        }
    ]
    status_lookup = {"po_1": "PAID"}
    result = silver_controls.transform_ledger_entries_records(
        records,
        run_id="run-1",
        allowed_entry_types=set(silver_controls.ALLOWED_ENTRY_TYPES),
        allowed_statuses={"PAID"},
        status_lookup=status_lookup,
    )
    assert len(result.records) == 1
    assert result.records[0]["status"] == "PAID"


def test_enforce_bad_records_rate_raises() -> None:
    rule = RuleDefinition.from_dict(
        {
            "rule_id": "silver_bad_records_default",
            "domain": "silver",
            "metric": "bad_records_rate",
            "threshold": 0.1,
        }
    )
    with pytest.raises(RuntimeError):
        silver_controls.enforce_bad_records_rate(valid_count=4, bad_count=1, rule=rule)


def test_transform_wallet_snapshot_with_rules_sets_rule_id() -> None:
    records = [
        {
            "user_id": "user_1",
            "balance": "100.00",
            "frozen_amount": "25.00",
            "source_extracted_at": "2026-02-01T00:00:00Z",
        }
    ]
    rules = [
        RuleDefinition.from_dict(
            {
                "rule_id": "silver_bad_records_default",
                "domain": "silver",
                "metric": "bad_records_rate",
                "threshold": 0.5,
            }
        )
    ]
    result, bad_rate = silver_controls.transform_wallet_snapshot_with_rules(
        records, run_id="run-1", rules=rules
    )
    assert bad_rate == 0.0
    assert result.records[0]["rule_id"] == "silver_bad_records_default"


def test_transform_ledger_entries_with_rules_applies_allowed_values() -> None:
    records = [
        {
            "tx_id": "tx-1",
            "wallet_id": "user_1",
            "type": "PAYMENT",
            "amount": "10.00",
            "related_id": "po_1",
            "created_at": "2026-02-01T00:00:00Z",
        }
    ]
    rules = [
        RuleDefinition.from_dict(
            {
                "rule_id": "silver_entry_type_allowed_v1",
                "domain": "silver",
                "metric": "entry_type_allowed",
                "allowed_values": ["PAYMENT"],
            }
        ),
        RuleDefinition.from_dict(
            {
                "rule_id": "silver_payment_status_allowed_v1",
                "domain": "silver",
                "metric": "payment_status_allowed",
                "allowed_values": ["PAID"],
            }
        ),
        RuleDefinition.from_dict(
            {
                "rule_id": "silver_bad_records_default",
                "domain": "silver",
                "metric": "bad_records_rate",
                "threshold": 0.5,
            }
        ),
    ]
    result, bad_rate = silver_controls.transform_ledger_entries_with_rules(
        records,
        run_id="run-1",
        rules=rules,
        status_lookup={"po_1": "PAID"},
    )
    assert bad_rate == 0.0
    assert len(result.records) == 1


def test_wallet_snapshot_sample_from_mock_data() -> None:
    records = prepare_bronze_records(
        "user_wallets_raw",
        ingested_at="2026-02-01T00:10:00Z",
        source_extracted_at="2026-02-01T00:00:00Z",
    )
    result = silver_controls.transform_wallet_snapshot_records(records, run_id="run-1")
    assert len(result.bad_records) == 0
    assert len(result.records) == len(records)

    sample = next(record for record in result.records if record["user_id"] == "user_1")
    assert sample["balance_total"] == Decimal("1000.00")
    assert sample["snapshot_date_kst"] == date(2026, 2, 1)
    assert sample["snapshot_ts"] == datetime(2026, 2, 1, tzinfo=timezone.utc)


def test_ledger_entries_amount_signed_sample_from_mock_data() -> None:
    records = prepare_bronze_records(
        "transaction_ledger_raw",
        ingested_at="2026-02-01T00:10:00Z",
    )
    result = silver_controls.transform_ledger_entries_records(
        records,
        run_id="run-1",
        allowed_entry_types=set(silver_controls.ALLOWED_ENTRY_TYPES),
    )
    assert len(result.bad_records) == 0
    assert len(result.records) == len(records)

    payment = next(record for record in result.records if record["tx_id"] == "tx_pay_1")
    receive = next(
        record for record in result.records if record["tx_id"] == "tx_recv_1"
    )
    assert payment["amount_signed"] == Decimal("-100.00")
    assert receive["amount_signed"] == Decimal("100.00")
    assert payment["event_date_kst"] == date(2026, 2, 1)
