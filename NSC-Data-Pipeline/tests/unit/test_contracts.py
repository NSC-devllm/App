from __future__ import annotations

import pytest

from src.common.contracts import (
    ContractValidationError,
    get_contract,
    missing_required_columns,
    validate_required_columns,
)


def test_missing_required_columns() -> None:
    contract = get_contract("silver.wallet_snapshot")
    missing = missing_required_columns(["snapshot_ts", "user_id"], contract)
    assert "balance_total" in missing
    assert "snapshot_date_kst" in missing


def test_validate_required_columns_raises() -> None:
    contract = get_contract("silver.ledger_entries")
    with pytest.raises(ContractValidationError):
        validate_required_columns(["tx_id"], contract)


def test_cast_map_contains_types() -> None:
    contract = get_contract("silver.ledger_entries")
    cast_map = contract.cast_map
    assert cast_map["amount_signed"] == "decimal(38,2)"
    assert cast_map["event_time"] == "timestamp"


def test_pipeline_state_contract_exists() -> None:
    contract = get_contract("gold.pipeline_state")
    cast_map = contract.cast_map
    assert cast_map["pipeline_name"] == "string"
    assert cast_map["last_processed_end"] == "timestamp"


def test_silver_bad_records_contract_exists() -> None:
    contract = get_contract("silver.bad_records")
    assert contract.required_columns == {
        "detected_date_kst",
        "source_table",
        "reason",
        "record_json",
        "run_id",
        "detected_at",
    }


def test_dim_rule_scd2_contract_exists() -> None:
    contract = get_contract("gold.dim_rule_scd2")
    cast_map = contract.cast_map
    assert cast_map["rule_id"] == "string"
    assert cast_map["effective_start_ts"] == "timestamp"
    assert cast_map["is_current"] == "boolean"
