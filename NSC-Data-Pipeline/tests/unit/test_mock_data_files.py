from __future__ import annotations

import json
from pathlib import Path

from src.common.contracts import get_contract

SCENARIO_TABLE_TO_BRONZE = {
    "user_wallets": "bronze.user_wallets_raw",
    "transaction_ledger": "bronze.transaction_ledger_raw",
    "payment_orders": "bronze.payment_orders_raw",
    "orders": "bronze.orders_raw",
    "order_items": "bronze.order_items_raw",
    "products": "bronze.products_raw",
}


def _read_jsonl(path: Path) -> list[dict]:
    rows: list[dict] = []
    if not path.exists():
        raise AssertionError(f"Missing jsonl file: {path}")
    for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            raise AssertionError(f"Invalid JSON in {path}:{line_no}: {exc}") from exc
        if not isinstance(payload, dict):
            raise AssertionError(f"JSONL payload must be an object: {path}:{line_no}")
        rows.append(payload)
    return rows


def _assert_records_match_contract(
    records: list[dict],
    *,
    contract_table: str,
    source_path: Path,
) -> None:
    contract = get_contract(contract_table)
    allowed_keys = set(contract.column_names)
    required_keys = set(contract.required_columns)

    for idx, record in enumerate(records):
        unknown = set(record.keys()) - allowed_keys
        if unknown:
            unknown_str = ", ".join(sorted(unknown))
            raise AssertionError(
                f"Unknown columns in {source_path} (record {idx}): {unknown_str}"
            )

        missing = required_keys - set(record.keys())
        if missing:
            missing_str = ", ".join(sorted(missing))
            raise AssertionError(
                f"Missing required columns in {source_path} (record {idx}): {missing_str}"
            )


def test_mock_data_scenarios_are_well_formed_and_in_sync_with_bronze() -> None:
    scenarios_root = Path("mock_data/scenarios")
    bronze_root = Path("mock_data/bronze")

    scenario_dirs = sorted(
        path
        for path in scenarios_root.iterdir()
        if path.is_dir() and not path.name.startswith(".")
    )
    assert scenario_dirs, "No mock_data scenarios found under mock_data/scenarios/"

    for scenario_dir in scenario_dirs:
        scenario = scenario_dir.name
        for scenario_table, bronze_table in SCENARIO_TABLE_TO_BRONZE.items():
            scenario_path = scenario_dir / f"{scenario_table}.jsonl"
            scenario_rows = _read_jsonl(scenario_path)
            _assert_records_match_contract(
                scenario_rows,
                contract_table=bronze_table,
                source_path=scenario_path,
            )

            bronze_path = (
                bronze_root
                / bronze_table.split(".", maxsplit=1)[1]
                / scenario
                / "data.jsonl"
            )
            bronze_rows = _read_jsonl(bronze_path)
            assert scenario_rows == bronze_rows, (
                f"Scenario file and bronze file diverged: {scenario_path} vs {bronze_path}"
            )


def test_mock_data_default_bronze_files_match_contracts() -> None:
    bronze_root = Path("mock_data/bronze")
    for bronze_table in SCENARIO_TABLE_TO_BRONZE.values():
        table_dir = bronze_root / bronze_table.split(".", maxsplit=1)[1]
        data_path = table_dir / "data.jsonl"
        rows = _read_jsonl(data_path)
        _assert_records_match_contract(
            rows, contract_table=bronze_table, source_path=data_path
        )
