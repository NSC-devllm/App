from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.common.contracts import ContractValidationError
from src.io import bronze_io


def _write_jsonl(path: Path, rows: list[dict[str, str]]) -> None:
    payload = "\n".join(json.dumps(row) for row in rows) + "\n"
    path.write_text(payload, encoding="utf-8")


def test_normalize_table_name_accepts_short_name() -> None:
    assert (
        bronze_io.normalize_table_name("user_wallets_raw") == "bronze.user_wallets_raw"
    )


def test_load_bronze_records_reads_jsonl(tmp_path: Path) -> None:
    table_dir = tmp_path / "user_wallets_raw"
    table_dir.mkdir()
    rows = [{"user_id": "user_1", "balance": "10.00", "frozen_amount": "0.00"}]
    _write_jsonl(table_dir / "data.jsonl", rows)

    loaded = bronze_io.load_bronze_records("user_wallets_raw", base_dir=tmp_path)

    assert loaded == rows


def test_enrich_bronze_records_adds_metadata() -> None:
    records = [{"user_id": "user_1", "balance": "10.00", "frozen_amount": "0.00"}]
    ingested_at = datetime(2026, 2, 1, tzinfo=timezone.utc)

    enriched = bronze_io.enrich_bronze_records(
        records,
        ingested_at=ingested_at,
        source_extracted_at=ingested_at,
        batch_id="batch-1",
        source_system="mock",
    )

    assert enriched[0]["ingested_at"] == ingested_at
    assert enriched[0]["source_extracted_at"] == ingested_at
    assert enriched[0]["batch_id"] == "batch-1"
    assert enriched[0]["source_system"] == "mock"
    assert "ingested_at" not in records[0]


def test_prepare_bronze_records_validates_required_columns(
    tmp_path: Path,
) -> None:
    table_dir = tmp_path / "user_wallets_raw"
    table_dir.mkdir()
    rows = [{"user_id": "user_1", "frozen_amount": "0.00"}]
    _write_jsonl(table_dir / "data.jsonl", rows)

    with pytest.raises(ContractValidationError):
        bronze_io.prepare_bronze_records(
            "user_wallets_raw", base_dir=tmp_path, batch_id="batch"
        )


def test_default_batch_id_prefix() -> None:
    batch_id = bronze_io.default_batch_id("mock")
    assert batch_id.startswith("mock_")


def test_prepare_bronze_records_default_base_dir() -> None:
    records = bronze_io.prepare_bronze_records(
        "user_wallets_raw",
        ingested_at="2026-02-01T00:00:00Z",
        source_extracted_at="2026-02-01T00:00:00Z",
        batch_id="batch-local",
        source_system="mock",
    )
    assert records
    assert records[0]["batch_id"] == "batch-local"
