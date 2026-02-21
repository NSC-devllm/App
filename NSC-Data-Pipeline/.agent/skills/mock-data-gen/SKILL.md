---
name: mock-data-gen
description: Generate deterministic pytest fixtures and mock_data scenario files from data contracts. Use whenever tests need mock data or fixtures, even without explicit schema mentions (e.g., "테스트 데이터 만들어줘", "mock 데이터 필요해", "테스트하려는데 데이터가 없어").
---

# Mock Data Generator

## Overview
Generate deterministic pytest fixtures and scenario mock_data files from the data contract. Support single-table fixtures and multi-table scenarios.

## Workflow
1. Determine whether the request is a table or scenario.
2. Run `python /home/salee/.codex/skills/mock-data-gen/scripts/mock_data_gen.py` with `--table` or `--scenario`.
3. Use stdout for quick copy or pass `--fixtures-out` to write a file.
4. For scenarios, use outputs under `mock_data/` for local tests or uploads.

## Commands
```bash
python /home/salee/.codex/skills/mock-data-gen/scripts/mock_data_gen.py --table user_wallets
python /home/salee/.codex/skills/mock-data-gen/scripts/mock_data_gen.py --table transaction_ledger --fixtures-out
python /home/salee/.codex/skills/mock-data-gen/scripts/mock_data_gen.py --scenario one_day_normal --mock-data-root mock_data
```

## Outputs
- Fixtures default to stdout, or `tests/unit/fixtures/mock_data_fixtures.py` when `--fixtures-out` is passed without a value.
- Scenario files are written to `mock_data/scenarios/<scenario>/<table>.jsonl`.
- Bronze convenience files are written to `mock_data/bronze/<table>_raw/<scenario>/data.jsonl`.
- Rule seed file at `mock_data/fixtures/dim_rule_scd2.json`.

## Notes
- Use deterministic values only. No randomness.
- Keep edits minimal and aligned to `.specs/data_contract.md`.
- If schema parsing fails, check table headings and schema tables in the contract.
