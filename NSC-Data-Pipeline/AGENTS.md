Data-pipeline LLM coding agent instructions
============================================

This file contains instructions for LLM coding agents working with the
Controls-first Ledger Pipelines codebase.


Project overview
----------------

Controls-first Ledger Pipelines on Azure Databricks for data quality and
reconciliation, with anonymized analytics outputs.

Pipelines:
- A (Guardrail): freshness/completeness/duplicates, 10-min micro-batch
- Silver (Materialization): Bronze -> Silver contract validation + quarantine
- B (Ledger/Admin): daily recon (`delta = net_flow`) + supply checks
- C (Analytics): anonymized payment marts

Key principles:
- Pipeline A provides DQ guardrails and gating context (stale/drop suppression policy).
- Pipeline B/C depend on `pipeline_silver` readiness and fail closed when upstream is not ready.
Current phase: workflow hardening + mock-data-backed development and E2E validation
(`gold.fact_market_price` remains planned).


Development environment
-----------------------

Local IDE + remote Databricks execution (Option B), Python (PySpark),
pytest locally, Databricks Dev E2E, Delta Lake on Azure.


Repository structure
--------------------

```
├── src/
│   ├── common/             # Shared config/params/rules/time utilities
│   ├── transforms/         # Pure transform logic (DB-independent)
│   ├── io/                 # IO layer (readers/writers)
│   └── jobs/               # Databricks job definitions
├── tests/
│   ├── unit/               # Local pytest
│   ├── integration/        # Local PySpark integration tests
│   └── e2e/                # Databricks E2E smoke skeleton tests
├── configs/                # Environment configs (dev/prod)
├── scripts/                # Databricks entrypoints and operational scripts
├── mock_data/              # Mock datasets
└── .specs/                 # Specifications
```


Core domain concepts
--------------------

Data layers: Bronze (raw), Silver (contract + validation), Gold (stable).
Schemas, mappings, and table definitions live in `.specs/data_contract.md`.


Code patterns and principles
----------------------------

- Transform/IO/Jobs separation (pure transforms; IO handles Delta/DB; jobs wire both)
- Idempotency required: MERGE keys, overwrite partitions; Silver/Gold partition by `date_kst`
- Contract validation + quarantine (`silver.bad_records`), fail-fast on bad rate
- Runtime rule SSOT is `gold.dim_rule_scd2` (no hardcoded thresholds), outputs include `rule_id`
- A/B/Silver rule loading uses `rule_load_mode` (`strict|fallback`) and `sync_dim_rule_scd2`
- Run tracking: `run_id` propagated, update `gold.pipeline_state`
- Config precedence: `CLI args > env vars > configs/{env}.yaml > configs/common.yaml`
- Env override pattern: `PIPELINE_CFG__<NESTED__KEY>` (unknown key fail-fast)


Common job parameters
---------------------

All A/Silver/B/C pipelines accept these standard parameters:

 -  `run_mode`: `incremental` | `backfill`
 -  `start_ts`, `end_ts`: Timestamp window (UTC)
 -  `date_kst_start`, `date_kst_end`: Day-level backfill range
 -  `run_id`: Unique execution ID (recorded in all outputs)

Pipeline-specific parameters:

 -  A/B/Silver: `rule_load_mode`, `rule_table`, `rule_seed_path`
 -  C: `secret_scope`, `secret_key` (optional `salt`)

Default window interpretation:

 -  A: empty window params -> auto incremental window (`start_ts=last_processed_end` or `now-10m`, `end_ts=now`)
 -  B/C/Silver: empty window params -> one-day backfill for yesterday (KST)


Pipelines overview
------------------

Pipeline A: Guardrail DQ → `silver.dq_status` + exceptions  
Pipeline Silver: Materialization/quarantine → `silver.wallet_snapshot`, `silver.ledger_entries`, `silver.order_events`, `silver.order_items`, `silver.products`, `silver.bad_records`  
Pipeline B: Recon/supply/ops/admin checks → `gold.recon_daily_snapshot_flow`, `gold.ledger_supply_balance_daily`, `gold.ops_payment_failure_daily`, `gold.ops_payment_refund_daily`, `gold.ops_ledger_pairing_quality_daily`, `gold.admin_tx_search`, `gold.exception_ledger`  
Pipeline C: Anonymized analytics → `gold.fact_payment_anonymized`  
Planned: `gold.fact_market_price` (FR-ANA-02)

Operational workflow inventory (Databricks jobs) includes:
- Scheduled: `pipeline_a_guardrail`, `pipeline_silver_materialization`, `pipeline_b_controls`, `pipeline_c_analytics`, `bad_records_retention_cleanup`
- On-demand support: `bootstrap_catalog`, `sync_dim_rule_scd2`
- E2E: `e2e_setup_mock_data`, `e2e_cleanup_mock_data`, `e2e_full_pipeline`

Key tables and decision defaults live in `.specs/data_contract.md` and
`.specs/project_specs.md`.


Testing strategy
----------------

**Test levels:**
 -  **Unit**: `tests/unit/` (pytest + mock DataFrames)
 -  **Integration**: `tests/integration/` (pytest + local PySpark)
 -  **E2E skeleton**: `tests/e2e/` (Databricks-trigger smoke skeleton)
 -  **Databricks Dev E2E (L3)**: workflow-level Delta/UC/idempotency checks

**Test case types:**
 -  **Happy path**: Normal input → expected output
 -  **Edge case**: Boundary values, empty data
 -  **Error case**: Bad records, schema mismatch → quarantine
 -  **Idempotency**: Re-run → same result

**Quality gates:**
 -  CI unit gate: `tests/unit/` (pass/fail, no coverage threshold)
 -  CI L2 combined coverage gate: `tests/unit/ tests/integration/` with `--cov-fail-under=80`
 -  Merge gate: Databricks Dev E2E (L3) idempotency checks pass

See `.specs/project_specs.md` and `.github/workflows/ci.yml` for current test and CI details.


Verification policy
-------------------

### Verification contract

 -  Verification is mandatory. Do not claim completion without running an
    appropriate verification level.
 -  Local pytest-based verification (L1/L2) must always run via the project
    virtual environment interpreter (`.venv/bin/python -m pytest ...`).
 -  Store evidence under `.agents/logs/verification/`.
 -  Do not encode verification level in commit messages.

### Verification ladder

| Level | When | Duration | Command | Pass Criteria |
|-------|------|----------|---------|---------------|
| L0 | Per edit | < 30s | `python -m py_compile` | No syntax errors |
| L1 | Pre-commit | < 2min | `.venv/bin/python -m pytest tests/unit/ -x` | All unit tests pass |
| L2 | Pre-PR | < 10min | `.venv/bin/python -m pytest tests/unit/ tests/integration/ --cov=src --cov-fail-under=80` | 80%+ coverage |
| L3 | Pre-merge | < 30min | Databricks Dev E2E | Idempotency verified |

### L3 execution standard

 -  Poll Databricks run status every 20 seconds.
 -  Use a 10-minute timeout for L3 verification runs.

### Escalation triggers

 -  If in doubt, move up one level.
 -  `src/transforms/` changes: Minimum L1
 -  `src/io/`, `src/jobs/` changes: Minimum L2
 -  Reconciliation logic, rule table changes: L3 required
 -  `data_contract.md` changes: L3 + manual review

### Exceptions

 -  Documentation-only changes: May skip L0-L2
 -  Emergency hotfix: May skip L1, L2 with post-hoc L3 within 24h

### Toolbox

 -  L0: `python -m py_compile ${FILE}`
 -  L1: `.venv/bin/python -m pytest tests/unit/ -v -x`
 -  L2: `.venv/bin/python -m pytest tests/unit/ tests/integration/ -v --cov=src --cov-fail-under=80`
 -  L3 (primary): `databricks bundle run e2e_full_pipeline -t dev --params run_mode=backfill,date_kst_start=${DATE},date_kst_end=${DATE},run_id=${RUN_ID}`
 -  L3 (optional): `databricks jobs run-now --job-id ${JOB_ID}`


Security
--------

 -  Secrets via Key Vault / Databricks Secret Scope
 -  All executions tracked via `run_id`
 -  Never commit credentials


Retry and alerting
------------------

 -  **Retry**: Max 2 retries, 5-min interval (transient errors only)
 -  **Monitoring baseline**: Azure Monitoring v1 (execution-signal based)
 -  **Core execution alerts**: Job failure, recent success delay, retry exhausted, cluster start/timeout failure
 -  **Current out-of-scope alerts**: table-driven alerts from `silver.dq_status`, `gold.exception_ledger`, `gold.pipeline_state`
 -  **Alert suppression policy**: Pipeline A stale/drop signals are applied as centralized alert suppression rules


Documentation lookup
--------------------

When looking up official documentation for external libraries or frameworks, always use the Context7 MCP server.


Key references
--------------

**For requirements, thresholds, or design decisions, consult `.specs/` first (SSOT).**

| Document | Content |
|----------|---------|
| `.specs/project_specs.md` | Full development plan, Decision Lock, thresholds |
| `.specs/data_contract.md` | Data contracts, schemas, mapping rules |
| `.specs/decision_open_items.md` | Open decisions and implementation assumptions |
| `.specs/ops/operations_runbook.md` | Operational baseline, recovery playbooks, rerun/idempotency guidance |
| `.specs/ops/azure_monitoring_integration_plan.md` | Monitoring scope, alert policy, and dashboard baseline |

Decision hygiene
----------------

- If any part of the implementation is unclear or handled as an assumption,
  add it to `.specs/decision_open_items.md` immediately and update its status
  once a decision is made.
