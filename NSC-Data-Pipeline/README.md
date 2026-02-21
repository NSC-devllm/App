# Controls-first Ledger Pipelines

원장 통제(Controls)와 익명화 분석(Analytics)을 위한 Azure Databricks 파이프라인입니다.

Last updated: 2026-02-12

## 프로젝트 개요 (Current)

Current:
- Pipeline A/Silver/B/C Databricks Workflow 운영
- `gold.recon_daily_snapshot_flow`, `gold.ledger_supply_balance_daily`, `gold.fact_payment_anonymized` 포함 핵심 Gold 산출물 운영
- `gold.dim_rule_scd2` 기반 룰 로딩(A/B/Silver), `pipeline_silver` readiness 기반 B/C fail-closed 운영
- `silver.bad_records` 영속화 + 월 1회 정리(`bad_records_retention_cleanup`)

## 파이프라인 구성

| 파이프라인 | 역할 | 운영 기준 |
|---|---|---|
| A (Guardrail) | Bronze 입력 품질 감시(`silver.dq_status`, 예외 기록) | 10분 주기 |
| Silver (Materialization) | Bronze -> Silver 계약 검증/표준화 + quarantine | 일배치 |
| B (Ledger Controls) | 일일 대사/공급 정합성/운영 지표 산출 | Silver 준비 상태 충족 시 실행 |
| C (Analytics) | 익명화 결제 팩트 산출 | Silver 준비 상태 충족 시 실행 |

의존성:
- `A -> Silver -> (B, C)`
- B/C는 `pipeline_silver.last_processed_end` 미충족 시 fail-closed로 즉시 실패합니다.

## 아키텍처

```text
Source DB
  -> Bronze (원본 보존)
  -> A: Guardrail DQ (품질 상태/예외)
  -> Silver (계약 검증 + 표준화 + bad_records quarantine)
  -> B: Ledger/Admin Controls (Recon/Supply/Ops/Admin)
  -> C: Analytics (Anonymized Facts)
```

## 주요 산출물

Silver:
- `silver.wallet_snapshot`
- `silver.ledger_entries`
- `silver.order_events`
- `silver.order_items`
- `silver.products`
- `silver.bad_records`
- `silver.dq_status`

Gold:
- `gold.recon_daily_snapshot_flow`
- `gold.ledger_supply_balance_daily`
- `gold.ops_payment_failure_daily`
- `gold.ops_payment_refund_daily`
- `gold.ops_ledger_pairing_quality_daily`
- `gold.admin_tx_search`
- `gold.fact_payment_anonymized`
- `gold.exception_ledger`
- `gold.pipeline_state`
- `gold.dim_rule_scd2`

## 실행 인터페이스

공통 파라미터(A/Silver/B/C):
- `run_mode`: `incremental` | `backfill`
- `start_ts`, `end_ts`: UTC window
- `date_kst_start`, `date_kst_end`: KST day window
- `run_id`

파이프라인별 추가 파라미터:
- A/Silver/B: `rule_load_mode`, `rule_table`, `rule_seed_path`
- C: `secret_scope`, `secret_key`, `salt`
- B/C: `--skip-upstream-readiness-check` (비상/수동 실행 우회)

기본 윈도우 해석:
- A: `run_mode`가 공백 또는 `incremental`이고 윈도우 파라미터가 모두 공백이면 자동 incremental 윈도우로 해석
  - `end_ts = now_utc`
  - `start_ts = pipeline_a.last_processed_end` 또는 `now_utc - 10분`
- B/C/Silver: `run_mode/start_ts/end_ts/date_kst_start/date_kst_end`가 모두 공백이면 전일(KST) 1일 backfill로 해석

## Databricks 워크플로우 진입점

정기 스케줄 jobs:

| Job | 목적 | 스케줄(KST) | 예시 |
|---|---|---|---|
| `pipeline_a_guardrail` | DQ guardrail 실행 | 매 10분 | `databricks bundle run pipeline_a_guardrail -t dev --params run_mode=incremental,run_id=manual_a_20260212` |
| `pipeline_silver_materialization` | Silver 물질화/격리 처리 | 매일 00:00 | `databricks bundle run pipeline_silver_materialization -t dev --params run_mode=backfill,date_kst_start=2026-02-11,date_kst_end=2026-02-11,run_id=manual_silver_20260212` |
| `pipeline_b_controls` | Recon/Supply/Ops/Admin 통제 | 매일 00:20 | `databricks bundle run pipeline_b_controls -t dev --params run_mode=backfill,date_kst_start=2026-02-11,date_kst_end=2026-02-11,run_id=manual_b_20260212` |
| `pipeline_c_analytics` | 익명화 분석 팩트 산출 | 매일 00:35 | `databricks bundle run pipeline_c_analytics -t dev --params run_mode=backfill,date_kst_start=2026-02-11,date_kst_end=2026-02-11,run_id=manual_c_20260212` |
| `bad_records_retention_cleanup` | bad_records 보존정리 | 매월 1일 00:50 | `databricks bundle run bad_records_retention_cleanup -t dev --params dry_run=true,retention_days=180` |

온디맨드 운영 jobs:

| Job | 목적 | 예시 |
|---|---|---|
| `bootstrap_catalog` | UC schema/table bootstrap | `databricks bundle run bootstrap_catalog -t dev` |
| `sync_dim_rule_scd2` | 룰 seed -> `gold.dim_rule_scd2` 반영 | `databricks bundle run sync_dim_rule_scd2 -t dev --params seed_path=mock_data/fixtures/dim_rule_scd2.json,table_name=gold.dim_rule_scd2` |

E2E jobs:

| Job | 목적 | 예시 |
|---|---|---|
| `e2e_setup_mock_data` | E2E용 mock data/테이블 준비 | `databricks bundle run e2e_setup_mock_data -t dev --params scenario=one_day_normal,run_id=e2e_setup_20260212` |
| `e2e_full_pipeline` | full workflow smoke | `databricks bundle run e2e_full_pipeline -t dev --params run_mode=backfill,date_kst_start=2026-02-11,date_kst_end=2026-02-11,run_id=e2e_full_20260212` |
| `e2e_cleanup_mock_data` | E2E 리소스 정리 | `databricks bundle run e2e_cleanup_mock_data -t dev --params drop_schemas=0` |

운영 장애 대응/복구 절차는 `.specs/ops/operations_runbook.md`를 SSOT로 사용합니다.

## 개발 및 검증 (Quickstart)

로컬 환경 준비:

```bash
python -m venv .venv
source .venv/bin/activate
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements-dev.txt
```

Unit 테스트:

```bash
.venv/bin/python -m pytest tests/unit/ -v -x
```

Integration 테스트(PySpark):

```bash
.venv/bin/python -m pip install -r requirements-spark.txt
.venv/bin/python -m pytest tests/integration/ -v
```

CI 게이트(`.github/workflows/ci.yml`):
- Unit coverage: `--cov-fail-under=80`
- Integration smoke coverage: `--cov-fail-under=60`

검증 증적 저장 경로:
- `.agents/logs/verification/`

참고:
- 문서 전용 변경은 정책상 L0-L2 생략 가능(AGENTS.md의 Exceptions 기준).

## 설정 및 보안

설정 우선순위:
- `CLI args > env vars > configs/{env}.yaml > configs/common.yaml`

환경변수 override:
- 패턴: `PIPELINE_CFG__<NESTED__KEY>`
- 존재하지 않는 키는 fail-fast로 오류 처리

룰 로딩 정책:
- A/B/Silver는 `rule_load_mode` 사용(`strict | fallback`)
- prod 문맥(`PIPELINE_ENV=prod`, `prod catalog` 등)에서는 `strict`만 허용

익명화 salt 해석 우선순위:
1. `ANON_USER_KEY_SALT`
2. Databricks Secret Scope (`analytics.secret_scope` + `analytics.secret_key`)
3. 로컬 fallback `local-salt-v1` (Databricks 런타임 외에서만 허용)

Databricks 런타임에서는 env/secret 모두 실패 시 fail-closed로 종료합니다.

## 참고 문서 (SSOT)

- `.specs/architecture_guide.md` — 팀 온보딩용 아키텍처 가이드
- `.specs/project_specs.md`
- `.specs/data_contract.md`
- `.specs/ops/operations_runbook.md`
- `.specs/ops/azure_monitoring_integration_plan.md`
- `tests/e2e/README.md`
