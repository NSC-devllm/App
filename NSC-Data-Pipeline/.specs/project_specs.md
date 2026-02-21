# 프로젝트 스펙 (Azure Databricks) — Ledger Controls & Analytics (v1.1)

Last updated: 2026-02-12

> 요구사항 SSOT: `.ref/SRS - Software Requirements Specification.md`  
> DB 스키마 스냅샷: `.ref/database_schema`  
> FR-ADM-02 Serving 경계: `.ref/backoffice_db_admin_api.md`  
> 데이터 계약 SSOT: `.specs/data_contract.md`  
> 운영 기준: `.specs/ops/operations_runbook.md`  
> 모니터링 SSOT: `.specs/ops/azure_monitoring_integration_plan.md`  
> 결정 로그: `.specs/decision_open_items.md`

---

## 0) 문서 상태 (Current / Planned)

### 0.1 Current (구현됨)

- Pipeline A/B/C Databricks Workflow 배포 모델 (`databricks.yml`) 반영
- `run_mode/start_ts/end_ts/date_kst_start/date_kst_end/run_id` 표준 파라미터 반영
- Pipeline A 빈 윈도우 파라미터 자동 해석 적용 (D-042)
  - `run_mode`가 공백/`incremental`이고 윈도우 파라미터가 모두 공백이면 자동 incremental 윈도우를 계산한다.
  - 우선순위: `start_ts = pipeline_state.last_processed_end`(존재 시), 없으면 `now-10분`; `end_ts = now`
- Gold 핵심 산출물 구현
  - `gold.recon_daily_snapshot_flow`
  - `gold.ledger_supply_balance_daily`
  - `gold.fact_payment_anonymized`
  - `gold.ops_payment_failure_daily`
  - `gold.ops_payment_refund_daily`
  - `gold.ops_ledger_pairing_quality_daily`
  - `gold.admin_tx_search`
  - `gold.exception_ledger`
  - `gold.pipeline_state`
- `gold.dim_rule_scd2`
- `silver.bad_records` 영속화 구현
  - 운영 경로: `run_pipeline_silver`에서 `append-only` 적재
  - fail-fast: `wallet_snapshot`/`ledger_entries` bad rate 임계치 초과 시 실패 처리
  - 보존 정책: 180일 + 월 1회 cleanup workflow(`bad_records_retention_cleanup`, KST 00:50)
- 운영 Silver 물질화 경로 구현 (`run_pipeline_silver`, `pipeline_silver_materialization`)
  - B/C는 `pipeline_silver` 체크포인트 기반 fail-closed dependency 적용
  - B/C/Silver는 빈 윈도우 파라미터 시 `전일(KST) 1일 backfill` 기본 해석
  - `e2e_full_pipeline` 의존 그래프: `sync_dim_rule_scd2 -> pipeline_a -> pipeline_silver -> pipeline_b/pipeline_c`
- 운영 카탈로그 부트스트랩 경로 구현 (D-034)
  - `bootstrap_catalog` job + `scripts/bootstrap_catalog.py` (`--catalog` 필수, `--dry-run` 지원)
  - 범위: UC schema/table 생성 및 존재 검증(데이터 적재 제외)
- `gold.dim_rule_scd2` 테이블 기반 룰 로딩 구현
  - Pipeline A/B/Silver: `--rule-load-mode` 기반(`strict|fallback`)
  - 룰 반영 경로: `sync_dim_rule_scd2` 전용 job
  - 운영 기본 정책: prod `strict`, dev/test `fallback`
- Spark-only 런타임 경로 정리 (D-038)
  - A/B/C/Silver 런타임은 direct `collect()` 대신 bounded `safe_collect(max_rows=...)`만 허용
  - `databricks.yml`에서 `engine_mode` 변수/파라미터/CLI 전달 인자 제거
- 설정 SSOT 적용 및 하드코딩 제거 (D-039)
  - A/B/C/Silver `--catalog`, C의 `--secret-scope/--secret-key` 기본값을 `configs/*.yaml` 기반으로 해석
  - `PIPELINE_CFG__<NESTED__KEY>` env override 지원(unknown key fail-fast, bool/null/number 파싱)

### 0.2 Planned / Backlog (미구현 또는 확정 전)

- `gold.fact_market_price` (FR-ANA-02) 미구현

---

## 1) 비즈니스 목적과 경계

### 1.1 비즈니스 목적 (Controls-first)

Databricks는 결제/지갑 OLTP를 대체하는 시스템이 아니라, NSC 결제 운영의
사후 통제(controls)와 분석 적재를 담당한다.

- 재무/운영 통제: 일일 대사와 공급-잔액 정합성으로 "원장 해석 가능성"을 확보한다.
- 예외 관리: 이상 신호를 `gold.exception_ledger`에 단일 원장으로 남겨 운영 대응 근거를 만든다.
- 분석 기반: PII 없이 결제 패턴을 적재해 운영/상품 의사결정을 지원한다.

### 1.2 왜 필요한가 (Business Need)

- 결제 서비스 규모가 커질수록 수동 점검만으로는 drift/supply mismatch를 제때 탐지하기 어렵다.
- 원장 이상 징후가 늦게 발견되면 고객 정산 신뢰, 재무 리스크, 운영 복구 비용이 동시에 증가한다.
- `run_id`/`rule_id` 기반 증적이 없으면 "언제/왜/어떤 기준으로" 판단했는지 감사 추적이 어렵다.

즉, 본 스펙의 목적은 "데이터를 쌓는 것"이 아니라,
"원장 통제를 운영 가능한 프로세스로 고정"하는 데 있다.

### 1.3 성공 기준 (Business + Operational DoD)

- FR-ADM-01/03에 대해 D+0(KST) 기준 통제 결과가 Gold에 생성된다.
- 임계치 초과/품질 이슈는 `gold.exception_ledger`에 누락 없이 기록된다.
- 재실행 시 B/C 산출물이 key/partition 전략에 따라 수렴한다.
- 분석 산출물(`gold.fact_payment_anonymized`)은 PII 없이 생성되고 salt 정책을 준수한다.
- 모든 파이프라인 실행은 `run_id`로 추적 가능하며 `gold.pipeline_state`가 상태를 반영한다.

### 1.4 In-Scope

- 원장/관리자 통제(일일 대사, 공급-잔액 정합성, 예외 원장)
- 익명화 분석 적재(결제 패턴 팩트)
- 운영 관측(실행 안정성 중심: Azure Monitoring v1)

### 1.5 Out-of-Scope (명시)

- Freeze/Settle/Rollback 등 OLTP 트랜잭션 처리
- ACID 원장 쓰기 및 초저지연 Serving API
- FR-ADM-02 초단위 단건 조회 Serving (Backoffice DB + Admin API 영역)

---

## 2) 데이터 범위 및 레이어

### 2.1 소스 테이블 (현재)

- `user_wallets`
- `transaction_ledger`
- `payment_orders`
- `orders`
- `order_items`
- `products`

소스-표준 컬럼 상세는 `.specs/data_contract.md`를 SSOT로 사용한다.

### 2.2 Bronze

- 테이블: `bronze.*_raw` 6종
- 원칙: append-only, 원본 보존
- 공통 메타: `ingested_at`, `source_extracted_at`, `batch_id`, `source_system`

### 2.3 Silver (Current)

- `silver.wallet_snapshot`
- `silver.ledger_entries`
- `silver.order_events`
- `silver.order_items`
- `silver.products`
- `silver.bad_records`
- `silver.dq_status`

주의:
- `silver.bad_records`는 운영 `run_pipeline_silver` 경로에서 append-only로 적재되며, `wallet_snapshot`/`ledger_entries`는 격리 저장 후 fail-fast를 적용한다 (D-033, D-035).

### 2.4 Gold (Current)

- 공통: `gold.exception_ledger`, `gold.pipeline_state`
- 통제: `gold.recon_daily_snapshot_flow`, `gold.ledger_supply_balance_daily`
- 운영지표: `gold.ops_payment_failure_daily`, `gold.ops_payment_refund_daily`, `gold.ops_ledger_pairing_quality_daily`
- 관리자 보조 인덱스: `gold.admin_tx_search`
- 분석: `gold.fact_payment_anonymized`
- 룰 SSOT: `gold.dim_rule_scd2`

Planned:
- `gold.fact_market_price` (FR-ANA-02)

---

## 3) 워크플로우 운영 기준 (Current)

### 3.1 스케줄 / 재시도 / 타임아웃

| Pipeline | Schedule (KST) | Timeout | Retry |
|---|---|---:|---|
| A | `0 0/10 * * * ?` | 3600s | 2회, 5분 간격 |
| Silver | `0 0 0 * * ?` | 3600s | 2회, 5분 간격 |
| B | `0 20 0 * * ?` | 3600s | task별 2회, 5분 간격 |
| C | `0 35 0 * * ?` | 3600s | 2회, 5분 간격 |
| bad_records cleanup | `0 50 0 1 * ?` | 3600s | 2회, 5분 간격 |

근거: `databricks.yml`

### 3.2 Pipeline B task 구조

- `pipeline_b_recon`
- `pipeline_b_supply_ops`
- `pipeline_b_finalize_success` (둘 다 성공 시 `pipeline_state` 성공 갱신)
- `pipeline_b_finalize_failure` (`AT_LEAST_ONE_FAILED` 시 실패 갱신)

### 3.3 공통 실행 파라미터

- `run_mode`: `incremental | backfill`
- `start_ts`, `end_ts` (UTC)
- `date_kst_start`, `date_kst_end`
- `run_id`

파라미터 검증 규칙 (Current):

- `incremental`: `start_ts/end_ts` 필수
- `backfill`: `date_kst_start/end` 또는 `start_ts/end_ts`로 날짜 범위 해석 필수
- 운영 기본값(D-042): A에서 `run_mode`가 공백/`incremental`이고 윈도우 파라미터가 모두 공백이면 자동 incremental 윈도우(`start_ts=last_processed_end|now-10m`, `end_ts=now`)를 해석한다.
- 운영 기본값(D-035): B/C/Silver에서 `run_mode/start_ts/end_ts/date_kst_start/date_kst_end`가 모두 공백이면 `run_mode=backfill`, `date_kst_start=end=전일(KST)`로 자동 해석

### 3.4 운영 Workflow 인벤토리

- 정기 스케줄 jobs
  - `pipeline_a_guardrail`, `pipeline_silver_materialization`, `pipeline_b_controls`, `pipeline_c_analytics`
  - `bad_records_retention_cleanup`
- 온디맨드 운영/지원 jobs
  - `bootstrap_catalog` (UC schema/table bootstrap)
  - `sync_dim_rule_scd2` (룰 테이블 반영)
- E2E/검증 jobs
  - `e2e_setup_mock_data`, `e2e_cleanup_mock_data`, `e2e_full_pipeline`

### 3.5 설정 SSOT와 우선순위

- 설정 SSOT: `configs/common.yaml`, `configs/{env}.yaml`
- 적용 우선순위: `CLI args > env vars > configs/{env}.yaml > configs/common.yaml`
- 환경변수 오버라이드: `PIPELINE_CFG__<NESTED__KEY>` 형식 지원

---

## 4) 파이프라인 처리 규칙 (Current)

### 4.1 Pipeline A (Guardrail DQ)

비즈니스 필요성:

- B/C 통제 결과가 "신뢰 가능한 입력" 위에서만 해석되도록 품질 가드를 제공한다.
- 데이터 공백/지연 상황에서 잘못된 경보를 줄여 운영 노이즈와 오판 리스크를 낮춘다.

입력 소스 (Current):

- `bronze.transaction_ledger_raw`
- `bronze.user_wallets_raw`
- `bronze.payment_orders_raw`

핵심 계산:

- freshness, duplicates, completeness(연속 0-window), contract bad rate
- `dq_zero_window_counts`를 `gold.pipeline_state`에 저장/갱신

산출:

- `silver.dq_status` (append)
- `gold.exception_ledger` (append)

### 4.2 Pipeline B (Ledger/Admin Controls)

비즈니스 필요성:

- FR-ADM-01/03의 핵심 질문(총량 정합성, 일일 대사)을 자동화한다.
- 사람이 수기로 맞추던 재무 통제를 일 배치 기준의 반복 가능한 운영 절차로 전환한다.

핵심 규칙:

- 스냅샷 경계: 대상 `date_kst` 내 `snapshot_ts` 최소/최대값 사용 (D-002)
- 대사: `delta_balance_total - net_flow_total`
- 공급량: `event_kst <= target_date` 누적에서 공급 타입만 합산
- upstream readiness(fail-closed):
  - 기준: `pipeline_silver.last_processed_end >= required_processed_end`
  - 미충족 시 실패 처리(산출물 미작성)
  - 비상/수동 실행에서만 `--skip-upstream-readiness-check` 우회 허용

산출:

- `gold.recon_daily_snapshot_flow`
- `gold.ledger_supply_balance_daily`
- `gold.ops_payment_failure_daily`
- `gold.ops_payment_refund_daily`
- `gold.ops_ledger_pairing_quality_daily`
- `gold.admin_tx_search`
- `gold.exception_ledger`

### 4.3 Pipeline C (Analytics)

비즈니스 필요성:

- 운영 통제와 별개로 결제/주문 패턴 분석 기반을 제공한다.
- 익명화를 기본값으로 적용해 분석 활용성과 개인정보 보호 요구를 동시에 만족시킨다.

핵심 규칙:

- `silver.order_events` 중 `order_source = PAYMENT_ORDERS`만 적재 (D-012)
- `category` 파생 조인키는 `order_ref`만 사용 (`order_id` fallback 금지, D-041)
- `category`는 대표 아이템(line_amount 최대) 기준
  - 동률 처리: `item_id` 최소 우선, 추가 tie-break로 `product_id`, `category` 오름차순 적용 (D-011, D-041)
- `user_key = sha256(user_id + salt)`
- upstream readiness(fail-closed):
  - 기준: `pipeline_silver.last_processed_end >= required_processed_end`
  - 미충족 시 실패 처리(산출물 미작성)
  - 비상/수동 실행에서만 `--skip-upstream-readiness-check` 우회 허용

산출:

- `gold.fact_payment_anonymized`

### 4.4 파이프라인별 의사결정 질문

| Pipeline | 핵심 비즈니스 질문 | 주 산출물 |
|---|---|---|
| A | "지금 이 데이터로 B/C 결과를 신뢰해도 되는가?" | `silver.dq_status`, `gold.exception_ledger`, `gold.pipeline_state` |
| B | "어제 기준 원장 정합성에 재무 리스크가 있는가?" | `gold.recon_daily_snapshot_flow`, `gold.ledger_supply_balance_daily`, `gold.exception_ledger` |
| C | "PII 없이 결제 패턴을 분석 가능한 형태로 제공했는가?" | `gold.fact_payment_anonymized` |

---

## 5) 임계치, 룰, 비교 방식

### 5.1 임계치 비교 규칙 (Current)

- DQ Guardrail: `>=` 비교
- Ledger Controls: `>` 비교

### 5.2 룰 로딩 소스 (Current)

- 룰 SSOT: `gold.dim_rule_scd2`
- Pipeline A/B/Silver 런타임:
  - `strict`: 테이블 로딩 실패 시 즉시 실패(fail-closed)
  - `fallback`: 테이블 미존재/접근 실패 시에만 `mock_data/fixtures/dim_rule_scd2.json` fallback
  - 테이블 payload 무결성 오류(중복 rule_id, domain+metric current 중복, 형식 오류)는 fallback 없이 fail-fast
- 룰 반영/배포 경로: `sync_dim_rule_scd2` job로 `gold.dim_rule_scd2`를 갱신하고 런타임은 해당 테이블을 읽는다.
- 기본 운영 정책:
  - prod: `strict`
  - dev/test: `fallback`
- prod 가드레일:
  - prod 문맥(`PIPELINE_ENV=prod` 또는 `catalog=prod_catalog` 또는 `catalog == configs/prod.yaml.databricks.catalog`)에서 `rule_load_mode != strict`는 즉시 차단한다.

### 5.3 주요 결정 연계

- D-005: drift/supply 임계치 비교 `value > threshold`
- D-019: salt 해석 우선순위
- D-020: `pipeline_state` 성공/실패 갱신 규칙
- D-033: `silver.bad_records` 영속화 방식
- D-034: 운영 부트스트랩(schema/table) 정책
- D-036: 룰 SSOT `gold.dim_rule_scd2` + prod strict 가드
- D-038: Spark-only 전환 + bounded collect 정책
- D-039: `configs/*.yaml` 런타임 SSOT + env override
- D-040: `gold.exception_ledger` MERGE key 확장 + 동일 `run_id` 재실행 수렴 기준
- D-041: Pipeline C category 조인키/동률 tie-break 정책
- D-042: Pipeline A 빈 윈도우 파라미터 자동 incremental 해석

---

## 6) 쓰기 전략과 멱등성

### 6.1 테이블별 쓰기 전략 (Current)

| 영역 | 테이블 | 전략 |
|---|---|---|
| Silver | `wallet_snapshot`, `ledger_entries`, `order_events`, `order_items`, `products` | MERGE |
| Silver | `dq_status` | append |
| Silver | `bad_records` | append (`detected_date_kst` partition) |
| Gold | `recon_daily_snapshot_flow`, `ledger_supply_balance_daily`, `ops_*`, `admin_tx_search`, `pipeline_state` | MERGE |
| Gold | `exception_ledger` | A는 append, B는 MERGE |
| Gold | `fact_payment_anonymized` | `date_kst` 파티션 overwrite (`replaceWhere`) |

### 6.2 멱등성 해석 주의

- B/C는 key/partition 전략 기준으로 재실행 수렴을 목표로 한다.
- A의 `dq_status`/`exception_ledger`는 append이므로 동일 윈도우 재실행 시 행이 누적될 수 있다.
- `gold.exception_ledger`(Pipeline B) MERGE key는 `(date_kst, domain, exception_type, run_id, metric, message)`다.
- Pipeline B는 동일 `run_id` 재실행을 허용하며, 동일 입력 기준으로 `exception_ledger`를 포함한 산출물 수렴을 보장한다.

### 6.3 `pipeline_state` 갱신 규칙

- 성공: `last_success_ts`, `last_processed_end`, `last_run_id`, `updated_at` 갱신
- 실패: `last_success_ts`, `last_processed_end` 유지, `last_run_id`, `updated_at`만 갱신

---

## 7) 모니터링/알림 기준 (Current = Azure Monitoring v1)

기준:
- Current = v1(Log Analytics only)
- Future = v2(테이블 기반 알림 확장)
- 모니터링 범위 충돌 시 최상위 SSOT는 `.specs/ops/azure_monitoring_integration_plan.md`를 따른다.

### 7.1 In-Scope

- Databricks 진단 로그 기반 실행 관측
- Activity Log 기반 플랫폼 이벤트
- A/B/C 통합 Workbook
- Core Execution Alerts 4종
  - Job 실패
  - 최근 성공 지연
  - 재시도 소진
  - 클러스터 시작 실패/타임아웃

### 7.2 Out-of-Scope (현재 단계)

- `silver.dq_status` row 기반 알림
- `gold.exception_ledger` CRITICAL row 집계 알림
- `gold.pipeline_state` row 기반 `last_success_ts` 정확 집계 알림
- `SOURCE_STALE`/`EVENT_DROP_SUSPECTED` 데이터 기반 억제 알림

주의:
- 따라서 “`exception_ledger` CRITICAL이면 즉시 알림”은 현재 v1 범위에서는 보장하지 않는다.
- `DQ CRITICAL`, `SOURCE_STALE`, `EVENT_DROP_SUSPECTED` 알림은 v2 확장 범위다.

---

## 8) 보안/익명화 기준

### 8.1 salt 해석 우선순위 (Current)

1. `ANON_USER_KEY_SALT` 환경변수
2. Databricks Secret Scope (`configs/*`의 `analytics.secret_scope` + `analytics.secret_key`)
3. 로컬 더미 `local-salt-v1` (Databricks 런타임 외에서만 허용)

### 8.2 Databricks 런타임 동작

- Databricks 런타임에서는 secret/env 모두 실패 시 fail-closed
- 로컬 더미 fallback은 허용되지 않는다

---

## 9) 테스트/검증/CI

### 9.1 테스트 레벨

- Unit: `tests/unit/`
- Integration: `tests/integration/`
- E2E skeleton: `tests/e2e/`
- 로컬 테스트 실행 원칙: 프로젝트 가상환경 기준으로
  `.venv/bin/python -m pytest ...`를 사용한다.

### 9.2 CI 게이트 (Current)

- Unit gate: `python -m pytest tests/unit/ -v -x`
- L2 combined coverage gate: `python -m pytest tests/unit/ tests/integration/ -v --cov=src --cov-fail-under=80`

주의:
- CI와 별도로, 로컬 검증 정책은 프로젝트 가상환경(`.venv/bin/python`) 기준을 유지한다.

근거: `.github/workflows/ci.yml`

### 9.3 검증 증적

- 저장 위치: `.agents/logs/verification/`
- 운영 증적 템플릿/런북은 ops 문서를 따른다.

---

## 10) 개발/운영 로드맵 기준

핵심 원칙:

- 현재는 테스트 리소스 기반 기능/운영 기준선 고정 단계(Phase 7~10)
- 보안 하드닝/서비스 프린시플 전환은 신규 secure 환경(Phase 11+)에서 수행

자세한 계획:

- `.roadmap/implementation_roadmap.md` (Workstream 기준, Legacy Phase 매핑 포함)
- `.specs/cloud/cloud_migration_rebuild_plan.md`
- `.specs/ops/cutover_preflight_exit_template.md`

---

## 11) 요구사항 매핑 (Current 관점)

| SRS ID | 비즈니스 질문 | 현재 상태 | Databricks 산출물 |
|---|---|---|---|
| FR-ADM-01 | "총 발행량과 잔액 합계가 일치하는가?" | 구현됨 | `gold.ledger_supply_balance_daily` |
| FR-ADM-02 | "특정 tx를 초단위로 추적 가능한가?" | 보조 경로만 구현 | `gold.admin_tx_search` (Serving SSOT 아님) |
| FR-ADM-03 | "일일 순흐름과 잔액 변화가 일치하는가?" | 구현됨 | `gold.recon_daily_snapshot_flow` |
| FR-ANA-01 | "개인정보 없이 결제 패턴 분석이 가능한가?" | 구현됨 | `gold.fact_payment_anonymized` |
| FR-ANA-02 | "외부 시세를 운영/분석 지표에 결합했는가?" | Planned | (미구현) |

---

## 12) 즉시 후속 정합화 항목

1. 운영 증적 축적: `bootstrap_catalog`/`bad_records_retention_cleanup` 정기 실행 로그를 기준선으로 누적
2. 룰 변경 거버넌스(runbook 절차) 운영 증적 축적 및 정기 점검
3. `data_contract.md`와 `project_specs.md`의 Current/Planned 표기를 동일 기준으로 유지
4. v2 모니터링 확장 준비(테이블 기반 알림: `dq_status`, `exception_ledger`, `pipeline_state`, stale/drop 억제)
