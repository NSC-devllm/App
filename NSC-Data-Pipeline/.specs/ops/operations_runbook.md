# Operations Runbook (Pipeline A/Silver/B/C)

Last updated: 2026-02-12

## 1. Scope

- Pipeline A (Guardrail DQ)
- Pipeline Silver (Bronze -> Silver materialization)
- Pipeline B (Ledger/Admin controls)
- Pipeline C (Analytics anonymized mart)
- 공통 상태 테이블: `gold.pipeline_state`
- 공통 예외 테이블: `gold.exception_ledger`

참조 문서:
- 모니터링 기준(SSOT): `.specs/ops/azure_monitoring_integration_plan.md`
- L3 실행/검증 절차: `.ref/l3_databricks_dev_e2e_runbook.md`
- 성능/파티션 점검: `.specs/ops/performance_partitioning_checklist.md`
- 컷오버 체크리스트: `.specs/ops/cutover_preflight_exit_template.md`

## 2. Operational Baseline

스케줄/재시도 기준(Workflows):

| Pipeline | Schedule (KST) | Timeout | Retry |
|---|---|---:|---|
| A | 매 10분 (`0 0/10 * * * ?`) | 3600s | 2회, 5분 간격 |
| Silver | 매일 00:00 (`0 0 0 * * ?`) | 3600s | 2회, 5분 간격 |
| B | 매일 00:20 (`0 20 0 * * ?`) | 3600s | 각 task별 2회, 5분 간격 |
| C | 매일 00:35 (`0 35 0 * * ?`) | 3600s | 2회, 5분 간격 |
| BadRecords Cleanup | 매월 1일 00:50 (`0 50 0 1 * ?`) | 3600s | 2회, 5분 간격 |

공통 실행 파라미터:
- `run_mode`: `incremental | backfill`
- `start_ts`, `end_ts` (UTC window)
- `date_kst_start`, `date_kst_end` (backfill day window)
- `run_id`
- D-038 이후 런타임 엔진은 spark-only로 고정한다(`engine_mode` 파라미터 제거).
- `rule_load_mode` (A/B/Silver): `strict | fallback`

운영 기본값(D-035):
- B/C/Silver는 `run_mode/start_ts/end_ts/date_kst_start/date_kst_end`가 모두 공백이면 `전일(KST) 1일 backfill`로 자동 해석한다.
- B/C는 `pipeline_silver`의 `last_processed_end`가 대상 윈도우를 충족하지 못하면 fail-closed로 즉시 실패한다.

운영 기본값(D-042):
- A는 `run_mode`가 공백/`incremental`이고 `start_ts/end_ts/date_kst_start/date_kst_end`가 모두 공백이면 자동 incremental 윈도우를 해석한다.
  - `end_ts = now_utc`
  - `start_ts = pipeline_a.last_processed_end` (존재 시), 없거나 역전이면 `end_ts - 10분`

Pipeline B task 구성:
- `pipeline_b_recon`
- `pipeline_b_supply_ops`
- `pipeline_b_finalize_success` (둘 다 성공 시 state 갱신)
- `pipeline_b_finalize_failure` (`AT_LEAST_ONE_FAILED` 시 state 갱신)

운영/지원(On-demand) jobs:
- `bootstrap_catalog` (운영 bootstrap: schema/table 생성/검증, 데이터 적재 제외)
- `sync_dim_rule_scd2` (룰 테이블 반영)

## 3. Monitoring Model (Azure Monitoring v1)

원칙:
- 알림 범위/정의 충돌 시 `.specs/ops/azure_monitoring_integration_plan.md`를 최우선 SSOT로 적용한다.
- 알림은 Azure Monitor + Log Analytics의 실행 신호 중심으로 운영한다.
- 데이터 테이블(`pipeline_state`, `dq_status`, `exception_ledger`)은 alert source가 아니라 triage/사후분석 SSOT로 사용한다.

현재 In-Scope (알림):
- Databricks 진단 로그 기반 실행 관측
- Activity Log 기반 플랫폼 이벤트
- A/B/C 통합 Workbook 1개
- Core Execution Alerts 4종 (v1 page 보장 범위)
  - Job 실패
  - 최근 성공 지연
  - 재시도 소진
  - 클러스터 시작 실패/타임아웃

현재 Out-of-Scope (알림):
- `silver.dq_status` 기반 알림
- `gold.exception_ledger` row 기반 CRITICAL 집계 알림
- `gold.pipeline_state` row 기반 `last_success_ts` 정확 집계 알림
- `SOURCE_STALE`/`EVENT_DROP_SUSPECTED`의 테이블 기반 억제 알림

모니터링 신호 역할 구분:
- Alert source(v1): Databricks 진단 로그, Azure Activity Log
- Triage source(v1): `gold.pipeline_state`, `silver.dq_status`, `gold.exception_ledger`

## 4. Alert Policy (Core Execution Alerts)

| Alert | Trigger | Source | Severity / Target | Notes |
|---|---|---|---|---|
| Job Failure | 단일 job run 실패 | Databricks diagnostic logs | P1 / 즉시 | Action Group 라우팅 |
| Recent Success Delay | A: 최근 성공이 20분 초과 지연, B/C: 스케줄+30분 지연 | Log Analytics query | P1/P2 운영 목표(10분/30분) | 초기 노이즈는 튜닝 |
| Retry Exhausted | 최대 재시도(2회) 소진 후 실패 | Databricks diagnostic logs | P1 | transient 장애 조기 탐지 |
| Cluster Start/Timeout Failure | 클러스터 기동 실패/타임아웃 | Databricks diagnostic logs + Activity log | P1 | infra성 이슈 우선 대응 |

채널 정책:
- Severity + Owner 기반 Action Group 분리
- dev/prod 공통 규칙 프레임 유지, 채널 강도만 차등 운영

## 5. Incident Triage

### 5.1 Step 1: 실행 컨텍스트 확보

```bash
databricks jobs get-run <RUN_ID> -o json
databricks jobs get-run-output <RUN_ID> -o json
```

확인 항목:
- `life_cycle_state`, `result_state`, `state_message`
- 실패 task key (특히 Pipeline B는 `recon/supply_ops/finalize_*` 분리 확인)
- retry 소진 여부

### 5.2 Step 2: 데이터 상태 점검 (수동 triage)

```sql
-- 파이프라인 상태
SELECT *
FROM ${catalog}.gold.pipeline_state
ORDER BY updated_at DESC;

-- 예외 추이
SELECT date_kst, domain, exception_type, severity, count(*) AS cnt
FROM ${catalog}.gold.exception_ledger
GROUP BY date_kst, domain, exception_type, severity
ORDER BY date_kst DESC, domain, exception_type;

-- DQ 상태 (A 후속 영향 분석)
SELECT date_kst, source_table, dq_tag, severity, event_count, bad_records_rate
FROM ${catalog}.silver.dq_status
ORDER BY window_end_ts DESC;

-- Quarantine bad records (원인 drill-down)
SELECT detected_date_kst, source_table, reason, count(*) AS cnt
FROM ${catalog}.silver.bad_records
GROUP BY detected_date_kst, source_table, reason
ORDER BY detected_date_kst DESC, source_table, cnt DESC;
```

### 5.3 Step 3: Gold 결과 최소 검증

```bash
python scripts/e2e/verify_gold_tables.py --catalog <CATALOG> --run-id <RUN_ID>
```

또는 SQL로 핵심 테이블 row count 확인:
- `gold.recon_daily_snapshot_flow`
- `gold.ledger_supply_balance_daily`
- `gold.fact_payment_anonymized`

## 6. Recovery Playbook

### 6.1 Pipeline A 실패

점검:
1. Bronze 입력 3종(`transaction_ledger_raw`, `user_wallets_raw`, `payment_orders_raw`) 접근/최근 데이터 확인
2. `bad_records_rate`, `dup_rate`, `freshness_sec` 급등 여부 확인
3. `gold.pipeline_state.dq_zero_window_counts`(연속 0-window 누적) 확인

재실행 예시:
```bash
databricks bundle run pipeline_a_guardrail -t dev \
  --params run_mode=backfill,date_kst_start=2026-02-01,date_kst_end=2026-02-01,run_id=rerun_a_20260211
```

주의:
- Pipeline A는 `silver.dq_status`, `gold.exception_ledger`를 append로 기록한다.
- 동일 윈도우를 동일 `run_id`로 반복 실행하면 중복 행이 생길 수 있으므로 운영 재실행 시 `run_id` 전략을 명확히 남긴다.

### 6.2 Pipeline Silver 실패

점검:
1. Bronze 입력 6종(`user_wallets_raw`, `transaction_ledger_raw`, `payment_orders_raw`, `orders_raw`, `order_items_raw`, `products_raw`) 대상 윈도우 데이터 존재 확인
2. `silver.bad_records` row 증가 추이와 `wallet_snapshot`/`ledger_entries` bad rate 임계치 초과 여부 확인
3. `gold.pipeline_state`의 `pipeline_silver` 상태(`last_success_ts`, `last_processed_end`, `last_run_id`) 확인

재실행 예시:
```bash
databricks bundle run pipeline_silver_materialization -t dev \
  --params run_mode=backfill,date_kst_start=2026-02-01,date_kst_end=2026-02-01,run_id=rerun_silver_20260211
```

주의:
- `silver.bad_records`는 append-only다.
- `wallet_snapshot`/`ledger_entries`는 fail-fast 초과여도 bad_records 선저장 이후 실패한다.

### 6.3 Pipeline B 실패

점검:
1. `gold.pipeline_state`에서 `pipeline_silver.last_processed_end`가 대상 윈도우 이상인지 확인
2. `silver.wallet_snapshot`, `silver.ledger_entries` 대상 `date_kst` 데이터 존재 확인
3. `gold.dim_rule_scd2`에서 `drift_abs`, `supply_diff_abs` 임계치/rule_id 확인
4. 임계치 비교는 `value > threshold` 규칙(경계값 동일 시 경보 아님)으로 해석
5. 실패 task가 `recon`인지 `supply_ops`인지 먼저 분리

재실행 예시:
```bash
databricks bundle run pipeline_b_controls -t dev \
  --params run_mode=backfill,date_kst_start=2026-02-01,date_kst_end=2026-02-01,run_id=rerun_b_20260211
```

주의:
- Gold 주요 산출물은 merge 기반으로 수렴한다.
- `gold.exception_ledger`(Pipeline B) merge key는 `(date_kst, domain, exception_type, run_id, metric, message)`다.
- 동일 `run_id` 재실행을 허용하며, 같은 입력 기준으로 결과가 수렴해야 한다.

### 6.4 Pipeline C 실패

점검:
1. `gold.pipeline_state`에서 `pipeline_silver.last_processed_end`가 대상 윈도우 이상인지 확인
2. Secret Scope/Key (`configs/{env}.yaml`의 `analytics.secret_scope`/`analytics.secret_key`) 접근 여부 확인
3. Databricks 런타임에서는 salt 해석 실패 시 fail-closed(로컬 더미 fallback 불가)
4. `silver.order_events/order_items/products` 조인 키 및 대상 `event_date_kst` 데이터 확인
5. category 파생 조인은 `order_ref`만 사용(`order_id` fallback 없음), 동률 시 tie-break(`item_id`, `product_id`, `category`) 규칙 확인

재실행 예시:
```bash
databricks bundle run pipeline_c_analytics -t dev \
  --params run_mode=backfill,date_kst_start=2026-02-01,date_kst_end=2026-02-01,run_id=rerun_c_20260211
```

주의:
- Pipeline C는 `date_kst` 파티션 overwrite(`replaceWhere`) 전략이다.
- 백필 범위를 좁혀 재실행해야 불필요한 파티션 교체를 줄일 수 있다.

## 7. Re-run And Idempotency Guardrails

| Pipeline | Write Strategy | Re-run Guidance |
|---|---|---|
| A | append (`silver.dq_status`, `gold.exception_ledger`) + `pipeline_state` merge | 동일 윈도우 반복 시 중복 가능. 운영 재실행은 목적(runbook 대응/재처리)과 `run_id`를 증적에 명시 |
| Silver | Silver MERGE + `silver.bad_records` append + `pipeline_state` merge | 동일 key 재실행 시 수렴. fail-fast 임계치 초과 시 `bad_records` 저장 후 실패 |
| B | Gold merge + `pipeline_state` merge | 동일 key 재실행 시 수렴. `exception_ledger`는 `(date_kst, domain, exception_type, run_id, metric, message)` 기준으로 동일 `run_id` 재실행도 수렴 |
| C | `gold.fact_payment_anonymized` partition overwrite + `pipeline_state` merge | 대상 `date_kst` 파티션만 교체. 재실행 시 백필 범위를 최소화 |

권장:
- 장애 복구 rerun은 가급적 `backfill` + 명시적 `date_kst_start/end` 사용
- 동일 장애 건의 반복 시 `run_id` 네이밍 규칙 고정(예: `rerun_<pipeline>_<yyyymmddhhmm>`)

Pipeline B `exception_ledger` 중복 점검 SQL:
```sql
SELECT
  date_kst, domain, exception_type, run_id, metric, message, COUNT(*) AS dup_cnt
FROM ${catalog}.gold.exception_ledger
GROUP BY date_kst, domain, exception_type, run_id, metric, message
HAVING COUNT(*) > 1;
```

### 7.1 `silver.bad_records` 보존/정리

- 보존 정책: 180일
- 정리 주기: 월 1회
- 기본 경로: Databricks Workflow `bad_records_retention_cleanup` (scheduled)
- 수동 SQL은 fallback 절차로만 사용

Dry-run 실행(권장 점검):
```bash
databricks bundle run bad_records_retention_cleanup -t <dev|prod> \
  --params dry_run=true,retention_days=180
```

Execute 실행(수동 트리거):
```bash
databricks bundle run bad_records_retention_cleanup -t <dev|prod> \
  --params dry_run=false,retention_days=180
```

Fallback SQL (job 비가용 시):
```sql
DELETE FROM ${catalog}.silver.bad_records
WHERE detected_date_kst < date_sub(current_date(), 180);
```

### 7.2 `gold.dim_rule_scd2` 변경 거버넌스 (SSOT)

원칙:
- 런타임 룰 SSOT는 `gold.dim_rule_scd2`다.
- seed 파일(`mock_data/fixtures/dim_rule_scd2.json`)은 sync job 입력 아티팩트이며, 운영 판정은 table 기준으로 수행한다.
- prod 문맥(`PIPELINE_ENV=prod` 또는 prod catalog 판별)에서는 `rule_load_mode=strict`만 허용된다.
- dev/test는 `fallback` 허용하되 table 미존재/접근 실패에서만 seed fallback을 사용한다(테이블 payload 무결성 오류는 fail-fast).

역할:
- Owner: 데이터 오너(룰 변경 요청/근거 작성)
- Reviewer: 온콜/운영 리뷰어(임계치/영향 검토 승인)

버전/유효기간 규칙:
- `rule_id`는 전역 유일해야 한다.
- 동일 `domain+metric`에서 `is_current=true`는 1건만 허용한다.
- 변경 시 기존 current rule은 `effective_end_ts`를 닫고 `is_current=false`로 전환한다.

적용 절차:
1. 변경 seed 파일(`mock_data/fixtures/dim_rule_scd2.json`) 업데이트
2. 배포 전 검토(Owner/Reviewer 승인 기록)
3. Databricks 수동 job 실행
```bash
databricks bundle run sync_dim_rule_scd2 -t <dev|prod> \
  --params seed_path=mock_data/fixtures/dim_rule_scd2.json
```
4. 검증 쿼리 실행
```sql
SELECT domain, metric, count_if(is_current) AS current_cnt
FROM ${catalog}.gold.dim_rule_scd2
GROUP BY domain, metric
ORDER BY domain, metric;
```
5. 증적 저장(`.agents/logs/verification/`): 승인자, 실행자, seed revision, 결과 row count

## 8. Escalation

즉시(P1):
1. Job failure/Retry exhausted/Cluster failure alert 발생 후 10분 이내 완화 불가
2. Pipeline B/C가 연쇄 실패하여 일일 통제 산출물 생성 불가

신속(P2):
1. 최근 성공 지연이 30분 목표를 초과
2. 핵심 Gold 테이블 갱신 누락이 배치 윈도우를 넘어 지속

추가:
- 누락/중복이 허용치 초과로 판단되면 데이터 오너 + 온콜 동시 에스컬레이션

## 9. Evidence

저장 위치:
- `.agents/logs/verification/`
- 템플릿: `.agents/logs/verification/templates/e2e_evidence_template.md`

최소 증적 필수 항목:
1. 실행 일시(UTC)와 오퍼레이터
2. Workspace host / target catalog / environment(dev/prod)
3. `run_mode`, `start_ts/end_ts` 또는 `date_kst_start/end`, `run_id`
4. 관련 alert id(or rule name), 감지 시각, 해제 시각
5. 실행 명령과 결과 로그(`databricks jobs get-run`, `bundle run`, 검증 쿼리 결과)
6. 조치 내용(원인, 수정, 재실행 여부, 후속 액션)
