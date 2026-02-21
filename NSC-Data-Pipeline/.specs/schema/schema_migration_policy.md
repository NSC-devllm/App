# Schema Migration Policy (Bronze/Silver/Gold)

Last updated: 2026-02-12

## 0. Scope & SSOT

이 문서는 Bronze/Silver/Gold 스키마 변경을 운영 실행 가능한 절차로 고정한다.

적용 범위:
- Current(구현됨) 기준 23개 계약 테이블(Bronze 6, Silver 7, Gold 10)
- 변경 분류, 실행 순서, 검증/증적, 롤백 규칙

비범위:
- Planned 항목(`gold.fact_market_price`)의 선반영 정책
- 코드 구현 자체(이 문서는 정책/절차 SSOT)

SSOT 우선순위(충돌 시 상위 우선):
1. Runtime 코드
- `src/common/table_metadata.py`
- `src/common/contracts.py`
- `src/io/bronze_io.py`
- `src/io/silver_io.py`
- `src/io/gold_io.py`
- `src/io/pipeline_state_io.py`
2. 운영/요구 문서
- `.specs/project_specs.md` (특히 5~7장)
- `.specs/data_contract.md`
- `.specs/decision_open_items.md` (D-033~D-042)
- `.specs/ops/operations_runbook.md`
- `.specs/cloud/cloud_migration_rebuild_plan.md`
3. 본 문서

## 1. Current Baseline Matrix

### 1.1 Bronze (6)

| Table | Write Strategy | Partition | Merge Key | Schema Evolution Policy |
|---|---|---|---|---|
| `bronze.user_wallets_raw` | append | 없음 | 없음 | 기존 테이블에 한해 `mergeSchema=true` 허용 |
| `bronze.transaction_ledger_raw` | append | 없음 | 없음 | 기존 테이블에 한해 `mergeSchema=true` 허용 |
| `bronze.payment_orders_raw` | append | 없음 | 없음 | 기존 테이블에 한해 `mergeSchema=true` 허용 |
| `bronze.orders_raw` | append | 없음 | 없음 | 기존 테이블에 한해 `mergeSchema=true` 허용 |
| `bronze.order_items_raw` | append | 없음 | 없음 | 기존 테이블에 한해 `mergeSchema=true` 허용 |
| `bronze.products_raw` | append | 없음 | 없음 | 기존 테이블에 한해 `mergeSchema=true` 허용 |

Bronze 공통 정책:
- append-only
- 최초 테이블 생성 경로는 `append` fallback
- `mergeSchema`는 기존 테이블일 때만 적용(최초 생성 시 미적용)

### 1.2 Silver (7)

| Table | Write Strategy | Partition | Merge Key | Notes |
|---|---|---|---|---|
| `silver.wallet_snapshot` | MERGE | `snapshot_date_kst` | `(snapshot_ts, user_id)` | fail-fast 대상 |
| `silver.ledger_entries` | MERGE | `event_date_kst` | `(tx_id, wallet_id)` | fail-fast 대상 |
| `silver.order_events` | MERGE | `event_date_kst` | `(order_ref, order_source)` |  |
| `silver.order_items` | MERGE | 없음 | `(item_id)` |  |
| `silver.products` | MERGE | 없음 | `(product_id)` |  |
| `silver.bad_records` | append | `detected_date_kst` | 없음 | 선저장 후 fail-fast |
| `silver.dq_status` | append | `date_kst` | 없음 | Pipeline A append |

### 1.3 Gold (10)

| Table | Write Strategy | Partition | Merge Key | Notes |
|---|---|---|---|---|
| `gold.recon_daily_snapshot_flow` | MERGE | `date_kst` | `(date_kst, user_id)` |  |
| `gold.ledger_supply_balance_daily` | MERGE | `date_kst` | `(date_kst)` |  |
| `gold.ops_payment_failure_daily` | MERGE | `date_kst` | `(date_kst, merchant_name)` |  |
| `gold.ops_payment_refund_daily` | MERGE | `date_kst` | `(date_kst, merchant_name)` |  |
| `gold.ops_ledger_pairing_quality_daily` | MERGE | `date_kst` | `(date_kst)` |  |
| `gold.admin_tx_search` | MERGE | `event_date_kst` | `(event_date_kst, tx_id)` |  |
| `gold.exception_ledger` | Pipeline A: append / Pipeline B: MERGE | `date_kst` | Pipeline B: `(date_kst, domain, exception_type, run_id, metric, message)` | D-040 반영 |
| `gold.pipeline_state` | MERGE | 없음 | `(pipeline_name)` | 실패 시 checkpoint 유지 |
| `gold.fact_payment_anonymized` | overwrite partitions | `date_kst` | 없음 | `replaceWhere` + non-empty partition guard |
| `gold.dim_rule_scd2` | MERGE | 없음 | `(rule_id)` | 런타임 룰 SSOT |

## 2. Change Classification

| Class | Definition | Typical Examples | Required Verification |
|---|---|---|---|
| Minor (호환) | 기존 write strategy/merge key/partition 의미를 깨지 않는 변경 | nullable 컬럼 추가, `allowed_values` 확장, 주석/설명 변경 | 기본 L2 |
| Major (비호환) | 재실행 수렴 조건 또는 저장 의미를 바꾸는 변경 | MERGE key 변경, partition 변경, write strategy 변경, 필수 컬럼 추가, 타입 축소/의미 변경 | 기본 L3 |

Major 예시:
- D-040: `gold.exception_ledger` MERGE key 확장  
`(date_kst, domain, exception_type, run_id)` ->  
`(date_kst, domain, exception_type, run_id, metric, message)`

분류 규칙:
- 아래 중 하나라도 해당하면 Major
1. `src/common/table_metadata.py`의 merge key/partition/write strategy 변경
2. `src/common/contracts.py`의 required 컬럼 추가 또는 타입 축소
3. 동일 `run_id` 재실행 수렴 조건 변경

## 3. Migration Workflow

### 3.1 Prepare

1. 변경 요청서(7장 템플릿) 작성
2. 영향 테이블/파이프라인 식별
3. 변경 분류(Minor/Major) 확정
4. 문서 동기화 범위 확정
- `.specs/data_contract.md`
- `.specs/project_specs.md`
- 필요 시 `.specs/decision_open_items.md`
5. 검증 레벨(L1/L2/L3) 사전 확정

### 3.2 Apply

1. 계약/메타/코드 동기화
- `src/common/contracts.py`
- `src/common/table_metadata.py`
- 관련 transform/io/job script
2. DDL 적용 원칙
- 신규 테이블 생성: `bootstrap_catalog` 사용 가능
- 기존 테이블 스키마 변경: 명시적 DDL(`ALTER TABLE ...`)로 적용
- `bootstrap_catalog`는 `CREATE ... IF NOT EXISTS` 범위(기존 스키마 진화 대체 불가)
3. 룰 변경 원칙
- 룰 payload/스키마 변경은 `sync_dim_rule_scd2`로 반영
- prod는 `rule_load_mode=strict` 고정

### 3.3 Backfill

1. 대상 윈도우를 `date_kst_start/date_kst_end`로 명시
2. 권장 순서
- `sync_dim_rule_scd2` -> Pipeline A -> Pipeline Silver -> Pipeline B/C
3. `run_id` 전략
- rerun 증적 추적 가능한 명시적 패턴 사용(예: `rerun_<pipeline>_<yyyymmddhhmm>`)

### 3.4 Incremental Resume

1. `gold.pipeline_state`의 `last_processed_end` 검증
2. 스케줄 재개 전 핵심 출력 테이블 row/중복 점검
3. 운영 모드 복귀 후 첫 1회 실행 결과를 별도 증적으로 저장

## 4. Strategy-Specific Runbooks

### 4.1 MERGE Key Change (Major)

적용 대상:
- MERGE key가 존재하는 Silver/Gold 테이블

절차:
1. 신규 key 기준 중복 가능성 사전 점검(SQL)
2. `table_metadata`와 테스트를 먼저 동기화
3. 변경 윈도우를 partition 단위로 재계산(backfill)
4. 동일 `run_id` 재실행 시 수렴 확인

필수 확인:
- `gold.exception_ledger`는 6-key 기준 중복 0건

### 4.2 Partition Overwrite Change (Major)

적용 대상:
- `gold.fact_payment_anonymized` 또는 overwrite_partitions 전략 테이블

절차:
1. partition 컬럼 단일성 유지 확인
2. 대상 partition 값이 non-empty인지 사전 확인
3. 좁은 `date_kst` 범위부터 단계 실행
4. 비대상 partition 불변성 확인

필수 확인:
- `replaceWhere` guard 위반(빈 partition set) 없음

### 4.3 Append Table Change

적용 대상:
- `silver.dq_status`, `silver.bad_records`, Pipeline A의 `gold.exception_ledger`

절차:
1. append 특성상 동일 윈도우 재실행 시 중복 가능성을 명시
2. `run_id` 운영 규칙을 변경 요청서에 반드시 기록
3. `silver.bad_records` 관련 변경 시 “선저장 -> fail-fast” 순서 유지 검증

### 4.4 Rule Table (`gold.dim_rule_scd2`) Change

절차:
1. seed/테이블 payload 무결성 확인
- `rule_id` 중복 금지
- 동일 `domain+metric`에 `is_current=true` 1건만 허용
2. `sync_dim_rule_scd2` 실행
3. A/B/Silver 런타임 룰 로딩 검증(`strict|fallback`)

## 5. Verification & Evidence

### 5.1 Verification Ladder

| Level | Command | Pass Criteria |
|---|---|---|
| L0 | `python -m py_compile <FILE>` | syntax error 없음 |
| L1 | `.venv/bin/python -m pytest tests/unit/ -v -x` | unit pass |
| L2 | `.venv/bin/python -m pytest tests/unit/ tests/integration/ -v --cov=src --cov-fail-under=80` | 80%+ |
| L3 | Databricks Dev E2E | idempotency 검증 |

상향 규칙:
- `src/transforms/` 변경: 최소 L1
- `src/io/`, `src/jobs/` 변경: 최소 L2
- reconciliation/rule table 변경: L3 필수
- `data_contract.md` 변경: L3 + manual review

예외:
- 문서-only 변경은 L0~L2 생략 가능

### 5.2 Required Evidence

- 증적 저장: `.agents/logs/verification/`
- 포함 항목:
1. 실행 명령/시간
2. 대상 window/date_kst
3. 사용 `run_id`
4. pass/fail 결과
5. 핵심 쿼리 결과 스냅샷

### 5.3 Mandatory Scenario Checks

1. MERGE key 변경(Major): `gold.exception_ledger` 6-key 정확성
2. overwrite_partitions: `replaceWhere` 및 partition 불변성
3. append+fail-fast: `silver.bad_records` 선저장 흐름
4. bootstrap/sync: `bootstrap_catalog` + `sync_dim_rule_scd2` 선행 확인
5. runbook 정합: `.specs/ops/operations_runbook.md` 7장과 충돌 없음
6. 테이블 커버리지: Bronze 6 + Silver 7 + Gold 10 = 23

## 6. Rollback Rules

1. 실패 시 Bronze 기준 재처리 가능해야 한다.
2. `gold.pipeline_state`는 실패 업데이트에서 checkpoint를 유지해야 한다.
- `last_success_ts`, `last_processed_end` 유지
- `last_run_id`, `updated_at` 갱신
3. 롤백 후 동일 윈도우 재실행 결과가 수렴해야 한다.
4. Major 변경 롤백 시:
- 코드/메타데이터를 직전 버전으로 복원
- 영향 partition/date_kst 재백필
- 수렴 검증 통과 후 incremental 재개

## 7. Migration Request Template

```yaml
title: ""
requester: ""
target_env: "dev|prod"
affected_tables:
  - ""
change_class: "minor|major"
write_strategy: ""
merge_keys_before: []
merge_keys_after: []
partition_before: []
partition_after: []
backfill_window:
  date_kst_start: "YYYY-MM-DD"
  date_kst_end: "YYYY-MM-DD"
run_id_strategy: ""
verification_level: "L1|L2|L3"
rollback_trigger: ""
verification_queries:
  - ""
approver: ""
exit_criteria:
  - ""
evidence_path: ".agents/logs/verification/"
```

## 8. SSOT Cross-Check Checklist

- [ ] `src/common/contracts.py`와 컬럼 계약 일치
- [ ] `src/common/table_metadata.py`와 strategy/merge key/partition 일치
- [ ] `src/io/gold_io.py`의 overwrite partition guard 정책 반영
- [ ] Pipeline A/Silver/B 실행 경로와 append/merge 분기 일치
- [ ] `.specs/project_specs.md` 6장과 write strategy 설명 일치
- [ ] `.specs/ops/operations_runbook.md` 7장과 rerun/idempotency 규칙 일치
- [ ] `.specs/decision_open_items.md` D-040 등 결정 로그와 충돌 없음
