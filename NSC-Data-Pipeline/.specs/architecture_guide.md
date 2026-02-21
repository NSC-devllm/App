# Architecture Guide — Controls-first Ledger Pipelines

팀원 온보딩 및 코드베이스 이해를 위한 아키텍처 가이드.

---

## 1. 전체 구조 한눈에 보기

### 데이터 레이어

```
Bronze (원본)  →  Silver (세척/검증)  →  Gold (비즈니스 결과물)
```

- **Bronze**: 외부 시스템에서 들어온 JSONL 원본 데이터 (6개 테이블)
- **Silver**: 계약(contract) 기준으로 검증 완료된 데이터 (7개 테이블)
- **Gold**: 비즈니스에서 소비하는 최종 테이블 (10개 테이블)

### 파이프라인 4개 요약

| 파이프라인 | 한 줄 요약 | 입력 | 출력 |
|-----------|-----------|------|------|
| **Silver** | Bronze를 세척해서 Silver로 | Bronze 6개 | Silver 6개 + `bad_records` |
| **A (Guardrail)** | Bronze 데이터 품질 검사 | Bronze 3개 | `silver.dq_status`, `gold.exception_ledger` |
| **B (Controls)** | 재무 정합성 + 운영 지표 | Silver + Bronze 일부 | Gold 7개 |
| **C (Analytics)** | 익명화 결제 팩트 | Silver 3개 | `gold.fact_payment_anonymized` |

### 프로덕션 스케줄

프로덕션에서 각 파이프라인은 **독립된 Databricks Job**이다. Job 간 `depends_on`은 없고, 스케줄 시간 + 코드 레벨 체크로 순서를 보장한다.

```
00:00 KST  Silver — Bronze 6개 → Silver (어제 KST 날짜 데이터)
매 10분     A      — Bronze 3개 DQ 검사 (마이크로배치)
00:20 KST  B      — 정산/공급량/운영 지표 (어제 KST)
00:35 KST  C      — 익명화 팩트 (어제 KST)
```

E2E 테스트(`e2e_full_pipeline`)에서는 단일 워크플로우로 묶여 있다:
```
sync_rules → A → Silver → B, C (병렬)
```

---

## 2. 각 파이프라인 상세

### Pipeline Silver — Bronze 세척기

Bronze 6개 테이블을 읽어서, 계약 기준으로 검증한 뒤 통과/불량을 분리한다.

| Bronze 입력 | Silver 출력 | 주요 검증 |
|------------|------------|----------|
| `user_wallets_raw` | `wallet_snapshot` | user_id 존재, 잔액 >= 0, `balance_total = available + frozen` 계산 |
| `transaction_ledger_raw` | `ledger_entries` | tx_id 존재, amount > 0, 유효한 entry_type, `amount_signed` 계산 |
| `payment_orders_raw` + `orders_raw` | `order_events` | 두 테이블 UNION, `order_source` 태그 부착 |
| `order_items_raw` | `order_items` | ID 유효성 |
| `products_raw` | `products` | product_id 유효성 |
| (불량 레코드 전체) | `bad_records` | 위 검증 실패한 모든 행 |

**`amount_signed` 계산** — 원장 거래의 부호 방향:
```
CHARGE: +1,  RECEIVE: +1,  REFUND_IN: +1,  MINT: +1
PAYMENT: -1, WITHDRAW: -1, REFUND_OUT: -1, BURN: -1
HOLD: 0,     RELEASE: 0

amount_signed = amount × sign
```
이 값이 나중에 Pipeline B 정산에서 `net_flow = sum(amount_signed)`로 사용된다.

**안전장치**: `enforce_bad_records_rate` — 불량률이 규칙 임계값을 넘으면 `RuntimeError` (fail-fast). 나쁜 데이터를 Silver에 넣느니 차라리 파이프라인을 멈추는 전략.

---

### Pipeline A — DQ 검수원

Bronze 3개 테이블(`user_wallets_raw`, `transaction_ledger_raw`, `payment_orders_raw`)의 데이터 품질을 4가지 지표로 검사한다.

| 지표 | 의미 | 예시 |
|------|------|------|
| `freshness_sec` | 가장 최근 레코드가 지금으로부터 몇 초 전인가 | 300초 = 5분 전이 최신 |
| `dup_rate` | 키 기준 중복 비율 | 0.05 = 5% 중복 |
| `bad_records_rate` | 계약 위반 비율 | 0.02 = 2% 불량 |
| `zero_windows` | 연속 빈 윈도우 횟수 | 3 = 3번 연속 데이터 0건 |

**임계값은 코드에 하드코딩되어 있지 않다.** `gold.dim_rule_scd2` 테이블에서 런타임에 로드한다.

**판정 로직** (`_evaluate_severity`):
- `value >= warn 임계값` → `WARN`
- `value >= crit 임계값` → `CRITICAL`

**여러 지표가 동시에 걸리면** 우선순위로 하나만 선택:
```
SOURCE_STALE > DUP_SUSPECTED > EVENT_DROP_SUSPECTED > CONTRACT_VIOLATION
```

**`zero_windows` 연속성**: `pipeline_state`에서 이전 `zero_window_counts`를 읽고, 이번 윈도우가 0건이면 +1, 데이터가 있으면 0으로 리셋. N번 연속이어야 `EVENT_DROP_SUSPECTED` 발동.

**출력**:
- `silver.dq_status` — 윈도우별 DQ 지표 + 태그 (append)
- `gold.exception_ledger` — 임계값 초과 시 예외 기록 (append, 조건부)
- `gold.pipeline_state` — A 자신의 상태 업데이트

---

### Pipeline B — 재무 정합성 + 운영 지표

6가지 점검을 수행한다. Databricks에서 `recon`과 `supply_ops`가 **병렬** 실행된 뒤 finalize.

#### B-1. 정산 (recon_daily_snapshot_flow)
```
각 유저별, 날짜별:
  delta_balance = end_balance - start_balance  (silver.wallet_snapshot)
  net_flow = sum(amount_signed)                (silver.ledger_entries)
  drift_abs = |delta_balance - net_flow|

  drift_abs > threshold → 예외
```

A가 쓴 `dq_tag`가 `SOURCE_STALE`이나 `EVENT_DROP_SUSPECTED`이면, 임계값 판정을 **억제**하고 결과에 태그를 첨부한다 (소프트 게이팅).

#### B-2. 공급량 체크 (ledger_supply_balance_daily)
```
total_issued = 누적 (MINT + CHARGE - BURN - WITHDRAW)
total_wallets = 전체 유저 잔액 합계
diff_abs = |total_issued - total_wallets|

diff_abs > threshold → 예외
```

참고: `_evaluate_severity`가 `>` 사용 (임계값 정확히 일치 시 미발동). A의 `>=`와 다름.

#### B-3. 결제 실패율 (ops_payment_failure_daily)
`bronze.payment_orders_raw`에서 직접 읽어서 머천트별 FAILED/CANCELLED 건수 집계.

#### B-4. 환불율 (ops_payment_refund_daily)
같은 패턴으로 REFUNDED 건수 집계.

#### B-5. 원장 페어링 품질 (ops_ledger_pairing_quality_daily)
- `related_id` null 비율
- 정확히 2건(차변/대변) 페어 비율
- 결제 주문 매칭 비율

#### B-6. 관리자 거래 검색 (admin_tx_search)
원장 거래를 `related_id`로 쌍 매칭한 플랫 인덱스 테이블.

---

### Pipeline C — 익명화 결제 팩트

결제 이벤트를 상품 카테고리와 조인하고, user_id를 익명화해서 분석용 팩트 테이블을 생성한다.

```
silver.order_events (PAYMENT_ORDERS만 필터)
  + silver.order_items (주문별 상품)
  + silver.products (상품 카테고리)
  ↓
주문별 "대표 카테고리" 선정 (price × quantity가 가장 큰 상품)
  ↓
user_id → SHA-256(user_id + salt) → user_key
  ↓
gold.fact_payment_anonymized
  (date_kst, user_key, merchant_name, amount, status, category, run_id)
```

**쓰기 전략**: 유일하게 `overwrite_partitions` 사용. 해당 날짜 파티션만 통째로 교체.

**Salt 해소 순서**: Databricks Secrets → 환경변수 → 로컬 더미 (`local-salt-v1`).

---

## 3. 게이팅 메커니즘

### 하드 게이트 — 코드 레벨 (`assert_pipeline_ready`)

B와 C는 시작 시 `gold.pipeline_state`에서 **`pipeline_silver`** 완료를 확인한다.

```
체크 3단계:
1. pipeline_state 테이블 존재?
2. pipeline_name = "pipeline_silver" 행 존재?
3. last_success_ts non-null & last_processed_end >= 요구 시점?

하나라도 실패 → UpstreamReadinessError → 즉시 중단
```

| 파이프라인 | 하드 게이트 대상 |
|-----------|----------------|
| Silver | 없음 — Bronze 직접 읽음 |
| A | 없음 — Bronze 직접 읽음 |
| **B** | **pipeline_silver** |
| **C** | **pipeline_silver** |

Pipeline A에 대한 하드 게이트는 **존재하지 않는다.**

### 소프트 게이트 — 데이터 레벨 (dq_tags)

```
A가 dq_tag 생성 → silver.dq_status에 기록
                        ↓
B가 읽어서 → SOURCE_STALE 또는 EVENT_DROP_SUSPECTED면
           → 정산/공급량 결과에 태그 첨부 + 임계값 판정 억제
```

C는 `dq_status`를 읽지 않으므로 소프트 게이트 영향을 받지 않는다.

### 프로덕션 vs E2E 차이

| 항목 | 프로덕션 | E2E 테스트 |
|------|---------|-----------|
| 실행 단위 | 각 파이프라인이 독립 Job | 단일 워크플로우 |
| 순서 보장 | 스케줄 시간 + `assert_pipeline_ready` | Databricks `depends_on` DAG |
| A-Silver 순서 | 독립 (A는 10분 주기, Silver는 일 1회) | A → Silver (순차) |

---

## 4. 코드 구조

```
src/
├── transforms/   ← 순수 계산 로직 (Spark, DB 의존 없음)
├── io/           ← Delta Lake 읽기/쓰기만 담당
├── jobs/         ← transforms + io를 Spark DataFrame으로 조립
└── common/       ← 설정, 시간, 규칙 등 공유 유틸
```

**분리 원칙**: transforms는 `dict → dict` 순수 함수라서 로컬 pytest로 단위 테스트 가능. io는 Spark/Delta 의존이라 통합 테스트에서 검증. jobs는 이 둘을 엮는 진입점.

### 공통 모듈 역할

| 모듈 | 역할 |
|------|------|
| `contracts.py` | 23개 테이블의 스키마 계약 (컬럼, 타입, 필수 컬럼) |
| `rules.py` | `RuleDefinition` — 런타임 임계값/허용값 정의 |
| `job_params.py` | 실행 파라미터 파서 (`incremental` / `backfill`, UTC 윈도우, KST 날짜) |
| `time_utils.py` | UTC/KST 변환 (저장은 UTC, 파티셔닝은 KST) |
| `window_defaults.py` | 파라미터 없을 때 기본값 (Silver/B/C: 어제 backfill, A: 이어서 incremental) |
| `config_loader.py` | YAML 설정 로더 (`common.yaml` → `{env}.yaml` → 환경변수 오버라이드) |
| `table_metadata.py` | Delta 쓰기 설정 (merge key, partition column, write strategy) |
| `run_tracking.py` | `run_id` 생성 — 모든 출력 행에 찍혀서 실행 추적 가능 |

### 관계 흐름

```
config_loader → 환경 설정 로드
    ↓
job_params ← window_defaults (기본값 주입)
    ↓
time_utils → UTC/KST 변환
    ↓
rules + rule_loader → 임계값 로드 ← rule_mode_guard (프로덕션 보호)
    ↓
contracts → 스키마 검증 기준
    ↓
table_metadata → Delta 쓰기 방식 결정
    ↓
run_tracking → run_id 생성
```

---

## 5. IO 레이어

### Delta 쓰기 전략

| 전략 | 대상 | 동작 |
|------|------|------|
| `merge` | Gold 대부분, Silver 전체 | merge key 기준 UPSERT (`WHEN MATCHED UPDATE / WHEN NOT MATCHED INSERT`) |
| `overwrite_partitions` | `fact_payment_anonymized`만 | 해당 `date_kst` 파티션만 통째로 교체 |
| `append` | `silver.dq_status`, `gold.exception_ledger`, `silver.bad_records` | 기존 데이터 유지 + 새 행 추가 |

### `pipeline_state` — 파이프라인 간 조율 중심

`gold.pipeline_state` 테이블 구조:
```
pipeline_name         "pipeline_a" | "pipeline_silver" | "pipeline_b" | "pipeline_c"
last_success_ts       마지막 성공 시각 (UTC)
last_processed_end    마지막 처리 완료 윈도우 끝 (UTC) — B/C 하드 게이트가 이걸 체크
last_run_id           마지막 실행 ID
dq_zero_window_counts JSON 문자열 — A만 사용 (연속 빈 윈도우 카운트)
status                "success" | "failure"
```

성공 시: `last_success_ts = now`, `last_processed_end = 이번 윈도우 끝`
실패 시: `last_success_ts = 기존값 유지`, `status = "failure"`

### 안전장치

| 안전장치 | 위치 | 역할 |
|---------|------|------|
| NULL merge key 차단 | `merge_utils.py` | merge key에 NULL 있으면 즉시 `ValueError` |
| `safe_collect` | `spark_safety.py` | DataFrame collect 시 행 수 제한 (드라이버 OOM 방지) |
| `rule_mode_guard` | `rule_mode_guard.py` | 프로덕션에서 `fallback` 규칙 모드 차단 |
| `enforce_bad_records_rate` | `silver_controls.py` | Silver 불량률 초과 시 fail-fast |

---

## 6. 실제 실행 흐름 예시

**시나리오**: 2024-01-16(화) 새벽, 어제(01-15) 데이터 처리

### 00:00 — Silver 시작

파라미터 없이 스케줄 실행 → `inject_default_daily_backfill` 적용:
```
run_mode = "backfill"
date_kst_start = date_kst_end = "2024-01-15"
→ UTC 윈도우: 2024-01-14T15:00:00Z ~ 2024-01-15T15:00:00Z
```

1. Bronze 6개 테이블을 UTC 윈도우로 필터링해서 읽기
2. 계약 기준 검증 → 통과/불량 분리
3. 불량률 체크 → 임계값 이하면 진행, 초과면 RuntimeError
4. Silver 테이블 쓰기 (merge)
5. `pipeline_state` 업데이트: `pipeline_silver`, `last_processed_end = 2024-01-15T15:00:00Z`

### 00:10 — A 실행 (10분 주기)

이전 `pipeline_state`에서 `last_processed_end` 읽기 → 이어서 처리:
```
run_mode = "incremental"
start_ts = last_processed_end (이전 실행 끝)
end_ts = now()
```

Bronze 3개 테이블 각각에 대해 4가지 DQ 지표 계산:
- 전부 임계값 미만 → `dq_tag = null` → `silver.dq_status`에 기록
- 하나라도 초과 → `dq_tag` 설정 + `gold.exception_ledger`에 예외 기록

### 00:20 — B 시작

1. **하드 게이트**: `assert_pipeline_ready("pipeline_silver")` → `last_processed_end >= 요구 시점` 확인 → 통과
2. **소프트 게이트**: `silver.dq_status`에서 01-15 태그 확인 → 태그 없으면 정상 진행
3. 정산: 유저별 `drift_abs` 계산 → 임계값 비교
4. 공급량: `total_issued` vs `total_wallets` 비교
5. 운영 지표: 실패율/환불율/페어링 품질 집계
6. Gold 테이블 쓰기 (전부 merge)

### 00:35 — C 시작

1. **하드 게이트**: `assert_pipeline_ready("pipeline_silver")` → 통과
2. `PAYMENT_ORDERS` 이벤트 필터링
3. 상품 카테고리 조인 → 대표 카테고리 선정
4. user_id 익명화 (SHA-256 + salt)
5. `gold.fact_payment_anonymized` 쓰기 (overwrite_partitions)

### 장애 시나리오

**Silver 실패 시**: B/C가 `assert_pipeline_ready`에서 `UpstreamReadinessError` → 즉시 중단. `pipeline_state`의 `last_processed_end`가 갱신되지 않았으므로.

**A가 SOURCE_STALE 태그 발행 시**: B는 중단하지 않고 실행하되, 정산 결과에 `dq_tag = "SOURCE_STALE"` 첨부 + 임계값 판정 억제. 소비자가 이 태그를 보고 "데이터 품질 이슈가 있을 수 있음"을 인지.

---

## 부록: 테이블 목록

### Bronze (6개)
`user_wallets_raw`, `transaction_ledger_raw`, `payment_orders_raw`, `orders_raw`, `order_items_raw`, `products_raw`

### Silver (7개)
`wallet_snapshot`, `ledger_entries`, `order_events`, `order_items`, `products`, `bad_records`, `dq_status`

### Gold (10개)
`recon_daily_snapshot_flow`, `ledger_supply_balance_daily`, `fact_payment_anonymized`, `admin_tx_search`, `ops_payment_failure_daily`, `ops_payment_refund_daily`, `ops_ledger_pairing_quality_daily`, `exception_ledger`, `pipeline_state`, `dim_rule_scd2`
