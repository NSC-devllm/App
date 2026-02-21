# 데이터 컨트랙트: Ledger Controls & Analytics (v1.1)

> **SSOT(요구사항)**: `.ref/SRS - Software Requirements Specification.md` (v1.1, 2026-01-30)  
> **SSOT(스키마 맥락)**: `.ref/database_schema`  
> **연계 문서**: `.specs/project_specs.md` (Databricks 스펙/파이프라인)  
> **Last Updated**: 2026-02-12  
> **범위**: SRS 2.3(원장 및 관리자), 2.4(Analytics - OLAP)

---

## 문서 상태 (Current / Planned)

### Current (구현됨)

- Silver: `silver.wallet_snapshot`, `silver.ledger_entries`, `silver.order_events`, `silver.order_items`, `silver.products`, `silver.bad_records`, `silver.dq_status`
- Gold: `gold.recon_daily_snapshot_flow`, `gold.ledger_supply_balance_daily`, `gold.fact_payment_anonymized`, `gold.admin_tx_search`, `gold.ops_payment_failure_daily`, `gold.ops_payment_refund_daily`, `gold.ops_ledger_pairing_quality_daily`, `gold.exception_ledger`, `gold.pipeline_state`, `gold.dim_rule_scd2`

### Planned / Backlog

- `gold.fact_market_price` (FR-ANA-02)

---

## 0) 목적 / 범위

### 0.1 목적

Databricks에서 다음을 안정적으로 수행하기 위한 **최소 데이터 계약(Contract)**을 정의한다.

- **원장/관리자 통제(Controls)**: 일일 대사(Δ잔고 = 순흐름), 총량 정합성(발행량 ↔ 잔액 합), 예외 원장 기록
- **분석(OLAP)**: 결제/주문 데이터를 **익명화(pseudonymization)**하여 분석 레이크하우스로 적재

### 0.2 비범위(명시)

다음은 Databricks 범위 밖이다(OLTP RDBMS + Kafka/서비스 영역).

- Freeze/Settle/Rollback 등 **트랜잭션 처리**
- ACID 보장(원장 원자성/격리성)
- 실시간(≤500ms) 조회 API 제공
- FR-ADM-02의 **초단위 `tx_id` 단건 조회(Serving)**: Backoffice DB + Admin API(상세: `.ref/backoffice_db_admin_api.md`, 본 문서는 Lakehouse 배치 인덱스만 정의)

### 0.3 현재 소스(스키마 스냅샷 기준)

> 실제 운영 DB명/스키마명은 환경 설정으로 주입하며, 컬럼 명세는 `.ref/database_schema`를 기준으로 한다.

- 지갑: `user_wallets`
- 원장 이벤트: `transaction_ledger`
- 결제 오더: `payment_orders`
- 커머스: `orders`, `order_items`, `products`
- (PII/인증) `users`, `accounts` 등은 분석 적재의 기본 범위에서 제외

---

## 1) 공통 원칙

### 1.1 시간(타임스탬프) 원칙

- **저장/계산 표준**: UTC
- **일일 기준(day boundary)**: KST 기준 `date_kst`
- 모든 이벤트에는 논리 시간 `event_time`이 있어야 한다.
  - 현재 스키마에서 `transaction_ledger.created_at`를 `event_time`으로 사용한다.
- 모든 스냅샷에는 스냅샷 기준 시각 `snapshot_ts`가 있어야 한다.
  - DB 컬럼이 없어도 됨(ETL/적재 시점 메타로 부여 가능)

### 1.2 멱등성(idempotency)

- Databricks 적재는 재실행해도 결과가 수렴해야 한다.
- 권장 메타(원본/표준 공통):
  - `ingested_at`(UTC), `source_extracted_at`, `batch_id`, `source_system`

### 1.3 금액 타입

- 원장/결제 금액은 `DECIMAL(38, 2)`로 표준화(소스는 `DECIMAL(18,2)`).
- 부동소수(float/double) 금지.

### 1.4 삭제/정정

- 원장 성격 데이터(ledger)는 **hard delete 금지**가 원칙.
- 정정이 필요하면 “정정 이벤트(역분개/반대 부호)” 또는 소프트 플래그로 표현을 권장한다.

### 1.5 개인정보/민감정보(Analytics)

- 분석용 테이블에는 PII/credential(비밀번호 등)을 적재하지 않는다.
- 분석용 user key는 `user_key = sha2(concat(user_id, salt), 256)` 형태로 **pseudonymization** 한다.
  - `salt`는 Secret Scope/Key Vault에서 관리(코드/문서에 하드코딩 금지).

---

## 2) 소스 계약(OLTP → Bronze)

> Bronze는 원본 보존 목적이며, Silver에서 계약을 강제한다.

### 2.0 Bronze Raw 표준(현재 운영 계약)

**Bronze Raw 테이블**

| Bronze 테이블 | 원본 소스 | 비고 |
|---|---|---|
| `bronze.user_wallets_raw` | `user_wallets` | 지갑 스냅샷 원본 |
| `bronze.transaction_ledger_raw` | `transaction_ledger` | 원장 이벤트 원본 |
| `bronze.payment_orders_raw` | `payment_orders` | 결제 오더 원본 |
| `bronze.orders_raw` | `orders` | 커머스 주문 원본 |
| `bronze.order_items_raw` | `order_items` | 커머스 아이템 원본 |
| `bronze.products_raw` | `products` | 상품 마스터 원본 |

**공통 메타 컬럼(6개 Raw 공통)**

| 컬럼 | 타입(권장) | 필수 | 의미 |
|---|---|---:|---|
| `ingested_at` | `timestamp` | ⭕️ | Bronze 적재 시각(UTC) |
| `source_extracted_at` | `timestamp` | ⭕️ | 소스 추출 시각(UTC) |
| `batch_id` | `string` | ⭕️ | 배치 식별자 |
| `source_system` | `string` | ⭕️ | 소스 시스템 식별자 |

### 2.1 `user_wallets` (지갑 상태)

**스키마(현재)**

| 컬럼 | 타입(소스) | 필수 | 의미 | 품질 규칙 |
|---|---|---:|---|---|
| `user_id` | `VARCHAR(255)` | ✅ | 지갑/사용자 식별자(PK) | NULL 불가 |
| `balance` | `DECIMAL(18,2)` | ✅ | 가용 잔액 | NULL 불가, 음수 불허(초기 원칙) |
| `frozen_amount` | `DECIMAL(18,2)` | ✅ | 동결 잔액 | NULL 불가, 음수 불허 |
| `updated_at` | `TIMESTAMP` | ✅(권장) | 최종 변경 시각 | NULL 가능(없으면 `snapshot_ts`로 대체) |

### 2.2 `transaction_ledger` (원장 이벤트)

**스키마(현재)**

| 컬럼 | 타입(소스) | 필수 | 의미 | 품질 규칙 |
|---|---|---:|---|---|
| `tx_id` | `VARCHAR(36)` | ✅ | 트랜잭션 ID(PK) | 유일/중복 불가 |
| `wallet_id` | `VARCHAR(255)` | ✅ | 지갑 식별자(FK → `user_wallets.user_id`) | NULL 불가 |
| `type` | `VARCHAR(50)` | ✅ | 이벤트 유형 | NULL 불가, 허용값은 룰로 관리 |
| `amount` | `DECIMAL(18,2)` | ✅ | 금액(절대값) | NULL 불가, `amount > 0` 권장 |
| `related_id` | `VARCHAR(255)` | ⭕️ | 외부 참조(주문/결제 등) | 문자열 표준 |
| `created_at` | `TIMESTAMP` | ✅(권장) | 이벤트 시각 | NULL 가능 시 적재 메타로 대체 불가(원장 대사에 치명) |

**중요: 대사 가능성 요구조건**

`transaction_ledger`는 현재 스키마만으로는 **부호/방향(±)** 및 **상대방(sender/receiver)** 정보가 없다.  
Databricks에서 일일 대사(Δ잔고 = 순흐름)를 하기 위해서는 아래 중 하나가 필수다.

1) 업스트림(서비스/ETL)이 `amount_signed`(±) 또는 `entry_side(debit/credit)`를 제공  
또는  
2) `type`을 근거로 **결정적 부호 파생**이 가능하고, 그 매핑이 룰 테이블로 관리됨

위가 불가능하면 `silver.ledger_entries.amount_signed`를 만들 수 없으므로, 해당 레코드는 **bad_records로 격리**하고 통제 산출물에 포함하지 않는다.

### 2.3 `payment_orders` (결제 오더)

| 컬럼 | 타입(소스) | 필수 | 의미 | 품질 규칙 |
|---|---|---:|---|---|
| `order_id` | `VARCHAR(255)` | ✅ | 결제 오더 ID(PK) | 유일/중복 불가 |
| `user_id` | `VARCHAR(255)` | ⭕️ | 결제 요청 사용자 | NULL 가능(운영 요건에 따라 강화) |
| `merchant_name` | `VARCHAR(255)` | ⭕️ | 가맹점명 |  |
| `amount` | `DECIMAL(18,2)` | ✅ | 결제 금액 | NULL 불가, `> 0` |
| `status` | `VARCHAR(50)` | ✅(권장) | 결제 상태 | 허용값 룰 관리 |
| `created_at` | `TIMESTAMP` | ✅(권장) | 생성 시각 |  |

### 2.4 커머스(`orders`, `order_items`, `products`)

> `orders.order_id`는 `BIGINT`, 반면 `payment_orders.order_id`는 `VARCHAR`이므로, Databricks에서는 **`order_ref`를 문자열로 표준화**한다.

**`orders`**

| 컬럼 | 타입(소스) | 필수 | 의미 |
|---|---|---:|---|
| `order_id` | `BIGINT` | ✅ | 주문 ID(PK) |
| `user_id` | `VARCHAR(255)` | ⭕️ | 사용자 식별자 |
| `total_amount` | `DECIMAL(18,2)` | ⭕️ | 주문 총액 |
| `status` | `VARCHAR(50)` | ⭕️ | 주문 상태 |
| `created_at` | `TIMESTAMP` | ⭕️ | 생성 시각 |

**`order_items`**

| 컬럼 | 타입(소스) | 필수 | 의미 |
|---|---|---:|---|
| `item_id` | `BIGINT` | ✅ | PK |
| `order_id` | `BIGINT` | ✅ | 주문 FK |
| `product_id` | `BIGINT` | ✅ | 상품 FK |
| `quantity` | `INT` | ⭕️ | 수량 |
| `price_at_purchase` | `DECIMAL(18,2)` | ⭕️ | 구매 단가 |

**`products`**

| 컬럼 | 타입(소스) | 필수 | 의미 |
|---|---|---:|---|
| `product_id` | `BIGINT` | ✅ | PK |
| `product_name` | `VARCHAR(255)` | ⭕️ | 상품명 |
| `price_krw` | `DECIMAL(18,2)` | ⭕️ | 가격 |
| `stock_quantity` | `INT` | ⭕️ | 재고 |
| `category` | `VARCHAR(100)` | ⭕️ | 카테고리 |
| `is_display` | `BOOLEAN` | ⭕️ | 노출 여부 |

---

## 3) Silver 표준 계약(Bronze → Silver)

### 3.1 `silver.wallet_snapshot` (스냅샷)

**목적**

- 일일 대사/총량 검증을 위해 지갑 상태를 시점 스냅샷으로 고정한다.

**스키마(표준)**

| 컬럼 | 타입(권장) | 필수 | 의미 |
|---|---|---:|---|
| `snapshot_ts` | `timestamp` | ✅ | 스냅샷 기준 시각(UTC) |
| `snapshot_date_kst` | `date` | ✅ | `snapshot_ts`를 KST로 변환한 날짜 |
| `user_id` | `string` | ✅ | 사용자/지갑 ID |
| `balance_available` | `decimal(38,2)` | ✅ | 가용 잔액 |
| `balance_frozen` | `decimal(38,2)` | ✅ | 동결 잔액 |
| `balance_total` | `decimal(38,2)` | ✅ | `available + frozen` |
| `source_updated_at` | `timestamp` | ⭕️ | 소스 갱신 시각(`user_wallets.updated_at`) |
| `run_id` | `string` | ✅ | 실행 추적 |
| `rule_id` | `string` | ⭕️ | 적용 룰 버전 |

**키(멱등성)**

- `(snapshot_ts, user_id)` MERGE

### 3.2 `silver.ledger_entries` (원장 엔트리 표준화)

**목적**

- 일일 대사에서 사용할 “순흐름(net flow)”을 계산 가능하게 한다.

**스키마(표준)**

| 컬럼 | 타입(권장) | 필수 | 의미 |
|---|---|---:|---|
| `tx_id` | `string` | ✅ | 원장 트랜잭션 ID |
| `wallet_id` | `string` | ✅ | 대상 지갑 |
| `event_time` | `timestamp` | ✅ | 이벤트 시각(UTC) |
| `event_date_kst` | `date` | ✅ | `event_time`의 KST 날짜 |
| `entry_type` | `string` | ✅ | 유형(`transaction_ledger.type`) |
| `amount` | `decimal(38,2)` | ✅ | 절대값 금액 |
| `amount_signed` | `decimal(38,2)` | ✅ | 부호 포함 금액(순흐름 계산용) |
| `related_id` | `string` | ⭕️ | 외부 참조 |
| `related_type` | `string` | ⭕️ | 참조 도메인(ORDER/PAYMENT_ORDER/ETC) |
| `status` | `string` | ⭕️ | 상태(가능 시 조인/파생) |
| `created_at` | `timestamp` | ⭕️ | 원본 생성 시각 |
| `run_id` | `string` | ✅ | 실행 추적 |
| `rule_id` | `string` | ⭕️ | 적용 룰 버전 |

**키(멱등성)**

- 기본: `(tx_id, wallet_id)` MERGE  
  (동일 tx_id가 다중 엔트리를 가질 수 있으면 `entry_seq` 추가 권장)

**필수 품질 규칙**

- `amount_signed`는 NULL 불가
- `amount` > 0 권장(0이면 bad_records)
- `event_time` NULL 불가(일일 버킷 불가)

### 3.3 `silver.order_events` (분석용 주문/결제 이벤트 표준화)

**목적**

- 주문/결제 데이터를 분석 적재로 연결할 수 있는 최소 스키마를 제공한다.

**스키마(표준)**

| 컬럼 | 타입(권장) | 필수 | 의미 |
|---|---|---:|---|
| `order_ref` | `string` | ✅ | 주문/결제 오더 참조(문자열 표준) |
| `order_source` | `string` | ✅ | `ORDERS` 또는 `PAYMENT_ORDERS` |
| `user_id` | `string` | ⭕️ | 내부 조인용(민감) |
| `merchant_name` | `string` | ⭕️ | 가맹점 |
| `amount` | `decimal(38,2)` | ⭕️ | 금액 |
| `status` | `string` | ⭕️ | 상태 |
| `event_time` | `timestamp` | ⭕️ | 이벤트 시각(생성/결제시각 등) |
| `event_date_kst` | `date` | ⭕️ | KST 날짜 |
| `run_id` | `string` | ✅ | 실행 추적 |

### 3.4 `silver.order_items` / `silver.products` (분석 보조 차원/아이템)

**`silver.order_items`**

| 컬럼 | 타입(권장) | 필수 | 의미 |
|---|---|---:|---|
| `item_id` | `bigint` | ✅ | 아이템 PK |
| `order_id` | `bigint` | ✅ | 원본 주문 ID |
| `order_ref` | `string` | ✅ | `cast(order_id as string)` |
| `product_id` | `bigint` | ✅ | 상품 FK |
| `quantity` | `int` | ⭕️ | 수량 |
| `price_at_purchase` | `decimal(38,2)` | ⭕️ | 구매 단가 |
| `run_id` | `string` | ✅ | 실행 추적 |

**`silver.products`**

| 컬럼 | 타입(권장) | 필수 | 의미 |
|---|---|---:|---|
| `product_id` | `bigint` | ✅ | 상품 PK |
| `product_name` | `string` | ⭕️ | 상품명 |
| `category` | `string` | ⭕️ | 카테고리 |
| `price_krw` | `decimal(38,2)` | ⭕️ | 가격 |
| `is_display` | `boolean` | ⭕️ | 노출 여부 |
| `run_id` | `string` | ✅ | 실행 추적 |

### 3.5 `silver.dq_status` (Guardrail DQ 상태)

| 컬럼 | 타입(권장) | 필수 | 의미 |
|---|---|---:|---|
| `source_table` | `string` | ✅ | 점검 대상 소스 |
| `window_start_ts` | `timestamp` | ✅ | 점검 윈도우 시작(UTC) |
| `window_end_ts` | `timestamp` | ✅ | 점검 윈도우 종료(UTC) |
| `date_kst` | `date` | ✅ | 윈도우 종료 시각 기준 KST 일자 |
| `freshness_sec` | `bigint` | ⭕️ | 최신 데이터 지연 초 |
| `event_count` | `bigint` | ✅ | 윈도우 이벤트 건수 |
| `dup_rate` | `decimal(38,6)` | ⭕️ | 중복 비율 |
| `bad_records_rate` | `decimal(38,6)` | ⭕️ | 계약 위반 비율 |
| `dq_tag` | `string` | ⭕️ | 대표 상태 태그 |
| `severity` | `string` | ⭕️ | 상태 심각도 |
| `run_id` | `string` | ✅ | 실행 추적 |
| `rule_id` | `string` | ⭕️ | 적용 룰 |
| `generated_at` | `timestamp` | ✅ | 산출 시각(UTC) |

**쓰기/파티션**

- 쓰기 전략: append
- 파티션: `date_kst`

---

## 4) Gold 계약(Silver → Gold)

### 4.1 `gold.recon_daily_snapshot_flow` (일일 대사 결과)

| 컬럼 | 타입(권장) | 필수 | 의미 |
|---|---|---:|---|
| `date_kst` | `date` | ✅ | 대상 일자 |
| `user_id` | `string` | ✅ | 사용자 |
| `delta_balance_total` | `decimal(38,2)` | ✅ | `balance_total_end - balance_total_start` |
| `net_flow_total` | `decimal(38,2)` | ✅ | `SUM(amount_signed)` |
| `drift_abs` | `decimal(38,2)` | ✅ | `abs(delta - net_flow)` |
| `drift_pct` | `decimal(38,6)` | ⭕️ | 퍼센트(분모 0 방지 룰 필요) |
| `dq_tag` | `string` | ⭕️ | dq 상태 태그(guardrail 연계) |
| `run_id` | `string` | ✅ | 실행 추적 |
| `rule_id` | `string` | ⭕️ | 적용 룰 |

### 4.2 `gold.ledger_supply_balance_daily` (총량 정합성)

| 컬럼 | 타입(권장) | 필수 | 의미 |
|---|---|---:|---|
| `date_kst` | `date` | ✅ | 대상 일자 |
| `issued_supply` | `decimal(38,2)` | ✅ | 누적 공급량(`event_date_kst <= date_kst`, `MINT/CHARGE` +, `BURN/WITHDRAW` -) |
| `wallet_total_balance` | `decimal(38,2)` | ✅ | 지갑 잔액 합 |
| `diff_amount` | `decimal(38,2)` | ✅ | `issued_supply - wallet_total_balance` |
| `is_ok` | `boolean` | ✅ | diff 0 여부(임계치 룰 가능) |
| `run_id` | `string` | ✅ | 실행 추적 |
| `rule_id` | `string` | ⭕️ | 적용 룰 |

### 4.3 `gold.fact_payment_anonymized` (익명화 분석 팩트)

| 컬럼 | 타입(권장) | 필수 | 의미 |
|---|---|---:|---|
| `date_kst` | `date` | ✅ | 일자 |
| `user_key` | `string` | ✅ | 익명화 키(sha2) |
| `merchant_name` | `string` | ⭕️ | 가맹점 |
| `amount` | `decimal(38,2)` | ⭕️ | 금액 |
| `status` | `string` | ⭕️ | 상태 |
| `category` | `string` | ⭕️ | 상품 카테고리(가능 시) |
| `run_id` | `string` | ✅ | 실행 추적 |

### 4.4 `gold.admin_tx_search` (tx_id 배치 인덱스)

> 본 테이블은 “검색 편의/감사/분석” 목적의 **배치 인덱스**다.  
> FR-ADM-02의 초단위 단건 조회는 Backoffice DB + Admin API가 SSOT이다.
> 현재 운영 구현 범위에 포함되며, Serving API의 SSOT를 대체하지 않는다.

| 컬럼 | 타입(권장) | 필수 | 의미 |
|---|---|---:|---|
| `event_date_kst` | `date` | ✅ | KST 기준 날짜(파티션) |
| `tx_id` | `string` | ✅ | 원장 엔트리(행) ID |
| `wallet_id` | `string` | ✅ | 지갑 ID |
| `entry_type` | `string` | ✅ | 유형 |
| `amount` | `decimal(38,2)` | ✅ | 절대값 금액 |
| `amount_signed` | `decimal(38,2)` | ⭕️ | 부호 포함 금액(가능 시) |
| `event_time` | `timestamp` | ✅ | 이벤트 시각(UTC) |
| `related_id` | `string` | ⭕️ | 외부 참조(페어링 키 후보) |
| `related_type` | `string` | ⭕️ | 참조 도메인 |
| `merchant_name` | `string` | ⭕️ | 결제 오더 조인 시 보강 |
| `payment_status` | `string` | ⭕️ | 결제 오더 조인 시 보강 |
| `paired_tx_id` | `string` | ⭕️ | (방법2) 페어링된 반대 엔트리 tx_id(가능 시) |
| `run_id` | `string` | ✅ | 실행 추적 |
| `rule_id` | `string` | ⭕️ | 적용 룰 |

### 4.5 `gold.ops_payment_failure_daily` (운영 지표: 결제 실패율)

| 컬럼 | 타입(권장) | 필수 | 의미 |
|---|---|---:|---|
| `date_kst` | `date` | ✅ | 대상 일자 |
| `merchant_name` | `string` | ⭕️ | 가맹점(없으면 NULL) |
| `total_cnt` | `bigint` | ✅ | 결제 오더 총 건수 |
| `failed_cnt` | `bigint` | ✅ | 실패 건수(실패 status 정의는 룰/합의로 고정) |
| `failure_rate` | `decimal(38,6)` | ✅ | `failed_cnt / total_cnt` |
| `run_id` | `string` | ✅ | 실행 추적 |
| `rule_id` | `string` | ⭕️ | 적용 룰 |

### 4.6 `gold.ops_payment_refund_daily` (운영 지표: 결제 환불율)

| 컬럼 | 타입(권장) | 필수 | 의미 |
|---|---|---:|---|
| `date_kst` | `date` | ✅ | 대상 일자 |
| `merchant_name` | `string` | ⭕️ | 가맹점(없으면 NULL) |
| `total_cnt` | `bigint` | ✅ | 결제 오더 총 건수 |
| `refunded_cnt` | `bigint` | ✅ | 환불 건수(`REFUNDED`) |
| `refund_rate` | `decimal(38,6)` | ✅ | `refunded_cnt / total_cnt` |
| `run_id` | `string` | ✅ | 실행 추적 |
| `rule_id` | `string` | ⭕️ | 적용 룰 |

**키(멱등성)**

- `(date_kst, merchant_name)` MERGE

### 4.7 `gold.ops_ledger_pairing_quality_daily` (운영 지표: 원장 페어링 품질)

> (방법2 가정) 결제 관련 엔트리는 서로 다른 `tx_id`를 가지며, `related_id`로 묶어서 페어링한다.

| 컬럼 | 타입(권장) | 필수 | 의미 |
|---|---|---:|---|
| `date_kst` | `date` | ✅ | 대상 일자 |
| `entry_cnt` | `bigint` | ✅ | 원장 엔트리 수 |
| `related_id_null_rate` | `decimal(38,6)` | ✅ | `related_id` NULL 비율 |
| `pair_candidate_rate` | `decimal(38,6)` | ✅ | (예시) `related_id` 그룹 중 “2엔트리+서로 다른 wallet” 비율 |
| `join_payment_orders_rate` | `decimal(38,6)` | ✅ | `related_id`→`payment_orders.order_id` 조인 성공 비율(가능 시) |
| `run_id` | `string` | ✅ | 실행 추적 |
| `rule_id` | `string` | ⭕️ | 적용 룰 |

### 4.8 `gold.exception_ledger` (공통 예외 원장)

| 컬럼 | 타입(권장) | 필수 | 의미 |
|---|---|---:|---|
| `date_kst` | `date` | ✅ | 대상 일자 |
| `domain` | `string` | ✅ | 예외 도메인(`dq`, `ledger`, `analytics`) |
| `exception_type` | `string` | ✅ | 예외 유형 |
| `severity` | `string` | ✅ | 심각도(`WARN`, `CRITICAL`) |
| `source_table` | `string` | ⭕️ | 발생 소스 테이블 |
| `window_start_ts` | `timestamp` | ⭕️ | 감시/집계 시작 시각 |
| `window_end_ts` | `timestamp` | ⭕️ | 감시/집계 종료 시각 |
| `metric` | `string` | ⭕️ | 예외 판단 메트릭 |
| `metric_value` | `decimal(38,6)` | ⭕️ | 메트릭 값 |
| `message` | `string` | ⭕️ | 상세 payload(JSON 문자열) |
| `run_id` | `string` | ✅ | 실행 추적 |
| `rule_id` | `string` | ⭕️ | 적용 룰 |
| `generated_at` | `timestamp` | ✅ | 예외 생성 시각(UTC) |

**키(멱등성)**

- Pipeline A: append 기록(동일 윈도우/동일 `run_id` 재실행 시 누적 가능)
- Pipeline B: MERGE key = `(date_kst, domain, exception_type, run_id, metric, message)`
  (동일 `run_id` 재실행 허용, 동일 입력 기준 수렴)

### 4.9 `gold.pipeline_state` (파이프라인 상태 SSOT)

| 컬럼 | 타입(권장) | 필수 | 의미 |
|---|---|---:|---|
| `pipeline_name` | `string` | ✅ | 파이프라인 식별자(`pipeline_a/b/c/silver`) |
| `last_success_ts` | `timestamp` | ⭕️ | 마지막 성공 시각(UTC) |
| `last_processed_end` | `timestamp` | ⭕️ | 마지막 성공 실행의 처리 종료 시각(UTC) |
| `last_run_id` | `string` | ⭕️ | 마지막 실행 ID(성공/실패 포함) |
| `dq_zero_window_counts` | `string` | ⭕️ | 소스별 연속 0-window 카운트(JSON 문자열) |
| `updated_at` | `timestamp` | ✅ | 상태 갱신 시각(UTC) |

**키(멱등성)**

- `(pipeline_name)` MERGE

운영 규칙:
- 성공 시 `last_success_ts`, `last_processed_end`, `last_run_id`, `updated_at`를 갱신한다.
- 실패 시 `last_success_ts`, `last_processed_end`는 유지하고 `last_run_id`, `updated_at`만 갱신한다.

### 4.10 `gold.dim_rule_scd2` (룰 SSOT, SCD2)

| 컬럼 | 타입(권장) | 필수 | 의미 |
|---|---|---:|---|
| `rule_id` | `string` | ✅ | 룰 식별자(버전 포함 권장) |
| `domain` | `string` | ✅ | 룰 도메인(`dq`, `silver`, `ledger`) |
| `metric` | `string` | ✅ | 룰 대상 메트릭 |
| `threshold` | `double` | ⭕️ | 기본 임계치 |
| `severity_map` | `map<string,double>` | ⭕️ | 단계별 임계치(`warn/crit/fail`) |
| `allowed_values` | `array<string>` | ⭕️ | 허용값 집합 |
| `comment` | `string` | ⭕️ | 운영 메모 |
| `effective_start_ts` | `timestamp` | ✅ | 효력 시작 시각(UTC) |
| `effective_end_ts` | `timestamp` | ⭕️ | 효력 종료 시각(UTC) |
| `is_current` | `boolean` | ✅ | 현재 룰 여부 |

운영 규칙:
- 동일 `domain+metric`에서 `is_current=true`는 1건만 허용한다.
- `rule_id`는 전 테이블에서 유일해야 한다.
- Pipeline A/B 런타임 룰 SSOT는 `gold.dim_rule_scd2`이며, fallback은 운영 정책으로 제어한다.

---

## 5) 매핑 규칙(예시) — `.ref/database_schema` 기준

### 5.1 `user_wallets` → `silver.wallet_snapshot`

- `snapshot_ts`:
  - 1순위: `source_extracted_at`
  - 2순위: `ingested_at`
- `balance_total = balance + frozen_amount`

### 5.2 `transaction_ledger` → `silver.ledger_entries`

- `event_time = created_at`
- `amount = cast(amount as decimal(38,2))`
- `amount_signed` 파생:
  - 업스트림 제공이 없으면 `entry_type(type)` 기반 룰 매핑으로 파생
  - 룰로 매핑 불가(unknown type) 또는 부호 결정 불가 시 → `silver.bad_records`에 격리
- `related_id`는 문자열 표준
  - `orders.order_id(BIGINT)`는 `cast(order_id as string)`로 맞춘다.

#### 5.2.1 entry_type 매핑 룰 (Mock 데이터 기준)

> 각 거래는 독립된 `tx_id`를 가지며, 관련 거래는 `related_id`로 연결한다.

| entry_type | amount_signed | 설명 | related_id |
|------------|---------------|------|------------|
| CHARGE | +amount | KRW → NSC 충전 | 입금 참조 ID |
| WITHDRAW | -amount | NSC → KRW 환전 | 출금 참조 ID |
| MINT | +amount | 공급 증가 이벤트 | 공급 참조 ID |
| BURN | -amount | 공급 소각 이벤트 | 소각 참조 ID |
| PAYMENT | -amount | 결제 지출 (구매자) | 결제 오더 ID |
| RECEIVE | +amount | 결제 수령 (판매자) | 결제 오더 ID |
| REFUND_OUT | -amount | 환불 지급 (판매자) | 결제 오더 ID |
| REFUND_IN | +amount | 환불 수령 (구매자) | 결제 오더 ID |
| HOLD | 0 | available → frozen | 결제 오더 ID |
| RELEASE | 0 | frozen → available | 결제 오더 ID |

**페어링 규칙:**
- PAYMENT ↔ RECEIVE: 같은 `related_id`(결제 오더 ID)로 연결
- REFUND_OUT ↔ REFUND_IN: 같은 `related_id`로 연결

### 5.3 `orders/payment_orders` → `silver.order_events`

- `order_ref`
  - `orders`: `cast(order_id as string)`
  - `payment_orders`: `order_id`(이미 string)
- `order_source`: 각각 `ORDERS` / `PAYMENT_ORDERS`
- `event_time`: 기본 `created_at` 사용(추후 결제 확정 시각이 있으면 대체 가능)

### 5.4 `silver.order_events` → `gold.fact_payment_anonymized`

- `user_key = sha2(concat(user_id, salt), 256)`
- `category`는 `silver.order_events(order_ref) → silver.order_items → silver.products` 조인으로 파생(가능한 경우)
- PII/credential 컬럼은 포함 금지

### 5.5 `payment_orders` → `gold.ops_payment_failure_daily`

- 집계 기준: `date_kst`(KST day)
- `failed_cnt`의 status 집합은 룰/합의로 고정(예: `FAILED`, `CANCELLED` 등)
- `merchant_name` 단위로도 집계(없으면 NULL)

### 5.6 `payment_orders` → `gold.ops_payment_refund_daily`

- 집계 기준: `date_kst`(KST day)
- `refunded_cnt`는 `status = REFUNDED`만 집계한다.
- 환불율은 실패율과 분리 산출한다(`gold.ops_payment_failure_daily`와 독립).

### 5.7 `silver.ledger_entries` → `gold.ops_ledger_pairing_quality_daily`

- 집계 기준: `date_kst`(= `event_date_kst`)
- `related_id` 그룹핑 기반 품질 지표 산출
  - `related_id_null_rate`: NULL 비율
  - `pair_candidate_rate`: “그룹 크기 2 + 서로 다른 wallet_id” 비율(예시)
  - `join_payment_orders_rate`: `related_id`→`payment_orders.order_id` 조인 성공 비율(가능 시)

### 5.8 `silver.ledger_entries` → `gold.admin_tx_search`

- 목적: tx_id 단건 조회를 위한 “배치 인덱스(감사/분석용)”
- `paired_tx_id`는 `related_id` 그룹핑으로 추정 가능(정답 보장은 아님)

---

## 6) 격리/Fail-fast(Quarantine)

- Silver 계약 위반 레코드는 `silver.bad_records`로 격리한다.
- `silver.bad_records`는 단일 통합 테이블로 운영하며, 계약 컬럼은
  `detected_date_kst`, `source_table`, `reason`, `record_json`, `run_id`, `rule_id`, `detected_at`로 고정한다.
- fail-fast 임계치(예: bad_records_rate)는 `gold.dim_rule_scd2`에서 관리한다.
- 예외/알림은 `gold.exception_ledger`에 기록한다(단일 테이블 원칙).

---

## 7) 변경 관리(Change Management)

- 컨트랙트 버전: `vMAJOR.MINOR`
  - MINOR: 컬럼 추가/허용값 추가 등 호환 변화
  - MAJOR: 의미/키/타입 변경 등 비호환 변화
- MAJOR 변경 시 샌드박스 데이터/병행 검증 기간을 운영 정책으로 둔다.
