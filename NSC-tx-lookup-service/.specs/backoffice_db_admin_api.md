# Backoffice DB + Admin API 설계 초안 (Serving Layer) — FR-ADM-02

> **연계 문서**
> - 프로젝트 스펙(Serving): `.specs/backoffice_project_specs.md`
> - 데이터 프로젝트(동기화): `.specs/backoffice_data_project.md`
> - 요구사항(원본/참고): `.specs/requirements/SRS - Software Requirements Specification.md` (FR-ADM-02)
> - 아키텍처 참고(비범위/보조 경로): `.specs/reference/entire_architecture.md`
>
> **목표**: `tx_id` 기반 **초 단위(near real-time)** 거래 추적 조회(관리자 화면/툴)

---

## 1) 범위 / 비범위

### 1.1 범위(In-scope)

- FR-ADM-02: `tx_id`로 조회 시 아래 정보를 반환
  - 시간(`event_time`)
  - 보낸 사람/받는 사람(지갑 ID 기준 `sender_wallet_id`, `receiver_wallet_id`)
  - 상태(`status`: 성공/실패/진행)
- 운영 요구를 위해 “결제 1건”을 구성하는 원장 엔트리(예: PAYMENT/RECEIVE)를 함께 조회 가능

### 1.2 비범위(Out-of-scope)

- 결제/정산의 트랜잭션 처리(Freeze/Settle/Rollback) 자체
- Databricks 산출물(FR-ADM-01/03, 통제 지표 등) 서빙

---

## 2) 핵심 결정(현재 스키마 기준)

### 2.1 `tx_id` 의미

- `transaction_ledger.tx_id`는 PK이므로 Backoffice에서도 **“원장 엔트리(행) ID”**로 취급한다.

### 2.2 PAYMENT/RECEIVE 페어링 방식(방법 2 가정)

동일 결제 건에 대해:

- 구매자 엔트리: `PAYMENT` (−amount)
- 판매자 엔트리: `RECEIVE` (+amount)

두 엔트리는 **서로 다른 `tx_id`**를 가지며, **동일 `related_id`**(권장: `payment_orders.order_id`)로 연결한다.

> 결론: Admin API는 “단건 tx_id 조회”를 받아도, 필요 시 `related_id` 기준으로 반대편 엔트리를 찾아 **보낸 사람/받는 사람을 구성**한다.

---

## 3) 아키텍처 개요

### 3.1 구성 요소

- **OLTP DB(서비스 DB)**: 결제/원장 기록(SSOT)
  - `transaction_ledger`, `payment_orders` 등
- **Event/CDC 파이프라인**: OLTP 변경을 초 단위로 전달
  - 옵션 A) Kafka 이벤트(서비스에서 publish)
  - 옵션 B) DB CDC(Debezium 등) → Kafka
- **Backoffice DB(Serving DB)**: Admin 조회 최적화 RDBMS (PostgreSQL/Azure SQL)
- **Admin API**: 관리자 조회 API(REST), RBAC/감사로그 포함

### 3.2 데이터 흐름(권장)

1) 서비스가 OLTP에 커밋(결제/원장 엔트리 생성/상태 변경)  
2) 변경 이벤트가 Kafka(또는 CDC)로 전달  
3) Backoffice Consumer가 Backoffice DB에 **Upsert(멱등)**  
4) Admin API가 Backoffice DB를 조회하여 응답

---

## 4) Backoffice DB 설계(초안)

> 네이밍은 예시이며, 실제 DB는 별도 DB 또는 스키마 `bo`로 분리 권장.

### 4.1 테이블: `bo.ledger_entries`

원장 엔트리(행) 단위 저장. OLTP `transaction_ledger`를 중심으로 “조회에 필요한 최소 필드”를 정규화한다.

- PK: `tx_id`
- 주요 컬럼(예시)
  - `tx_id` (string, PK)
  - `wallet_id` (string, index)
  - `entry_type` (string)
  - `amount` (decimal)
  - `amount_signed` (decimal, **가능하면 저장**)  
    - 업스트림이 제공하지 않으면 `NULL`로 저장(Consumer 파생은 운영 합의 후 고려) — 결정: `DEC-204` (`.specs/decision_open_items.md`)
  - `related_id` (string, index)
  - `related_type` (string, nullable)
  - `event_time` (timestamp, index)
  - `created_at` (timestamp)
  - `ingested_at` (timestamp)

인덱스(예시)
- `PRIMARY KEY (tx_id)`
- `INDEX (related_id)`
- `INDEX (wallet_id, event_time DESC)`
- `INDEX (event_time DESC)`

### 4.2 테이블: `bo.payment_orders`

OLTP `payment_orders`를 서빙 관점으로 복제.

- PK: `order_id`
- 주요 컬럼(예시)
  - `order_id` (string, PK)
  - `user_id` (string, nullable, index)
  - `merchant_name` (string, index)
  - `amount` (decimal)
  - `status` (string, index)
  - `created_at` (timestamp, index)
  - `updated_at` (timestamp, nullable, 권장)

### 4.3 테이블: `bo.payment_ledger_pairs` (권장: 성능/단순성)

결제 오더(= `payment_orders.order_id`) 기준으로 PAYMENT/RECEIVE 페어링 결과를 저장해, Admin 조회 시 join 1~2번으로 끝나게 한다.

- UK: `(payment_order_id)`
- 주요 컬럼(예시)
  - `payment_order_id` (string, unique)
  - `payment_tx_id` (string, nullable, index)
  - `receive_tx_id` (string, nullable, index)
  - `payer_wallet_id` (string, nullable)
  - `payee_wallet_id` (string, nullable)
  - `amount` (decimal, nullable)
  - `status` (string, nullable)  — `payment_orders.status`를 denormalize 가능
  - `event_time` (timestamp, nullable) — 페어의 대표 시각(정의 필요)
  - `updated_at` (timestamp)

운영 규칙(권장)
- 엔트리 도착 순서가 뒤바뀔 수 있으므로, pair row는 **부분적으로** 채워진 상태를 허용(둘 중 하나만 존재)
- 일정 시간 내 페어 완성 실패 시 “pair_incomplete” 상태로 관측/알림(데이터 품질 지표)

### 4.4 뷰(또는 테이블): `bo.admin_tx_search` (옵션)

`ledger_entries` + `payment_orders` + `payment_ledger_pairs`를 조인하여, Admin API가 바로 쓰기 좋은 형태로 제공.

- 키: `tx_id`
- 포함 필드(예시)
  - `tx_id`, `event_time`, `entry_type`, `amount`, `amount_signed`
  - `related_id(payment_order_id)`
  - `sender_wallet_id`, `receiver_wallet_id` (페어링 결과로 파생)
  - `payment_status`, `merchant_name`
  - `paired_tx_id`

---

## 5) Admin API 설계(초안)

### 5.1 인증/권한(최소)

- 사내 SSO(OIDC) 또는 JWT 기반 인증
- Role 기반 접근 제어(RBAC): `ADMIN_READ`, `ADMIN_AUDIT` 등
- 조회 요청(성공/404 포함)은 감사로그로 남긴다(누가/언제/어떤 키로 조회했는지)
- 인증/인가 실패(401/403)는 라우트 진입 전 차단될 수 있으므로 인증 계층 로그로 추적한다

### 5.2 엔드포인트

#### (필수) `GET /admin/tx/{tx_id}`

목적: FR-ADM-02 단건 조회.

응답(예시)
```json
{
  "tx_id": "tx-001",
  "event_time": "2026-02-04T03:12:45Z",
  "entry_type": "PAYMENT",
  "amount": "10000.00",
  "amount_signed": "-10000.00",
  "status": "SETTLED",
  "status_group": "SUCCESS",
  "sender_wallet_id": "A",
  "receiver_wallet_id": "B",
  "related": {
    "related_id": "po-001",
    "related_type": "PAYMENT_ORDER"
  },
  "paired_tx_id": "tx-002",
  "merchant_name": "MERCHANT_1",
  "pairing_status": "COMPLETE",
  "data_lag_sec": 3
}
```

동작 규칙(권장)
- 먼저 `bo.ledger_entries(tx_id)` 조회
- `related_id`가 있고 `related_type=PAYMENT_ORDER`이면:
  - `bo.payment_ledger_pairs`에서 페어링 조회(있으면 사용)
  - 없으면 `bo.ledger_entries`에서 같은 `related_id`를 가진 반대편 엔트리 탐색(백업 경로)

응답 정책(결정)
- `tx_id` 미존재: **404**
- 페어링 불완전: **200 + `pairing_status=INCOMPLETE`**, `paired_tx_id`/`receiver_wallet_id`는 NULL 허용
- `related_id` 자체가 없으면: `pairing_status=UNKNOWN`
- `status`는 **원문값 유지**, `status_group`는 v1 매핑표로 계산하고 매핑이 없으면 `UNKNOWN` — 결정: `DEC-206` (`.specs/decision_open_items.md`)
- `data_lag_sec`는 `now - max(ingested_at, event_time)`로 계산(가용한 값 기준)

#### (권장) `GET /admin/payment-orders/{order_id}`

목적: 결제 오더 기준으로 원장 페어링/상태를 한 번에 조회(운영 편의).

#### (권장) `GET /admin/wallets/{wallet_id}/tx?from=&to=&limit=`

목적: 특정 지갑의 최근 거래 리스트(지원 여부는 운영 요구에 따라).

### 5.3 성능/SLO(초안)

- `GET /admin/tx/{tx_id}`: p95 200ms(내부망), p99 500ms 목표(조정 가능)
- 데이터 최신성: OLTP 커밋 이후 Backoffice 반영까지 p95 5초(이벤트 파이프라인 SLO)

---

## 6) 데이터 동기화/멱등성

### 6.1 Upsert 키

- `bo.ledger_entries`: `tx_id` 기반 upsert
- `bo.payment_orders`: `order_id` 기반 upsert
- `bo.payment_ledger_pairs`: `payment_order_id` 기반 upsert

### 6.2 중복/순서 뒤바뀜 대응

- 동일 이벤트 재전송(At-least-once)을 허용하고, DB upsert로 수렴시킨다.
- 페어링은 “부분 완성”을 허용하고, 두 엔트리 도착 시 최종 완성한다.

---

## 7) 운영 지표(Serving Layer 자체)

Serving 계층도 관측이 필요하다(데이터브릭스 통제 지표와 별개).

- API: QPS, p95/p99 latency, error rate
- DB: connection pool, slow query, replication lag
- 동기화: consumer lag, event 처리 지연
- 페어링 품질: pair_incomplete 비율, 평균 완성 시간

---

## 8) 오픈 이슈(결정 필요하지만 개발은 진행 가능)

- `status`의 SSOT와 상태 전이 시각(`updated_at/settled_at/failed_at`) 표준화
- `amount_signed`의 SSOT(업스트림 저장 vs 룰 기반 파생) 및 unknown type 처리
- `related_id`가 여러 도메인을 가리킬 경우 `related_type` 제공 여부
- 환불/취소/정정(역분개) 엔트리 타입 정의 및 페어링 규칙 확장

---

## 9) 클라우드 개발 단계(테스트 -> 운영형)

### 9.1 Cloud-Test 단계(폐기형)

- 목적: FR-ADM-02 조회 경로(API -> Backoffice DB -> Consumer)를 빠르게 연결 검증
- 환경: 퍼블릭 엔드포인트 허용, Event Hubs(Kafka) + Container Apps
- 검증: synthetic 이벤트로 `publish -> consume -> upsert -> API(200/404)` 스모크 확인

### 9.2 Cloud-Secure 단계(운영형)

- 목적: 보안 네트워크/권한 정책이 적용된 최종 아키텍처에서 동일 기능 재검증
- 환경: 보안용 별도 리소스(인플레이스 전환 금지), Key Vault + Managed Identity
- 검증: 마이그레이션/백필/증분 동기화 재실행 후 동일 스모크 재확인

### 9.3 전환 원칙

- 테스트 리소스와 운영형 리소스는 분리 생성하고, 검증 완료 후 컷오버한다.
- Backoffice DB는 파생 저장소이므로 전환 시 재적재(backfill + sync)를 기본 경로로 한다.
