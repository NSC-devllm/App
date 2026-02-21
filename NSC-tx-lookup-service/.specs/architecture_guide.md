# Architecture Guide — tx-lookup-service

팀원 온보딩 및 코드베이스 이해를 위한 아키텍처 가이드.

---

## 1. 전체 구조 한눈에 보기

### 서비스 범위

```
OLTP DB → Kafka/Event Hubs → [Sync Consumer] → Backoffice DB → [Admin API] → Admin UI
                               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                               우리 서비스 범위 (consumer-only, producer 아님)
```

- **In-scope**: Kafka 이벤트 수신 → Backoffice DB 동기화 → Admin 조회 API 제공
- **Out-of-scope**: 이벤트 발행(업스트림 서비스 소유), Kafka 인프라(인프라팀 소유), 고객-facing API

### 컴포넌트 2개 요약

| 컴포넌트 | 한 줄 요약 | 실행 방식 |
|----------|-----------|----------|
| **Admin API** | tx_id/order_id/wallet_id 기반 관리자 조회 | FastAPI, `uvicorn src.api.main:app` |
| **Sync Consumer** | Kafka 이벤트 → Backoffice DB 멱등 upsert | CLI, `python -m src.consumer.main consume` |

두 컴포넌트는 **Backoffice DB를 공유**하되, 독립된 프로세스(컨테이너)로 실행된다.

### 코드 구조

```
src/
├── api/         ← Admin API (FastAPI 엔드포인트, 인증, 감사, 스키마)
├── consumer/    ← Kafka Consumer (이벤트 파싱, 처리, 페어링, DLQ)
├── db/          ← DB 레이어 (모델, 쿼리, 멱등 upsert)
└── common/      ← 공통 인프라 (설정, 로깅, 메트릭, OTel)
```

**분리 원칙**: `api/`와 `consumer/`는 서로를 import하지 않는다. 둘 다 `db/`와 `common/`만 의존한다.

---

## 2. DB 모델 — 먼저 이해해야 할 것

모든 코드가 이 5개 테이블을 중심으로 동작한다. 스키마: `bo`.

| 테이블 | PK | 역할 | 누가 쓰는가 |
|--------|------|------|------------|
| `bo.ledger_entries` | `tx_id` | 원장 엔트리 파생 저장소 | Consumer |
| `bo.payment_orders` | `order_id` | 결제 오더 파생 저장소 | Consumer |
| `bo.payment_ledger_pairs` | `payment_order_id` | PAYMENT/RECEIVE 페어링 상태 | Consumer |
| `bo.admin_audit_logs` | `audit_id` (auto) | 관리자 조회 감사 기록 | API |
| `bo.consumer_dlq_events` | `dlq_id` (auto) | Consumer 실패 이벤트 DLQ | Consumer |

### 핵심 관계

```
ledger_entries.related_id ──→ payment_orders.order_id
                           └→ payment_ledger_pairs.payment_order_id
```

- `related_id`가 페어링의 핵심 조인 키다
- 하나의 `order_id`에 `PAYMENT` 엔트리 1건 + `RECEIVE` 엔트리 1건이 매칭되면 "페어링 완성"

### 인덱스 전략

조회 패턴에 맞춘 인덱스:
- `ledger_entries`: `related_id`, `(wallet_id, event_time)`, `event_time`
- `payment_orders`: `user_id`, `merchant_name`, `status`, `created_at`
- `admin_audit_logs`: `requested_at`, `resource_id`, `actor_id`, `(resource_id, requested_at)`

---

## 3. Consumer 상세 — 이벤트가 DB에 들어가는 경로

### 이벤트 계약

2개 토픽을 구독한다:

| 토픽 | 이벤트 타입 | 필수 필드 |
|------|-----------|----------|
| `ledger.entry.upserted` | `LedgerEntryUpserted` | `tx_id`, `wallet_id`, `entry_type`, `amount`, `event_time` |
| `payment.order.upserted` | `PaymentOrderUpserted` | `order_id`, `amount`, `status`, `created_at` |

**업스트림 결합**: JSON 스키마에만 결합. 업스트림이 Flink/Debezium/직접 발행, 뭘 쓰든 같은 JSON 구조로 같은 토픽에 보내면 영향 없음.

### 처리 흐름

```
Kafka 메시지 수신
  ↓
events.py: JSON → 이벤트 객체 (LedgerEntryUpserted / PaymentOrderUpserted)
  - 필수 필드 검증, 타입 변환
  - 실패 시 → EventValidationError
  ↓
processor.py: 이벤트 → DB upsert 오케스트레이션
  - upsert_ledger_entry() → latest_wins_upsert → bo.ledger_entries
  - upsert_payment_order() → latest_wins_upsert → bo.payment_orders
  ↓
pairing.py: (ledger만) 페어링 계산
  - related_type이 PAYMENT_ORDER 또는 NULL일 때만 실행
  - 그 외의 related_type → skip (메트릭 기록)
  ↓
main.py: commit + 메트릭 기록
  - 성공 → consumer.commit(msg), consumer_messages_total{status=success}
  - 실패 → DLQ 저장 + commit (poison pill 방지)
```

### 멱등 upsert (latest-wins)

`upsert.py`의 `latest_wins_upsert`는 PostgreSQL `ON CONFLICT DO UPDATE`로 멱등성을 보장한다.

```
우선순위 3단계 (위에서 먼저 적용):

1. updated_at 비교: incoming.updated_at >= existing.updated_at → 업데이트
2. source_version 비교: updated_at 없고, incoming.version >= existing.version → 업데이트
3. Fallback (ingested_at): 양쪽 모두 updated_at과 version이 NULL일 때만
   incoming.ingested_at >= existing.ingested_at → 업데이트
```

**안전장치**: metadata 있는 기존 레코드를 metadata 없는 후행 이벤트가 덮어쓰지 못한다 (DEC-210).

### 페어링 로직

```
related_id로 ledger_entries 조회
  ↓
PAYMENT 엔트리 + RECEIVE 엔트리 → PairingSnapshot
  - 둘 다 있으면 complete = true
  - 하나만 있으면 complete = false + incomplete_age_sec 계산
  ↓
payment_orders에서 amount/status 보강
  ↓
should_update_pair() 체크
  - 기존 complete → 새로운 incomplete: 업데이트 차단 (회귀 방지, DEC-004)
  - 그 외: 업데이트 허용
  ↓
bo.payment_ledger_pairs에 upsert
```

### DLQ 전략

```
이벤트 처리 실패 시:
  1. DLQ에 저장 (DB 또는 JSONL 파일)
  2. 원본 메시지 commit (poison pill 반복 방지)
  3. 에러 메트릭 기록

DLQ 저장소:
  DLQ_BACKEND=db   → bo.consumer_dlq_events (운영용)
  DLQ_BACKEND=file → JSONL 파일 (로컬 테스트용)

재처리: scripts/replay_dlq.py (수동, 선택/범위 기반)
보관기간: 기본 14일, 자동 정리 (DLQ_RETENTION_DAYS)
```

### Consumer CLI

```bash
python -m src.consumer.main consume              # 실시간 소비
python -m src.consumer.main consume --max-messages 100   # N건 처리 후 종료
python -m src.consumer.main backfill --ledger-file data.jsonl  # JSONL 파일에서 백필
```

---

## 4. Admin API 상세 — 데이터를 꺼내서 응답하는 경로

### 엔드포인트 3개

| 메서드 | 경로 | 동작 | 응답 |
|--------|------|------|------|
| `GET` | `/admin/tx/{tx_id}` | 거래 단건 조회 | 200 / 404 |
| `GET` | `/admin/payment-orders/{order_id}` | 주문 기준 조회 | 200 / 404 |
| `GET` | `/admin/wallets/{wallet_id}/tx?from=&to=&limit=` | 지갑 거래 목록 | 200 (빈 목록이어도 200) |

### 조회 흐름 — `GET /admin/tx/{tx_id}` 기준

```
main.py: 라우트 진입
  ↓
auth.py: 인증/인가 (AUTH_MODE=oidc일 때)
  - JWT 검증 → 역할 체크 (ADMIN_READ 또는 ADMIN_AUDIT)
  - 실패 → 401/403 (라우트 진입 전 차단, DB 감사로그 없음)
  ↓
db/admin_tx.py: fetch_admin_tx_context()
  - ledger_entries LEFT JOIN payment_orders LEFT JOIN payment_ledger_pairs
  - 필요 시 peer 조회 (반대 entry_type 필터 + event_time DESC 정렬)
  ↓
service.py: build_admin_tx_response()
  - _resolve_pairing() → pairing_status(COMPLETE/INCOMPLETE/UNKNOWN)
  - _resolve_status_group() → status_group(SUCCESS/FAIL/IN_PROGRESS/UNKNOWN)
  - _compute_data_lag_sec() → now - max(ingested_at, event_time)
  ↓
audit.py: build_audit_fields() → bo.admin_audit_logs 기록
  - result_count: FOUND=1, NOT_FOUND=0
```

### pairing_status 판정 규칙

| 조건 | 결과 |
|------|------|
| `related_id` 없거나 `related_type != PAYMENT_ORDER` | `UNKNOWN` |
| PAYMENT + RECEIVE 둘 다 존재 | `COMPLETE` |
| 하나만 존재 | `INCOMPLETE` |

### status_group 매핑 (v1)

```
SUCCESS     = SETTLED, COMPLETED, SUCCESS, SUCCEEDED, PAID
FAIL        = FAILED, CANCELLED, CANCELED, REJECTED, DECLINED
IN_PROGRESS = CREATED, PENDING, PROCESSING, AUTHORIZED
UNKNOWN     = 그 외 또는 NULL
```

대소문자 무시. 매핑표는 `src/api/service.py`의 `_resolve_status_group()`에 있다.

### 인증/인가

```
AUTH_MODE=disabled → 로컬 개발용 (인증 없음)
AUTH_MODE=oidc     → JWT 검증 + 역할 체크

역할: ADMIN_READ 또는 ADMIN_AUDIT (둘 중 하나만 있으면 접근 허용)
actor_id: oid 우선, 없으면 sub (Entra ID 기준)
```

---

## 5. 공통 인프라

### 설정 (`common/config.py`)

모든 런타임 설정은 환경변수로 주입. 샘플: `configs/env.example`.

| 카테고리 | 주요 변수 |
|---------|----------|
| DB | `DATABASE_URL`, `DB_POOL_SIZE`, `DB_SLOW_QUERY_MS` |
| Kafka | `KAFKA_BOOTSTRAP_SERVERS`, `LEDGER_TOPIC`, `PAYMENT_ORDER_TOPIC`, `CONSUMER_GROUP_ID` |
| 인증 | `AUTH_MODE`, `AUTH_ISSUER`, `AUTH_JWKS_URL`, `AUTH_AUDIENCE` |
| DLQ | `DLQ_BACKEND`, `DLQ_PATH`, `DLQ_RETENTION_DAYS` |
| 관측 | `APPLICATIONINSIGHTS_CONNECTION_STRING` |

### 관측성

```
OpenTelemetry → Azure Monitor (Application Insights)
  - APPLICATIONINSIGHTS_CONNECTION_STRING이 비어 있으면 OTel exporter 비활성화
  - 상관관계 헤더: X-Correlation-ID (API 응답 / Consumer Kafka 헤더 / 로그)
```

대표 메트릭:

| 영역 | 메트릭 |
|------|--------|
| API | `api_request_latency_seconds`, `api_requests_total`, `api_requests_inflight` |
| Consumer | `consumer_messages_total`, `consumer_event_lag_seconds`, `consumer_kafka_lag`, `consumer_freshness_seconds`, `consumer_dlq_total` |
| Pairing | `pairing_total`, `pairing_incomplete_total`, `pairing_incomplete_age_seconds`, `pairing_skipped_non_payment_order_total` |
| DB | `db_query_latency_seconds`, `db_pool_size`, `db_pool_checked_out`, `db_pool_checkout_latency_seconds`, `db_replication_lag_seconds` |

### 알림 규칙 (`docker/observability/alert_rules.yml`)

| 규칙 | 심각도 | 조건 |
|------|--------|------|
| `ApiLatencyHigh` | WARNING | p95 > 200ms / 5m |
| `ApiErrorRateHigh` | WARNING | 4xx+5xx > 2% / 5m |
| `DataFreshnessHigh` | CRITICAL | freshness > 5s / 5m |
| `DlqActivity` | WARNING | DLQ 건수 > 0 / 5m |
| `DbPoolExhausted` | WARNING | pool 고갈 |
| `DbReplicationLagHigh` | CRITICAL | lag > 10s / 5m |

---

## 6. 실제 실행 흐름 예시

### 시나리오: 결제 이벤트 처리 → 관리자 조회

#### 1단계 — Consumer가 이벤트를 처리한다

Kafka에서 두 개의 메시지가 도착한다:

```json
// 토픽: payment.order.upserted
{"order_id": "ORD-001", "amount": 50000, "status": "SETTLED", "created_at": "2026-02-13T10:00:00Z"}

// 토픽: ledger.entry.upserted
{"tx_id": "TX-P-001", "wallet_id": "W-BUYER", "entry_type": "PAYMENT", "amount": 50000,
 "related_id": "ORD-001", "related_type": "PAYMENT_ORDER", "event_time": "2026-02-13T10:00:01Z"}
```

처리 순서:
1. `payment.order.upserted` → `upsert_payment_order()` → `bo.payment_orders`에 ORD-001 저장
2. `ledger.entry.upserted` → `upsert_ledger_entry()` → `bo.ledger_entries`에 TX-P-001 저장
3. `related_type=PAYMENT_ORDER` → 페어링 트리거 → `bo.payment_ledger_pairs`에 `{payment_tx_id: TX-P-001, receive_tx_id: NULL}` → `complete = false`

#### 2단계 — 두 번째 원장 이벤트 (RECEIVE)

```json
// 토픽: ledger.entry.upserted
{"tx_id": "TX-R-001", "wallet_id": "W-SELLER", "entry_type": "RECEIVE", "amount": 50000,
 "related_id": "ORD-001", "related_type": "PAYMENT_ORDER", "event_time": "2026-02-13T10:00:02Z"}
```

1. `bo.ledger_entries`에 TX-R-001 저장
2. 페어링 재계산 → `{payment_tx_id: TX-P-001, receive_tx_id: TX-R-001}` → `complete = true`
3. `should_update_pair(false, true)` → 업데이트 허용 → `bo.payment_ledger_pairs` 갱신

#### 3단계 — 관리자가 조회한다

```
GET /admin/tx/TX-P-001
```

1. `fetch_admin_tx_context("TX-P-001")`:
   - `ledger_entries` JOIN `payment_orders`(ORD-001) JOIN `payment_ledger_pairs`(ORD-001)
2. `build_admin_tx_response()`:
   - `pairing_status = COMPLETE` (PAYMENT + RECEIVE 모두 존재)
   - `paired_tx_id = TX-R-001` (현재가 PAYMENT이므로 반대편 RECEIVE의 tx_id)
   - `status_group = SUCCESS` (SETTLED → SUCCESS 매핑)
   - `data_lag_sec = now - max(ingested_at, event_time)`
3. 감사로그: `bo.admin_audit_logs`에 `{action: "GET", resource_id: "TX-P-001", result: "FOUND", result_count: 1}` 기록

### 장애 시나리오

**이벤트 파싱 실패**: `tx_id` 누락된 메시지 도착 → `EventValidationError` → DLQ에 원본 저장 + Kafka commit + `consumer_dlq_total` 메트릭 증가 → 다음 메시지로 진행.

**중복 이벤트 도착**: 동일 `tx_id`로 2번 도착 → `latest_wins_upsert`가 `updated_at`/`version` 비교 → 동일하거나 과거 버전이면 업데이트 무시 → 멱등하게 수렴.

**페어링 역전 방지**: RECEIVE가 먼저 처리된 후 일부 데이터만 가진 이벤트(PAYMENT만)가 재처리 → `should_update_pair(true, false) → false` → 업데이트 차단 → COMPLETE 상태 보존.

---

## 부록: 모듈 역할 요약

### `src/api/`

| 파일 | 역할 |
|------|------|
| `main.py` | FastAPI 앱 + 3개 엔드포인트 라우팅 + 미들웨어(CORS, 상관관계ID, 메트릭) |
| `service.py` | 비즈니스 로직 (페어링 해석, status_group 매핑, data_lag 계산) |
| `schemas.py` | 요청/응답 Pydantic 스키마 (7개 모델) |
| `auth.py` | OIDC JWT 검증 + RBAC (JWKS 캐싱, 역할 추출) |
| `audit.py` | 감사 필드 조립 (who/when/what/result) |
| `observability.py` | API 전용 메트릭 + 미들웨어 |

### `src/consumer/`

| 파일 | 역할 |
|------|------|
| `main.py` | Kafka consumer 루프 + CLI (consume / backfill) |
| `events.py` | JSON → 이벤트 객체 변환 + 필수 필드 검증 |
| `processor.py` | 이벤트 → DB upsert 오케스트레이션 + 페어링 트리거 |
| `pairing.py` | PAYMENT/RECEIVE 매칭 + 회귀 방지 |
| `dlq.py` | DLQ 저장 (DB/파일) + 보관기간 정리 |
| `metrics.py` | Consumer 전용 메트릭 (freshness, lag, version_missing) |

### `src/db/`

| 파일 | 역할 |
|------|------|
| `models.py` | SQLAlchemy 모델 5개 (스키마 `bo`) |
| `upsert.py` | `latest_wins_upsert` — PostgreSQL ON CONFLICT + 3단계 우선순위 |
| `admin_tx.py` | Admin API 조회 쿼리 3개 (tx/order/wallet) |
| `session.py` | DB 엔진/세션/풀 관리 + checkout latency 계측 |
| `observability.py` | DB 메트릭 (slow query, pool 상태, replication lag) |

### `src/common/`

| 파일 | 역할 |
|------|------|
| `config.py` | 환경변수 → `Settings` dataclass |
| `kafka.py` | Kafka 클라이언트 공통 설정 (SASL/SSL 포함) |
| `logging.py` | 구조화된 로깅 설정 |
| `metrics.py` | OTel Meter + 전역 메트릭 정의 |
| `observability.py` | Correlation ID 추출/전파 |
| `otel.py` | Azure Monitor OpenTelemetry 초기화 |
