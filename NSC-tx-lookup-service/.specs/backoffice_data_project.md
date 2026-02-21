# Backoffice Data Project (Sync) 스펙 초안 (v0.1)

> **목적**: OLTP에서 생성/변경되는 `payment_orders`, `transaction_ledger`를 **초 단위**로 Backoffice DB(Serving DB)에 동기화하여 Admin API(FR-ADM-02)가 안정적으로 조회할 수 있게 한다.
>
> **연계 문서**
> - Backoffice DB + Admin API: `.specs/backoffice_db_admin_api.md`
> - 아키텍처 참고(비범위/보조 경로): `.specs/reference/entire_architecture.md`

---

## 1) 범위

### 1.1 In-scope

- OLTP → Backoffice DB **증분 동기화**
  - `transaction_ledger` 엔트리(원장 엔트리/행)
  - `payment_orders` 결제 오더
- 멱등 upsert / 재처리 / backfill(초기 적재)
- 동기화 지연/누락/중복에 대한 관측 지표 및 알림

### 1.2 Out-of-scope

- 결제/정산 트랜잭션 처리(Freeze/Settle/Rollback)
- Admin API 자체(별도 스펙에서 다룸)

---

## 2) 도메인 가정(현재 스키마 기반, 협의 전 “기본값”)

### 2.1 tx_id / 페어링(방법 2)

- `transaction_ledger.tx_id`는 PK → **원장 엔트리(행) ID**
- 결제 1건의 더블엔트리(PAYMENT/RECEIVE)는 **서로 다른 tx_id**, 동일 `related_id`로 연결
  - 권장: `related_id = payment_orders.order_id`

### 2.2 amount_signed

- 목표: Serving DB에 `amount_signed`를 **가능하면 저장**
  - 업스트림이 제공하면 SSOT로 저장
  - 미제공 시 `NULL`로 저장(Consumer 파생은 운영 합의 후 고려) — 결정: `DEC-204` (`.specs/decision_open_items.md`)

---

## 3) 데이터 동기화 방식(선택지)

### 옵션 A) 서비스 이벤트 → Kafka(권장)

- 서비스가 OLTP 커밋 성공 후 이벤트 발행
- Consumer가 Backoffice DB에 upsert

장점: 초저지연, 구현 단순(SSOT는 서비스)  
단점: 이벤트 스키마/발행 보장(트랜잭션 아웃박스 등) 필요

### 옵션 B) OLTP CDC → Kafka(Debezium 등)

- OLTP 변경 로그 기반으로 테이블 변경을 이벤트화
- Consumer가 Backoffice DB에 upsert

장점: 서비스 변경 최소화  
단점: CDC 운영 복잡도(스키마 변경/DDL 처리) 증가

### 옵션 C) 폴링/배치(비권장: “초 단위” 요구와 충돌)

**결정(현 시점)**  
- 동기화 방식: **옵션 A) 서비스 이벤트**
- 토픽 전략: **기존 Kafka 토픽 “얹어가기” 우선**, 필드 부족 시 이벤트 스펙 보강/신규 토픽 합의

---

## 4) 이벤트/레코드 모델(권장)

> 이벤트는 **At-least-once**를 전제로 하고, Consumer는 **멱등 upsert**로 수렴시킨다.

### 4.1 `LedgerEntryUpserted`

- key: `tx_id`
- payload(예시)
  - `tx_id`, `wallet_id`, `entry_type`, `amount`, `amount_signed?`
  - `related_id?`, `related_type?`
  - `event_time`, `source_created_at`
  - `occurred_at`(이벤트 발행 시각), `schema_version`

### 4.2 `PaymentOrderUpserted`

- key: `order_id`
- payload(예시)
  - `order_id`, `user_id?`, `merchant_name?`, `amount`, `status`
  - `created_at`, `updated_at?`
  - `occurred_at`, `schema_version`

### 4.3 (선택) `PaymentLedgerPaired`

> 페어링을 서비스에서 확정해 줄 수 있으면(정확도/단순성↑) 이벤트로 주는 편이 가장 좋다.

- key: `payment_order_id`
- payload: `payment_tx_id`, `receive_tx_id`, `payer_wallet_id`, `payee_wallet_id`, `amount`, `status`, `updated_at`

### 4.4 통합 테스트용 “필수 토픽/필드” 체크리스트

> **목적**: "얹어가기(기존 Kafka 토픽 consume)"가 가능한지 빠르게 판단하고,
> 통합 테스트 시 최소 메시지 요건을 팀에 요청하기 위함.
>
> **이벤트 발행 책임**: 이벤트 발행(프로듀서 코드)은 업스트림 서비스가 담당한다(DEC-112).
> 이 체크리스트는 업스트림 팀에 전달하는 이벤트 계약이다.
> 상세: `configs/topic_checklist.md`

**필수 토픽(2개)**

1) **원장 엔트리 토픽** (예: `ledger.entry.upserted`)
- 목적: `bo.ledger_entries` upsert
- **필수 필드**
  - `tx_id` (PK)
  - `wallet_id`
  - `entry_type` (PAYMENT/RECEIVE 등)
  - `amount`
  - `related_id` (권장: `payment_orders.order_id`)
  - `event_time` 또는 `source_created_at`
  - `updated_at` 또는 `version` (**최신판 판단용**)
- **선택 필드**
  - `amount_signed` (있으면 저장, 없으면 파생)
  - `related_type`, `schema_version`, `occurred_at`

2) **결제 오더 토픽** (예: `payment.order.upserted`)
- 목적: `bo.payment_orders` upsert
- **필수 필드**
  - `order_id` (PK)
  - `amount`
  - `status`
  - `created_at`
  - `updated_at` 또는 `version` (**최신판 판단용**)
- **선택 필드**
  - `user_id`, `merchant_name`
  - `schema_version`, `occurred_at`

**선택 토픽(1개)**

3) **페어링 확정 토픽** (예: `payment.ledger.paired`)
- 목적: `bo.payment_ledger_pairs`를 서비스 기준으로 확정/가속
- **필수 필드**
  - `payment_order_id`
  - `payment_tx_id`, `receive_tx_id`
  - `payer_wallet_id`, `payee_wallet_id`
  - `amount`, `updated_at`

> **통합 테스트 요청 시 체크**  
> - 샘플 메시지 1~2건(정상/실패 케이스)  
> - out-of-order(늦게 온 이벤트) 케이스 1건  
> - 중복 메시지 재전송 케이스 1건

---

## 5) Consumer(동기화 서비스) 설계

### 5.1 책임

- Kafka(또는 CDC)에서 이벤트 consume
- Backoffice DB에 upsert
- DLQ(Dead Letter Queue)로 실패 이벤트 격리
- 처리 지연/오류율/레코드 커버리지 관측

### 5.2 멱등/버전 처리

필수 정책 중 하나를 선택:

1) **단순 upsert + “최신 이벤트만 적용”**  
   - 이벤트에 `updated_at` 또는 `version` 필드 필요
2) **단순 upsert + 마지막 write wins**  
   - out-of-order 이벤트에서 역전 가능(권장하지 않음)

> `payment_orders.updated_at` 컬럼은 존재한다. 다만 초 단위 운영 안정성을 위해 업스트림 이벤트에서 `updated_at` 또는 `version` 제공률을 보장하는 방식을 권장한다.

### 5.3 페어링 처리(방법 2 가정)

페어링을 Backoffice DB에서 계산하는 경우:

- 입력: `bo.ledger_entries`의 `related_id`
- 규칙(예시)
  - `related_id` 그룹 내 `PAYMENT` 1개 + `RECEIVE` 1개가 존재하면 페어 완성
  - 둘 중 하나가 없으면 “부분 완성” 상태 유지
- 산출: `bo.payment_ledger_pairs` upsert

### 5.4 구현 방식(결정)

- **커스텀 Consumer 서비스(Python)**로 구현
  - 이유: 페어링 로직/멱등 처리/오류 격리(DLQ) 등 **업무 로직**이 필요함
  - Kafka Connect는 단순 sink에는 유리하지만, **도메인 로직/지표 산출**에 한계

### 5.5 운영 복잡도 증가 포인트 & 가드레일

**복잡도 증가 포인트**
- 오프셋/재처리/재시도(backoff) 정책을 직접 운영
- 중복/역순(out-of-order) 이벤트에 대한 멱등 처리
- 스키마 변경/버저닝 대응
- consumer lag, freshness, DLQ 등 운영 지표 모니터링

**가드레일(최소)**
- `updated_at` 또는 `version` 기준 “최신 이벤트만 적용”
- DLQ + 재처리(runbook) 정의
- 이벤트 스키마 버전 관리 + 샘플 메시지 계약 테스트
- 핵심 지표 알림: lag / freshness / error rate

### 5.6 기본 동작(자체 결정)

- DB upsert는 **키 기준 멱등**으로 처리(`tx_id`, `order_id`)
- `updated_at/version`이 **없을 경우**:
  - **ingested_at 기준 last-write-wins**로 수렴
  - `version_missing_cnt`(메트릭)로 관측하고 경보 기준에 포함
- 모든 레코드에 `ingested_at` 기록(운영 신선도 계산용)

---

## 6) Backoffice DB(Serving DB) 모델(요약)

상세는 `.specs/backoffice_db_admin_api.md` 참고.

- `bo.ledger_entries` (PK: `tx_id`)
- `bo.payment_orders` (PK: `order_id`)
- `bo.payment_ledger_pairs` (UK: `payment_order_id`) — 권장

---

## 7) 운영 지표(필수)

### 7.1 동기화/지연

- consumer lag(토픽/파티션)
- end-to-end freshness: `now - max(occurred_at)` / `now - max(event_time)` (본 프로젝트 기준: ledger=`event_time`, order=`updated_at||created_at`) — 결정: `DEC-205` (`.specs/decision_open_items.md`)
- 처리량(QPS), error rate, DLQ rate

### 7.2 데이터 커버리지/품질

- `ledger_entries` upsert 성공률
- `payment_orders` upsert 성공률
- `related_id` null rate
- 페어링 품질
  - pair complete rate
  - pair incomplete age(p95)

---

## 8) 보안/컴플라이언스(최소)

- Backoffice DB 접근은 Admin API/Consumer만 허용(네트워크+계정 분리)
- 민감 필드 최소화(필요시 `user_id`는 해시/가명 처리 옵션)
- 감사로그: Admin API 조회 + 운영자 접근(SSH/DB) 이력 관리

---

## 9) 단계별 구현 계획(개정)

### 9.1 기능 구현 단계

1) **초기 적재(backfill)**: 최근 N일 `payment_orders`, `transaction_ledger` 덤프 → Backoffice DB 적재
2) **증분 동기화**: Kafka 이벤트 or CDC 도입, 멱등 upsert
3) **페어링 테이블 도입**: `bo.payment_ledger_pairs` + 지표/알림
4) **SLO 강화**: out-of-order/중복/재처리 정책 확정, 회복 자동화

### 9.2 클라우드 승격 단계

1) **Cloud-Test(폐기형)**: 퍼블릭 허용 테스트 리소스에서 synthetic 이벤트로 E2E 검증
2) **Cloud-Secure(운영형)**: 보안 네트워크/권한 정책 적용 리소스에서 동일 파이프라인 재검증
3) **승격 게이트**: Cloud-Test 검증 통과 후에만 Cloud-Secure 반영

---

## 10) 개발 환경/배포 전략(초안)

> 본 문서는 “동기화(Consumer)” 관점의 개발/운영 전략을 요약한다.  
> Serving 전체 관점은 `.specs/backoffice_project_specs.md`를 따른다.

### 10.1 로컬 개발(Local)

- 의존성
  - Backoffice DB(PostgreSQL) 컨테이너
  - Kafka 호환 브로커 컨테이너(권장) 또는 로컬 mock(선택)
- 개발 흐름(예시)
  1) `docker compose up`으로 DB/브로커 기동
  2) backfill 모드로 초기 적재 실행(최근 N일)
  3) 이벤트 publish → upsert 동작 확인
  4) 실패 이벤트는 DLQ로 격리되는지 확인
- 로컬 검증 체크리스트
  - 멱등 upsert: 동일 이벤트 재처리 시 결과 수렴
  - out-of-order: 늦게 온 이벤트가 “상태 역전”을 유발하지 않는지(버전/updated_at 필요)
  - 페어링: `related_id` 기준으로 pair가 완성/부분완성 되는지

### 10.2 Azure 배포(권장 레퍼런스)

- **Cloud-Test 프로파일**
  - 이벤트: Event Hubs(Kafka endpoint), 개인 분리 namespace
  - 실행: Azure Container Apps
    - consumer는 **min replicas 1 / max replicas 1** 권장
  - 시크릿: SAS/env 주입 우선
  - 관측: 최소 lag/DLQ/freshness 확인
- **Cloud-Secure 프로파일**
  - 이벤트: 보안 정책 적용 Event Hubs/Kafka
  - 실행: Container Apps 또는 AKS(조직 표준)
  - 시크릿: Key Vault + Managed Identity
  - 네트워크: Private Endpoint/VNet/Firewall 적용
  - 관측: 운영 알림 임계치까지 확정

### 10.3 릴리즈/재처리

- 배포는 rolling을 기본으로 하되, “이중 소비(중복)”는 멱등 upsert로 흡수한다.
- 장애/배포 실패 시:
  - 컨슈머는 재기동 시에도 동일 offset부터 재처리되므로, 멱등성이 깨지지 않게 설계한다.
  - DLQ replay(선택)로 복구 경로를 제공한다.

---

## 11) 오픈 이슈(협의 필요)

- `payment_orders.status` 허용값 및 “실패” 정의(실패율 지표와 연결)
- `amount_signed`의 SSOT(업스트림 제공 vs 룰 파생)
- `related_id`가 비는 케이스/여러 도메인을 참조하는 케이스(`related_type` 필요 여부)
- 환불/취소/정정(역분개) 이벤트 타입/페어링 확장 규칙
