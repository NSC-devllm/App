# Backoffice Serving Project Specs (DB + Admin API) (v0.1)

> **목표**: SRS FR-ADM-02(거래 내역 추적)를 위해 `tx_id` 단건 조회를 **초 단위**로 제공하는 Serving Layer를 구축한다.
>
> **핵심 아이디어**: Admin 조회는 Databricks/ADLS가 아니라, **Backoffice DB(인덱스) + Admin API**로 서빙한다.
>
> **연계 문서**
> - DB/API 상세 설계: `.specs/backoffice_db_admin_api.md`
> - 데이터 동기화(OLTP→BO) 설계: `.specs/backoffice_data_project.md`
> - 아키텍처 참고(비범위/보조 경로): `.specs/reference/entire_architecture.md`

---

## 1) 범위 정의

### 1.1 In-scope

- Admin API(REST)
  - `GET /admin/tx/{tx_id}`: 단건 조회(FR-ADM-02)
  - (권장) `GET /admin/payment-orders/{order_id}`: 결제 오더 기준 조회
  - (권장) `GET /admin/wallets/{wallet_id}/tx`: 지갑 기준 최근 거래
- Backoffice DB(Serving DB) 스키마/인덱스/마이그레이션
- OLTP→Backoffice DB 동기화(이벤트/CDC) + 멱등/재처리
- 인증/인가(RBAC), 감사로그, 운영 모니터링/알림

### 1.2 Out-of-scope

- 결제/정산 트랜잭션 처리(Freeze/Settle/Rollback)
- Lakehouse(데이터브릭스) 통제 산출물 서빙
- 고객-facing 실시간 API(관리자 전용)

---

## 2) 성공 기준(DoD)

- `GET /admin/tx/{tx_id}`가 p95 200ms 이내(내부망 기준, 조정 가능)
- OLTP 커밋 이후 Backoffice 반영 p95 5초 이내(초 단위 목표)
- 장애 시에도 데이터 정합성이 깨지지 않고 재처리로 수렴(멱등)
- Cloud-Secure(운영형) 기준 모든 요청이 인증/인가되고, 조회 요청 감사로그가 남는다

---

## 3) 핵심 결정(Decision Lock)

### 3.1 tx_id / 페어링(방법 2)

- `tx_id`는 **원장 엔트리(행) ID**
- 결제 더블엔트리(PAYMENT/RECEIVE)는 서로 다른 `tx_id`
- 페어링 키는 `related_id`(권장: `payment_orders.order_id`)

> 이 결정이 바뀌면(DB 스키마 변경 포함) API/DB 모델이 크게 달라진다.

### 3.2 시간대

- API/DB 저장은 UTC 기준을 기본으로 한다.
- 화면 표시는 필요에 따라 KST 변환(프론트/클라이언트 책임 가능).

### 3.3 Backoffice DB 포지셔닝(고정)

- Backoffice DB는 **`tx_id`/`order_id` 조회를 위한 Read 인덱스(derived store)**로 고정한다.
- Admin/운영 업무의 “쓰기 트랜잭션”은 OLTP에서만 수행하며, Backoffice DB에는 **Admin API/Sync Consumer만 write**한다.

### 3.4 구현 스택/정책(자체 결정)

- **Admin API**: Python + FastAPI
- **Sync Consumer**: Python(커스텀 서비스)
- **Serving DB**: PostgreSQL (로컬 컨테이너 / Azure Database for PostgreSQL)
- **페어링 테이블**: `bo.payment_ledger_pairs`를 기본 유지(부분 완성 허용)
- **응답 정책**: 페어링 불완전 시에도 응답 반환 + `pairing_status`/`data_lag_sec` 제공(상세는 DB/API 설계 문서)

### 3.5 협의 전 가정(임시 결정)

> 아래 항목은 **협의 전까지 기본값**으로 적용한다. 합의 시 업데이트한다.

- **`tx_id` 의미**: 원장 엔트리(행) ID로 고정 → 페어링은 `related_id` 기준(방법 2)
- **`related_id/related_type`**: 기본은 `payment_orders.order_id`; `related_type` 없으면 조인 성공 시 `PAYMENT_ORDER`, 실패 시 `UNKNOWN`
- **`status` 매핑**: 원문 상태는 그대로 노출, `status_group(SUCCESS/FAIL/IN_PROGRESS/UNKNOWN)`는 v1 매핑표로 계산한다 — 결정: `DEC-206` (`.specs/decision_open_items.md`)
- **`updated_at/version` 부재 시**: `ingested_at` 기준 LWW로 수렴 + `version_missing_cnt` 지표 관측
- **SLO/보안 기본값**: 데이터 신선도 p95 5초, API p95 200ms, RBAC(`ADMIN_READ`, `ADMIN_AUDIT`) + 감사로그 + PII 최소화

---

## 4) 아키텍처(요약)

- OLTP DB(SSOT)
- Kafka(또는 CDC) — 이벤트 스트림
- Consumer(동기화 서비스)
- Backoffice DB(Serving DB)
- Admin API(REST)
- 모니터링/알림(로그, 메트릭, 트레이싱)

상세는 `.specs/backoffice_db_admin_api.md` 및 `.specs/backoffice_data_project.md` 참고.

---

## 5) 데이터 모델(요약)

Serving DB는 “조회 최적화”를 위해 필요한 데이터를 최소로 복제/정규화한다.

- `bo.ledger_entries` (PK: `tx_id`)
- `bo.payment_orders` (PK: `order_id`)
- `bo.payment_ledger_pairs` (UK: `payment_order_id`) — 권장(페어링 가속)
- (옵션) `bo.admin_tx_search` 뷰/테이블

---

## 6) API 설계 원칙

- 단건 조회는 항상 Backoffice DB에서 처리(클러스터/웨어하우스 의존 금지)
- 페어링이 불완전한 경우에도 응답을 반환하되, `paired_tx_id`/`receiver_wallet_id` 등을 NULL로 둘 수 있다
- 응답에는 “데이터 최신성/커버리지” 힌트를 포함하는 것을 권장
  - 예: `data_lag_sec`, `pairing_status`(COMPLETE/INCOMPLETE/UNKNOWN)

---

## 7) 보안/감사(필수)

- 인증: OIDC/JWT(사내 SSO)
- 인가: RBAC (`ADMIN_READ`, `ADMIN_AUDIT` 등)
- 감사로그: `who/when/what(tx_id or order_id)/result_count/ip/user_agent`
- `local/dev`에서는 개발 편의를 위해 `AUTH_MODE=disabled`를 허용하되, 운영형에서는 금지한다
- 인증/인가 실패(401/403)는 라우트 진입 전 차단될 수 있으므로 DB 감사로그 대신 API/게이트웨이 로그로 추적한다
- 비밀번호/credential 데이터는 절대 적재/노출 금지

---

## 8) 관측/운영(필수)

### 8.1 API 지표

- latency(p50/p95/p99), error rate(4xx/5xx), QPS
- slow query 상관관계(트레이싱)

### 8.2 동기화 지표

- consumer lag, DLQ rate
- end-to-end freshness(p95)
- pair completion rate / incomplete age

---

## 9) 단계별 출시 계획(개정)

### 9.1 기능 성숙 단계(서비스 기능 축)

1) **Phase F1 — Read-only MVP**
   - BO DB 스키마 + backfill + 증분 동기화
   - `GET /admin/tx/{tx_id}` 제공(페어링은 best-effort)
2) **Phase F2 — Pairing 강화**
   - `bo.payment_ledger_pairs` 확정 테이블 도입
   - 페어링 품질 지표/알림
3) **Phase F3 — SLO 강화**
   - out-of-order/중복/재처리 정책 확정
   - status 변경 시각/버전 필드 정착

### 9.2 환경 승격 단계(배포/인프라 축)

1) **Phase E1 — Cloud-Test(폐기형, Public 허용)**
   - 테스트 전용 리소스 생성(Event Hubs/ACA/PostgreSQL)
   - synthetic 이벤트 기반 E2E 스모크 검증
   - `Destroy -> Recreate` 재현성 검증
2) **Phase E2 — Cloud-Secure(운영형, 보안 네트워크)**
   - 보안용 별도 리소스 생성(인플레이스 전환 금지)
   - Private Endpoint/VNet/Firewall + Key Vault/Managed Identity 전환
   - backfill + 증분 동기화 재실행 후 컷오버
3) **Phase E3 — 운영 자동화**
   - CI/CD 게이트, 스모크 자동화, 재처리/복구 체계 확정

---

## 10) 개발 환경/배포 전략(초안)

> 목적: 로컬에서 빠르게 개발/테스트하고, Azure에 동일한 형태로 배포/운영한다.

### 10.1 프로젝트 분리(권장)

- `bo-admin-api`(Serving): Admin API + DB 마이그레이션/스키마
- `bo-sync-consumer`(Data): 이벤트/CDC Consumer(동기화) + DLQ/재처리 도구
- `bo-infra`(IaC): Azure 리소스(네트워크/DB/이벤트/컴퓨트/모니터링) 프로비저닝

> 단, 초기에는 운영 단순화를 위해 `bo-admin-api`와 `bo-sync-consumer`를 단일 repo/단일 배포 파이프라인로 시작해도 된다.

### 10.2 로컬 개발(Local)

권장 방식: Docker 기반으로 “의존성”을 로컬에서 재현하고, 애플리케이션은 로컬 실행(또는 컨테이너 실행)한다.

- 의존성(로컬)
  - Backoffice DB: PostgreSQL(컨테이너)
  - 이벤트 버스: Kafka 호환 브로커(컨테이너) 또는 로컬 mock(선택)
- 개발 흐름(예시)
  1) `docker compose up`으로 DB/브로커 기동
  2) 마이그레이션 적용(스키마 생성)
  3) Consumer 실행(샘플 이벤트 publish 또는 backfill 실행)
  4) Admin API 실행(단건 조회로 검증)
- 로컬 테스트
  - Unit: 페어링/응답 조립 로직(순수 함수)
  - Integration: “이벤트 → Consumer upsert → API 조회” E2E(컨테이너 기반)

### 10.3 Azure 배포(권장 레퍼런스)

> 제품/조직 상황에 따라 AKS/App Service 등으로 대체 가능. 여기서는 관리 난이도가 낮은 구성을 기본으로 둔다.

- **Cloud-Test(Phase E1)** — Phase 8에서 검증 완료, 폐기 대상 (프로비저닝 스크립트는 인프라팀 이관 후 제거됨)
  - 데이터베이스: Azure Database for PostgreSQL(Flexible Server, test profile)
  - 이벤트 버스: Azure Event Hubs(Kafka endpoint, isolated namespace)
  - 실행 환경: Azure Container Apps
  - 시크릿: SAS/env 주입 우선(테스트 속도 우선)
  - 관측: 최소 App Insights + Log Analytics 연결
- **Cloud-Secure(Phase E2)** — 리소스 소유 모델 적용
  - 데이터베이스: Azure Database for PostgreSQL(Flexible Server, secure profile) — **서비스 전용**
  - 이벤트 버스: 기존 Event Hubs namespace 활용, 토픽만 소유 — **RG 공유**
  - 실행 환경: AKS(RG 공유 클러스터, namespace 분리 배포) — **RG 공유**
  - 이미지 레지스트리: ACR — **RG 공유**
  - 시크릿: Key Vault(RG 공유) + Managed Identity(서비스 전용)
  - 네트워크: Private Endpoint/VNet/Firewall 기반
  - 관측: App Insights + Log Analytics — **RG 공유**

> **리소스 소유 모델**: 리소스 생성은 인프라팀이 수행한다. 이 레포는 네이밍 컨벤션과 리소스 요구사항만 정의한다.
> PostgreSQL만 서비스 전용으로 요청한다. AKS/ACR/Key Vault/App Insights/Log Analytics는 RG 공유 리소스를 활용하며,
> 서비스 격리는 namespace/리포지토리/secret prefix/cloud_roleName으로 구분한다.
> Event Hubs namespace는 이미 존재하며 토픽(hub)만 이 서비스가 소유한다.
> 네이밍 컨벤션 상세는 `.specs/infra/cloud_migration_rebuild_plan.md` 3.3항 참조.
>
> **이벤트 발행 책임**: 카프카 프로듀서 코드는 업스트림 서비스(CryptoSvc, AccountSvc, CommerceSvc)가 소유한다.
> tx-lookup-service는 컨슈머 전용이며, 이벤트 계약(스키마)은 `configs/topic_checklist.md`에서 정의한다.
> 카프카/모니터링 인프라 프로비저닝은 인프라팀이 수행한다.

### 10.4 환경 분리 / 설정 관리

- `local`(Docker Compose) + Azure 단일 소스(환경 분리 없음)
- 설정은 12-factor 원칙(환경변수/시크릿)로 관리하고, 저장소에 자격 증명은 커밋하지 않는다.
- 네이밍 컨벤션: `.specs/infra/cloud_migration_rebuild_plan.md` 3.3항 참조

### 10.5 CI/CD(초안)

- PR: lint + unit test + (가능하면) docker 기반 integration test
- main merge:
  - 컨테이너 이미지 빌드/스캔 → ACR push
  - IaC 적용(필요 시)
  - DB 마이그레이션(배포 전 단계, 실패 시 중단)
  - ACA/AKS에 배포(rolling)
  - smoke test(`GET /admin/tx/{tx_id}`) 후 완료

---

## 11) 오픈 이슈(협의 필요)

- `payment_orders.status` 허용값 및 “성공/실패/진행” 정의
- 상태 변경 시각(`updated_at/settled_at/failed_at`) 또는 버전 필드 제공 여부
- `amount_signed` SSOT(저장 vs 파생) 및 type enum 표준
- `related_id` 공백/다중 도메인(`related_type`) 처리
- 환불/취소/정정(역분개) 표기/페어링 규칙
