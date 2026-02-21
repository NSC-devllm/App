# 결정 필요 항목 목록 (Open Decisions)

작성일: 2026-02-06
업데이트: 2026-02-12

## 목적

구현 중 **명확히 결정되지 않았거나 가정으로 처리한 항목**을 기록하고,
결정이 내려지면 본 문서를 갱신한다.

## 업데이트 규칙

- 구현 중 새로운 가정/모호점이 생기면 **즉시 본 문서에 추가**한다.
- 결정이 확정되면 항목을 `결정됨`으로 변경하고 **근거(코드/문서/룰/테이블)** 를 명시한다.

## 결정 필요 항목

### DEC-001 OIDC 클레임 매핑(roles / actor id)

- 상태: **결정됨(2026-02-09)**
- 결정: 역할(Role)은 `roles`+`scp`에서 추출하고, actor id는 `oid` 우선/없으면 `sub`를 사용한다. (`AUTH_ROLES_CLAIM=roles,scp`, `AUTH_ACTOR_ID_CLAIMS=oid,sub`)
- 영향: Admin API 인가(RBAC) 및 감사로그(actor 식별자) 안정성
- 재검토 트리거: Entra 토큰 클레임 형태 변경 또는 게이트웨이 표준 변경
- 근거: `src/api/auth.py`, `tests/unit/test_auth.py`, `configs/README.md`, `configs/env.example`

### DEC-002 DLQ 저장소 baseline(Cloud-Test/local)

- 상태: **결정됨(2026-02-09)**
- 결정: Cloud-Test/local에서는 DLQ를 JSONL 파일(`DLQ_PATH`)에 append 저장한다.
- 영향: 테스트 환경에서는 컨테이너 로컬 파일이므로 내구성이 약하다(테스트 목적 상 허용).
- 재검토 트리거: 운영형 DLQ 정책 확정(DEC-201)
- 근거: `src/consumer/dlq.py`, `src/consumer/main.py`, `configs/env.example`

### DEC-003 `status_group` 매핑

- 상태: **결정됨(2026-02-09)**
- 결정: `payment_orders.status`(원문)을 v1 매핑표로 `status_group(SUCCESS/FAIL/IN_PROGRESS/UNKNOWN)`로 변환한다. 매핑 없는 값은 `UNKNOWN`.
- 영향: Admin 조회에서 상태를 “성공/실패/진행”으로 빠르게 해석 가능
- 재검토 트리거: 업스트림 status taxonomy 확정/변경(운영형 상태 집합 재정의)
- 근거: `src/api/service.py` (`_resolve_status_group`), `tests/unit/test_api_service.py`, `.specs/backoffice_db_admin_api.md`

### DEC-004 페어링 회귀(regression) 방지 정책

- 상태: **결정됨(2026-02-09)**
- 결정: 완성 페어(complete)는 불완전 페어(incomplete)로 덮어쓰지 않는다. (complete -> incomplete 업데이트 차단)
- 영향: out-of-order/부분 데이터 유입 시 페어링 상태가 역전되는 것을 방지
- 재검토 트리거: 환불/정정(역분개) 등으로 “완성 페어가 다시 깨지는” 도메인 정책이 정의될 때
- 근거: `src/consumer/pairing.py` (`should_update_pair`), `tests/unit/test_pairing.py`

### DEC-005 `related_type` 페어링 범위

- 상태: **결정됨(2026-02-09)**
- 결정: 페어링은 `related_type=PAYMENT_ORDER`만 지원한다. 그 외는 `pairing_status=UNKNOWN`.
- 영향: 도메인 확장 전까지 안정적으로 단일 경로 유지
- 재검토 트리거: 다른 related domain 페어링 요구가 생길 때
- 근거: `src/api/service.py` (`_resolve_pairing`, `_resolve_related_type`), `.specs/backoffice_project_specs.md`

### DEC-006 `amount_signed` baseline(Cloud-Test/local)

- 상태: **결정됨(2026-02-09)**
- 결정: 이벤트에 `amount_signed`가 있으면 저장, 없으면 `NULL`로 저장한다(Consumer 파생 없음).
- 영향: 데모/검증에서 돈 의미 오해를 방지하고, 업스트림 계약 부재를 `NULL`로 표현
- 재검토 트리거: 운영형 SSOT 확정(DEC-204)
- 근거: `src/consumer/events.py`, `src/consumer/processor.py`, `configs/topic_checklist.md`

### DEC-007 Slow query 임계치

- 상태: **결정됨(2026-02-09)**
- 결정: slow query 로그 임계치는 기본 `DB_SLOW_QUERY_MS=200`으로 두고, 환경변수로 조정한다.
- 영향: SLO(p95 200ms)와 함께 느린 쿼리 상관관계를 관측 가능
- 재검토 트리거: 운영 환경의 p95 목표/DB 성능 프로파일 변경 시
- 근거: `src/common/config.py`, `configs/README.md`, `.specs/backoffice_project_specs.md`

### DEC-008 Freshness baseline(Cloud-Test/local)

- 상태: **결정됨(2026-02-09)**
- 결정: Consumer freshness는 도메인 시각을 기준으로 측정한다. ledger=`event_time`, order=`updated_at` 없으면 `created_at`.
- 영향: commit-time이 없더라도 일관된 “마지막 이벤트 기준” 신선도 지표 확보
- 재검토 트리거: commit-time/occurred_at 제공이 표준화될 때(운영형 기준 전환)
- 근거: `src/consumer/main.py`, `src/consumer/metrics.py`, `.specs/backoffice_data_project.md`

### DEC-009 Correlation ID 헤더

- 상태: **결정됨(2026-02-09)**
- 결정: 표준 헤더는 `X-Correlation-ID`로 한다. API는 해당 헤더를 응답에 포함하며, consumer는 헤더/페이로드의 alias를 허용한다.
- 영향: API/Consumer 로그 상관관계(tracing-lite) 확보
- 재검토 트리거: 조직 표준 헤더가 별도로 확정될 때
- 근거: `src/common/observability.py`, `src/api/observability.py`

### DEC-010 알림 임계치 baseline(Phase 8/9)

- 상태: **결정됨(2026-02-09)**
- 결정: baseline alert rule을 채택한다(API p95>200ms/5m, error rate(4xx/5xx)>2%/5m, freshness>5s/5m, DLQ activity>0/5m).
- 영향: 최소 운영 알림(데모/검증) 기준을 단일 세트로 유지
- 재검토 트리거: 온콜/운영 체계가 생기면 4xx 분리 및 severity 재설계
- 근거: `docker/observability/alert_rules.yml`, `README.md`

### DEC-101 OIDC provider

- 상태: **결정됨(2026-02-09)**
- 결정: OIDC provider는 Microsoft Entra ID를 사용한다.
- 영향: auth discovery/issuer/jwks/audience 기준이 Entra 형태에 맞춰짐
- 근거: `configs/README.md`

### DEC-102 Audit log 저장소

- 상태: **결정됨(2026-02-09)**
- 결정: Admin API 조회 감사로그는 `bo.admin_audit_logs` 테이블에 저장한다.
- 영향: 조회 이력 감사/추적 가능
- 근거: `src/db/models.py`, `migrations/20260205_0003_create_admin_audit_logs.py`

### DEC-103 페어링 저장소

- 상태: **결정됨(2026-02-09)**
- 결정: 페어링 결과는 Backoffice DB의 `bo.payment_ledger_pairs`에 저장한다.
- 영향: API 조회에서 빠른 페어링 조회 가능
- 근거: `migrations/20260205_0001_create_backoffice_schema.py`

### DEC-104 Event Hubs 소유 모델

- 상태: **결정됨(2026-02-09) → 수정됨(2026-02-10)**
- 결정(Phase 8): Cloud-Test는 owner별로 분리된 Event Hubs namespace를 사용했다.
- 결정(Phase 9 이후): 기존 공유 Event Hubs namespace를 활용한다. tx-lookup-service는 토픽(hub)만 소유하고, namespace는 생성하지 않는다.
- 영향: 리소스 중복 제거, 운영 일관성 확보
- 근거: `.roadmap/implementation_roadmap.md`, `.specs/infra/cloud_migration_rebuild_plan.md`, 전체 아키텍처 다이어그램

### DEC-105 Cloud 런타임

- 상태: **결정됨(2026-02-09) → 수정됨(2026-02-10)**
- 결정(Phase 8): Cloud-Test는 `Event Hubs(Kafka) + Azure Container Apps`로 구성했다.
- 결정(Phase 9 이후): Cloud-Secure는 `Event Hubs(Kafka) + AKS(RG 공유 클러스터)`로 구성한다. tx-lookup-service는 공유 AKS 클러스터에 namespace 분리 배포한다.
- 영향: AKS를 조직 표준 실행 환경으로 채택, Container Apps는 Phase 8 테스트 한정
- 근거: `.roadmap/implementation_roadmap.md`, `.specs/archive/cloud_phase8_execution_report.md`, 전체 아키텍처 다이어그램

### DEC-106 Cloud-Test 시크릿 전달

- 상태: **결정됨(2026-02-09)**
- 결정: Cloud-Test는 SAS/env 주입을 우선하고, Key Vault + Managed Identity는 secure rebuild 단계로 이연한다.
- 영향: 테스트 속도 우선(보안 하드닝은 별도 환경에서)
- 근거: `.specs/infra/cloud_migration_rebuild_plan.md`

### DEC-107 Event Hubs baseline contract(Phase 8 test)

- 상태: **결정됨(2026-02-09)**
- 결정: hubs=`ledger.entry.upserted`, `payment.order.upserted`; partition=2; retention=3d; consumer group naming=`bo-sync-<env>-<owner>-v1`.
- 영향: 프로비저닝/배포 자동화가 계약값에 의존
- 근거: `.specs/infra/cloud_migration_rebuild_plan.md`, `.specs/archive/cloud_phase8_execution_report.md`

### DEC-108 Cloud 승격 모델

- 상태: **결정됨(2026-02-09)**
- 결정: Cloud-Test(disposable/public) -> Cloud-Secure(separate/secured) 2단계 승격, in-place hardening 금지.
- 영향: 보안 적용은 별도 리소스에서 재검증 후 컷오버
- 근거: `.specs/infra/cloud_migration_rebuild_plan.md`, `.specs/backoffice_project_specs.md`

### DEC-109 Container Apps test naming

- 상태: **결정됨(2026-02-09)**
- 결정: `team4-txlookup-*` 패턴을 유지하되, Container Apps 계열은 32자 제한으로 축약 패턴을 사용한다.
- 영향: Azure naming limit 회피
- 근거: `.specs/infra/cloud_migration_rebuild_plan.md`, ~~`scripts/cloud/phase8/provision_resources.sh`~~ (제거됨), `.specs/archive/cloud_phase8_execution_report.md`

### DEC-110 Destroy/Recreate test under RG lock

- 상태: **결정됨(2026-02-09)**
- 결정: RG lock으로 delete가 막히면 consumer scale cycle(`min 1 -> 0 -> 1`)로 recreate를 대체 검증한다.
- 영향: lock 환경에서도 파이프라인 복구 동선 확보
- 근거: ~~`scripts/cloud/phase8/destroy_recreate_check.sh`~~ (제거됨), `.specs/archive/cloud_phase8_execution_report.md`

### DEC-111 Azure 리소스 소유 모델(Cloud-Secure)

- 상태: **결정됨(2026-02-10)**
- 결정: tx-lookup-service의 Azure 리소스를 아래와 같이 분류한다.
  - **서비스 전용**: Azure Database for PostgreSQL Flexible Server (Backoffice Serving DB)
  - **RG 공유**: AKS(클러스터), ACR, Key Vault, App Insights, Log Analytics
  - **기존 활용**: Event Hubs namespace(이미 존재, 토픽만 소유)
  - **플랫폼 소유**: App Gateway, Bastion, Firewall, VNet, Private DNS Zone
  - **비범위**: Confidential Ledger(CryptoSvc), SQL Database(AccountSvc), Databricks, ADLS Gen2
- 영향: 리소스 생성은 인프라팀이 수행한다. 이 레포는 네이밍 컨벤션과 리소스 요구사항만 정의하고 인프라팀에 전달한다.
- 근거: 전체 아키텍처 다이어그램 분석, `.specs/backoffice_project_specs.md` 10.3항, `.specs/infra/cloud_migration_rebuild_plan.md`

### DEC-201 DLQ 저장소(Cloud-Secure/Prod)

- 상태: **결정됨(2026-02-09)**
- 결정: 운영형 DLQ는 PostgreSQL 테이블(`bo.consumer_dlq_events`)에 저장하고, 기본 보관기간은 14일로 한다. 재처리는 수동 replay(선택/범위)로 재실행한다.
- 영향: 인프라 추가 없이 데모/검증용 운영 동선(SQL triage + replay) 확보
- 재검토 트리거: Kafka/Event Hubs DLQ 토픽을 운영 표준으로 채택할 때
- 근거: `src/consumer/main.py`(DLQ write + commit), `src/consumer/dlq.py`(baseline), `docker/observability/alert_rules.yml`

### DEC-202 알림 임계치/심각도(Cloud-Secure/Prod)

- 상태: **결정됨(2026-02-09)**
- 결정: 운영 알림도 baseline rule을 그대로 채택한다(튜닝 유예). `DataFreshnessHigh=critical`, 나머지=warning.
- 영향: 정책 논쟁을 피하고, 문서/구현 불일치 최소화
- 재검토 트리거: 실제 트래픽/온콜 체계 도입 시(4xx 분리, DLQ 2단계 severity 등)
- 근거: `docker/observability/alert_rules.yml`, `README.md`

### DEC-203 Entra OIDC 클레임 매핑(roles / actor id)

- 상태: **결정됨(2026-02-09)**
- 결정: Entra 토큰을 고려해 `roles,scp`에서 role을, `oid,sub`에서 actor id를 추출한다(환경변수 오버라이드 가능).
- 영향: 역할/식별자 클레임 차이로 인한 인증 실패 가능성을 낮춤
- 근거: `src/api/auth.py`, `configs/README.md`, `configs/env.example`, `tests/unit/test_auth.py`

### DEC-204 `amount_signed` SSOT(운영)

- 상태: **결정됨(2026-02-09)**
- 결정: `amount_signed`는 업스트림 제공 값을 SSOT로 저장한다. 미제공 시 Consumer는 파생하지 않고 `NULL`로 저장한다.
- 영향: 부호 규칙 오해로 인한 money semantics 오류 위험을 최소화
- 재검토 트리거: 업스트림이 `entry_type` 확장/표준 부호 계약을 제공할 때(파생 전환 검토)
- 근거: `src/consumer/events.py`, `src/consumer/processor.py`, `configs/topic_checklist.md`, `.specs/backoffice_db_admin_api.md`

### DEC-205 Freshness 기준 시각(운영)

- 상태: **결정됨(2026-02-09)**
- 결정: Consumer freshness는 도메인 시각을 기준으로 한다(ledger=`event_time`, order=`updated_at||created_at`). API `data_lag_sec`는 `now - max(ingested_at, event_time)`로 계산한다.
- 영향: commit-time 부재 상황에서도 일관된 신선도/지연 해석 가능
- 재검토 트리거: commit-time/occurred_at가 이벤트 계약으로 확정될 때
- 근거: `src/consumer/main.py`, `src/consumer/metrics.py`, `src/api/service.py`, `.specs/backoffice_db_admin_api.md`

### DEC-206 `status_group` 매핑 v1

- 상태: **결정됨(2026-02-09)**
- 결정: `payment_orders.status` 원문은 유지하고, 아래 v1 매핑표로 `status_group`를 계산한다(대소문자 무시).
- 매핑표(v1): `SUCCESS=SETTLED,COMPLETED,SUCCESS,SUCCEEDED,PAID` / `FAIL=FAILED,CANCELLED,CANCELED,REJECTED,DECLINED` / `IN_PROGRESS=CREATED,PENDING,PROCESSING,AUTHORIZED` / 그 외 또는 NULL은 `UNKNOWN`
- 영향: 운영/CS가 상태를 그룹 단위로 빠르게 해석 가능
- 재검토 트리거: status taxonomy 확정/변경 시 v2 매핑으로 재정의
- 근거: `src/api/service.py`, `tests/unit/test_api_service.py`, `.specs/backoffice_project_specs.md`, `.specs/backoffice_db_admin_api.md`

### DEC-112 이벤트 발행 책임 분리

- 상태: **결정됨(2026-02-12)**
- 결정: 카프카 프로듀서 코드(이벤트 발행)는 업스트림 서비스(CryptoSvc, AccountSvc, CommerceSvc)가 소유한다. tx-lookup-service는 컨슈머 전용이다. 카프카/모니터링 인프라 프로비저닝은 인프라팀이 수행한다.
- 영향: Phase 8 프로비저닝 스크립트(`scripts/cloud/phase8/`) 제거. 이벤트 계약(스키마)은 `configs/topic_checklist.md`에서 정의하고 업스트림 팀에 전달한다.
- 근거: 팀 간 업무 범위 조정, `.specs/backoffice_project_specs.md` 10.3항, `.specs/reference/entire_architecture.md`

### DEC-207 갭 1 - API 엔드포인트/응답 계약 점검

- 상태: **해결됨(구현 완료, 2026-02-12)**
- 확인 결과: 필수 `GET /admin/tx/{tx_id}`의 404/INCOMPLETE/UNKNOWN 응답 정책은 구현과 일치하나, 권장 엔드포인트 2개는 미구현 상태다.
- 갭 설명: `.specs/backoffice_project_specs.md` 1.1, `.specs/backoffice_db_admin_api.md` 5.2에 있는 권장 엔드포인트 `GET /admin/payment-orders/{order_id}`, `GET /admin/wallets/{wallet_id}/tx`가 `src/api/main.py`에 없다.
- 근거(문서/코드 경로): `.specs/backoffice_project_specs.md`, `.specs/backoffice_db_admin_api.md`, `src/api/main.py`, `src/api/service.py`, `src/api/schemas.py`, `src/db/admin_tx.py`, `src/api/constants.py`, `tests/unit/test_api_routes.py`, `tests/unit/test_api_service.py`, `tests/integration/test_admin_tx_integration.py`
- 영향: 운영/CS 조회 동선이 `tx_id` 단건 조회에만 묶여, 주문/지갑 관점 조회 요구를 API 레벨에서 직접 처리할 수 없다.
- 구현:
  - `GET /admin/payment-orders/{order_id}`: 주문 기준 페어링 조회 (404 if not found). `AdminOrderResponse` = 주문 상세 + 원장 엔트리 목록 + 전체 페어링 상태.
  - `GET /admin/wallets/{wallet_id}/tx?from=&to=&limit=`: 지갑 기준 거래 리스트 (200 + 빈 리스트). `AdminWalletTxResponse` = 지갑ID + 엔트리 목록 + 건수. limit(1~100, 기본 20), from/to(ISO 8601).
  - 스키마: `LedgerEntryItem`, `PaymentOrderDetail`, `AdminOrderResponse`, `AdminWalletTxResponse` (`src/api/schemas.py`)
  - DB 쿼리: `fetch_admin_order_context()`, `fetch_admin_wallet_tx()` (`src/db/admin_tx.py`)
  - 서비스: `build_admin_order_response()`, `build_admin_wallet_tx_response()`, `_build_ledger_entry_item()` (`src/api/service.py`)
  - 감사: 두 엔드포인트 모두 `result_count` 기록 (DEC-208 연동)
- 검증: L0(py_compile) 통과, L1(단위 테스트 128건) 통과, 커버리지 81.32%(80% 게이트 통과). 통합 테스트는 Docker DB 필요(L1+).
- 재검토 트리거: 서비스 범위를 F1(`tx_id` 단건 전용)로 공식 축소 결정하는 경우.

### DEC-208 갭 2 - RBAC/감사로그 계약 점검

- 상태: **해결됨(구현 완료, 2026-02-12)**
- 확인 결과: 감사로그의 핵심 필드(`who/when/what/ip/user_agent`)는 기록되지만, spec에 명시된 `result_count` 필드는 저장되지 않는다.
- 갭 설명: `bo.admin_audit_logs` 스키마와 `build_audit_fields()`에 `result_count`가 없다.
- 근거(문서/코드 경로): `.specs/backoffice_project_specs.md` 7, `src/api/audit.py`, `src/db/models.py`, `migrations/versions/20260205_0003_create_admin_audit_logs.py`, `migrations/versions/20260212_0005_add_audit_result_count.py`, `tests/unit/test_api_routes.py`, `tests/integration/test_admin_tx_integration.py`
- 영향: 감사로그에서 "조회 결과 건수"를 기반으로 이상 조회 패턴을 집계하기 어렵다.
- 구현:
  - Migration: `migrations/versions/20260212_0005_add_audit_result_count.py` — `bo.admin_audit_logs`에 `result_count INTEGER NULL` 컬럼 추가
  - Model: `src/db/models.py` `AdminAuditLog.result_count` 필드 추가
  - Audit: `src/api/audit.py` `build_audit_fields()` 에 `result_count: int | None = None` 파라미터 추가
  - Route: `GET /admin/tx/{tx_id}`에서 `FOUND=1`, `NOT_FOUND=0` 기록. 신규 엔드포인트(DEC-207)에서 실제 건수 기록.
- 검증: L0(py_compile) 통과, L1(단위 테스트 128건) 통과. 통합 테스트(`result_count` 영속성)는 Docker DB 필요(L1+).
- 재검토 트리거: 감사 정책에서 `result_count`를 메트릭 시스템으로 이관하고 DB 저장을 제외하기로 합의되는 경우.

### DEC-209 갭 3 - Serving DB 스키마/인덱스 계약 점검

- 상태: **결정됨(일치, 2026-02-12)**
- 확인 결과: `bo.ledger_entries`, `bo.payment_orders`, `bo.payment_ledger_pairs`의 PK/주요 컬럼/핵심 인덱스는 SSOT와 실구현이 일치한다.
- 갭 설명: 확인된 필수 스키마 갭 없음. `bo.admin_tx_search`는 문서상 옵션이므로 미구현을 갭으로 판정하지 않는다.
- 근거(문서/코드 경로): `.specs/backoffice_db_admin_api.md` 4.1~4.4, `src/db/models.py`, `migrations/versions/20260205_0001_create_backoffice_schema.py`, `migrations/versions/20260205_0002_add_source_version_columns.py`, `.agents/logs/verification/dec207_214/03_models_rg.log`, `.agents/logs/verification/dec207_214/04_migrations_rg.log`
- 영향: 스키마 관점에서 즉시 수정이 필요한 차이는 없다.
- 개선안 설계: 현재 단계 변경 없음. `admin_tx_search`를 실제 운영 쿼리 패턴 기반으로 도입할지 별도 성능 검토에서 결정한다.
- 검증 계획: 현행 integration test(`tests/integration/test_db_integration.py`) 유지 + 신규 migration 추가 시 schema diff 점검.
- 재검토 트리거: API 조회 패턴이 증가해 join 비용이 SLA를 위반할 때(뷰/머터리얼라이즈드 뷰 검토).

### DEC-210 갭 4 - 멱등/LWW/버전 처리 계약 점검

- 상태: **해결됨(구현 완료, 2026-02-12)**
- 확인 결과: `updated_at`/`source_version` 우선 처리 자체는 구현되어 있으나, metadata 없는 이벤트가 metadata 있는 기존 레코드를 덮어쓸 수 있는 fallback 경로가 존재한다.
- 갭 설명: `src/db/upsert.py`의 fallback 조건은 `incoming_updated is NULL AND incoming_version is NULL AND incoming_ingested >= existing_ingested`만 확인하므로, 기존 레코드에 `updated_at/source_version`이 있어도 후행 무버전 이벤트가 업데이트를 수행한다.
- 근거(문서/코드 경로): `.specs/backoffice_data_project.md` 5.2, 5.6, `src/db/upsert.py`, `src/consumer/processor.py`, `tests/integration/test_db_integration.py`, `.agents/logs/verification/dec207_214/05_lww_rg.log`
- 영향: out-of-order + 부분 필드 이벤트가 섞인 환경에서 상태 역전(regression) 가능성이 남는다.
- 개선안 설계: fallback 적용 전제에 `existing_updated IS NULL` 및 `existing_version IS NULL`을 추가해 "양쪽 모두 metadata 부재"일 때만 `ingested_at` LWW를 허용한다. 동시에 혼합 시나리오(기존=metadata 있음, 신규=metadata 없음) 통합 테스트를 추가한다.
- 구현: `src/db/upsert.py` fallback 조건에 `existing_updated.is_(None)` + `existing_version.is_(None)` 추가. `tests/integration/test_db_integration.py`에 혼합 메타데이터 시나리오(`test_latest_wins_upsert_versioned_not_overwritten_by_unversioned`) 및 양쪽 unversioned 정상 동작(`test_latest_wins_upsert_both_unversioned_uses_ingested_at`) 통합 테스트 추가.
- 검증: L0(py_compile) 통과, L1(단위 테스트) 통과. 통합 테스트는 Docker DB 필요(L1+).
- 재검토 트리거: 업스트림이 전 이벤트에 `updated_at` 또는 `version`을 강제 제공하게 되는 경우.

### DEC-211 갭 5 - 페어링 규칙/회귀 방지 계약 점검

- 상태: **해결됨(구현 완료, 2026-02-12)**
- 확인 결과: complete->incomplete 회귀 차단은 구현되어 있으나, API fallback peer 탐색이 "반대편 엔트리" 제약 없이 동작한다.
- 갭 설명: `src/db/admin_tx.py`의 peer 조회는 `related_id`와 `tx_id != current` 조건만 사용해, 다건 엔트리(예: PAYMENT 2건 이상)에서 반대편이 아닌 행을 선택할 수 있다.
- 근거(문서/코드 경로): `.specs/backoffice_db_admin_api.md` 5.2, `src/db/admin_tx.py`, `src/api/service.py`, `tests/unit/test_api_service.py`, `.agents/logs/verification/dec207_214/06_pairing_rg.log`
- 영향: `sender_wallet_id`/`receiver_wallet_id` 및 `paired_tx_id`가 잘못 계산되어 관리자 조회 정확도가 떨어질 수 있다.
- 개선안 설계: `fetch_admin_tx_context()` peer 조회에 "현재 entry_type의 반대 타입" 필터를 추가하고, 동일 타입 다건일 때를 대비해 정렬 기준(event_time DESC, tx_id)을 명시한다.
- 구현: `src/db/admin_tx.py` peer 조회에 `LedgerEntry.entry_type == opposite_type` 필터 + `.order_by(event_time.desc(), tx_id)` 추가. `tests/unit/test_api_service.py`에 동일 타입 peer 시나리오(`test_build_admin_tx_response_same_type_peer_not_used`) 추가.
- 검증: L0(py_compile) 통과, L1(단위 테스트 33건) 통과. 통합 테스트(다건 peer DB 검증)는 Docker DB 필요(L1+).
- 재검토 트리거: refund/reversal 등으로 pairing 규칙이 PAYMENT/RECEIVE 1:1이 아닌 정책으로 바뀌는 경우.

### DEC-212 갭 6 - 관측성/SLO/알림 계약 점검

- 상태: **해결됨(구현 완료, 2026-02-12)**
- 확인 결과: API/Consumer/DB 기본 메트릭은 존재하지만, 문서/DEC에서 기준으로 참조하는 alert rule 파일이 저장소에 없다.
- 갭 설명: `docker/observability/alert_rules.yml` 경로가 구현체에 없어서 baseline 알림 정책을 코드/운영 설정으로 추적할 수 없다.
- 근거(문서/코드 경로): `.specs/backoffice_project_specs.md` 8, `src/api/observability.py`, `src/common/metrics.py`, `src/consumer/metrics.py`, `src/db/observability.py`, `.agents/logs/verification/dec207_214/07_observability_rg.log`, `.agents/logs/verification/dec207_214/10_alert_rules_presence.log`
- 영향: SLO 위반 시 경보 기준이 문서상 선언만 있고 배포 산출물에서 검증되지 않는다.
- 구현:
  - `docker/observability/alert_rules.yml` 신규 생성 — 플랫폼 무관 참조 YAML + Azure Monitor KQL 스니펫
  - 6개 baseline rule: ApiLatencyHigh(WARNING), ApiErrorRateHigh(WARNING), DataFreshnessHigh(CRITICAL), DlqActivity(WARNING), DbPoolExhausted(WARNING), DbPoolCheckoutSlow(WARNING)
  - DEC-010/DEC-202 임계치와 OTel metric name 1:1 매핑 완료
  - `tests/unit/test_alert_rules.py` — 메트릭명 드리프트 방지 CI 테스트 (6건)
- 검증: L0(py_compile) 통과, L1(단위 테스트) 통과.
- 재검토 트리거: Alerting 플랫폼이 Prometheus rule이 아닌 Azure Monitor Alert Rule로 완전 전환되는 경우.

### DEC-213 갭 7 - 테스트 커버리지 계약 점검

- 상태: **해결됨(테스트 보강 완료, 2026-02-12)**
- 확인 결과: gap으로 식별했던 핵심 회귀 시나리오(403, 혼합 metadata LWW, peer fallback 정확도)가 테스트 세트에 반영되었다.
- 갭 설명(기존): 403 인가 거부(역할 부족), 혼합 metadata LWW 역전 방지, 다건 fallback peer 선택 정확도 시나리오가 현재 테스트 세트에 없었다.
- 근거(문서/코드 경로): `AGENTS.md` Testing strategy, `tests/unit/test_api_routes.py`, `tests/integration/test_db_integration.py`, `tests/unit/test_processor.py`, `.agents/logs/verification/dec207_214/12_test_coverage_rg.log`
- 영향: 권한/정합성 회귀의 조기 탐지 가능성이 높아졌다.
- 구현:
  - `tests/unit/test_api_routes.py`에 403 인가 거부 케이스 추가(`test_get_admin_tx_forbidden_no_matching_role`)
  - `tests/integration/test_db_integration.py`에 혼합 metadata LWW 역전 방지 케이스 추가(`test_latest_wins_upsert_versioned_not_overwritten_by_unversioned`)
  - `tests/unit/test_api_service.py`에 peer fallback 정확도 케이스 추가(`test_build_admin_tx_response_same_type_peer_not_used`)
- 검증 근거: `.agents/logs/verification/dec207_214/17_l1_targeted_unit_venv.log`, `.agents/logs/verification/dec207_214/18_l1_integration_db_venv.log`
- 재검토 트리거: 신규 endpoint 추가 또는 LWW/pairing 조건식 변경 시.

### DEC-214 갭 8 - 문서 참조 무결성/드리프트 점검

- 상태: **결정됨(일치, 2026-02-12)**
- 확인 결과: `.specs/requirements/SRS - Software Requirements Specification.md` 파일이 정상적으로 존재하며, `.specs/backoffice_db_admin_api.md` 및 `.specs/backoffice_project_specs.md`의 참조 경로와 일치한다. 최초 갭 판정은 파일명 공백으로 인한 검증 스크립트 오류(False Positive)였다.
- 근거(문서/코드 경로): `.specs/requirements/SRS - Software Requirements Specification.md`, `.specs/backoffice_db_admin_api.md`, `.specs/backoffice_project_specs.md`, `.agents/logs/verification/dec207_214/09_specs_ls.log`
- 영향: 스키마 관점에서 즉시 수정이 필요한 차이는 없다.
- 재검토 트리거: SRS 저장소를 별도 repo로 분리해 서브모듈/URL 참조 정책이 바뀌는 경우.

### DEC-215 갭 2-추가 - `ADMIN_AUDIT` 역할 적용 범위

- 상태: **결정됨(2026-02-12)**
- 결정: 옵션 C 채택 — `ADMIN_READ|ADMIN_AUDIT` 다중 허용. `auth_required_roles`에 `ADMIN_AUDIT`를 추가해 두 역할 중 하나만 있어도 조회 endpoint 접근을 허용한다. 향후 감사 전용 endpoint 추가 시 액션별 세분화를 검토한다.
- 근거: `src/api/auth.py`(`auth_required_roles` set), `src/api/constants.py`, `tests/unit/test_auth.py`(역할 매트릭스), `tests/unit/test_api_routes.py`(403/AUDIT 케이스)
- 영향: `ADMIN_AUDIT` 단독 역할 사용자도 조회 가능. 스펙과 구현의 역할 정책이 일치.
- 재검토 트리거: 감사 전용 API 또는 감사 데이터 export 기능이 추가되는 시점.

### DEC-216 갭 5-추가 - Consumer 페어링 범위(`related_type`) 제약

- 상태: **해결됨(구현 완료, 2026-02-12)**
- 확인 결과: Consumer는 `related_id`만 있으면 pairing 업데이트를 수행해, `PAYMENT_ORDER` 외 도메인도 pair 테이블에 유입될 수 있다.
- 갭 설명: `upsert_ledger_entry()`에서 `event.related_type` 검사 없이 `update_pairing_for_related_id()`를 호출한다.
- 근거: `.specs/backoffice_project_specs.md` 3.5, `src/consumer/processor.py`, `src/consumer/pairing.py`, `.agents/logs/verification/dec207_214/05_lww_rg.log`
- 영향: `bo.payment_ledger_pairs`의 도메인 순도가 낮아지고, 운영 지표(pair completion rate) 해석이 왜곡될 수 있다.
- 개선안 설계: 호출 조건을 `related_type in {None, PAYMENT_ORDER}`로 제한하고, 명시적 비대상 타입은 skip metric(`pairing_skipped_non_payment_order_total`)으로 분리 관측한다.
- 구현: `src/consumer/processor.py` 가드 조건을 `event.related_id and event.related_type in (None, "PAYMENT_ORDER")`로 변경. `tests/unit/test_processor.py`에 3개 시나리오 추가(`test_upsert_ledger_entry_skips_pairing_for_non_payment_order`, `test_upsert_ledger_entry_pairs_when_related_type_none`, `test_upsert_ledger_entry_pairs_when_related_type_payment_order`).
- 검증: L0(py_compile) 통과, L1(단위 테스트 8건) 통과.
- skip metric: 묶음 D에서 `pairing_skipped_non_payment_order_total` Counter 구현 완료 (`src/consumer/metrics.py`, `src/consumer/processor.py`).
- 재검토 트리거: 멀티 도메인 pairing(`related_type` 확장)을 공식 지원하기로 결정되는 경우.

### DEC-217 갭 6-추가 - DB 관측 지표 커버리지(풀/리플리카)

- 상태: **해결됨(구현 완료, 2026-02-12)**
- 확인 결과: slow query/query latency + pool 상태 + replication lag까지 계측 경로를 갖췄다.
- 갭 설명(기존): `src/db/observability.py`, `src/common/metrics.py`에는 풀 상태/리플리카 지연 계측 항목이 없어 DB 병목 원인 분리가 어려웠다.
- 근거: `.specs/backoffice_project_specs.md` 8.1, `src/db/observability.py`, `src/common/metrics.py`, `.agents/logs/verification/dec207_214/07_observability_rg.log`
- 영향: DB 병목 원인을 API/consumer 지표만으로 분리 진단하기 어렵다.
- 구현:
  - `src/common/metrics.py` — Observable Gauge 4개(`db_pool_size`, `db_pool_checked_out`, `db_pool_overflow`, `db_pool_checked_in`) + Histogram 1개(`db_pool_checkout_latency_seconds`) 추가. `register_pool_engine()` 함수로 엔진 등록.
  - `src/db/session.py` — `get_engine()`에서 `register_pool_engine()` 호출 + `session_scope()`에 checkout latency 계측 추가.
  - `src/common/metrics.py` — Observable Gauge `db_replication_lag_seconds` + `register_replication_lag_provider()` 추가.
  - `src/db/observability.py` — replication lag provider 구현(1차 `pg_stat_replication` 조회, 2차 `pg_last_xact_replay_timestamp()` fallback) 및 엔진 observability 설치 시 provider 등록.
  - `docker/observability/alert_rules.yml` — `DbReplicationLagHigh`(critical, `>10s/5m`) rule 추가.
  - `tests/unit/test_alert_rules.py` — `db_replication_lag_seconds` allowlist 및 `DbReplicationLagHigh` severity 테스트 추가.
  - `tests/unit/test_db_pool_metrics.py` — replication lag callback/provider 단위 테스트 추가.
- 검증: L0(py_compile) 통과, L1(unit) 통과. 실행 인터프리터 차이로 발생한 의존성 이슈는 아래 실행 로그 섹션에 기록.
- 재검토 트리거: Cloud-Secure에서 read-replica 토폴로지/권한 정책이 변경되어 SQL 기반 lag 계산식 조정이 필요한 경우.

### DEC-218 문서-코드 괴리 - SRS FR-ADM-02 구현 상태

- 상태: **해결됨(문서 반영 완료, 2026-02-12)**
- 확인 결과: SRS에는 FR-ADM-02가 `[ ]`로 남아 있었지만, 코드/테스트 기준으로는 구현되어 있었다.
- 갭 설명: 요구사항 구현 현황 표가 실제 구현 상태와 달라 문서 신뢰도를 떨어뜨린다.
- 근거(문서/코드 경로): `.specs/requirements/SRS - Software Requirements Specification.md`, `src/api/main.py`, `tests/e2e/test_admin_tx_e2e.py`
- 영향: FR-ADM-02 진행 상태 커뮤니케이션 혼선 가능.
- 구현: `.specs/requirements/SRS - Software Requirements Specification.md`의 FR-ADM-02 구현 상태를 `[x]`로 갱신.
- 재검토 트리거: FR-ADM-02 범위가 축소/변경되는 경우.

### DEC-219 문서-코드 괴리 - `.specs/project_specs.md` 참조 무결성

- 상태: **해결됨(문서 정정 완료, 2026-02-12)**
- 확인 결과: 여러 스펙 문서가 `.specs/project_specs.md`를 참조하지만 해당 파일이 저장소에 없다.
- 갭 설명: 문서 내부 링크가 끊겨 설계 추적 경로가 단절된다.
- 근거(문서/코드 경로): `.specs/backoffice_project_specs.md`, `.specs/backoffice_db_admin_api.md`, `.specs/backoffice_data_project.md`
- 영향: 신규 참여자가 연계 스펙을 찾지 못해 의사결정/구현 속도가 저하될 수 있다.
- 구현: 3개 문서의 참조를 `.specs/reference/entire_architecture.md`로 일괄 정정.
- 재검토 트리거: Lakehouse 보조 경로 문서를 신규/복구 배치할 때.

### DEC-220 문서-코드 괴리 - `payment_orders.updated_at` 설명 드리프트

- 상태: **해결됨(문서 정정 완료, 2026-02-12)**
- 확인 결과: 문서 일부는 `payment_orders.updated_at` 부재를 전제로 서술하지만, 현재 스키마/모델에는 필드가 존재한다.
- 갭 설명: 스키마 설명이 최신 구현보다 뒤처져 이벤트 계약 가이드를 혼동시킨다.
- 근거(문서/코드 경로): `.specs/backoffice_db_admin_api.md`, `.specs/backoffice_data_project.md`, `src/db/models.py`, `migrations/versions/20260205_0001_create_backoffice_schema.py`
- 영향: 업스트림 이벤트 계약 협의 시 불필요한 “updated_at 부재” 논의가 반복될 수 있다.
- 구현: 두 문서의 관련 문구를 “컬럼은 존재, 이벤트 제공률/표준화는 별도 결정”으로 정정.
- 재검토 트리거: `updated_at`/`version` 강제 정책이 이벤트 계약으로 확정될 때.

### DEC-221 문서-코드 괴리 - 인증/감사 정책과 기본 런타임 동작

- 상태: **해결됨(문서 정정 완료, 2026-02-12)**
- 확인 결과: 스펙은 “모든 요청 인증/인가 + 감사로그”를 요구하나, 기본 설정(`AUTH_MODE=disabled`)에서는 인증이 비활성화될 수 있고, 인증 실패(401/403)는 라우트 진입 전 발생해 DB 감사로그가 남지 않는다.
- 갭 설명: 운영 정책 문구와 실제 기본 런타임/실패 경로 동작 사이에 해석 차이가 있다.
- 근거(문서/코드 경로): `.specs/backoffice_project_specs.md`, `.specs/backoffice_db_admin_api.md`, `configs/env.example`, `src/common/config.py`, `src/api/auth.py`, `src/api/main.py`
- 영향: “모든 요청 감사” 범위를 성공/실패 경로까지 어떻게 볼지 팀 내 해석이 달라질 수 있다.
- 구현:
  - `.specs/backoffice_project_specs.md`에 `local/dev`의 `AUTH_MODE=disabled` 예외를 명시
  - 운영형(Cloud-Secure) 기준 인증/인가 요구사항을 명시
  - `.specs/backoffice_db_admin_api.md`에 “조회 요청은 DB 감사로그, 401/403은 인증 계층 로그”를 명시
- 재검토 트리거: Cloud-Secure 운영 정책 확정 또는 보안 감사 요구사항 강화 시.

### DEC-222 이벤트 계약 - `entry_type` 허용 값 정책

- 상태: **결정됨(2026-02-13)**
- 결정: 이벤트 계약에서 `entry_type`은 **`PAYMENT`/`RECEIVE`만 필수 계약으로 명시**한다. 그 외 값(`CHARGE`, `WITHDRAW`, `REFUND_IN` 등)은 자유 텍스트로 허용하고, Consumer는 저장만 수행한다.
- 근거: 현재 코드에서 `entry_type`에 의존하는 로직은 페어링(`src/consumer/pairing.py`, `src/api/service.py`)뿐이며, 페어링은 `PAYMENT`/`RECEIVE`만 사용한다. 나머지 값은 `bo.ledger_entries`에 원문 저장되지만 비즈니스 로직에 영향을 주지 않는다. Databricks Ledger Pipelines에서 사용하는 10종(`CHARGE, RECEIVE, PAYMENT, WITHDRAW, REFUND_IN, REFUND_OUT, MINT, BURN, HOLD, RELEASE`)과의 조직 전체 통일은 별도 합의가 필요하므로, 현 단계에서는 최소 계약만 확정한다.
- 영향: 업스트림이 기존 `entry_type` 값을 변경할 필요 없이 `PAYMENT`/`RECEIVE`만 올바르게 태깅하면 페어링이 동작한다. 매핑 안 되는 값은 `pairing_status=UNKNOWN`으로 처리된다.
- 재검토 트리거: Databricks 쪽과 `entry_type` vocabulary 통일 합의가 이루어질 때, 또는 페어링 외 로직에서 특정 `entry_type`을 분기 처리해야 할 때.

### DEC-223 이벤트 계약 - `status` 허용 값 정책

- 상태: **결정됨(2026-02-13)**
- 결정: `payment_orders.status`는 **자유 텍스트로 허용**한다. Consumer는 원문을 그대로 저장하고, API는 v1 매핑표(DEC-206)로 `status_group`을 계산한다. 매핑 안 되는 값은 `UNKNOWN`으로 그룹핑된다.
- 근거: 현재 `src/api/service.py`의 `_resolve_status_group()`이 이미 이 방식으로 동작한다. `status` 원문은 `bo.payment_orders.status` 필드에 항상 보존되므로 관리자가 원문 확인 가능하다. 업스트림 status taxonomy가 확정되지 않은 상태에서 값을 강제하면 불필요한 변환 부담이 생긴다.
- 영향: 업스트림이 자체 status 값(예: `APPROVED`, `REFUNDING`)을 그대로 보내도 Consumer 처리에 문제없다. `UNKNOWN` 그룹이 많아지면 v1 매핑표에 값을 추가하면 된다(코드 변경: `_resolve_status_group()` 1곳).
- 재검토 트리거: 업스트림 status taxonomy가 공식 확정되어 v2 매핑표를 정의할 때, 또는 `UNKNOWN` 비율이 운영 SLO를 위반할 때.

### DEC-224 이벤트 계약 - 삭제/취소 이벤트 정책

- 상태: **결정됨(2026-02-13)**
- 결정: **삭제(tombstone) 이벤트는 지원하지 않는다.** 원장 거래의 취소/환불은 별도 `entry_type`(예: `REFUND_IN`/`REFUND_OUT`)의 새 이벤트로 표현한다. 결제 주문의 취소는 `status` 변경 이벤트(예: `status=CANCELLED`)로 처리하며, 기존 upsert 경로에서 자연스럽게 반영된다.
- 근거: 금융 원장(ledger)은 삭제 불가가 원칙이며, 취소/수정은 역분개(reversal) 이벤트로 처리하는 것이 복식부기의 기본 설계다. 현재 코드는 upsert 전용(`src/db/upsert.py`)이며, delete 경로를 추가하면 페어링 상태 연쇄 업데이트, 감사로그 정합성 등 복잡도가 크게 증가한다. 테스트 데이터 정리 등 운영 목적의 삭제가 필요하면 DB 직접 조작(운영 스크립트)으로 처리한다.
- 영향: 업스트림은 삭제 이벤트를 발행할 필요가 없다. 취소된 거래도 Backoffice DB에 남으며, `status_group=FAIL`로 조회된다. 관리자는 취소된 거래의 이력을 추적할 수 있다.
- 재검토 트리거: GDPR 등 규제로 개인정보 포함 거래의 물리적 삭제가 요구되는 경우, 또는 운영 요구에 의해 soft delete 상태 관리가 필요해지는 경우.

## DEC-207~217 의존성 작업 묶음

### 묶음 A - 정책/참조 정합성 선행 ✓ 완료

- 포함 DEC: `DEC-214`(일치, False Positive 종결), `DEC-215`(옵션 C 채택, 구현 완료)
- 산출: DEC-214 SRS 참조 경로 정상 확인, DEC-215 `ADMIN_READ|ADMIN_AUDIT` 다중 허용 + 역할 매트릭스 테스트 추가

### 묶음 B - 데이터 정합성 핵심 로직 ✓ 완료

- 포함 DEC: `DEC-210`, `DEC-211`, `DEC-216`
- 선행 의존성: 묶음 A
- 선행 이유: LWW/페어링/related_type 범위가 API 응답 정확도와 운영 지표의 기준값을 결정한다.
- 산출:
  - DEC-210: `src/db/upsert.py` fallback에 `existing_updated/version IS NULL` 가드 추가 + 통합 테스트 2건
  - DEC-211: `src/db/admin_tx.py` peer 쿼리에 `entry_type` 필터 + `ORDER BY` 추가 + 단위 테스트 1건
  - DEC-216: `src/consumer/processor.py` 가드 조건을 `related_type in {None, PAYMENT_ORDER}`로 변경 + 단위 테스트 3건

### 묶음 C - API/감사 인터페이스 ✓ 완료

- 포함 DEC: `DEC-207`, `DEC-208`
- 선행 의존성: 묶음 A, 묶음 B
- 선행 이유: endpoint 확장/감사 필드(`result_count`)는 역할 정책과 데이터 정합성 규칙이 고정된 뒤 설계해야 재작업이 줄어든다.
- 산출:
  - DEC-208: `bo.admin_audit_logs`에 `result_count` nullable int 컬럼 추가 (migration `20260212_0005`) + `build_audit_fields()` 파라미터 확장 + 기존/신규 라우트에서 건수 기록
  - DEC-207: `GET /admin/payment-orders/{order_id}` + `GET /admin/wallets/{wallet_id}/tx?from=&to=&limit=` 구현. 스키마 4개(`LedgerEntryItem`, `PaymentOrderDetail`, `AdminOrderResponse`, `AdminWalletTxResponse`), DB 쿼리 2개, 서비스 함수 3개, 라우트 2개, 단위 테스트 6건 + 서비스 테스트 5건 + 통합 테스트 8건 추가

### 묶음 D - 관측/알림 운영화 ✓ 완료

- 포함 DEC: `DEC-212`, `DEC-217`
- 선행 의존성: 묶음 B, 묶음 C
- 선행 이유: 메트릭/알림 설계는 최종 API/DB 동작과 role/pairing 정책을 반영해야 이름/임계치 드리프트를 줄일 수 있다.
- 산출:
  - DEC-212: `docker/observability/alert_rules.yml` 신규 생성 — 6 baseline alert rule(KQL 스니펫 포함) + `tests/unit/test_alert_rules.py` 메트릭명 드리프트 방지 테스트
  - DEC-217: `src/common/metrics.py`에 Observable Gauge 5개(풀 4 + replication lag 1) + Histogram 1개 + `register_pool_engine()`/`register_replication_lag_provider()`, `src/db/session.py` checkout latency 계측, `src/db/observability.py` replication lag provider, `tests/unit/test_db_pool_metrics.py`/`tests/unit/test_alert_rules.py` 회귀 테스트
  - DEC-216 skip metric: `pairing_skipped_non_payment_order_total` Counter 구현 (`src/consumer/metrics.py`, `src/consumer/processor.py`)

### 묶음 E - 검증 체계 마감

- 포함 DEC: `DEC-213`
- 선행 의존성: 묶음 B, 묶음 C, 묶음 D
- 선행 이유: 누락 테스트는 앞선 묶음의 확정 설계를 반영해 한 번에 회귀셋으로 고정해야 한다.
- 주요 산출: 403 권한 테스트, 혼합 metadata LWW 테스트, 다건 peer fallback 테스트, 관측성 회귀 테스트

### 독립 트랙

- `DEC-209`는 현재 **일치 판정(갭 없음)** 으로 독립 관리한다.
- 단, 묶음 C/D에서 스키마 변경(`admin_audit_logs` 확장 등)이 발생하면 `DEC-209`에 재검토 링크를 남긴다.

### 권장 수행 순서

1. 묶음 A
2. 묶음 B
3. 묶음 C
4. 묶음 D
5. 묶음 E

## DEC-207~214 실행 검증 결과 (2026-02-12)

- 증빙 로그 경로: `.agents/logs/verification/dec207_214/`
- 검증 실행 기준: 프로젝트 가상환경 인터프리터(`.venv/bin/python`) 사용
- 증빙 수집 명령 실행: 완료 (`01`~`12` 로그)
- 테스트 실행 결과:
  - (참고/비표준 실행) 시스템 인터프리터 `pytest`로 단위 대상 테스트 실행 시 실패
  - 실패 사유: `ModuleNotFoundError: No module named 'fastapi'`
  - 로그: `.agents/logs/verification/dec207_214/13_unit_targeted_pytest.log`
  - (참고/비표준 실행) 시스템 인터프리터 `pytest`로 통합 대상 테스트 실행 시 실패
  - 실패 사유: `ModuleNotFoundError: No module named 'sqlalchemy'`
  - 로그: `.agents/logs/verification/dec207_214/14_integration_db_pytest.log`
  - `.venv/bin/python -m pytest tests/unit/test_api_routes.py tests/unit/test_api_service.py tests/unit/test_auth.py tests/unit/test_processor.py tests/unit/test_pairing.py -x` 성공
  - 로그: `.agents/logs/verification/dec207_214/17_l1_targeted_unit_venv.log`
  - `.venv/bin/python -m pytest tests/integration/test_db_integration.py -x` 성공
  - 로그: `.agents/logs/verification/dec207_214/18_l1_integration_db_venv.log`
- L0 검증:
  - `.venv/bin/python -m py_compile $(find src -name '*.py')` 성공
  - 로그: `.agents/logs/verification/dec207_214/19_l0_py_compile_venv.log` (기존 참고: `15_l0_py_compile.log`)

## 묶음 D 후속(DEC-217) 실행 검증 결과 (2026-02-12)

- 증빙 로그 경로: `.agents/logs/verification/bundle_d_followup_20260212/`
- 검증 실행 기준: 프로젝트 가상환경 인터프리터(`.venv/bin/python`) 사용
- L0 검증:
  - `.venv/bin/python -m py_compile $(find src -name '*.py')` 성공
  - 로그: `16_l0_py_compile_venv.log` (기존 참고: `01_l0_py_compile.log`, `13_l0_py_compile_final.log`)
- L1 검증:
  - (참고/비표준 실행) 시스템 인터프리터 `pytest`로 대상 단위 테스트 실행 시 실패
  - 실패 사유: 실행 인터프리터 환경(`pytest` 커맨드, Python 3.12)에서 `opentelemetry` 미설치
  - 로그: `02_l1_targeted_pytest.log`, `04_l1_targeted_pytest_after_install.log`
  - `.venv/bin/python -m pytest tests/unit/test_alert_rules.py tests/unit/test_db_pool_metrics.py tests/unit/test_config.py -x` 성공
  - 로그: `14_l1_targeted_venv.log` (기존 참고: `05_l1_targeted_python_pytest.log`, `11_l1_targeted_after_ruff_fix.log`)
  - `.venv/bin/python -m pytest tests/unit/ -x` 성공
  - 로그: `15_l1_unit_full_venv.log` (기존 참고: `06_l1_unit_full.log`, `12_l1_unit_full_after_ruff_fix.log`)
- 정적 점검:
  - 변경 파일 대상 `ruff check` 성공
  - 로그: `10_ruff_touched_after_fix.log`
