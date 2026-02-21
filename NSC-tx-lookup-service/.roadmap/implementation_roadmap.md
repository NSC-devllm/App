# F/E 축 기반 구현 로드맵 (SSOT 파생)

Last updated: 2026-02-12

> 본 문서는 SSOT 문서 기준으로 현재 구현 상태를 요약한 파생 로드맵이다.
> 기존 `Phase 1~11` 참조는 본 개정에서 폐기한다.

## 1) 기준 문서 / 판정 규칙

### 1.1 기준 문서
- `.specs/backoffice_project_specs.md`
- `.specs/backoffice_db_admin_api.md`
- `.specs/backoffice_data_project.md`
- `.specs/infra/cloud_migration_rebuild_plan.md`
- `.specs/decision_open_items.md`
- `.specs/archive/cloud_phase8_execution_report.md`
- `README.md`

### 1.2 판정 규칙
- 상태는 `COMPLETE`, `IN PROGRESS`, `NOT STARTED` 3단계로만 표기한다.
- 상태 근거는 SSOT 문서 + 코드/테스트 증빙 + DEC 결과를 우선 적용한다.
- 기준 시점은 워킹트리 문서 상태(2026-02-12)다.
- 일정 날짜/스프린트는 두지 않고, 게이트(진입/완료 조건) 기준으로만 관리한다.

## 2) 상태 스냅샷

| Stage | Status | 근거 문서 |
| --- | --- | --- |
| F1 | COMPLETE | `.specs/backoffice_project_specs.md` 9.1, `src/api/main.py`, `tests/e2e/test_admin_tx_e2e.py` |
| F2 | COMPLETE | `.specs/backoffice_project_specs.md` 9.1, `.specs/decision_open_items.md` DEC-211/216, `src/consumer/pairing.py` |
| F3 | IN PROGRESS | `.specs/backoffice_project_specs.md` 9.1, `.specs/backoffice_project_specs.md` 11, `.specs/backoffice_data_project.md` 11 |
| E1 | COMPLETE | `.specs/backoffice_project_specs.md` 9.2, `.specs/archive/cloud_phase8_execution_report.md`, `.specs/infra/cloud_migration_rebuild_plan.md` 3.1 |
| E2 | NOT STARTED | `.specs/backoffice_project_specs.md` 9.2, `.specs/infra/cloud_migration_rebuild_plan.md` 3.2(Stage B) |
| E3 | NOT STARTED | `.specs/backoffice_project_specs.md` 9.2, `.specs/infra/cloud_migration_rebuild_plan.md` 3.2(Stage C) |

## 3) 완료 단계 요약 (짧게)

### F1 — COMPLETE
- Backoffice DB + Admin API read path가 운영 기준으로 동작한다.
- API 기준선 3종(`GET /admin/tx/{tx_id}`, `GET /admin/payment-orders/{order_id}`, `GET /admin/wallets/{wallet_id}/tx`)이 구현되어 있다.
- 감사로그 `bo.admin_audit_logs.result_count` 반영까지 완료됐다.

### F2 — COMPLETE
- `bo.payment_ledger_pairs` 기반 페어링 경로와 품질 보정(반대 타입 필터, 회귀 방지, 범위 제한)이 적용됐다.

### E1 — COMPLETE
- Cloud-Test(E1) E2E 스모크 및 재생성 검증이 완료됐고 결과가 아카이브 문서에 정리돼 있다.

## 4) 미완료 단계 실행 백로그 (대형 태스크 + 세부 태스크)

### F3 — IN PROGRESS

#### 대형 태스크 F3-1: 상태/버전 이벤트 계약 표준화
- [ ] `payment_orders.status` 운영 표준 집합(v2) 확정 및 `status_group` 매핑표 동결
- [ ] 토픽별 필수 메타(`updated_at` 또는 `version`) 제공률 기준 합의
- [ ] `configs/topic_checklist.md` 계약 문구 갱신 및 업스트림 전달 기록
- [ ] 표준화 결과를 `.specs/backoffice_project_specs.md`/`.specs/backoffice_data_project.md`에 반영

#### 대형 태스크 F3-2: SLO 알림 규칙 운영 적용
- [ ] `docker/observability/alert_rules.yml`의 규칙을 실제 알림 플랫폼(Azure Monitor) 규칙으로 이식
- [ ] `api_request_latency_seconds`, `consumer_freshness_seconds`, `db_pool_*`, `db_replication_lag_seconds` 대시보드 연결
- [ ] 임계치 튜닝(오탐/미탐 점검)과 severity 운영안 확정
- [ ] 알림 발생 -> 조치 -> 복구까지 운영 증빙 로그 확보

#### 대형 태스크 F3-3: 품질 게이트 마감
- [ ] L2 게이트(`.venv/bin/python -m pytest --cov-fail-under=80`) 정기 통과 상태 확보
- [ ] 페어링/상태 변경 관련 L3 스모크 기준 시나리오 확정
- [ ] F3 완료 판정 체크리스트(계약, 알림, 테스트) 문서화

### E2 — NOT STARTED

#### 대형 태스크 E2-1: Cloud-Secure 리소스 준비(보안형)
- [ ] 인프라팀에 서비스 전용/공유 리소스 요청서 제출(PostgreSQL 전용 + AKS/ACR/Key Vault 공유 모델)
- [ ] 네트워크 요구사항(Private Endpoint/VNet/Firewall) 상세 명세 확정
- [ ] 리소스 네이밍/태그/소유권 검증 체크리스트 작성

#### 대형 태스크 E2-2: 시크릿/권한 전환
- [ ] SAS/env 기반 전달에서 Key Vault + Managed Identity 방식으로 전환 설계
- [ ] 최소권한 RBAC 매트릭스(DB/Event Hubs/AKS/Key Vault) 확정
- [ ] 접근 실패(401/403/권한오류) 관측 경로와 감사 경로 분리 정의

#### 대형 태스크 E2-3: 재적재/컷오버 리허설
- [ ] `alembic upgrade head -> backfill -> consumer sync` 순서 리허설
- [ ] `GET /admin/tx/{tx_id}` 200/404 스모크 + 멱등 재처리 검증
- [ ] 컷오버 시점(`T_cutover`) 및 롤백 의사결정 기준 문서화

#### 대형 태스크 E2-4: 승인 게이트
- [ ] 보안 통제 체크(네트워크/시크릿/권한) 통과 증빙 확보
- [ ] 데이터 신선도/API 지연 SLO 재검증 결과 확보
- [ ] E2 완료 승인 레코드(결재/리뷰 로그) 저장

### E3 — NOT STARTED

#### 대형 태스크 E3-1: CI 게이트 구축
- [ ] PR 게이트에 lint + unit + integration + coverage(>=80) 구성
- [ ] `.venv` 기준 검증 명령 고정 및 실패 시 로그 아카이브 자동화
- [ ] 계약 드리프트 방지 테스트(`tests/unit/test_alert_rules.py` 등) 상시 실행 편입

#### 대형 태스크 E3-2: CD 파이프라인 구축
- [ ] build/scan -> migration -> deploy -> smoke 단계 정의
- [ ] 배포 중단 조건(마이그레이션 실패, 스모크 실패)과 자동 롤백 기준 정의
- [ ] Cloud-Test 통과 결과를 Cloud-Secure 반영 게이트로 연동

#### 대형 태스크 E3-3: 운영 복구 체계 운영화
- [ ] DLQ replay/runbook/backfill 절차를 운영 문서로 고정
- [ ] 장애/데이터 불일치 대응 체크리스트와 책임자/연락 체계 명시
- [ ] 정기 복구 훈련(리플레이/재동기화) 일정 및 증빙 방식 확정

#### 대형 태스크 E3-4: 이벤트 계약 테스트 자동화
- [ ] 토픽 필수 필드 계약 테스트를 CI 파이프라인에 통합
- [ ] 계약 불일치 시 fail-fast 규칙과 알림 채널 정의
- [ ] 업스트림 계약 변경 시 추적 가능한 변경 이력 정책 확정

## 5) 미완료 단계 최소 완료 게이트

| Stage | 최소 완료 게이트 |
| --- | --- |
| F3 (IN PROGRESS) | 상태/버전 이벤트 계약 표준화 완료 + 알림 규칙 운영 적용 증빙 + L2/L3 검증 체계 고정 |
| E2 (NOT STARTED) | Cloud-Secure 리소스/보안 통제 준비 완료 + 재적재/스모크 리허설 완료 + 컷오버/롤백 기준 승인 |
| E3 (NOT STARTED) | CI/CD 자동화 파이프라인 동작 + 운영 복구 체계 문서/훈련 완료 + 계약 테스트 CI 상시화 |

## 6) 검증 증빙 경로

- DEC 통합 검증: `.agents/logs/verification/dec207_214/`
- 관측성 후속 검증: `.agents/logs/verification/bundle_d_followup_20260212/`
- 본 로드맵 개정 검증 로그:
  - `.agents/logs/verification/20260212_fe_roadmap_refresh_l0_py_compile.log`
  - `.agents/logs/verification/20260212_fe_roadmap_refresh_structure_check.log`
  - `.agents/logs/verification/20260212_fe_roadmap_enhance_l0_py_compile.log`
  - `.agents/logs/verification/20260212_fe_roadmap_enhance_structure_check.log`
