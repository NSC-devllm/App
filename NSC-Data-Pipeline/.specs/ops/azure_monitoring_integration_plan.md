# Azure Monitoring Integration Plan (Databricks Pipelines)

Last updated: 2026-02-11

SSOT:
- 본 문서는 모니터링 범위와 알림 정책의 최상위 SSOT다.
- 관련 결정 로그는 `.specs/decision_open_items.md`의 D-037, D-022, D-024를 따른다.

## 1. Goal

- 목적: Azure Monitor + Log Analytics만으로 즉시 구현 가능한 운영 관측을 먼저 구축한다.
- 범위: 초기에는 Pipeline A/B/C 실행 안정성(잡/클러스터/재시도/지연) 중심으로 운영한다.
- 원칙: 테이블 데이터 export 없이 가능한 항목만 우선 구현한다.

## 2. In-Scope (Now)

1. Databricks 진단 로그 기반 실행 관측
2. Azure Activity Log 기반 플랫폼 이벤트 관측
3. A/B/C 통합 대시보드 1개(Workbook)
4. 실행 안정성 알림 규칙(Core Execution Alerts 4종)
   - Job 실패
   - 최근 성공 지연
   - 재시도 소진
   - 클러스터 시작 실패/타임아웃

## 3. Out-of-Scope (Now)

다음 항목은 현재 단계에서 Azure Monitor 단독 구현 대상에서 제외한다.

1. `silver.dq_status` 기반 KPI/알림
2. `gold.exception_ledger` row 기준 CRITICAL 집계 알림
3. `gold.pipeline_state` row 기반 `last_success_ts` 정확 집계
4. `SOURCE_STALE`/`EVENT_DROP_SUSPECTED`의 데이터 기반 억제 로직

비고:
- 위 항목은 v2 scope에서 Databricks SQL Alert 또는 별도 export job 도입 시 확장한다.

## 4. KPI v1 (Log Analytics Only)

1. Pipeline별 성공 실행 건수 (`success_count`)
2. Pipeline별 실패 실행 건수 (`failure_count`)
3. Pipeline별 실패율 (`failure_rate_24h`)
4. 재시도 발생/소진 건수 (`retry_count`, `retry_exhausted_count`)
5. 실행 시간 분포 (`duration_p50/p95`)
6. 타임아웃/취소 건수 (`timeout_count`, `cancel_count`)
7. 클러스터 기동 실패/인프라 실패 건수
8. 최근 성공 지연(로그 이벤트 기준)

## 5. Alert v1 (Core Execution Alerts)

1. Job 실패 1회 즉시 P1
2. 최근 성공 지연
   - Pipeline A: 20분 이상
   - Pipeline B/C: 스케줄 기준 + 30분
3. 재시도 소진(최대 재시도 후 실패) P1
4. 클러스터 시작 실패/타임아웃 P1

환경 정책:
- dev/prod 공통 규칙 프레임 유지
- dev는 채널 강도만 낮게 운영(노티 최소화)

## 6. Dashboard v1 (Single Workbook)

초기 구성(대상: A/B/C):

1. Health Summary: 최근 24h 성공/실패/실패율
2. Timeline: 파이프라인별 실행 결과 시계열
3. Retry & Timeout: 재시도/타임아웃 추이
4. Runtime: p50/p95 실행시간 추이
5. Infra Errors: 클러스터/플랫폼 오류 추이

## 7. Execution Plan

### Phase M1: Signal Wiring

1. Databricks 진단 로그 -> Log Analytics 연결
2. A/B/C 리소스 매핑 확정
3. dev/prod 공통 명명 규칙 고정

### Phase M2: Alert Setup

1. Alert v1 규칙 4개 생성
2. Action Group 연결(Severity + Owner 라우팅)
3. 테스트 알람 1회 발화 확인

### Phase M3: Dashboard Setup

1. Workbook v1 생성
2. 운영팀 공유 및 읽기 권한 부여
3. `operations_runbook`에 대시보드 링크/경보 ID 반영

### Phase M4: Validation

1. 월 1회 game-day (의도적 실패/지연 시나리오)
2. 임계치 노이즈 점검 후 조정
3. 비용/보존 정책 점검

## 8. Expansion Backlog (Later = v2 Scope)

1. D-022(v2): 테이블 기반 모니터링 신호 확장
   - `gold.pipeline_state`
   - `silver.dq_status`
   - `gold.exception_ledger`
2. D-024(v2): 테이블 기반 데이터 품질 알림 확장
   - `DQ CRITICAL` 알림
   - `SOURCE_STALE`/`EVENT_DROP_SUSPECTED` 지속 알림
3. Databricks SQL Alert로 DQ KPI(`dq_status`, `exception_ledger`) 추가
4. 필요 시 테이블 KPI 일부를 Log Analytics로 export하는 경량 job 도입
5. 팀원 워크플로우 온보딩(A/B/C 이후)
6. RBAC 역할 분리(Contributor -> Admin/Operator/Viewer)
