# Cutover Preflight / Exit Criteria Template

Last updated: 2026-02-11  
Reference: `.specs/cloud/cloud_migration_rebuild_plan.md`

## 1. Cutover Metadata

- `T_cutover_utc`:
- Operator:
- Source env:
- Target env:
- Change window:

## 2. Preflight Checklist

- [ ] 구독/RG/워크스페이스/스토리지 이름 재확인
- [ ] UC 권한(`USE CATALOG`, `USE SCHEMA`, `SELECT`, `MODIFY`) 검증
- [ ] Secret 주입 상태(`salt`, 토큰) 검증
- [ ] 파이프라인 파라미터(`run_mode`, `start_ts`, `end_ts`, `run_id`) 확정
- [ ] 기존 Workflows 정지/재개 순서 확정
- [ ] Bootstrap job(`bootstrap_catalog`) 배포 완료 확인
- [ ] `sync_dim_rule_scd2` job 배포 + 실행 경로 확인
- [ ] `pipeline_silver_materialization` job 배포 + 스케줄/의존성 확인
- [ ] `gold.dim_rule_scd2` row 수/`is_current` 유효성(동일 domain+metric당 1건) 검증
- [ ] Pipeline B 동일 `run_id` 재실행 검증 계획 수립(`exception_ledger` 6-key 기준)
- [ ] 모니터링 범위 확인(`v1 Core 4` 활성, table-based alert 비활성)

## 3. Execution Checklist

- [ ] 신규 환경 인프라/UC 준비 완료
- [ ] `bootstrap_catalog` 실행: 3 스키마 + 23 계약 테이블 생성 확인
- [ ] `sync_dim_rule_scd2` 실행: `gold.dim_rule_scd2` 최신 seed 반영 확인
- [ ] Pipeline A backfill 실행
- [ ] Pipeline Silver backfill 실행
- [ ] Pipeline B/C backfill 실행
- [ ] `T_cutover_utc` 이후 incremental 전환
- [ ] 기존 환경 Workflows 중지
- [ ] 신규 환경 Workflows 활성화

## 4. Exit Criteria

| Stage | Exit Criteria | Status | Notes |
|---|---|---|---|
| Infra/UC | External Location 접근 성공 |  |  |
| Pipeline A | `silver.dq_status` 생성, CRITICAL 급증 없음 |  |  |
| Pipeline Silver | `silver.wallet_snapshot/ledger_entries/order_events/order_items/products` 생성 + `silver.bad_records` 적재/검증 완료 |  |  |
| Pipeline B/C | Gold 핵심 테이블 생성 완료 |  |  |
| Rule SSOT | `gold.dim_rule_scd2` 존재 + current rule 유효성 통과 |  |  |
| Pipeline B Rerun | 동일 `run_id` 재실행 성공 + `exception_ledger` 6-key 중복 0건 |  |  |
| Incremental | `T_cutover_utc` 이후 누락/중복 없음 |  |  |
| State | `gold.pipeline_state.last_run_id` 최신 반영 |  |  |
| Monitoring | `v1 Core 4` 경보 규칙 활성 + table-based alert 비활성 확인 |  |  |

## 5. Rollback Trigger

- [ ] 연속 CRITICAL로 B/C 게이팅 지속
- [ ] Gold 핵심 테이블 미생성/갱신 실패
- [ ] 누락/중복이 허용 기준 초과

## 6. Evidence Links

- Verification logs:
- Query evidence:
- Incident tickets:
