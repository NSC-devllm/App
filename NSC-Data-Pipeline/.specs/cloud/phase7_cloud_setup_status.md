# Phase 7 Cloud Setup Baseline and Verification Status (Dev Minimal)

Last updated: 2026-02-12

## 1. 목적과 문서 성격

이 문서는 Phase 7 dev minimal 구성의 "레포 기준 베이스라인"과 "실클라우드 검증 상태"를 함께 관리한다.
실제 Azure/Databricks 자원 상태의 최종 SSOT는 audit 증적이며, 본 문서의 값은 증적 확보 전까지 확정값이 아니다.

## 2. 기준 소스(레포 기준)

### 2.1 `configs/dev.yaml` 기준값

- `databricks.workspace_host`: `https://adb-1234567890123456.7.azuredatabricks.net`
- `databricks.workspace_id`: `57710310442212`
- `databricks.catalog`: `2dt_final_team4_databricks_test`
- `databricks.cluster_policy_id`: `00023C6641CF09E4`
- `storage.external_location`: `team4_adls_test`
- `storage.base_path`: `abfss://2dt-final-team4-adls-test@2dtfinalteam4storagetest.dfs.core.windows.net`
- `analytics.secret_scope`: `ledger-analytics-dev`

### 2.2 `configs/common.yaml` 기준값

- `analytics.secret_key`: `salt_user_key`
- `workflows.max_retries`: `2`
- `workflows.retry_interval_seconds`: `300`

### 2.3 `scripts/phase7/setup_minimal_cloud.sh` 기본값

- `EXT_LOCATION_NAME`: `team4_adls_test`
- `STORAGE_CREDENTIAL_NAME`: `2dt_final_team4_databricks_test`
- `SCOPE_NAME`: `ledger-analytics-dev`
- `SCOPE_KEY`: `salt_user_key`
- `PIPELINE_A_JOB_ID`: `57710310442212`
- `POLICY_NAME`: `data-pipeline-single-node-dev`
- policy 정의 기준 runtime: `13.3.x-scala2.12`

## 3. 실클라우드 검증 상태 (2026-02-12 기준)

- 2026-02-09 선폐기 이후 dev 테스트 리소스의 재생성/현재 식별값은 실측 기준으로 재확정이 필요하다.
- 따라서 2장의 값은 운영 적용 "기준값"이며, "실측 확정값"이 아니다.
- audit 로그 예시:
  - `.agents/logs/verification/20260206_phase7_audit.log`
  - `.agents/logs/verification/20260209_phase7_audit_for_deletion_check.txt`

## 4. 항목별 상태 매트릭스

| 항목 | 레포 기준(출처) | 실클라우드 검증 상태 | 검증 방법 |
|---|---|---|---|
| Workspace Host/ID | `configs/dev.yaml` | 검증 필요 | `scripts/phase7/audit_cloud_state.sh` 출력의 `workspace_url`, `workspace_name` 확인 |
| Catalog/Schema | `configs/dev.yaml` + `setup_minimal_cloud.sh` | 검증 필요 | UC catalog/schema 조회 + audit 로그 첨부 |
| External Location/Base Path | `configs/dev.yaml` + `setup_minimal_cloud.sh` | 검증 필요 | external location 조회/경로 접근 확인 |
| Secret Scope/Key | `configs/dev.yaml`, `configs/common.yaml`, `setup_minimal_cloud.sh` | 검증 필요 | scope/key 조회 및 접근 테스트 |
| Cluster Policy/Pipeline A Job | `configs/dev.yaml` + `setup_minimal_cloud.sh` | 검증 필요 | policy/job 조회 + retry/email 설정 확인 |

## 5. ADLS 경로 규칙(레포 정책)

`storage.base_path` 아래 prefix 규칙:

- `bronze/<table_name>/date_kst=YYYY-MM-DD/`
- `silver/<table_name>/date_kst=YYYY-MM-DD/`
- `gold/<table_name>/date_kst=YYYY-MM-DD/`
- `quarantine/<table_name>/date_kst=YYYY-MM-DD/`
- `checkpoints/<pipeline_name>/`

참고:
- 일부 테이블이 UC managed storage를 사용하더라도, 신규/이관 경로는 external location prefix 기준으로 검증한다.

## 6. 증적 저장 규칙

- audit 실행 결과는 반드시 `.agents/logs/verification/`에 저장한다.
- 권장 명명: `YYYYMMDD_phase7_audit_<purpose>.log`
- 문서 갱신 시, 최신 audit 파일명을 본문(3장) 또는 관련 의사결정 근거에 함께 반영한다.

예시:

```bash
scripts/phase7/audit_cloud_state.sh \
  > .agents/logs/verification/20260212_phase7_audit_rebuild_check.log
```

## 7. 남은 작업(Phase 7 관점)

- 서비스 프린시플 기반 실행 주체/권한 모델 확정 및 적용
- Slack 알림 채널 연동(현재 email only)
- Key Vault 연동 Secret Scope 전환(재구축 단계)

## 8. 재현/점검 스크립트

- 설정 적용: `scripts/phase7/setup_minimal_cloud.sh`
- 상태 점검: `scripts/phase7/audit_cloud_state.sh`
