# Databricks E2E Smoke Skeleton

`tests/e2e/test_databricks_smoke_skeleton.py`는 Databricks Dev 워크스페이스에서
Pipeline A/B/C Job을 트리거하는 최소 스켈레톤이다.

## Required Environment Variables

```bash
export DATABRICKS_HOST="https://<workspace-host>"
export DATABRICKS_TOKEN="<pat-token>"
export DATABRICKS_PIPELINE_A_JOB_ID="<job-id-a>"
export DATABRICKS_PIPELINE_B_JOB_ID="<job-id-b>"
export DATABRICKS_PIPELINE_C_JOB_ID="<job-id-c>"
```

## Execute

```bash
python -m pytest tests/e2e/test_databricks_smoke_skeleton.py -v -m e2e
```

## Notes

- 실제 검증 로그는 `.agents/logs/verification/e2e_smoke_*.txt`에 저장된다.
- 스켈레톤은 `run-now` 트리거만 수행한다.
- 완료 대기/결과 판정은 운영 단계에서 확장한다.
