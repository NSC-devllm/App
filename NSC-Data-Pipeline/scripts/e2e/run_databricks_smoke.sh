#!/usr/bin/env bash
set -euo pipefail

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "missing required env: ${name}" >&2
    exit 1
  fi
}

require_env "DATABRICKS_HOST"
require_env "DATABRICKS_TOKEN"
require_env "DATABRICKS_PIPELINE_A_JOB_ID"
require_env "DATABRICKS_PIPELINE_B_JOB_ID"
require_env "DATABRICKS_PIPELINE_C_JOB_ID"

if ! command -v databricks >/dev/null 2>&1; then
  echo "databricks CLI not found in PATH" >&2
  exit 1
fi

SMOKE_WAIT="${SMOKE_WAIT:-0}"
SMOKE_TIMEOUT_MINUTES="${SMOKE_TIMEOUT_MINUTES:-30}"
SMOKE_POLL_SECONDS="${SMOKE_POLL_SECONDS:-20}"
SMOKE_RUN_MODE="${SMOKE_RUN_MODE:-incremental}"
SMOKE_RUN_ID="${SMOKE_RUN_ID:-}"
SMOKE_DATE_KST_START="${SMOKE_DATE_KST_START:-}"
SMOKE_DATE_KST_END="${SMOKE_DATE_KST_END:-}"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
log_file=".agents/logs/verification/e2e_smoke_${timestamp}.txt"
mkdir -p ".agents/logs/verification"

if [[ "${SMOKE_WAIT}" != "0" ]]; then
  if ! command -v jq >/dev/null 2>&1; then
    echo "jq not found in PATH (required when SMOKE_WAIT is enabled)" >&2
    exit 1
  fi
fi

wait_for_run() {
  local pipeline="$1"
  local run_id="$2"
  local timeout_seconds="$((SMOKE_TIMEOUT_MINUTES * 60))"
  local deadline="$(( $(date +%s) + timeout_seconds ))"

  while true; do
    local run_json
    run_json="$(databricks jobs get-run "${run_id}" -o json)"
    local life
    local result
    local message
    life="$(echo "${run_json}" | jq -r '.state.life_cycle_state // empty')"
    result="$(echo "${run_json}" | jq -r '.state.result_state // empty')"
    message="$(echo "${run_json}" | jq -r '.state.state_message // empty')"

    echo "[${pipeline}] run_id=${run_id} state=${life}/${result} ${message}" | tee -a "${log_file}"

    case "${life}" in
      TERMINATED|SKIPPED|INTERNAL_ERROR)
        echo "${run_json}" | tee -a "${log_file}" >/dev/null
        ;;
      *)
        ;;
    esac

    if [[ "${life}" == "TERMINATED" || "${life}" == "SKIPPED" || "${life}" == "INTERNAL_ERROR" ]]; then
      if [[ "${life}" == "TERMINATED" && "${result}" == "SUCCESS" ]]; then
        return 0
      fi
      echo "[${pipeline}] run failed (life_cycle_state=${life}, result_state=${result})" | tee -a "${log_file}"
      return 1
    fi

    if [[ "$(date +%s)" -ge "${deadline}" ]]; then
      echo "[${pipeline}] timed out waiting for run_id=${run_id}" | tee -a "${log_file}"
      return 1
    fi

    sleep "${SMOKE_POLL_SECONDS}"
  done
}

trigger_job() {
  local pipeline="$1"
  local job_id="$2"
  local run_id
  if [[ -n "${SMOKE_RUN_ID}" ]]; then
    run_id="${SMOKE_RUN_ID}"
  else
    run_id="e2e_${pipeline}_${timestamp}"
  fi

  local end_ts=""
  local start_ts=""
  local date_kst_start=""
  local date_kst_end=""

  if [[ "${SMOKE_RUN_MODE}" == "incremental" ]]; then
    local window_minutes="${SMOKE_WINDOW_MINUTES:-10}"
    end_ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    start_ts="$(date -u -d "-${window_minutes} minutes" +%Y-%m-%dT%H:%M:%SZ)"
  elif [[ "${SMOKE_RUN_MODE}" == "backfill" ]]; then
    if [[ -z "${SMOKE_DATE_KST_START}" || -z "${SMOKE_DATE_KST_END}" ]]; then
      echo "missing required env for backfill: SMOKE_DATE_KST_START/SMOKE_DATE_KST_END" >&2
      exit 1
    fi
    date_kst_start="${SMOKE_DATE_KST_START}"
    date_kst_end="${SMOKE_DATE_KST_END}"
  else
    echo "invalid SMOKE_RUN_MODE: ${SMOKE_RUN_MODE} (expected incremental|backfill)" >&2
    exit 1
  fi

  local payload
  payload="$(cat <<JSON
{"job_parameters":{"run_mode":"${SMOKE_RUN_MODE}","start_ts":"${start_ts}","end_ts":"${end_ts}","date_kst_start":"${date_kst_start}","date_kst_end":"${date_kst_end}","run_id":"${run_id}"}}
JSON
)"

  echo "[${pipeline}] triggering job_id=${job_id} run_id=${run_id}" | tee -a "${log_file}"
  # Use --no-wait to keep this as a lightweight trigger-only smoke check.
  local response
  response="$(databricks jobs run-now "${job_id}" --no-wait --json "${payload}" -o json)"
  echo "${response}" | tee -a "${log_file}"

  if [[ "${SMOKE_WAIT}" != "0" ]]; then
    local databricks_run_id
    databricks_run_id="$(echo "${response}" | jq -r '.run_id // empty')"
    if [[ -z "${databricks_run_id}" ]]; then
      echo "[${pipeline}] unable to parse run_id from response" | tee -a "${log_file}"
      return 1
    fi
    wait_for_run "${pipeline}" "${databricks_run_id}"
  fi
}

trigger_job "pipeline_a" "${DATABRICKS_PIPELINE_A_JOB_ID}"
trigger_job "pipeline_b" "${DATABRICKS_PIPELINE_B_JOB_ID}"
trigger_job "pipeline_c" "${DATABRICKS_PIPELINE_C_JOB_ID}"

echo "Triggered all E2E smoke jobs. See ${log_file}" | tee -a "${log_file}"
