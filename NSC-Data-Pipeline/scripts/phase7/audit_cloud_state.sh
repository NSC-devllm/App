#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
AZURE_ENV_FILE="${AZURE_ENV_FILE:-$ROOT_DIR/.env/azure.md}"
DATABRICKS_ENV_FILE="${DATABRICKS_ENV_FILE:-$ROOT_DIR/.env/databricks.md}"

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "missing required command: $cmd" >&2
    exit 1
  fi
}

read_env_value() {
  local pattern="$1"
  local file_path="$2"
  awk -F': ' -v p="$pattern" '$0 ~ p { print $2; exit }' "$file_path"
}

require_cmd az
require_cmd databricks
require_cmd jq
require_cmd awk

if [[ ! -f "$AZURE_ENV_FILE" ]]; then
  echo "missing azure env file: $AZURE_ENV_FILE" >&2
  exit 1
fi
if [[ ! -f "$DATABRICKS_ENV_FILE" ]]; then
  echo "missing databricks env file: $DATABRICKS_ENV_FILE" >&2
  exit 1
fi

SUBSCRIPTION_ID="$(read_env_value "구독 ID" "$AZURE_ENV_FILE")"
RESOURCE_GROUP="$(read_env_value "리소스 그룹 이름" "$AZURE_ENV_FILE")"
WORKSPACE_NAME="$(read_env_value "워크스페이스 이름" "$AZURE_ENV_FILE")"
STORAGE_ACCOUNT="$(read_env_value "스토리지 계정 이름" "$AZURE_ENV_FILE")"
CONTAINER_NAME="$(read_env_value "컨테이너 이름" "$AZURE_ENV_FILE")"
DATABRICKS_TOKEN="$(read_env_value "Databricks PAT 토큰" "$DATABRICKS_ENV_FILE")"

az account set --subscription "$SUBSCRIPTION_ID"

DATABRICKS_HOST="https://$(az databricks workspace show -g "$RESOURCE_GROUP" -n "$WORKSPACE_NAME" --query workspaceUrl -o tsv)"
export DATABRICKS_HOST
export DATABRICKS_TOKEN

WORKSPACE_JSON="$(az databricks workspace show -g "$RESOURCE_GROUP" -n "$WORKSPACE_NAME" --output json)"
WORKSPACE_URL="$(echo "$WORKSPACE_JSON" | jq -r '.workspaceUrl')"
UC_ENABLED="$(echo "$WORKSPACE_JSON" | jq -r '.isUcEnabled')"

RESOURCE_COUNT="$(az resource list --resource-group "$RESOURCE_GROUP" --query 'length(@)' -o tsv)"
CONTAINER_EXISTS="$(az storage container exists --account-name "$STORAGE_ACCOUNT" --name "$CONTAINER_NAME" --auth-mode login --query exists -o tsv)"
KEYVAULT_COUNT="$(az keyvault list -g "$RESOURCE_GROUP" --query 'length(@)' -o tsv)"

CATALOG_COUNT="$(databricks catalogs list --output json | jq 'length')"
EXT_LOC_COUNT="$(databricks external-locations list --output json | jq 'length')"
SECRET_SCOPE_COUNT="$(databricks secrets list-scopes --output json | jq 'length')"
POLICY_MATCH_COUNT="$(databricks cluster-policies list --output json | jq '[.[] | select(.name=="data-pipeline-single-node-dev")] | length')"

PIPELINE_A_JOB_ID="${PIPELINE_A_JOB_ID:-57710310442212}"
PIPELINE_A_RETRY="unknown"
PIPELINE_A_EMAIL="unknown"
if databricks jobs get "$PIPELINE_A_JOB_ID" --output json >/tmp/pipeline_a_audit.json 2>/dev/null; then
  PIPELINE_A_RETRY="$(jq -r '.settings.tasks[0].max_retries // "unset"' /tmp/pipeline_a_audit.json)"
  PIPELINE_A_EMAIL="$(jq -r '.settings.email_notifications.on_failure[0] // "unset"' /tmp/pipeline_a_audit.json)"
fi

printf '%s\n' "phase7_audit_timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
printf '%s\n' "subscription_id=$SUBSCRIPTION_ID"
printf '%s\n' "resource_group=$RESOURCE_GROUP"
printf '%s\n' "workspace_name=$WORKSPACE_NAME"
printf '%s\n' "workspace_url=$WORKSPACE_URL"
printf '%s\n' "uc_enabled=$UC_ENABLED"
printf '%s\n' "resource_count_rg=$RESOURCE_COUNT"
printf '%s\n' "storage_account=$STORAGE_ACCOUNT"
printf '%s\n' "container_name=$CONTAINER_NAME"
printf '%s\n' "container_exists=$CONTAINER_EXISTS"
printf '%s\n' "keyvault_count=$KEYVAULT_COUNT"
printf '%s\n' "catalog_count=$CATALOG_COUNT"
printf '%s\n' "external_location_count=$EXT_LOC_COUNT"
printf '%s\n' "secret_scope_count=$SECRET_SCOPE_COUNT"
printf '%s\n' "policy_data_pipeline_single_node_dev=$POLICY_MATCH_COUNT"
printf '%s\n' "pipeline_a_job_id=$PIPELINE_A_JOB_ID"
printf '%s\n' "pipeline_a_max_retries=$PIPELINE_A_RETRY"
printf '%s\n' "pipeline_a_failure_email=$PIPELINE_A_EMAIL"
