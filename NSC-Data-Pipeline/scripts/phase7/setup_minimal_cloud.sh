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

ensure_role_assignment() {
  local role_name="$1"
  local principal_id="$2"
  local scope="$3"

  local count
  count="$(
    az role assignment list \
      --assignee-object-id "$principal_id" \
      --scope "$scope" \
      --query "[?roleDefinitionName=='$role_name'] | length(@)" \
      -o tsv
  )"

  if [[ "$count" == "0" ]]; then
    az role assignment create \
      --assignee-object-id "$principal_id" \
      --assignee-principal-type ServicePrincipal \
      --role "$role_name" \
      --scope "$scope" >/dev/null
    echo "created role assignment: $role_name"
  else
    echo "role assignment exists: $role_name"
  fi
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

if [[ -z "$SUBSCRIPTION_ID" || -z "$RESOURCE_GROUP" || -z "$WORKSPACE_NAME" || -z "$STORAGE_ACCOUNT" || -z "$CONTAINER_NAME" || -z "$DATABRICKS_TOKEN" ]]; then
  echo "required values are missing in env files" >&2
  exit 1
fi

az account set --subscription "$SUBSCRIPTION_ID"

DATABRICKS_HOST="https://$(az databricks workspace show -g "$RESOURCE_GROUP" -n "$WORKSPACE_NAME" --query workspaceUrl -o tsv)"
export DATABRICKS_HOST
export DATABRICKS_TOKEN

EXT_LOCATION_NAME="${EXT_LOCATION_NAME:-team4_adls_test}"
STORAGE_CREDENTIAL_NAME="${STORAGE_CREDENTIAL_NAME:-2dt_final_team4_databricks_test}"
EXT_LOCATION_URL="abfss://${CONTAINER_NAME}@${STORAGE_ACCOUNT}.dfs.core.windows.net/"

SCOPE_NAME="${SCOPE_NAME:-ledger-analytics-dev}"
SCOPE_KEY="${SCOPE_KEY:-salt_user_key}"
ANON_SALT_VALUE="${ANON_SALT_VALUE:-dev-salt-user-key-v1}"

POLICY_NAME="${POLICY_NAME:-data-pipeline-single-node-dev}"
PIPELINE_A_JOB_ID="${PIPELINE_A_JOB_ID:-57710310442212}"
ALERT_EMAIL="${ALERT_EMAIL:-$(az account show --query user.name -o tsv)}"

MANAGED_RG_NAME="$(az databricks workspace show -g "$RESOURCE_GROUP" -n "$WORKSPACE_NAME" --query managedResourceGroupId -o tsv | awk -F/ '{print $NF}')"
ACCESS_CONNECTOR_NAME="${ACCESS_CONNECTOR_NAME:-unity-catalog-access-connector}"
ACCESS_CONNECTOR_PRINCIPAL_ID="$(az databricks access-connector show -g "$MANAGED_RG_NAME" -n "$ACCESS_CONNECTOR_NAME" --query identity.principalId -o tsv)"
STORAGE_SCOPE="/subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.Storage/storageAccounts/${STORAGE_ACCOUNT}"

ensure_role_assignment "Storage Blob Data Reader" "$ACCESS_CONNECTOR_PRINCIPAL_ID" "$STORAGE_SCOPE"
ensure_role_assignment "Storage Blob Data Contributor" "$ACCESS_CONNECTOR_PRINCIPAL_ID" "$STORAGE_SCOPE"

if databricks external-locations get "$EXT_LOCATION_NAME" --output json >/dev/null 2>&1; then
  echo "external location exists: $EXT_LOCATION_NAME"
else
  databricks external-locations create \
    "$EXT_LOCATION_NAME" \
    "$EXT_LOCATION_URL" \
    "$STORAGE_CREDENTIAL_NAME" \
    --comment "team4 adls dev external location" >/dev/null
  echo "external location created: $EXT_LOCATION_NAME"
fi

if databricks secrets list-scopes --output json | jq -e --arg scope "$SCOPE_NAME" '.[] | select(.name == $scope)' >/dev/null; then
  echo "secret scope exists: $SCOPE_NAME"
else
  databricks secrets create-scope "$SCOPE_NAME" --scope-backend-type DATABRICKS --initial-manage-principal users >/dev/null
  echo "secret scope created: $SCOPE_NAME"
fi

databricks secrets put-secret "$SCOPE_NAME" "$SCOPE_KEY" --string-value "$ANON_SALT_VALUE" >/dev/null
echo "secret upserted: ${SCOPE_NAME}/${SCOPE_KEY}"

CURRENT_USER="$(databricks current-user me --output json | jq -r '.userName')"
cat > /tmp/grants_extloc_user.json <<JSON
{
  "changes": [
    {
      "principal": "$CURRENT_USER",
      "add": ["READ_FILES", "WRITE_FILES", "CREATE_EXTERNAL_TABLE"]
    }
  ]
}
JSON

databricks grants update external_location "$EXT_LOCATION_NAME" --json @/tmp/grants_extloc_user.json >/dev/null
for schema_name in bronze silver gold; do
  cat > /tmp/grants_schema_user.json <<JSON
{
  "changes": [
    {
      "principal": "$CURRENT_USER",
      "add": ["USE_SCHEMA", "CREATE_TABLE", "SELECT", "MODIFY"]
    }
  ]
}
JSON
  databricks grants update schema "2dt_final_team4_databricks_test.${schema_name}" --json @/tmp/grants_schema_user.json >/dev/null
  echo "schema grant upserted: ${schema_name}"
done

if databricks cluster-policies list --output json | jq -e --arg name "$POLICY_NAME" '.[] | select(.name == $name)' >/dev/null; then
  echo "cluster policy exists: $POLICY_NAME"
else
  POLICY_DEF='{"cluster_type":{"type":"fixed","value":"job"},"spark_version":{"type":"fixed","value":"13.3.x-scala2.12"},"node_type_id":{"type":"allowlist","values":["Standard_DS3_v2","Standard_D4ds_v5","Standard_D8ds_v5"],"defaultValue":"Standard_DS3_v2"},"num_workers":{"type":"fixed","value":0,"hidden":true},"spark_conf.spark.databricks.cluster.profile":{"type":"fixed","value":"singleNode","hidden":true},"spark_conf.spark.master":{"type":"fixed","value":"local[*]","hidden":true},"data_security_mode":{"type":"fixed","value":"SINGLE_USER"},"autotermination_minutes":{"type":"range","minValue":10,"maxValue":120,"defaultValue":30},"custom_tags.project":{"type":"fixed","value":"data-pipeline"},"custom_tags.env":{"type":"fixed","value":"dev"}}'
  jq -n \
    --arg name "$POLICY_NAME" \
    --arg description "Data-pipeline dev single-node job policy" \
    --arg definition "$POLICY_DEF" \
    '{name: $name, description: $description, definition: $definition, max_clusters_per_user: 5}' >/tmp/create_policy.json
  databricks cluster-policies create --json @/tmp/create_policy.json >/dev/null
  echo "cluster policy created: $POLICY_NAME"
fi

if databricks jobs get "$PIPELINE_A_JOB_ID" --output json >/tmp/pipeline_a_job.json 2>/dev/null; then
  jq \
    --arg email "$ALERT_EMAIL" \
    '{
      job_id: .job_id,
      new_settings: (
        .settings
        | .email_notifications = {"on_failure": [$email]}
        | .tasks = ((.tasks // []) | map(.max_retries = 2 | .min_retry_interval_millis = 300000 | .retry_on_timeout = true))
      )
    }' /tmp/pipeline_a_job.json >/tmp/pipeline_a_job_reset.json
  databricks jobs reset --json @/tmp/pipeline_a_job_reset.json >/dev/null
  echo "pipeline A retry/email policy applied: $PIPELINE_A_JOB_ID"
else
  echo "pipeline A job not found, skipped retry/email update: $PIPELINE_A_JOB_ID"
fi

echo "phase7 minimal setup complete"
