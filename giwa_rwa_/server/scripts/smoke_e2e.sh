#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
ADMIN_TOKEN="${ADMIN_TOKEN:-}" 
IDEMP_PREFIX="${IDEMP_PREFIX:-smoke-$(date +%s)}"
BUY_LISTING_ID="${BUY_LISTING_ID:-1}"
SHARE_SCALE="${SHARE_SCALE:-1000000000000000000}"
MUSD_FAUCET_AMOUNT="${MUSD_FAUCET_AMOUNT:-1000000000000000000000}"
UNIT_PRICE="${UNIT_PRICE:-100000000000000000000}"
POLL_INTERVAL_SEC="${POLL_INTERVAL_SEC:-3}"
POLL_MAX="${POLL_MAX:-120}"

if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required"
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required"
  exit 1
fi

if [[ -z "$ADMIN_TOKEN" ]]; then
  echo "ADMIN_TOKEN is required"
  exit 1
fi

echo "[smoke] BASE_URL=$BASE_URL"

auth_header() {
  local key="$1"
  printf -- "-HIdempotency-Key:%s" "$key"
}

post_json() {
  local path="$1"
  local body="$2"
  local key="$3"
  local include_admin="${4:-false}"

  if [[ "$include_admin" == "true" ]]; then
    curl -sS -X POST "$BASE_URL$path" \
      -H "Content-Type: application/json" \
      -H "Idempotency-Key: $key" \
      -H "X-Admin-Token: $ADMIN_TOKEN" \
      -d "$body"
  else
    curl -sS -X POST "$BASE_URL$path" \
      -H "Content-Type: application/json" \
      -H "Idempotency-Key: $key" \
      -d "$body"
  fi
}

get_json() {
  local path="$1"
  local include_admin="${2:-false}"
  if [[ "$include_admin" == "true" ]]; then
    curl -sS "$BASE_URL$path" -H "X-Admin-Token: $ADMIN_TOKEN"
  else
    curl -sS "$BASE_URL$path"
  fi
}

wait_outbox_mined() {
  local outbox_id="$1"
  local i=0
  while (( i < POLL_MAX )); do
    local tx
    tx="$(get_json "/tx/outbox/$outbox_id")"
    local status
    status="$(echo "$tx" | jq -r '.status')"
    local tx_hash
    tx_hash="$(echo "$tx" | jq -r '.txHash // ""')"
    echo "[smoke] outbox=$outbox_id status=$status txHash=$tx_hash"

    if [[ "$status" == "MINED" ]]; then
      return 0
    fi
    if [[ "$status" == "FAILED" ]]; then
      echo "[smoke] tx failed: $tx"
      return 1
    fi

    sleep "$POLL_INTERVAL_SEC"
    ((i+=1))
  done

  echo "[smoke] timeout waiting outbox=$outbox_id"
  return 1
}

# health check
health="$(get_json "/actuator/health")"
echo "[smoke] health=$health"

# 1) signup seller
seller_signup="$(post_json "/auth/signup" "{}" "$IDEMP_PREFIX-auth-seller")"
seller_user_id="$(echo "$seller_signup" | jq -r '.userId')"
seller_address="$(echo "$seller_signup" | jq -r '.address')"
echo "[smoke] seller userId=$seller_user_id address=$seller_address"

# 2) signup buyer
buyer_signup="$(post_json "/auth/signup" "{}" "$IDEMP_PREFIX-auth-buyer")"
buyer_user_id="$(echo "$buyer_signup" | jq -r '.userId')"
buyer_address="$(echo "$buyer_signup" | jq -r '.address')"
echo "[smoke] buyer userId=$buyer_user_id address=$buyer_address"

# 3) compliance approve
approve_seller="$(post_json "/admin/compliance/approve" "{\"userId\":\"$seller_user_id\"}" "$IDEMP_PREFIX-approve-seller" true)"
approve_buyer="$(post_json "/admin/compliance/approve" "{\"userId\":\"$buyer_user_id\"}" "$IDEMP_PREFIX-approve-buyer" true)"
echo "[smoke] approve seller=$approve_seller"
echo "[smoke] approve buyer=$approve_buyer"

# 4) complex import
complex_payload='{
  "rawItemJson": {
    "kaptCode": "SMOKE-KAPT-001",
    "kaptName": "Smoke Apartment",
    "kaptAddr": "Seoul Sample",
    "doroJuso": "Smoke-ro 1",
    "hoCnt": 2,
    "kaptMparea60": 1,
    "kaptMparea85": 1,
    "kaptMparea135": 0,
    "kaptMparea136": 0
  }
}'
import_res="$(post_json "/admin/complexes/import" "$complex_payload" "$IDEMP_PREFIX-import" true)"
echo "[smoke] import=$import_res"

classes="$(get_json "/complexes/SMOKE-KAPT-001/classes")"
class_id="$(echo "$classes" | jq -r '.[0].classId')"
if [[ -z "$class_id" || "$class_id" == "null" ]]; then
  echo "[smoke] classId not found"
  exit 1
fi
echo "[smoke] classId=$class_id"

# 5) tokenize
# NOTE: tokenization may take multiple txs; wait for first reserve tx at least.
tokenize_res="$(post_json "/admin/classes/$class_id/tokenize" '{"chunkSize":100}' "$IDEMP_PREFIX-tokenize" true)"
echo "[smoke] tokenize=$tokenize_res"
first_tokenize_outbox="$(echo "$tokenize_res" | jq -r '.outboxIds[0]')"
if [[ -n "$first_tokenize_outbox" && "$first_tokenize_outbox" != "null" ]]; then
  wait_outbox_mined "$first_tokenize_outbox"
fi

units="$(get_json "/classes/$class_id/units")"
unit_id="$(echo "$units" | jq -r '.[0].unitId')"
if [[ -z "$unit_id" || "$unit_id" == "null" ]]; then
  echo "[smoke] unitId not found"
  exit 1
fi
echo "[smoke] unitId=$unit_id"

# 6) faucet buyer musd
faucet_res="$(post_json "/admin/faucet/musd" "{\"toUserId\":\"$buyer_user_id\",\"amount\":$MUSD_FAUCET_AMOUNT}" "$IDEMP_PREFIX-faucet" true)"
faucet_outbox="$(echo "$faucet_res" | jq -r '.outboxId')"
echo "[smoke] faucet=$faucet_res"
wait_outbox_mined "$faucet_outbox"

# 7) distribute one full unit share to seller
dist_res="$(post_json "/admin/distribute/shares" "{\"toUserId\":\"$seller_user_id\",\"unitId\":\"$unit_id\",\"amount\":$SHARE_SCALE}" "$IDEMP_PREFIX-distribute" true)"
dist_outbox="$(echo "$dist_res" | jq -r '.outboxId')"
echo "[smoke] distribute=$dist_res"
wait_outbox_mined "$dist_outbox"

# 8) seller list
list_res="$(post_json "/trade/list" "{\"sellerUserId\":\"$seller_user_id\",\"unitId\":\"$unit_id\",\"amount\":$SHARE_SCALE,\"unitPrice\":$UNIT_PRICE}" "$IDEMP_PREFIX-list")"
list_outbox="$(echo "$list_res" | jq -r '.outboxIds[-1]')"
echo "[smoke] list=$list_res"
wait_outbox_mined "$list_outbox"

# 9) buyer buy (listingId는 환경에 맞게 조정 가능)
buy_res="$(post_json "/trade/buy" "{\"buyerUserId\":\"$buyer_user_id\",\"listingId\":$BUY_LISTING_ID,\"amount\":$SHARE_SCALE}" "$IDEMP_PREFIX-buy")"
buy_outbox="$(echo "$buy_res" | jq -r '.outboxIds[-1]')"
echo "[smoke] buy=$buy_res"
wait_outbox_mined "$buy_outbox"

echo "[smoke] DONE"
echo "[smoke] sellerUserId=$seller_user_id buyerUserId=$buyer_user_id classId=$class_id unitId=$unit_id listingId=$BUY_LISTING_ID"
