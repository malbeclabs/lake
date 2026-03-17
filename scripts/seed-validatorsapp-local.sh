#!/usr/bin/env bash
# Seed local ClickHouse with validator data from the validators.app API.
# Reads credentials from .env file.
# Usage: ./scripts/seed-validatorsapp-local.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

# Load .env
if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  source "$ROOT_DIR/.env"
  set +a
fi

API_KEY="${SEED_VALIDATORSAPP_API_KEY:-}"
if [[ -z "$API_KEY" ]]; then
  echo "Error: SEED_VALIDATORSAPP_API_KEY not set in .env"
  exit 1
fi

# Local ClickHouse
LOCAL_HOST="${CLICKHOUSE_ADDR_TCP:-localhost:9100}"
LOCAL_USER="${CLICKHOUSE_USERNAME:-default}"
LOCAL_PASS="${CLICKHOUSE_PASSWORD:-}"
LOCAL_PORT="${LOCAL_HOST##*:}"
LOCAL_ADDR="${LOCAL_HOST%%:*}"

CH_ARGS=(--host "$LOCAL_ADDR" --port "$LOCAL_PORT" --user "$LOCAL_USER")
if [[ -n "$LOCAL_PASS" ]]; then
  CH_ARGS+=(--password "$LOCAL_PASS")
fi

echo "==> Fetching validators from validators.app API..."
TMPFILE=$(mktemp /tmp/validatorsapp.XXXXXX.json)
trap 'rm -f "$TMPFILE"' EXIT

curl -sS "https://www.validators.app/api/v1/validators/mainnet.json?limit=9999&active_only=true" \
  -H "Token: $API_KEY" \
  > "$TMPFILE"

COUNT=$(jq length "$TMPFILE")
echo "==> Got $COUNT validators"

if [[ "$COUNT" -eq 0 ]]; then
  echo "Error: API returned 0 validators"
  exit 1
fi

echo "==> Inserting into local dim_validatorsapp_validators_history..."

NOW=$(date -u '+%Y-%m-%d %H:%M:%S.000')
OP_ID=$(uuidgen | tr '[:upper:]' '[:lower:]')

# Transform JSON to TSV and insert
jq -r --arg now "$NOW" --arg op "$OP_ID" '
  .[] | select(.account != null and .account != "") |
  [
    .account,
    $now,
    $now,
    $op,
    "0",
    "0",
    .account,
    (.name // ""),
    (.vote_account // ""),
    (.software_version // ""),
    (.software_client // ""),
    (.software_client_id // 0),
    (if .jito then 1 else 0 end),
    (.jito_commission // 0),
    (if .is_active then 1 else 0 end),
    (if .is_dz then 1 else 0 end),
    (.active_stake // 0),
    (.commission // 0),
    (if .delinquent then 1 else 0 end),
    (.epoch // 0),
    (.epoch_credits // 0),
    (.skipped_slot_percent // ""),
    (.total_score // 0),
    (.data_center_key // ""),
    (.autonomous_system_number // 0),
    (.latitude // ""),
    (.longitude // ""),
    (.ip // ""),
    ((.stake_pools_list // []) | join(","))
  ] | @tsv
' "$TMPFILE" > /tmp/validatorsapp.tsv

ROWS=$(wc -l < /tmp/validatorsapp.tsv | tr -d ' ')
echo "==> Inserting $ROWS rows..."

clickhouse client "${CH_ARGS[@]}" \
  --query "INSERT INTO dim_validatorsapp_validators_history (entity_id, snapshot_ts, ingested_at, op_id, is_deleted, attrs_hash, account, name, vote_account, software_version, software_client, software_client_id, jito, jito_commission, is_active, is_dz, active_stake, commission, delinquent, epoch, epoch_credits, skipped_slot_percent, total_score, data_center_key, autonomous_system_number, latitude, longitude, ip, stake_pools_list) FORMAT TabSeparated" \
  < /tmp/validatorsapp.tsv

rm -f /tmp/validatorsapp.tsv

echo "==> Done! Seeded $ROWS validators into local dim_validatorsapp_validators_history"
echo "==> Verify: clickhouse client ${CH_ARGS[*]} -q \"SELECT count() FROM validatorsapp_validators_current\""
