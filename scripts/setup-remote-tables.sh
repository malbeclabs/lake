#!/bin/bash
set -euo pipefail

# Set up remote ClickHouse proxy tables for local UI testing.
#
# This is OPTIONAL — only for internal developers who want real data
# in their local ClickHouse without running the indexer.
#
# Note: Credentials are passed via CLI arguments to clickhouse-client and
# embedded in SQL queries, so they may be visible in process listings.
# Use a read-only account with minimal permissions.
#
# Prerequisites:
#   1. Run ./scripts/dev-setup.sh first (starts local ClickHouse)
#   2. Set REMOTE_CH_HOST, REMOTE_CH_USER, REMOTE_CH_PASSWORD in .env
#
# Usage:
#   ./scripts/setup-remote-tables.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$ROOT_DIR"

# ─── Configuration ────────────────────────────────────────────────────────────

# Remote ClickHouse Cloud databases to proxy
REMOTE_LAKE_DB="lake"

# External tables from other services (format: "remote_db.remote_table:local_table_name")
EXTERNAL_TABLES=(
  "shredder.publisher_shred_stats:publisher_shred_stats"
  "shredder_qa.publisher_shred_stats:publisher_shred_stats"
)

# ─── Load environment ─────────────────────────────────────────────────────────

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

# Local ClickHouse connection (matches docker-compose.yml defaults)
local_addr="${CLICKHOUSE_ADDR_TCP:-localhost:9100}"
LOCAL_CH_HOST="${local_addr%%:*}"
LOCAL_CH_PORT="${local_addr##*:}"

# Validate required variables
missing=()
[[ -z "${REMOTE_CH_HOST:-}" ]] && missing+=("REMOTE_CH_HOST")
[[ -z "${REMOTE_CH_USER:-}" ]] && missing+=("REMOTE_CH_USER")
[[ -z "${REMOTE_CH_PASSWORD:-}" ]] && missing+=("REMOTE_CH_PASSWORD")

if [[ ${#missing[@]} -gt 0 ]]; then
  echo "Error: Missing required environment variables: ${missing[*]}" >&2
  echo "" >&2
  echo "Set these in your .env file:" >&2
  echo "  REMOTE_CH_HOST=your-instance.us-east-1.aws.clickhouse.cloud" >&2
  echo "  REMOTE_CH_USER=lake_dev_reader" >&2
  echo "  REMOTE_CH_PASSWORD=your-password" >&2
  exit 1
fi

REMOTE_CH_SECURE_PORT="9440"
REMOTE_ADDR="${REMOTE_CH_HOST}:${REMOTE_CH_SECURE_PORT}"

# ─── Helper functions ─────────────────────────────────────────────────────────

local_query() {
  clickhouse-client --host "$LOCAL_CH_HOST" --port "$LOCAL_CH_PORT" --query "$1"
}

remote_query() {
  clickhouse-client \
    --host "$REMOTE_CH_HOST" \
    --port "$REMOTE_CH_SECURE_PORT" \
    --user "$REMOTE_CH_USER" \
    --password "$REMOTE_CH_PASSWORD" \
    --secure \
    --query "$1"
}

create_proxy_table() {
  local remote_db="$1"
  local remote_table="$2"
  local local_table="$3"

  local_query "CREATE DATABASE IF NOT EXISTS \`${remote_db}\`"
  local_query "CREATE OR REPLACE TABLE \`${remote_db}\`.\`${local_table}\` AS remoteSecure('${REMOTE_ADDR}', '${remote_db}.${remote_table}', '${REMOTE_CH_USER}', '${REMOTE_CH_PASSWORD}')"
}

# ─── Check dependencies ───────────────────────────────────────────────────────

if ! command -v clickhouse-client &>/dev/null; then
  echo "Error: clickhouse-client is not installed or not on PATH." >&2
  echo "Install it from https://clickhouse.com/docs/en/install" >&2
  exit 1
fi

# ─── Check local ClickHouse is running ────────────────────────────────────────

if ! local_query "SELECT 1" &>/dev/null; then
  echo "Error: Local ClickHouse is not running on port ${LOCAL_CH_PORT}." >&2
  echo "Run ./scripts/dev-setup.sh first." >&2
  exit 1
fi

# ─── Check remote ClickHouse is reachable ─────────────────────────────────────

echo "=== Connecting to remote ClickHouse Cloud ==="
if ! remote_query "SELECT 1" &>/dev/null; then
  echo "Error: Cannot connect to remote ClickHouse at ${REMOTE_CH_HOST}." >&2
  echo "Check your REMOTE_CH_HOST, REMOTE_CH_USER, and REMOTE_CH_PASSWORD." >&2
  exit 1
fi
echo "Connected to ${REMOTE_CH_HOST}"
echo ""

# ─── Discover and create lake tables ──────────────────────────────────────────

echo "=== Creating proxy tables for ${REMOTE_LAKE_DB} database ==="

lake_tables=$(remote_query "SELECT name FROM system.tables WHERE database = '${REMOTE_LAKE_DB}' ORDER BY name")
lake_count=0

while IFS= read -r table; do
  [[ -z "$table" ]] && continue
  echo "  ${table}"
  create_proxy_table "$REMOTE_LAKE_DB" "$table" "$table"
  lake_count=$((lake_count + 1))
done <<< "$lake_tables"

echo "Created ${lake_count} proxy tables from ${REMOTE_LAKE_DB}"
echo ""

# ─── Create external service tables ──────────────────────────────────────────

if [[ ${#EXTERNAL_TABLES[@]} -gt 0 ]]; then
  echo "=== Creating proxy tables for external services ==="

  for entry in "${EXTERNAL_TABLES[@]}"; do
    remote_ref="${entry%%:*}"
    local_name="${entry##*:}"
    remote_db="${remote_ref%%.*}"
    remote_table="${remote_ref##*.}"

    echo "  ${local_name} -> ${remote_ref}"
    # Create external tables in the lake database so the API can find them
    local_query "CREATE OR REPLACE TABLE \`${REMOTE_LAKE_DB}\`.\`${local_name}\` AS remoteSecure('${REMOTE_ADDR}', '${remote_db}.${remote_table}', '${REMOTE_CH_USER}', '${REMOTE_CH_PASSWORD}')"
  done

  echo "Created ${#EXTERNAL_TABLES[@]} external proxy tables"
  echo ""
fi

# ─── Summary ─────────────────────────────────────────────────────────────────

total=$((lake_count + ${#EXTERNAL_TABLES[@]}))
echo "=== Done! ==="
echo "Created ${total} proxy tables in local ClickHouse."
echo "Start the API server and web app to test the UI with real data."
