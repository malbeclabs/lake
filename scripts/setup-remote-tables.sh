#!/bin/bash
set -euo pipefail

# Set up remote ClickHouse proxy tables for local UI testing.
#
# This is OPTIONAL — only for internal developers who want real data
# in their local ClickHouse without running the indexer.
#
# Proxy tables are created in a separate database (default: lake_remote)
# to avoid accidentally overwriting local data tables.
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
#   ./scripts/setup-remote-tables.sh            # Create proxies in lake database
#   ./scripts/setup-remote-tables.sh --force     # Overwrite existing non-proxy tables

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$ROOT_DIR"

# ─── Parse flags ─────────────────────────────────────────────────────────────

FORCE=""
for arg in "$@"; do
  case "$arg" in
    --force) FORCE="--force" ;;
    *) echo "Unknown flag: $arg" >&2; exit 1 ;;
  esac
done

# ─── Configuration ────────────────────────────────────────────────────────────

# Remote ClickHouse Cloud database to discover tables from
REMOTE_DB="${REMOTE_CH_DATABASE:-lake}"

# Local database for proxy tables (must match remote db name for JOINs to work)
PROXY_DB="${REMOTE_DB}"

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

# Check if a table exists and is not a proxy table. Returns 0 if it should be skipped.
should_skip_table() {
  local db="$1"
  local table="$2"

  local engine
  engine=$(local_query "SELECT engine FROM system.tables WHERE database = '${db}' AND name = '${table}'" 2>/dev/null || true)

  if [[ -z "$engine" ]]; then
    # Table doesn't exist — safe to create
    return 1
  fi

  if [[ "$engine" == "StorageProxy" ]]; then
    # Existing proxy — safe to replace
    return 1
  fi

  # Real data table exists
  if [[ -n "$FORCE" ]]; then
    echo "  WARNING: overwriting existing non-proxy table ${db}.${table} (engine: ${engine})"
    return 1
  fi

  echo "  SKIP: ${db}.${table} exists with engine ${engine} (use --force to overwrite)"
  return 0
}

create_proxy_table() {
  local local_db="$1"
  local local_table="$2"
  local remote_db="$3"
  local remote_table="$4"

  if should_skip_table "$local_db" "$local_table"; then
    return 0
  fi

  local_query "CREATE OR REPLACE TABLE \`${local_db}\`.\`${local_table}\` AS remoteSecure('${REMOTE_ADDR}', '${remote_db}.${remote_table}', '${REMOTE_CH_USER}', '${REMOTE_CH_PASSWORD}')"
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

# ─── Create proxy database ────────────────────────────────────────────────────

local_query "CREATE DATABASE IF NOT EXISTS \`${PROXY_DB}\`"

# ─── Discover and create lake tables ──────────────────────────────────────────

echo "=== Creating proxy tables in ${PROXY_DB} (from remote ${REMOTE_DB}) ==="

lake_tables=$(remote_query "SELECT name FROM system.tables WHERE database = '${REMOTE_DB}' ORDER BY name")
lake_count=0
skip_count=0

while IFS= read -r table; do
  [[ -z "$table" ]] && continue
  if should_skip_table "$PROXY_DB" "$table"; then
    skip_count=$((skip_count + 1))
    continue
  fi
  echo "  ${table}"
  create_proxy_table "$PROXY_DB" "$table" "$REMOTE_DB" "$table"
  lake_count=$((lake_count + 1))
done <<< "$lake_tables"

echo "Created ${lake_count} proxy tables in ${PROXY_DB}"
if [[ $skip_count -gt 0 ]]; then
  echo "Skipped ${skip_count} existing non-proxy tables"
fi
echo ""

# ─── Create external service tables ──────────────────────────────────────────

# External tables from other services
EXTERNAL_TABLES=(
  "shredder:publisher_shred_stats"
  "shredder_qa:publisher_shred_stats"
)

if [[ ${#EXTERNAL_TABLES[@]} -gt 0 ]]; then
  echo "=== Creating external proxy tables ==="

  ext_count=0
  for entry in "${EXTERNAL_TABLES[@]}"; do
    remote_db="${entry%%:*}"
    remote_table="${entry##*:}"

    local_query "CREATE DATABASE IF NOT EXISTS \`${remote_db}\`"
    echo "  ${remote_db}.${remote_table}"
    create_proxy_table "$remote_db" "$remote_table" "$remote_db" "$remote_table"
    ext_count=$((ext_count + 1))
  done

  echo "Created ${ext_count} external proxy tables"
  echo ""
fi

# ─── Summary ─────────────────────────────────────────────────────────────────

total=$((lake_count + ${ext_count:-0}))
echo "=== Done! ==="
echo "Created ${total} proxy tables in ${PROXY_DB}."
echo ""
echo "To use remote data with the API server, run:"
echo "  go run ./api/main.go --use-remote"
