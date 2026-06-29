#!/usr/bin/env bash
# Point the local ClickHouse `feeds` tables at production via a remoteSecure() proxy, so the
# API serves real production Hyperliquid data locally — the equivalent of what
# setup-remote-tables.sh does for shreds, but using the dedicated feeds_reader credentials.
#
# Reads FEEDS_CH_* from .env. Re-run after recreating the local cluster.
# Usage: ./scripts/setup-feeds-remote-local.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
[[ -f "$ROOT_DIR/.env" ]] && { set -a; source "$ROOT_DIR/.env"; set +a; }

REMOTE_HOST="${FEEDS_CH_HOST:?set FEEDS_CH_HOST in .env}"
REMOTE_DB="${FEEDS_CH_DB:-feeds}"
REMOTE_USER="${FEEDS_CH_USER:?set FEEDS_CH_USER in .env}"
REMOTE_PASS="${FEEDS_CH_PASSWORD:?set FEEDS_CH_PASSWORD in .env}"
REMOTE_SECURE_PORT="${FEEDS_CH_SECURE_PORT:-9440}"  # ClickHouse Cloud secure native port

LOCAL_HOST="${CLICKHOUSE_ADDR_TCP:-localhost:9100}"
LOCAL_ADDR="${LOCAL_HOST%%:*}"
LOCAL_PORT="${LOCAL_HOST##*:}"

# summary feeds the scoreboard; observations feeds the composite-latency hero stat.
TABLES=("hyperliquid_bbo_feed_race_summary" "hyperliquid_bbo_observations")

clickhouse client --host "$LOCAL_ADDR" --port "$LOCAL_PORT" --query "CREATE DATABASE IF NOT EXISTS feeds"
for TABLE in "${TABLES[@]}"; do
  echo "==> Creating local proxy feeds.${TABLE} -> ${REMOTE_HOST}:${REMOTE_SECURE_PORT}/${REMOTE_DB}.${TABLE}"
  clickhouse client --host "$LOCAL_ADDR" --port "$LOCAL_PORT" --query "
CREATE OR REPLACE TABLE feeds.${TABLE} AS remoteSecure('${REMOTE_HOST}:${REMOTE_SECURE_PORT}', '${REMOTE_DB}.${TABLE}', '${REMOTE_USER}', '${REMOTE_PASS}')"
done
echo "==> Done. Local feeds tables now serve production data via remoteSecure()."
echo "    (Note: 24h aggregations over the full tables are slow; the API caches them.)"
