#!/usr/bin/env bash
# Seed local ClickHouse with recent rows from remote feeds.hyperliquid_bbo_feed_race_summary.
# Reads credentials from .env (FEEDS_CH_*). Usage: ./scripts/seed-hyperliquid-local.sh [MINUTES]
set -euo pipefail
trap 'rm -f /tmp/hl_feed_race.tsv' EXIT

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
[[ -f "$ROOT_DIR/.env" ]] && { set -a; source "$ROOT_DIR/.env"; set +a; }

REMOTE_HOST="${FEEDS_CH_HOST:?set FEEDS_CH_HOST in .env}"
REMOTE_HTTPS_PORT="${FEEDS_CH_PORT:-8443}"
REMOTE_DB="${FEEDS_CH_DB:-feeds}"
REMOTE_USER="${FEEDS_CH_USER:?set FEEDS_CH_USER in .env}"
REMOTE_PASS="${FEEDS_CH_PASSWORD:?set FEEDS_CH_PASSWORD in .env}"
MINUTES="${1:-10}"
if ! [[ "$MINUTES" =~ ^[0-9]+$ ]]; then echo "Error: MINUTES must be a positive integer, got '$MINUTES'" >&2; exit 1; fi

LOCAL_HOST="${CLICKHOUSE_ADDR_TCP:-localhost:9100}"
LOCAL_ADDR="${LOCAL_HOST%%:*}"
LOCAL_PORT="${LOCAL_HOST##*:}"

COLS="event_ts, ingested_at, capture_run_id, measurement_node_id, host, location_code, feed_type, symbol, source_ts_ms, bbo_hash, feed, loser_feed, total_events, events_won, lead_time_p50_ms, lead_time_p95_ms, send_lead_time_p50_ms, send_lead_time_p95_ms"

echo "==> Creating local feeds.hyperliquid_bbo_feed_race_summary..."
clickhouse client --host "$LOCAL_ADDR" --port "$LOCAL_PORT" --multiquery <<'SQL'
CREATE DATABASE IF NOT EXISTS feeds;
DROP TABLE IF EXISTS feeds.hyperliquid_bbo_feed_race_summary;
CREATE TABLE feeds.hyperliquid_bbo_feed_race_summary (
  event_ts DateTime64(9), ingested_at DateTime64(9) DEFAULT now64(9),
  capture_run_id String, measurement_node_id String, host String,
  location_code LowCardinality(String), feed_type LowCardinality(String) DEFAULT 'bbo',
  symbol LowCardinality(String), source_ts_ms UInt64, bbo_hash UInt64,
  feed LowCardinality(String), loser_feed LowCardinality(String) DEFAULT '',
  total_events UInt64, events_won UInt64,
  lead_time_p50_ms Float64 DEFAULT 0, lead_time_p95_ms Float64 DEFAULT 0,
  send_lead_time_p50_ms Nullable(Float64) DEFAULT NULL, send_lead_time_p95_ms Nullable(Float64) DEFAULT NULL
) ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toDate(event_ts)
ORDER BY (measurement_node_id, symbol, source_ts_ms, bbo_hash, feed, loser_feed);
SQL

echo "==> Fetching last ${MINUTES}m from remote..."
curl -sS --fail-with-body "https://${REMOTE_HOST}:${REMOTE_HTTPS_PORT}/?database=${REMOTE_DB}" \
  --user "${REMOTE_USER}:${REMOTE_PASS}" \
  --data-binary "SELECT ${COLS} FROM hyperliquid_bbo_feed_race_summary WHERE event_ts >= now() - INTERVAL ${MINUTES} MINUTE FORMAT TabSeparated" \
  > /tmp/hl_feed_race.tsv

ROWS=$(wc -l < /tmp/hl_feed_race.tsv | tr -d ' ')
echo "==> Inserting ${ROWS} rows locally..."
clickhouse client --host "$LOCAL_ADDR" --port "$LOCAL_PORT" \
  --query "INSERT INTO feeds.hyperliquid_bbo_feed_race_summary (${COLS}) FORMAT TabSeparated" \
  < /tmp/hl_feed_race.tsv
rm -f /tmp/hl_feed_race.tsv
echo "==> Done. Seeded ${ROWS} rows into feeds.hyperliquid_bbo_feed_race_summary"
