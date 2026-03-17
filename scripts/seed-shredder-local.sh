#!/usr/bin/env bash
# Seed local ClickHouse with slot_feed_races data from remote shredder_qa.
# Reads credentials from .env file.
# Usage: ./scripts/seed-shredder-local.sh [LIMIT]
#   LIMIT = max rows to fetch (omit for all data)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

# Load .env
if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  source "$ROOT_DIR/.env"
  set +a
fi

# Remote ClickHouse Cloud
REMOTE_HOST="${SEED_CH_SHREDDER_HOST:-btjr1b5uy8.us-east-1.aws.clickhouse.cloud}"
REMOTE_PORT="${SEED_CH_SHREDDER_PORT:-8443}"
REMOTE_DB="${SEED_CH_SHREDDER_DB:-shredder_qa}"
REMOTE_USER="${SEED_CH_SHREDDER_USER:-shredder_qa}"
REMOTE_PASS="${SEED_CH_SHREDDER_PASSWORD:-}"

if [[ -z "$REMOTE_PASS" ]]; then
  echo "Error: SEED_CH_SHREDDER_PASSWORD not set in .env"
  exit 1
fi

# Local ClickHouse
LOCAL_HOST="${CLICKHOUSE_ADDR_TCP:-localhost:9100}"
LOCAL_USER="${CLICKHOUSE_USERNAME:-default}"
LOCAL_PASS="${CLICKHOUSE_PASSWORD:-}"
LOCAL_PORT="${LOCAL_HOST##*:}"
LOCAL_ADDR="${LOCAL_HOST%%:*}"

# Optional row limit
LIMIT="${1:-}"
LIMIT_CLAUSE=""
if [[ -n "$LIMIT" ]]; then
  LIMIT_CLAUSE="LIMIT $LIMIT"
fi

echo "==> Creating local database and table (truncating existing data)..."
clickhouse client \
  --host "$LOCAL_ADDR" --port "$LOCAL_PORT" \
  --user "$LOCAL_USER" ${LOCAL_PASS:+--password "$LOCAL_PASS"} \
  --multiquery <<'SQL'
CREATE DATABASE IF NOT EXISTS shredder_qa;

DROP TABLE IF EXISTS shredder_qa.slot_feed_races;

CREATE TABLE shredder_qa.slot_feed_races (
    event_ts DateTime64(3),
    ingested_at DateTime64(3) DEFAULT now64(3),
    node_id String,
    feed_type String DEFAULT 'shred',
    epoch UInt64,
    slot UInt64,
    feed String,
    loser_feed String DEFAULT '',
    total_shreds UInt64,
    shreds_won UInt64,
    lead_time_p50_ms Float64 DEFAULT 0,
    lead_time_p95_ms Float64 DEFAULT 0
) ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_ts)
ORDER BY (node_id, slot, feed, loser_feed);
SQL

echo "==> Fetching${LIMIT:+ $LIMIT} rows from remote shredder_qa.slot_feed_races..."

# Export from remote as TSV
curl -sS "https://${REMOTE_HOST}:${REMOTE_PORT}/?database=${REMOTE_DB}" \
  --user "${REMOTE_USER}:${REMOTE_PASS}" \
  --data-binary "SELECT event_ts, ingested_at, node_id, feed_type, epoch, slot, feed, loser_feed, total_shreds, shreds_won, lead_time_p50_ms, lead_time_p95_ms FROM slot_feed_races WHERE event_ts >= now() - INTERVAL 1 HOUR ORDER BY event_ts DESC ${LIMIT_CLAUSE} FORMAT TabSeparated" \
  > /tmp/slot_feed_races.tsv

ROWS=$(wc -l < /tmp/slot_feed_races.tsv | tr -d ' ')
echo "==> Got $ROWS rows, inserting into local shredder_qa.slot_feed_races..."

# Insert into local
clickhouse client \
  --host "$LOCAL_ADDR" --port "$LOCAL_PORT" \
  --user "$LOCAL_USER" ${LOCAL_PASS:+--password "$LOCAL_PASS"} \
  --query "INSERT INTO shredder_qa.slot_feed_races (event_ts, ingested_at, node_id, feed_type, epoch, slot, feed, loser_feed, total_shreds, shreds_won, lead_time_p50_ms, lead_time_p95_ms) FORMAT TabSeparated" \
  < /tmp/slot_feed_races.tsv

rm -f /tmp/slot_feed_races.tsv

echo "==> Done! Seeded $ROWS rows into local shredder_qa.slot_feed_races"
echo "==> Verify: clickhouse client --host $LOCAL_ADDR --port $LOCAL_PORT -q \"SELECT loser_feed, count() FROM shredder_qa.slot_feed_races GROUP BY loser_feed ORDER BY loser_feed\""
