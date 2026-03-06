-- Seed script for publisher_shred_stats table (normally created by shredder service).
-- Run against local ClickHouse after dev-setup.sh:
--   clickhouse-client --port 9100 --multiquery < scripts/seed-publisher-shred-stats.sql

CREATE TABLE IF NOT EXISTS publisher_shred_stats (
    event_ts DateTime64(3),
    ingested_at DateTime64(3),
    host String,
    publisher_ip String,
    client_ip String,
    node_pubkey String,
    vote_pubkey String,
    activated_stake UInt64,
    dz_user_pubkey String,
    dz_device_code String,
    dz_metro_code String,
    epoch UInt64,
    slot UInt64,
    total_packets UInt64,
    unique_shreds UInt64,
    data_shreds UInt64,
    coding_shreds UInt64,
    max_data_index Int64,
    needs_repair Bool,
    first_seen_ns Int64,
    last_seen_ns Int64,
    is_scheduled_leader Bool
) ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_ts)
ORDER BY (host, slot, publisher_ip);

-- Sample publishers: mix of healthy and unhealthy configurations.
-- Epoch 800, ~10 publishers with varying states:
--   - Some publishing leader shreds (good)
--   - Some retransmitting (bad)
--   - Various validator clients and versions
--   - Some with needs_repair = true

INSERT INTO publisher_shred_stats VALUES
-- Publisher 1: Healthy Jito validator, leader shreds, no retransmit
(now(), now(), 'shredder-local', '10.1.0.1', '203.0.113.1', 'nodepub_healthy1', 'votepub_healthy1', 500000000000000, 'dzuser_healthy1', 'ams1', 'AMS', 800, 1001, 128, 64, 32, 32, 31, false, 0, 0, true),
(now(), now(), 'shredder-local', '10.1.0.1', '203.0.113.1', 'nodepub_healthy1', 'votepub_healthy1', 500000000000000, 'dzuser_healthy1', 'ams1', 'AMS', 800, 1002, 128, 64, 32, 32, 31, false, 0, 0, true),
(now(), now(), 'shredder-local', '10.1.0.1', '203.0.113.1', 'nodepub_healthy1', 'votepub_healthy1', 500000000000000, 'dzuser_healthy1', 'ams1', 'AMS', 800, 1003, 128, 64, 32, 32, 31, false, 0, 0, true),

-- Publisher 2: Healthy Agave validator, leader shreds, no retransmit
(now(), now(), 'shredder-local', '10.1.0.2', '203.0.113.2', 'nodepub_healthy2', 'votepub_healthy2', 200000000000000, 'dzuser_healthy2', 'nyc1', 'NYC', 800, 2001, 128, 64, 32, 32, 31, false, 0, 0, true),

-- Publisher 3: Bad - retransmitting shreds (is_scheduled_leader=false)
(now(), now(), 'shredder-local', '10.1.0.3', '203.0.113.3', 'nodepub_retransmit', 'votepub_retransmit', 100000000000000, 'dzuser_retransmit', 'lax1', 'LAX', 800, 1001, 64, 32, 16, 16, 15, false, 0, 0, false),
(now(), now(), 'shredder-local', '10.1.0.3', '203.0.113.3', 'nodepub_retransmit', 'votepub_retransmit', 100000000000000, 'dzuser_retransmit', 'lax1', 'LAX', 800, 1002, 64, 32, 16, 16, 15, false, 0, 0, false),

-- Publisher 4: Mixed - has both leader and retransmit shreds
(now(), now(), 'shredder-local', '10.1.0.4', '203.0.113.4', 'nodepub_mixed', 'votepub_mixed', 300000000000000, 'dzuser_mixed', 'ewr1', 'EWR', 800, 3001, 128, 64, 32, 32, 31, false, 0, 0, true),
(now(), now(), 'shredder-local', '10.1.0.4', '203.0.113.4', 'nodepub_mixed', 'votepub_mixed', 300000000000000, 'dzuser_mixed', 'ewr1', 'EWR', 800, 3002, 64, 32, 16, 16, 15, false, 0, 0, false),

-- Publisher 5: Has needs_repair slots
(now(), now(), 'shredder-local', '10.1.0.5', '203.0.113.5', 'nodepub_repair', 'votepub_repair', 80000000000000, 'dzuser_repair', 'fra1', 'FRA', 800, 4001, 100, 50, 25, 25, 30, true, 0, 0, true),

-- Publisher 6: Outdated Agave version
(now(), now(), 'shredder-local', '10.1.0.6', '203.0.113.6', 'nodepub_oldver', 'votepub_oldver', 60000000000000, 'dzuser_oldver', 'sin1', 'SIN', 800, 5001, 128, 64, 32, 32, 31, false, 0, 0, true);
