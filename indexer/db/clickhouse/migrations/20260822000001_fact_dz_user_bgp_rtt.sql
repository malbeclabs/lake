-- +goose Up
--
-- The BGP TCP round-trip time between a client box and the DoubleZero device it
-- attaches to, as the client agent reports it onchain (serviceability User account,
-- BgpRttNs — "the smoothed BGP TCP RTT in nanoseconds", read from the kernel's
-- tcp_info for the BGP socket). It is the only measurement of the access path that
-- exists anywhere: fact_dz_device_link_latency is device-to-device across the
-- backbone, fact_dz_internet_metro_latency is metro-to-metro over the public
-- internet, and the telemetry mirror carries no timing of the user tunnel at all.
--
-- A FACT and not a column on dim_dz_users_history, deliberately. attrs_hash covers
-- every payload column of that dimension, so a hashed rtt would mint a new history
-- row per user on every onchain rewrite — the same churn 20260708000001 refused for
-- last_bgp_reported_at — and an unhashed one would freeze at whatever value happened
-- to be current when some other attribute last changed. Neither trade is necessary:
-- this is a measurement over time, which is what a fact table is for, and modelling
-- it as one also leaves the series behind to answer whether a customer's path is
-- degrading.
--
-- Keyed on (user_pk, reported_at_slot) so the row count follows the onchain WRITES
-- and not the indexer's snapshot cadence. The agent submits only on a BGP status
-- change or on its ~6-hourly keepalive (doublezero controlplane, bgpstatus
-- shouldSubmit), so a stable session contributes about four rows a day while the
-- indexer polls every 60s. ReplacingMergeTree collapses the re-observations.
--
-- last_bgp_reported_at is the slot of that write, which is exactly the event
-- identity, and it is ingested HERE rather than into the dimension for the same
-- reason the rtt is: it changes on every keepalive.
--
-- Two values are recorded as reported, without interpretation. bgp_rtt_ns is 0 when
-- the agent reports the session down — the contract clears it — so a reader wanting
-- "the last known good RTT" has to filter on bgp_status, and a reader wanting "what
-- the account said" has it either way.

-- Column order is part of the contract: WriteBatch issues a bare INSERT with no column
-- list, so it must match dzsvc.userBGPRTTRow exactly.
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS fact_dz_user_bgp_rtt
(
    event_ts         DateTime64(3),
    ingested_at      DateTime64(3),
    user_pk          String,
    device_pk        String,
    client_ip        String,
    dz_ip            String,
    tunnel_id        Int32,
    reported_at_slot UInt64,
    up_at_slot       UInt64,
    bgp_status       String,
    bgp_rtt_ns       UInt64
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_ts)
ORDER BY (user_pk, reported_at_slot);
-- +goose StatementEnd

-- The newest report per user. Same shape as the dim *_current views: a window over
-- the whole table, which stays cheap because the row count follows onchain writes
-- rather than the poll loop.
-- +goose StatementBegin
CREATE OR REPLACE VIEW dz_user_bgp_rtt_current
AS
WITH ranked AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY user_pk ORDER BY reported_at_slot DESC, ingested_at DESC) AS rn
    FROM fact_dz_user_bgp_rtt
)
SELECT
    event_ts,
    ingested_at,
    user_pk,
    device_pk,
    client_ip,
    dz_ip,
    tunnel_id,
    reported_at_slot,
    up_at_slot,
    bgp_status,
    bgp_rtt_ns
FROM ranked
WHERE rn = 1;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP VIEW IF EXISTS dz_user_bgp_rtt_current;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS fact_dz_user_bgp_rtt;
-- +goose StatementEnd
