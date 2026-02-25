-- +goose Up

-- Fix validator views to use client_ip instead of dz_ip for gossip_ip joins.
-- client_ip is the correct column because multicast users advertise their
-- client_ip (not dz_ip) as their gossip address. client_ip is a strict
-- superset of dz_ip matches.

-- +goose StatementBegin
CREATE OR REPLACE VIEW solana_validators_on_dz_current
AS
SELECT
    va.vote_pubkey AS vote_pubkey,
    va.node_pubkey AS node_pubkey,
    u.owner_pubkey AS owner_pubkey,
    u.dz_ip AS dz_ip,
    u.client_ip AS client_ip,
    u.device_pk AS device_pk,
    d.code AS device_code,
    m.code AS device_metro_code,
    m.name AS device_metro_name,
    va.activated_stake_lamports AS activated_stake_lamports,
    va.activated_stake_lamports / 1000000000.0 AS activated_stake_sol,
    va.commission_percentage AS commission_percentage,
    va.epoch AS epoch,
    -- Connection timestamp is the latest of when each component appeared
    GREATEST(u.snapshot_ts, gn.snapshot_ts, va.snapshot_ts) AS connected_ts
FROM dz_users_current u
JOIN solana_gossip_nodes_current gn ON u.client_ip = gn.gossip_ip
JOIN solana_vote_accounts_current va ON gn.pubkey = va.node_pubkey
LEFT JOIN dz_devices_current d ON u.device_pk = d.pk
LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
WHERE u.status = 'activated'
  AND u.client_ip != ''
  AND va.epoch_vote_account = 'true'
  AND va.activated_stake_lamports > 0;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE VIEW solana_validators_on_dz_connections
AS
WITH connection_events AS (
    -- Find all times when a validator was connected (user, gossip node, and vote account all exist together)
    -- The connection timestamp is the maximum of the three snapshot_ts values
    SELECT
        va.vote_pubkey,
        va.node_pubkey,
        u.owner_pubkey,
        u.client_ip,
        u.device_pk,
        va.activated_stake_lamports,
        va.commission_percentage,
        GREATEST(u.snapshot_ts, gn.snapshot_ts, va.snapshot_ts) AS connected_ts
    FROM dim_dz_users_history u
    JOIN dim_solana_gossip_nodes_history gn ON u.client_ip = gn.gossip_ip AND gn.gossip_ip != ''
    JOIN dim_solana_vote_accounts_history va ON gn.pubkey = va.node_pubkey
    WHERE u.is_deleted = 0 AND u.status = 'activated' AND u.client_ip != ''
      AND gn.is_deleted = 0
      AND va.is_deleted = 0 AND va.epoch_vote_account = 'true' AND va.activated_stake_lamports > 0
),
first_connections AS (
    -- Get first connection time per validator (GROUP BY only immutable identifiers)
    SELECT
        vote_pubkey,
        node_pubkey,
        MIN(connected_ts) AS first_connected_ts,
        MAX(connected_ts) AS last_connected_ts
    FROM connection_events
    GROUP BY vote_pubkey, node_pubkey
),
latest_values AS (
    -- Get latest stake/commission values per validator using row_number
    SELECT
        vote_pubkey,
        node_pubkey,
        owner_pubkey,
        client_ip,
        device_pk,
        activated_stake_lamports,
        commission_percentage,
        ROW_NUMBER() OVER (PARTITION BY vote_pubkey, node_pubkey ORDER BY connected_ts DESC) AS rn
    FROM connection_events
)
SELECT
    fc.vote_pubkey AS vote_pubkey,
    fc.node_pubkey AS node_pubkey,
    lv.owner_pubkey AS owner_pubkey,
    lv.client_ip AS dz_ip,
    lv.device_pk AS device_pk,
    d.code AS device_code,
    m.code AS device_metro_code,
    m.name AS device_metro_name,
    lv.activated_stake_lamports AS activated_stake_lamports,
    lv.activated_stake_lamports / 1000000000.0 AS activated_stake_sol,
    lv.commission_percentage AS commission_percentage,
    fc.first_connected_ts AS first_connected_ts
FROM first_connections fc
JOIN latest_values lv ON fc.vote_pubkey = lv.vote_pubkey AND fc.node_pubkey = lv.node_pubkey AND lv.rn = 1
LEFT JOIN dz_devices_current d ON lv.device_pk = d.pk
LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE VIEW solana_validators_disconnections
AS
WITH connection_events AS (
    -- Find all times when a validator was connected (user, gossip node, and vote account all exist together)
    SELECT
        va.vote_pubkey,
        va.node_pubkey,
        u.owner_pubkey,
        u.client_ip,
        u.device_pk,
        u.entity_id AS user_entity_id,
        va.activated_stake_lamports,
        va.commission_percentage,
        GREATEST(u.snapshot_ts, gn.snapshot_ts, va.snapshot_ts) AS connected_ts
    FROM dim_dz_users_history u
    JOIN dim_solana_gossip_nodes_history gn ON u.client_ip = gn.gossip_ip AND gn.gossip_ip != ''
    JOIN dim_solana_vote_accounts_history va ON gn.pubkey = va.node_pubkey
    WHERE u.is_deleted = 0 AND u.status = 'activated' AND u.client_ip != ''
      AND gn.is_deleted = 0
      AND va.is_deleted = 0 AND va.epoch_vote_account = 'true' AND va.activated_stake_lamports > 0
),
disconnection_events AS (
    -- Find when users were deleted (disconnected)
    SELECT
        entity_id AS user_entity_id,
        snapshot_ts AS disconnected_ts
    FROM dim_dz_users_history
    WHERE is_deleted = 1
),
validator_disconnections AS (
    -- Join connection events with disconnection events
    -- A validator disconnected if they had a connection and the user was later deleted
    SELECT
        ce.vote_pubkey,
        ce.node_pubkey,
        ce.owner_pubkey,
        ce.client_ip,
        ce.device_pk,
        ce.activated_stake_lamports,
        ce.commission_percentage,
        ce.connected_ts,
        de.disconnected_ts,
        ROW_NUMBER() OVER (PARTITION BY ce.vote_pubkey, ce.node_pubkey ORDER BY de.disconnected_ts DESC) AS rn
    FROM connection_events ce
    JOIN disconnection_events de ON ce.user_entity_id = de.user_entity_id
    WHERE de.disconnected_ts > ce.connected_ts  -- Disconnection must be after connection
)
SELECT
    vd.vote_pubkey AS vote_pubkey,
    vd.node_pubkey AS node_pubkey,
    vd.owner_pubkey AS owner_pubkey,
    vd.client_ip AS dz_ip,
    vd.device_pk AS device_pk,
    d.code AS device_code,
    m.code AS device_metro_code,
    m.name AS device_metro_name,
    vd.activated_stake_lamports AS activated_stake_lamports,
    vd.activated_stake_lamports / 1000000000.0 AS activated_stake_sol,
    vd.commission_percentage AS commission_percentage,
    vd.connected_ts AS connected_ts,
    vd.disconnected_ts AS disconnected_ts
FROM validator_disconnections vd
LEFT JOIN dz_devices_current d ON vd.device_pk = d.pk
LEFT JOIN dz_metros_current m ON d.metro_pk = m.pk
-- Most recent disconnection per validator, excluding currently connected
WHERE vd.rn = 1
  AND vd.vote_pubkey NOT IN (SELECT vote_pubkey FROM solana_validators_on_dz_current);
-- +goose StatementEnd

-- +goose Down
-- Note: Down migrations would re-create the views using dz_ip.
-- Since we use CREATE OR REPLACE, re-running up is safe.
