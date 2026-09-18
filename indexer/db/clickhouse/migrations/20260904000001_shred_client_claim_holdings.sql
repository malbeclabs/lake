-- +goose Up

-- Live SPL token balances held by the client-claim PDAs. The indexer replaces
-- this snapshot on every refresh, so a closed holding is represented by a
-- tombstone and never remains claimable after a sweep or a completed claim.
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS stg_dim_dz_shred_client_claim_holdings_snapshot (
    entity_id                   String,
    snapshot_ts                 DateTime64(3),
    ingested_at                 DateTime64(3),
    op_id                       UUID,
    is_deleted                  UInt8 DEFAULT 0,
    attrs_hash                  UInt64,
    pk                          String,
    client_id                   UInt16,
    validator_client_rewards    String,
    claim_holding               String,
    subscription_epoch          UInt64,
    mint                        String,
    base_unit_balance           UInt64
) ENGINE = MergeTree
PARTITION BY toDate(snapshot_ts)
ORDER BY (op_id, entity_id)
TTL ingested_at + INTERVAL 7 DAY;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS dim_dz_shred_client_claim_holdings_history (
    entity_id                   String,
    snapshot_ts                 DateTime64(3),
    ingested_at                 DateTime64(3),
    op_id                       UUID,
    is_deleted                  UInt8 DEFAULT 0,
    attrs_hash                  UInt64,
    pk                          String,
    client_id                   UInt16,
    validator_client_rewards    String,
    claim_holding               String,
    subscription_epoch          UInt64,
    mint                        String,
    base_unit_balance           UInt64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (entity_id, snapshot_ts, ingested_at, op_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE VIEW dim_dz_shred_client_claim_holdings_current AS
WITH ranked AS (
    SELECT *,
        row_number() OVER (PARTITION BY entity_id ORDER BY snapshot_ts DESC, ingested_at DESC, op_id DESC) AS rn
    FROM dim_dz_shred_client_claim_holdings_history
)
SELECT entity_id, snapshot_ts, ingested_at, op_id, attrs_hash, pk,
    client_id, validator_client_rewards, claim_holding, subscription_epoch, mint, base_unit_balance
FROM ranked
WHERE rn = 1 AND is_deleted = 0;
-- +goose StatementEnd

-- +goose Down

DROP VIEW IF EXISTS dim_dz_shred_client_claim_holdings_current;
DROP TABLE IF EXISTS dim_dz_shred_client_claim_holdings_history;
DROP TABLE IF EXISTS stg_dim_dz_shred_client_claim_holdings_snapshot;
