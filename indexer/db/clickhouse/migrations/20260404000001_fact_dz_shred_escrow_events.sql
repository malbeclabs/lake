-- +goose Up

-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS fact_dz_shred_escrow_events
(
    event_ts           DateTime64(3),
    ingested_at        DateTime64(3),
    escrow_pk          String,
    client_seat_pk     String,
    tx_signature       String,
    slot               UInt64,
    event_type         String,
    amount_usdc        Nullable(Int64),
    balance_after_usdc Nullable(Int64),
    epoch              Nullable(UInt64),
    status             String DEFAULT 'ok',
    signer             String DEFAULT ''
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_ts)
ORDER BY (escrow_pk, slot, tx_signature, event_type);
-- +goose StatementEnd

-- +goose Down
DROP TABLE IF EXISTS fact_dz_shred_escrow_events;
