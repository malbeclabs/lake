-- +goose Up

-- Append-only audit trail of serviceability Permission-management instructions
-- (grant / change / suspend / resume / revoke). One row per decoded instruction.
-- Column order must match permissionevents.permissionEventSchema.ToRow().
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS fact_dz_permission_events
(
    event_ts                 DateTime64(3),
    ingested_at              DateTime64(3),
    tx_signature             String,
    slot                     UInt64,
    instruction_index        UInt16,
    signer                   String DEFAULT '',
    permission_pk            String,
    target_pubkey            String DEFAULT '',
    event_type               String,
    permissions_added        String DEFAULT '',
    permissions_removed      String DEFAULT '',
    permissions_added_mask   String DEFAULT '',
    permissions_removed_mask String DEFAULT '',
    success                  UInt8 DEFAULT 1
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_ts)
ORDER BY (permission_pk, slot, tx_signature, instruction_index);
-- +goose StatementEnd

-- Scan cursor for the program-wide signature sweep. The fact table only holds
-- permission events, so it cannot serve as the resume cursor for a program-wide scan
-- (rare permission events among many serviceability txs). This tracks the newest
-- signature already scanned per watched program, so each signature is fetched once.
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS dz_permission_events_scan_cursor
(
    program_pk        String,
    last_tx_signature String,
    last_slot         UInt64,
    updated_at        DateTime64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (program_pk);
-- +goose StatementEnd

-- +goose Down
DROP TABLE IF EXISTS fact_dz_permission_events;
DROP TABLE IF EXISTS dz_permission_events_scan_cursor;
