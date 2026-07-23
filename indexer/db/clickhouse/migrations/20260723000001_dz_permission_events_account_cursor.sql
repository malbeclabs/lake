-- +goose Up

-- Durable per-account resume cursor for the steady-state Permission PDA watch.
-- The fact table cannot serve as the cursor: a Permission PDA is also referenced
-- by non-permission serviceability instructions (e.g. multicast allowlist ops)
-- that decode to zero audit rows, so max(slot) over fact rows never advances
-- through them. This tracks the newest fully-processed signature per PDA so each
-- refresh resumes where the last one stopped, even mid-backlog.
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS dz_permission_events_account_cursor
(
    permission_pk     String,
    last_tx_signature String,
    last_slot         UInt64,
    updated_at        DateTime64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (permission_pk);
-- +goose StatementEnd

-- +goose Down
DROP TABLE IF EXISTS dz_permission_events_account_cursor;
