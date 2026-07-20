-- +goose Up
-- Scoreboard feed allow-list. Only enabled rows are raced, counted, or displayed on the
-- Hyperliquid scoreboard. Intentionally seeded with no rows: the rows are environment
-- config, inserted out of band, so they never live in this repository.
CREATE TABLE hyperliquid_scoreboard_entry (
    feed          TEXT PRIMARY KEY,
    label         TEXT NOT NULL,
    display_order INT  NOT NULL DEFAULT 0,
    enabled       BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- +goose Down
DROP TABLE IF EXISTS hyperliquid_scoreboard_entry;
