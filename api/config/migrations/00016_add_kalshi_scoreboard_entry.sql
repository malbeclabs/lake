-- +goose Up
-- Scoreboard feed allow-list for the Kalshi venue. Only enabled rows are raced, counted, or
-- displayed on the Kalshi scoreboard. Intentionally seeded with no rows: the rows are
-- environment config, inserted out of band, so they never live in this repository.
--
-- Kept separate from the Hyperliquid allow-list rather than keyed by a shared `subject`
-- column: the two venues have different feed namespaces and are operated independently, so a
-- shared table would couple their config changes without buying anything.
CREATE TABLE kalshi_scoreboard_entry (
    feed          TEXT PRIMARY KEY,
    label         TEXT NOT NULL CHECK (length(label) BETWEEN 1 AND 64),
    display_order INT  NOT NULL DEFAULT 0,
    enabled       BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- +goose Down
DROP TABLE IF EXISTS kalshi_scoreboard_entry;
