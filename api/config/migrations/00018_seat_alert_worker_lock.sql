-- +goose Up
CREATE TABLE seat_alert_worker_lock (
    id          INTEGER PRIMARY KEY DEFAULT 1,
    claimed_by  TEXT,
    claimed_at  TIMESTAMPTZ,
    CONSTRAINT seat_alert_worker_lock_singleton CHECK (id = 1)
);
INSERT INTO seat_alert_worker_lock (id) VALUES (1);

-- +goose Down
DROP TABLE IF EXISTS seat_alert_worker_lock;
