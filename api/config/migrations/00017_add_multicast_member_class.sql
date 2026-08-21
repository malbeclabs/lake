-- +goose Up
-- Operator-asserted classification for multicast group members, keyed by the member's
-- client_ip in dz_users_current. It answers one question the ledger cannot: which of a
-- group's subscribers are DoubleZero's own recording and measurement boxes, and which are
-- customers receiving the feed they paid for.
--
-- Keyed on client_ip and NOT on the user pk. A DoubleZero user account is torn down and
-- re-created constantly — the operator wallet alone has minted thousands of distinct pks
-- against a few dozen distinct client_ips over the history table's life — so a pk-keyed row
-- goes stale within hours while an IP-keyed one survives every tunnel rebuild. owner_pubkey
-- is stable but far too coarse: one wallet owns recorders, probes and lab boxes at once.
--
-- A client_ip is not globally unique across owners (a box commonly runs one IBRL user and one
-- multicast user), which is fine: the assertion here is "this box is ours", and that holds for
-- every account on it.
--
-- Intentionally seeded with no rows. Which boxes are DoubleZero's is environment config, it
-- differs between mainnet and testnet, and it is inserted out of band — it never lives in this
-- repository. An unseeded environment falls back to the derived signal and then to 'customer',
-- which is also the expected local-dev state.
CREATE TABLE IF NOT EXISTS multicast_member_class (
    client_ip  TEXT PRIMARY KEY CHECK (length(client_ip) BETWEEN 3 AND 45),
    class      TEXT NOT NULL CHECK (class IN ('recorder', 'internal_probe', 'customer')),
    label      TEXT NOT NULL DEFAULT '' CHECK (length(label) <= 64),
    note       TEXT NOT NULL DEFAULT '' CHECK (length(note) <= 256),
    enabled    BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- +goose Down
DROP TABLE IF EXISTS multicast_member_class;
