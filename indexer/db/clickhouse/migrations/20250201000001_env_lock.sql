-- +goose Up
CREATE TABLE IF NOT EXISTS _env_lock (
    dz_env String
) ENGINE = MergeTree ORDER BY dz_env;

-- +goose Down
DROP TABLE IF EXISTS _env_lock;
