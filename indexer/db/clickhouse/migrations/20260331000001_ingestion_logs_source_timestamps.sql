-- +goose Up

-- +goose StatementBegin
ALTER TABLE log_ingestion_runs
    ADD COLUMN IF NOT EXISTS source_min_event_ts Nullable(DateTime64(3)),
    ADD COLUMN IF NOT EXISTS source_max_event_ts Nullable(DateTime64(3));
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
ALTER TABLE log_ingestion_runs
    DROP COLUMN IF EXISTS source_min_event_ts,
    DROP COLUMN IF EXISTS source_max_event_ts;
-- +goose StatementEnd
