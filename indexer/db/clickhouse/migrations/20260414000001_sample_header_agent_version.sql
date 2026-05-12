-- +goose Up

-- +goose StatementBegin
ALTER TABLE fact_dz_device_link_latency_sample_header
    ADD COLUMN IF NOT EXISTS agent_version String DEFAULT '',
    ADD COLUMN IF NOT EXISTS agent_commit String DEFAULT '';
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
ALTER TABLE fact_dz_device_link_latency_sample_header
    DROP COLUMN IF EXISTS agent_commit,
    DROP COLUMN IF EXISTS agent_version;
-- +goose StatementEnd
