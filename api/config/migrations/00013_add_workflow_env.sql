-- +goose Up
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS env VARCHAR(20) NOT NULL DEFAULT 'mainnet-beta';

-- +goose Down
ALTER TABLE workflow_runs DROP COLUMN IF EXISTS env;
