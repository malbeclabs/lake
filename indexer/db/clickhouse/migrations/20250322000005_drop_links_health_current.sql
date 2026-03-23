-- +goose Up

DROP VIEW IF EXISTS dz_links_health_current;

-- +goose Down

-- View can be recreated from migration 20250321000002_link_health_use_sample_headers.sql if needed.
