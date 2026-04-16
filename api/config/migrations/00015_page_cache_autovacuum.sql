-- +goose Up
-- Aggressive autovacuum on page_cache to reclaim dead tuples from frequent upserts.
-- With ~4MB payloads upserted every 30s, dead tuple bloat fills disk quickly without this.
ALTER TABLE page_cache SET (
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 10,
    autovacuum_analyze_scale_factor = 0,
    autovacuum_analyze_threshold = 10
);

-- +goose Down
ALTER TABLE page_cache RESET (
    autovacuum_vacuum_scale_factor,
    autovacuum_vacuum_threshold,
    autovacuum_analyze_scale_factor,
    autovacuum_analyze_threshold
);