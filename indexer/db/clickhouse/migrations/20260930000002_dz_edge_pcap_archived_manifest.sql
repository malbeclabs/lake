-- +goose Up
-- Offload manifests the pcap warehouse sync found archived. The bucket's lifecycle
-- rule moves every object over 128 KB to GLACIER after 90 days, and a manifest that
-- large (the catch-up offload after an outage) cannot be read until it is restored.
-- The indexer only meets them while backfilling history — it reads manifests hours
-- after they are written — so this is the set a one-time restore has to cover.
--
-- The sync writes a key here with state 'archived' when a read is refused, retries
-- every archived key on each pass (a restore takes hours and needs a role that may
-- write to the bucket, so it is an operator's step), and writes 'indexed' once the
-- manifest is read, or 'refused' if it turns out to be malformed. The newest
-- updated_at per key is the current state. The Edge History page counts the hours
-- still 'archived' per recorder, so a gap there reads as unread, not as lost.
--
-- Column order is part of the contract: inserts are issued with an explicit column
-- list by pcapwarehouse.Store, not through a dataset.
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS dz_edge_pcap_archived_manifest
(
    manifest_key String,
    recorder     LowCardinality(String),
    recorder_ip  LowCardinality(String),
    hour_ts      DateTime,
    state        LowCardinality(String),
    updated_at   DateTime64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY manifest_key;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS dz_edge_pcap_archived_manifest;
-- +goose StatementEnd
