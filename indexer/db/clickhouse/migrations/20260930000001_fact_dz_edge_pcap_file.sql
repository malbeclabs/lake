-- +goose Up
-- One row per pcap file the edge recorders offloaded to the multicast pcap warehouse
-- (s3://malbeclabs-multicast-pcap-warehouse/<network>/<host>-<public IP>/YYYY/MM/DD/HH/),
-- read from the manifest_*.yaml each offload writes beside its files. The manifest is
-- what carries first/last packet time, so coverage and gaps are measurable at file
-- grain rather than inferred from which hour directories exist.
--
-- A key can hold more than one file over time. A recorder that restarts resets its file
-- counter, so its next capture_<group>_000001.pcap overwrites the object an earlier
-- offload in the same hour uploaded — observed on dub, cmh and fra. Two manifests then
-- name one key with different contents, and only the later one describes what the
-- bucket still holds. Both are kept, keyed on (s3_key, manifest_key): the earlier one
-- is the record that a capture existed and was lost, which a replace on s3_key alone
-- would merge away. dz_edge_pcap_file_current resolves each key to its newest offload.
-- The replace here only collapses a manifest read twice. hour_ts is in the key's path,
-- so partitioning on it keeps every version of a key in one partition.
--
-- Column order is part of the contract: WriteBatch issues a bare INSERT with no column
-- list, so it must match pcapwarehouse.fileSchema.ToRow exactly.
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS fact_dz_edge_pcap_file
(
    first_packet_ts DateTime64(6),
    ingested_at     DateTime64(3),
    recorder        LowCardinality(String),
    recorder_ip     LowCardinality(String),
    hour_ts         DateTime,
    s3_key          String,
    multicast_group LowCardinality(String),
    multicast_port  UInt16,
    size_bytes      UInt64,
    packets         UInt64,
    last_packet_ts  DateTime64(6),
    md5             String,
    manifest_key    String,
    offload_ts      DateTime64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(hour_ts)
ORDER BY (multicast_group, recorder, recorder_ip, s3_key, manifest_key);
-- +goose StatementEnd

-- What the bucket holds now: each key as its newest offload describes it, with the
-- number of distinct offloads that wrote it (overwrites = versions - 1).
-- +goose StatementBegin
CREATE OR REPLACE VIEW dz_edge_pcap_file_current
AS
-- Source columns are qualified: the output aliases reuse their names, and an
-- unqualified offload_ts inside argMax would resolve to max(offload_ts) below.
SELECT
    f.multicast_group                       AS multicast_group,
    f.recorder                              AS recorder,
    f.recorder_ip                           AS recorder_ip,
    f.s3_key                                AS s3_key,
    f.hour_ts                               AS hour_ts,
    argMax(f.first_packet_ts, f.offload_ts) AS first_packet_ts,
    argMax(f.last_packet_ts, f.offload_ts)  AS last_packet_ts,
    argMax(f.size_bytes, f.offload_ts)      AS size_bytes,
    argMax(f.packets, f.offload_ts)         AS packets,
    argMax(f.multicast_port, f.offload_ts)  AS multicast_port,
    argMax(f.md5, f.offload_ts)             AS md5,
    argMax(f.manifest_key, f.offload_ts)    AS manifest_key,
    max(f.offload_ts)                       AS offload_ts,
    max(f.ingested_at)                      AS ingested_at,
    uniqExact(f.manifest_key)               AS versions
FROM fact_dz_edge_pcap_file AS f
GROUP BY f.multicast_group, f.recorder, f.recorder_ip, f.s3_key, f.hour_ts;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP VIEW IF EXISTS dz_edge_pcap_file_current;
-- +goose StatementEnd
-- +goose StatementBegin
DROP TABLE IF EXISTS fact_dz_edge_pcap_file;
-- +goose StatementEnd
