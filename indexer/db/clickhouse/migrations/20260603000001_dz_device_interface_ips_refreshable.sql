-- +goose Up

-- +goose StatementBegin
-- dz_device_interface_ips was originally defined as a plain VIEW that did
-- ARRAY JOIN JSONExtractArrayRaw on dz_devices_current.interfaces. That
-- works for direct SELECT, but ClickHouse 25.12's optimizer fails to
-- estimate sizes when downstream queries JOIN against a column derived
-- from ARRAY JOIN + JSONExtract — the planner allocates max-int sized
-- hash tables and the query OOMs (observed in production: enriched_ip_msdp_peers
-- and enriched_ip_msdp_sa_cache both crash with 128 TiB allocation
-- attempts when their peer_device / remote_device columns are selected).
--
-- Fix: drop the regular view and recreate dz_device_interface_ips as a
-- REFRESHABLE MATERIALIZED VIEW. The JSON parsing happens once per refresh
-- and the result is stored in a real MergeTree table, so downstream JOINs
-- see a normal table with proper size statistics. Refresh cadence is 60
-- seconds — device interface lists change rarely (only when devices are
-- added/removed/reconfigured) so the freshness lag is acceptable.
DROP VIEW IF EXISTS dz_device_interface_ips;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE MATERIALIZED VIEW dz_device_interface_ips
REFRESH EVERY 1 MINUTE
ENGINE = MergeTree()
ORDER BY (ip_address, device_pk, interface_name)
AS SELECT
    d.pk AS device_pk,
    d.code AS device_code,
    d.status AS device_status,
    JSONExtractString(iface, 'name') AS interface_name,
    JSONExtractString(iface, 'status') AS interface_status,
    splitByChar('/', JSONExtractString(iface, 'ip'))[1] AS ip_address
FROM dz_devices_current d
ARRAY JOIN JSONExtractArrayRaw(d.interfaces) AS iface
WHERE d.status = 'activated'
  AND JSONExtractString(iface, 'ip') != '';
-- +goose StatementEnd

-- +goose Down

-- +goose StatementBegin
DROP TABLE IF EXISTS dz_device_interface_ips;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE OR REPLACE VIEW dz_device_interface_ips
AS
SELECT
    d.pk AS device_pk,
    d.code AS device_code,
    d.status AS device_status,
    JSONExtractString(iface, 'name') AS interface_name,
    JSONExtractString(iface, 'status') AS interface_status,
    splitByChar('/', JSONExtractString(iface, 'ip'))[1] AS ip_address
FROM dz_devices_current d
ARRAY JOIN JSONExtractArrayRaw(d.interfaces) AS iface
WHERE d.status = 'activated'
  AND JSONExtractString(iface, 'ip') != '';
-- +goose StatementEnd
