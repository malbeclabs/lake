# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Changed

- Indexer telemetry-usage baseline query no longer re-scans ClickHouse on nearly every refresh: the in-memory baseline cache is now validated against the data watermark it represents instead of a wall-clock TTL, so steady-state refreshes reuse it and the `argMaxIf` scan runs only on startup/backfill. A cache hit is additionally gated on ClickHouse's max `event_ts`: if another writer (e.g. the admin backfill's continue mode) advanced it past the watermark, the cache is discarded and re-scanned instead of serving stale baselines. A failed already-written dedup query now fails the refresh instead of re-emitting overlap rows against cached end-of-window baselines. The raw lookback is cut 7d→2d, and backfill carries baselines across contiguous chunks, including zero-row chunks (fixes the June-backfill 60s-timeout mode). Adds `doublezero_data_indexer_clickhouse_baseline_query_total{dz_env}` to track cache misses (#702)
- Non-actionable failures no longer log at ERROR (which pages on-call): indexer view refreshes and ingest workflows escalate WARN→ERROR only on sustained consecutive failures; slack, agent, and worker log sites classify transient and client-caused errors as WARN; probe-write, optional-listener, and shutdown-cleanup failures demote to WARN (#696)
- `dberror` moved from `api/handlers/dberror` to `utils/pkg/dberror` so all services can share transient-error classification (#696)

### Fixed

- Shred subscribers dashboard now reflects per-escrow activation: seat status, balance, and prepaid epochs derive from the greatest single escrow (`max`), not the sum across escrows (`sum`), matching how the oracle evaluates activation and renewal. A seat with two escrows of $5.83 + $25.65 at a $30/epoch price now correctly shows as inactive/expired (spendable $25.65, 0 prepaid) instead of pending. Internal handlers rename `total_usdc_balance` to `spendable_usdc_balance` and add `all_escrows_usdc_balance` (the across-escrow total, so operators can still see stranded funds). The public `/api/v1/edge/shreds/*` responses add both new fields and keep `total_usdc_balance` as a deprecated alias of the across-escrow sum, so the change is backward compatible (#715)
- Indexer interface-counter data (`fact_dz_device_interface_counters`) is no longer permanently ~1h stale: the telemetry-usage catch-up cap is now anchored at `maxTime` (the ingest watermark) instead of `queryStart`, which had made the cap equal the 5m re-read overlap so an incremental refresh could never query past `maxTime` — every cycle re-read only rows that dedup out and inserted nothing, and `max(event_ts)` sawtoothed ~1h behind `now()`. Steady state now ingests the overlap plus one chunk (~10m, 2 Flux queries) and advances every refresh. `RefreshTelemetryUsage` gets a dedicated 10m activity `StartToCloseTimeout` (was the shared 5m) to bound the two-query worst case plus ClickHouse work, preventing the #665/#671 timeout loop. The post-downtime catch-up memory bound is unchanged (#708)

### Added

- Shared `utils/pkg/logger` helpers: transient-aware `logger.Error`/`logger.Warn` and consecutive-failure `logger.Escalator`; logging-level convention documented in CLAUDE.md (#696)
