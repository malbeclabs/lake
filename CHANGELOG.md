# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Changed

- Indexer telemetry-usage baseline query no longer re-scans ClickHouse on nearly every refresh: the in-memory baseline cache is now validated against the data watermark it represents instead of a wall-clock TTL, so steady-state refreshes reuse it and the `argMaxIf` scan runs only on startup/backfill. The raw lookback is cut 7d→2d, and backfill carries baselines across contiguous chunks (fixes the June-backfill 60s-timeout mode). Adds `doublezero_data_indexer_clickhouse_baseline_query_total{dz_env}` to track cache misses (#702)
- Non-actionable failures no longer log at ERROR (which pages on-call): indexer view refreshes and ingest workflows escalate WARN→ERROR only on sustained consecutive failures; slack, agent, and worker log sites classify transient and client-caused errors as WARN; probe-write, optional-listener, and shutdown-cleanup failures demote to WARN (#696)
- `dberror` moved from `api/handlers/dberror` to `utils/pkg/dberror` so all services can share transient-error classification (#696)

### Added

- Shared `utils/pkg/logger` helpers: transient-aware `logger.Error`/`logger.Warn` and consecutive-failure `logger.Escalator`; logging-level convention documented in CLAUDE.md (#696)
