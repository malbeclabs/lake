# Changelog

All notable changes to this project are documented here, following
[Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Fixed
- Publisher-check no longer serves a `200` with silently-zeroed totals when a
  totals query fails; the error now propagates so callers retry, keep the last
  complete cached payload, or return `500` (#698).

### Changed
- Page-cache worker refreshes `publisher_check` every 4th slow cycle (~2 min)
  instead of every 30s, cutting steady-state load on the shared ClickHouse; its
  data only changes on epoch timescales (#698).
- The v1 `/edge/shreds/publishers/leaders` endpoint now serves default-shape
  requests from the page cache and bounds live fallbacks with a 20s deadline,
  removing an unbounded live run of the heavy query on every call (#698).
