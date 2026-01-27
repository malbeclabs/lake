# Claude Code Instructions

## Git Commits

- Do not add "Co-Authored-By" lines to commit messages
- Use the format `component: short description` (e.g., `indexer: fix flaky staging test`, `telemetry: use CLICKHOUSE_PASS env var`)
- Keep the description lowercase (except proper nouns) and concise

## Makefile

- `make build` — build all packages with CGO disabled
- `make lint` — run golangci-lint with the repo's `.golangci.yaml` config
- `make fmt` — run `go fmt` on all packages
- `make test` — run all tests with race detector
- `make ci` — run build, lint, and test in sequence
