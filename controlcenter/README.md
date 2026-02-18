# Control Center

A service management dashboard for DoubleZero Data platform. Provides centralized control, monitoring, and log viewing for all platform services (Indexer, API, Web).

## Features

- **Service Management**: Start/stop services individually or all at once
- **Real-time Log Streaming**: View logs from all services in real-time with SSE
- **Log Filtering**: Filter by service, log level (ERROR/WARN/INFO/DEBUG), and keyword search
- **Log Level Parsing**: Automatic detection and color-coding of log levels
- **Scheduling**: Automatic start/stop of services at configured times (e.g., 08:00-23:00)
- **Process Persistence**: PID tracking and state persistence across restarts
- **Modern UI**: React-based dashboard with Tailwind CSS, matching main app design

## Quick Start

### Build

```bash
# Build UI
cd controlcenter/ui
bun install
bun run build

# Build backend (from lake root)
cd ../..
go build -o bin/controlcenter ./controlcenter/cmd/controlcenter/
```

### Run

```bash
# From lake root
./bin/controlcenter --verbose

# Or with custom port and data directory
./bin/controlcenter --port 9090 --data-dir ./my-data --verbose
```

Then open http://localhost:9090 (or http://<your-ip>:9090 from other machines) in your browser.

## CLI Flags

- `--port` (default: 9090) - HTTP server port
- `--data-dir` (default: ./controlcenter-data) - Data directory for logs and state
- `--verbose` - Enable debug logging

## Architecture

### Backend (Go)

- **Process Manager**: Manages service lifecycles using `os/exec`
- **Log Aggregator**: Captures stdout/stderr, parses log levels, stores in ring buffer
- **Log Parser**: Detects log levels (ERROR/WARN/INFO/DEBUG) from various formats
- **Scheduler**: Automatic service start/stop at configured times
- **HTTP Server**: REST API and SSE log streaming

### Frontend (React)

- **Dashboard**: Service cards with status, controls, and quick stats
- **Logs Page**: Real-time log viewer with filtering and virtualized rendering
- **Settings Page**: Schedule configuration

## API Endpoints

### Service Control
- `GET /api/status` - Get status of all services
- `POST /api/services/{name}/start` - Start a service
- `POST /api/services/{name}/stop` - Stop a service
- `POST /api/services/start-all` - Start all services
- `POST /api/services/stop-all` - Stop all services

### Logs
- `GET /api/logs/stream?service=X&level=Y` - SSE log stream
- `GET /api/logs?service=X&level=Y&limit=N` - Get recent logs

### Configuration
- `GET /api/config` - Get current configuration
- `PUT /api/config` - Update configuration
- `GET /api/schedule/next` - Get next scheduled action

## Configuration

Configuration is stored in `{data-dir}/config.json`:

```json
{
  "port": 9090,
  "schedule": {
    "enabled": false,
    "startTime": "08:00",
    "stopTime": "23:00"
  },
  "services": {
    "indexer": "go run ./indexer/cmd/indexer/ --verbose ...",
    "api": "go run ./api/main.go",
    "web": "cd web && bun dev --host 0.0.0.0"
  }
}
```

## Data Directory Structure

```
controlcenter-data/
├── config.json           # Configuration
├── processes.json        # Process state (PIDs, status)
└── logs/
    ├── indexer.log       # Indexer logs
    ├── api.log           # API logs
    └── web.log           # Web logs
```

## Development

### UI Development

```bash
cd controlcenter/ui

# Install dependencies
bun install

# Dev server with HMR (proxies /api to :9090)
bun run dev

# Build for production
bun run build
```

### Backend Development

```bash
# Run from source
go run ./controlcenter/cmd/controlcenter/ --verbose

# Run tests
go test ./controlcenter/internal/logs/ -v

# Build binary
go build -o bin/controlcenter ./controlcenter/cmd/controlcenter/
```

## Log Level Detection

The log parser automatically detects log levels from:

- slog format: `level=ERROR`, `level=WARN`, `level=INFO`, `level=DEBUG`
- Common patterns: `ERROR:`, `WARN:`, `[INFO]`, `[DEBUG]`
- Keywords: `fatal`, `panic`, `error`, `warn`, `warning`, `info`, `debug`, `trace`

Detected levels are color-coded in the UI:
- **ERROR** - Red
- **WARN** - Yellow
- **INFO** - Green
- **DEBUG** - Blue

## Scheduling

When scheduling is enabled:
- Services start automatically at the configured start time (e.g., 08:00)
- Services stop automatically at the configured stop time (e.g., 23:00)
- Times are in local timezone
- The scheduler checks every 30 seconds
- Next scheduled action is displayed on the dashboard

## Notes

- Control Center does not auto-start on production - it must be started explicitly
- Process recovery after crash: Control Center can detect previously running services via PID file
- Logs are kept in memory (ring buffer: 10,000 lines per service) and written to disk
- SSE connections are used for real-time log streaming (no polling)
- Graceful shutdown: Services receive SIGTERM, then SIGKILL after 10 seconds if needed
