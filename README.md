# Lake

Lake is the data analytics platform for DoubleZero. It provides a web interface and API for querying network telemetry and Solana validator data stored in ClickHouse.

## Components

### api/

HTTP API server that powers the web UI. Provides endpoints for:
- SQL query execution against ClickHouse
- AI-powered natural language to SQL generation
- Conversational chat interface for data analysis
- MCP server for Claude Desktop and other MCP clients
- Schema catalog and visualization recommendations

Serves the built web UI as static files in production.

### web/

React/TypeScript single-page application. Features:
- SQL editor with syntax highlighting
- Natural language query interface
- Chat mode for conversational data exploration
- Query results with tables and charts
- Session history

### agent/

LLM-powered workflow for answering natural language questions. Implements a multi-step process: classify → decompose → generate SQL → execute → synthesize answer. Includes evaluation tests for validating agent accuracy.

See [agent/README.md](agent/README.md) for architecture details.

### indexer/

Background service that continuously syncs data from external sources into ClickHouse:
- Network topology from Solana (DZ programs)
- Latency measurements from Solana (DZ programs)
- Device usage metrics from InfluxDB
- Solana validator data from mainnet
- GeoIP enrichment from MaxMind

See [indexer/README.md](indexer/README.md) for architecture details.

### slack/

Slack bot that provides a chat interface for data queries. Users can ask questions in Slack and receive answers powered by the agent workflow.

### dev/controlcenter/

Web dashboard for managing all platform services locally. Provides start/stop controls, real-time log streaming with filtering, and a log activity histogram. Replaces running services manually in separate terminals.

- Runs at http://localhost:5174 (one port above the data app)
- Use `--bind 0.0.0.0` for network access — automatically enables HTTPS for the web service
- Bind address is mirrored to the web dev server automatically

### admin/

CLI tool for maintenance operations:
- Database reset
- Data backfills (latency, usage metrics)
- Schema migrations

### migrations/

ClickHouse schema migrations for dimension and fact tables. These are applied automatically by the indexer on startup.

### utils/

Shared Go packages used across lake services (logging, retry logic, test helpers).

## Data Flow

```
External Sources              Lake Services              Storage
────────────────              ─────────────              ───────

Solana (DZ) ───────────────► Indexer ──────────────────► ClickHouse
InfluxDB    ───────────────►    │
MaxMind     ───────────────►    │
                                │
                                ▼
                    ┌───────────────────────┐
                    │      API Server       │◄────── Web UI
                    │  • Query execution    │◄────── Slack Bot
                    │  • Agent workflow     │
                    │  • Chat interface     │
                    └───────────────────────┘
```

## Development

### Local Setup

Run the setup script to get started:

```bash
./scripts/dev-setup.sh
```

This will:
- Start Docker services (ClickHouse, PostgreSQL, Neo4j)
- Create `.env` from `.env.example`
- Download GeoIP databases

Then start the services in separate terminals:

```bash
# Terminal 1: Run the mainnet indexer (imports data into ClickHouse)
go run ./indexer/cmd/indexer/ --verbose --migrations-enable

# Optional: run additional environment indexers (each in its own terminal)
go run ./indexer/cmd/indexer/ --dz-env devnet --migrations-enable --create-database --listen-addr :3011
go run ./indexer/cmd/indexer/ --dz-env testnet --migrations-enable --create-database --listen-addr :3012

# Terminal 2: Run the API server
go run ./api/main.go

# Terminal 3: Run the web dev server
cd web
bun install
bun dev

# Optional: for non-localhost access (HTTPS needed for WebGPU)
VITE_HTTPS=1 bun dev --host 0.0.0.0
```

The web app will be at http://localhost:5173, API at http://localhost:8080.

### Using the Control Center

Instead of managing services in separate terminals, you can use the control center dashboard:

```bash
# Build (only needed once or after changes)
cd dev/controlcenter/ui && bun install && bun run build && cd ..
go build -o bin/controlcenter ./cmd/controlcenter/
cd ../..

# Run (always from the lake root)
./dev/controlcenter/bin/controlcenter

# For network access (enables HTTPS on the web service automatically)
./dev/controlcenter/bin/controlcenter --bind 0.0.0.0
```

The control center will be at http://localhost:5174.

### Running Agent Evals

The agent has evaluation tests that validate the natural language to SQL workflow. Run them with:

```bash
./scripts/run-evals.sh                 # Run all evals in parallel
./scripts/run-evals.sh --show-failures # Show failure logs at end
./scripts/run-evals.sh -s              # Short mode (code validation only, no API)
./scripts/run-evals.sh -r 2            # Retry failed tests up to 2 times
```

Output goes to `eval-runs/<timestamp>/` - check `failures.log` for any failures.

## Deployment

Lake uses automated CI/CD via GitHub Actions and ArgoCD.

### Automatic Staging Deploys

Pushes to staging branches automatically build and deploy:
- Build web assets and upload to S3
- Build Docker image and push to `ghcr.io/malbeclabs/lake`
- Tag image as `staging` (ArgoCD picks up changes automatically)

Current staging branches are configured in `.github/workflows/release.docker.lake.yml`.

### PR Previews

Add the `preview-lake` label to a PR to trigger a preview build. Assets go to a branch-prefixed location in the preview bucket.

### Promoting to Production

To promote a staging image to production:

**Via GitHub Actions (recommended):**
1. Go to Actions → "promote.lake" workflow
2. Run workflow with source_tag=`staging` and target_tag=`prod`

**Via CLI:**
```bash
./scripts/promote-to-prod.sh           # staging → prod (prompts for confirmation)
./scripts/promote-to-prod.sh -n        # dry-run, show what would happen
./scripts/promote-to-prod.sh -y        # skip confirmation
./scripts/promote-to-prod.sh main prod # promote specific tag
```

ArgoCD will automatically sync the new image.

### Static Asset Fallback

The API server fetches missing static assets from S3 to handle rolling deployments gracefully. When users have cached HTML referencing old JS/CSS bundles, the API fetches those assets from S3 instead of returning 404s.

Configure with:
```bash
ASSET_BUCKET_URL=https://my-bucket.s3.amazonaws.com/assets
```

## Environment

Key dependencies:
- **ClickHouse** - Analytics database
- **Anthropic API** - LLM for natural language features
- **InfluxDB** (optional) - Device usage metrics source
- **MaxMind GeoIP** - IP geolocation databases

## MCP Server

The API exposes an MCP (Model Context Protocol) server at `/api/mcp` for use with Claude Desktop and other MCP clients.

### Tools

| Tool | Description |
|------|-------------|
| `execute_sql` | Run SQL queries against ClickHouse |
| `execute_cypher` | Run Cypher queries against Neo4j (topology, paths) |
| `get_schema` | Get database schema (tables, columns, types) |
| `read_docs` | Read DoubleZero documentation |

### Claude Desktop

1. Open Settings → Manage Connectors
2. Click "Add Custom Connector"
3. Enter URL: `https://data.malbeclabs.com/api/mcp`

### Claude Code / Cursor

Add a `.mcp.json` file to your project:

```json
{
  "mcpServers": {
    "doublezero": {
      "type": "http",
      "url": "https://data.malbeclabs.com/api/mcp"
    }
  }
}
```

## Authentication

Lake supports user authentication with daily usage limits.

| Tier | Auth Method | Daily Limit |
|------|-------------|-------------|
| Domain users | Google OAuth (allowed domains) | Unlimited |
| Wallet users | Solana wallet (SIWS) | 50 questions |
| Anonymous | IP-based | 5 questions |

Configure with `GOOGLE_CLIENT_ID`, `VITE_GOOGLE_CLIENT_ID`, and `AUTH_ALLOWED_DOMAINS` environment variables. See `.env.example` for details.
