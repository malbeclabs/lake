#!/bin/bash
set -euo pipefail

# Set up local development environment
#
# Usage:
#   ./scripts/dev-setup.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$ROOT_DIR"

echo "=== Lake Dev Setup ==="
echo ""

# Step 1: Start Docker services
echo "=== Starting Docker services ==="
docker compose up -d
docker compose ps
echo ""

# Step 2: Copy .env.example to .env if needed
if [[ ! -f .env ]]; then
    echo "=== Creating .env from .env.example ==="
    cp .env.example .env
    echo "Created .env - you may want to edit it with your local settings"
else
    echo "=== .env already exists, skipping ==="
fi
echo ""

# Step 3: Download and extract GeoIP databases
echo "=== Setting up GeoIP databases ==="
GEOIP_DIR="$ROOT_DIR/.tmp/geoip"
mkdir -p "$GEOIP_DIR"

GEOIP_BASE_URL="https://malbeclabs-dev-public-artifacts.s3.us-east-1.amazonaws.com/geoip"

for db in GeoLite2-ASN GeoLite2-City; do
    if [[ -f "$GEOIP_DIR/${db}.mmdb" ]]; then
        echo "${db}.mmdb already exists, skipping"
    else
        echo "Downloading ${db}..."
        curl -fsSL "${GEOIP_BASE_URL}/${db}.tar.gz" | tar -xzf - -C "$GEOIP_DIR"
        echo "Extracted ${db}.mmdb"
    fi
done

echo ""
echo "GeoIP databases installed to: $GEOIP_DIR"
echo "Update your .env with:"
echo "  GEOIP_CITY_DB_PATH=$GEOIP_DIR/GeoLite2-City.mmdb"
echo "  GEOIP_ASN_DB_PATH=$GEOIP_DIR/GeoLite2-ASN.mmdb"
echo ""

# Step 4: Set up remote proxy tables (if credentials are configured)
if [[ -f .env ]]; then
    set -a
    source .env
    set +a
fi

if [[ -n "${REMOTE_CH_HOST:-}" && -n "${REMOTE_CH_USER:-}" && -n "${REMOTE_CH_PASSWORD:-}" ]]; then
    echo "=== Setting up remote proxy tables ==="
    local_addr="${CLICKHOUSE_ADDR_TCP:-localhost:9100}"
    go run ./admin/cmd/admin/ \
        --clickhouse-addr "$local_addr" \
        --setup-remote-tables \
        --remote-clickhouse-addr "$REMOTE_CH_HOST" \
        --remote-clickhouse-user "$REMOTE_CH_USER" \
        --remote-clickhouse-password "$REMOTE_CH_PASSWORD"
    echo ""
else
    echo "=== Skipping remote proxy tables (REMOTE_CH_HOST/USER/PASSWORD not set) ==="
    echo "To use remote data, add REMOTE_CH_* vars to .env and re-run this script."
    echo ""
fi

# Step 5: Check dependencies
echo "=== Checking dependencies ==="
if ! command -v bun &> /dev/null; then
    echo "bun is not installed. Install it with:"
    echo "  curl -fsSL https://bun.sh/install | bash"
    echo ""
else
    echo "bun: $(bun --version)"
fi

if ! command -v go &> /dev/null; then
    echo "go is not installed. Install it from https://go.dev/dl/"
    echo ""
else
    echo "go: $(go version | awk '{print $3}')"
fi
echo ""

# Step 6: Print next steps
echo "=== Setup complete! ==="
echo ""
echo "Next steps:"
echo ""
echo "1. Run the mainnet indexer (imports data into ClickHouse):"
echo "   go run ./indexer/cmd/indexer/ --verbose --migrations-enable"
echo ""
echo "   Optional: run additional environment indexers:"
echo "   go run ./indexer/cmd/indexer/ --dz-env devnet --migrations-enable --create-database --listen-addr :3011"
echo "   go run ./indexer/cmd/indexer/ --dz-env testnet --migrations-enable --create-database --listen-addr :3012"
echo ""
echo "2. Run the API server (in a separate terminal):"
echo "   go run ./api/main.go"
echo ""
echo "3. Run the web dev server (in a separate terminal):"
echo "   cd web"
echo "   bun install"
echo "   bun dev"
echo ""
echo "   For non-localhost access (HTTPS needed for WebGPU):"
echo "   VITE_HTTPS=1 bun dev --host 0.0.0.0"
echo ""
echo "The web app will be available at http://localhost:5173"
echo "The API will be available at http://localhost:8080"
