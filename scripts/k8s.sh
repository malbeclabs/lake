#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Lake K8s Development Environment
# =============================================================================
# Single command to manage the local k3d + Tilt dev environment.
#
# Usage:
#   ./scripts/k8s.sh up [name]      # Create cluster and start Tilt
#   ./scripts/k8s.sh down [name]    # Destroy cluster
#   ./scripts/k8s.sh status [name]  # Show cluster and service status
#   ./scripts/k8s.sh list           # List all lake clusters
#
# The optional [name] lets you run multiple isolated clusters, e.g.:
#   ./scripts/k8s.sh up             # cluster: lake-snormore
#   ./scripts/k8s.sh up feature-x   # cluster: lake-snormore-feature-x
#
# Prerequisites: docker, k3d, tilt, kubectl
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAKE_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
KUBECONFIG_DIR="$LAKE_ROOT/.tmp/k8s"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# ---------------------------------------------------------------------------
# Isolated kubeconfig — never touches ~/.kube/config
# ---------------------------------------------------------------------------
use_isolated_kubeconfig() {
    mkdir -p "$KUBECONFIG_DIR"
    export KUBECONFIG="$KUBECONFIG_DIR/$CLUSTER_NAME.kubeconfig"
}

cluster_exists() {
    k3d cluster list 2>/dev/null | grep -q "^$CLUSTER_NAME "
}

# Default ports that Tilt will forward
DEFAULT_PORTS=(5432 8123 9100 7474 7687 7233 8233 8080 3010 5173 10350)

# Check if a port is in use
port_in_use() {
    lsof -iTCP:"$1" -sTCP:LISTEN -t &>/dev/null
}

# Find a port offset where none of the forwarded ports conflict
detect_port_offset() {
    # Try offset 0 first (standard ports), then 100, 200, ...
    for offset in 0 100 200 300; do
        local conflict=false
        for port in "${DEFAULT_PORTS[@]}"; do
            if port_in_use $((port + offset)); then
                conflict=true
                break
            fi
        done
        if [ "$conflict" = false ]; then
            echo "$offset"
            return
        fi
    done
    # All tried offsets have conflicts — fall back to 0 and let Tilt warn
    echo "0"
}

# ---------------------------------------------------------------------------
# Prerequisites
# ---------------------------------------------------------------------------
check_prereqs() {
    missing=()
    for cmd in docker k3d tilt kubectl; do
        if ! command -v "$cmd" &>/dev/null; then
            missing+=("$cmd")
        fi
    done

    if [ ${#missing[@]} -gt 0 ]; then
        error "Missing required tools: ${missing[*]}"
        echo ""
        echo "Install them:"
        echo "  docker:  https://docs.docker.com/get-docker/"
        echo "  k3d:     brew install k3d"
        echo "  tilt:    brew install tilt-dev/tap/tilt"
        echo "  kubectl: brew install kubectl"
        exit 1
    fi

    if ! docker info &>/dev/null; then
        error "Docker is not running. Start Docker Desktop and try again."
        exit 1
    fi
}

# ---------------------------------------------------------------------------
# Ensure .env exists
# ---------------------------------------------------------------------------
ensure_env() {
    if [ ! -f "$LAKE_ROOT/.env" ]; then
        info "Creating .env from .env.example..."
        cp "$LAKE_ROOT/.env.example" "$LAKE_ROOT/.env"
        warn "Edit .env to fill in required values (e.g., ANTHROPIC_API_KEY)"
    fi
}

# ---------------------------------------------------------------------------
# Download GeoIP databases
# ---------------------------------------------------------------------------
ensure_geoip() {
    local geoip_dir="$LAKE_ROOT/.tmp/geoip"
    if [ -f "$geoip_dir/GeoLite2-City.mmdb" ] && [ -f "$geoip_dir/GeoLite2-ASN.mmdb" ]; then
        return
    fi

    info "Downloading GeoIP databases..."
    mkdir -p "$geoip_dir"

    local s3_base="https://malbeclabs-lake-dev-geoip.s3.amazonaws.com"
    for db in GeoLite2-City GeoLite2-ASN; do
        if [ ! -f "$geoip_dir/$db.mmdb" ]; then
            echo "  Downloading $db..."
            curl -sL "$s3_base/$db.tar.gz" | tar -xz -C "$geoip_dir" --strip-components=1 "*/$db.mmdb" 2>/dev/null || {
                warn "Failed to download $db — indexer/api may not start correctly without it."
            }
        fi
    done
}

# ---------------------------------------------------------------------------
# Create k3d cluster
# ---------------------------------------------------------------------------
ensure_registry() {
    local registry_name="lake-registry"
    if k3d registry list 2>/dev/null | grep -q "$registry_name"; then
        return
    fi
    info "Creating local registry '$registry_name'..."
    k3d registry create "$registry_name" --port 5050
}

cluster_stopped() {
    k3d cluster list 2>/dev/null | grep "^$CLUSTER_NAME " | grep -q "0/1"
}

ensure_cluster() {
    if cluster_exists; then
        if cluster_stopped; then
            info "Starting stopped cluster '$CLUSTER_NAME'..."
            k3d cluster start "$CLUSTER_NAME"
            info "Cluster started."
        else
            info "Cluster '$CLUSTER_NAME' already running."
        fi
        # Re-export kubeconfig in case it was lost
        k3d kubeconfig get "$CLUSTER_NAME" > "$KUBECONFIG" 2>/dev/null
        return
    fi

    ensure_registry

    local geoip_dir="$LAKE_ROOT/.tmp/geoip"
    info "Creating k3d cluster '$CLUSTER_NAME'..."
    k3d cluster create "$CLUSTER_NAME" \
        --volume "$geoip_dir:/data/geoip@server:0" \
        --registry-use k3d-lake-registry:5050 \
        --kubeconfig-update-default=false \
        --wait
    # Write kubeconfig to our isolated file
    k3d kubeconfig get "$CLUSTER_NAME" > "$KUBECONFIG"
    info "Cluster created."
}

# ---------------------------------------------------------------------------
# Sync secrets from .env into the cluster
# ---------------------------------------------------------------------------
sync_secrets() {
    [ ! -f "$LAKE_ROOT/.env" ] && return

    info "Syncing secrets from .env..."
    kubectl apply -f "$LAKE_ROOT/k8s/base/namespace.yaml" >/dev/null

    # Keys managed by the configmap (in-cluster addresses) — skip these from .env
    # so the configmap values aren't overridden by host-local addresses.
    local -A skip_keys=(
        [CLICKHOUSE_ADDR_TCP]=1 [CLICKHOUSE_DATABASE]=1 [CLICKHOUSE_USERNAME]=1 [CLICKHOUSE_PASSWORD]=1
        [POSTGRES_HOST]=1 [POSTGRES_PORT]=1 [POSTGRES_DB]=1 [POSTGRES_USER]=1 [POSTGRES_PASSWORD]=1
        [POSTGRES_RUN_MIGRATIONS]=1
        [NEO4J_URI]=1 [NEO4J_USERNAME]=1 [NEO4J_PASSWORD]=1 [NEO4J_DATABASE]=1
        [GEOIP_CITY_DB_PATH]=1 [GEOIP_ASN_DB_PATH]=1
        [TEMPORAL_HOST_PORT]=1 [TEMPORAL_NAMESPACE]=1
        [WEB_BASE_URL]=1 [SOLANA_RPC_URL]=1 [SENTRY_ENVIRONMENT]=1
    )

    local secret_args=()
    while IFS= read -r line || [ -n "$line" ]; do
        [[ "$line" =~ ^[[:space:]]*# ]] && continue
        [[ -z "${line// /}" ]] && continue
        if [[ "$line" =~ ^([A-Za-z_][A-Za-z0-9_]*)=(.+)$ ]]; then
            local key="${BASH_REMATCH[1]}"
            [[ -n "${skip_keys[$key]+x}" ]] && continue
            secret_args+=("--from-literal=$key=${BASH_REMATCH[2]}")
        fi
    done < "$LAKE_ROOT/.env"

    if [ ${#secret_args[@]} -gt 0 ]; then
        kubectl -n lake-dev delete secret lake-secrets --ignore-not-found >/dev/null
        kubectl -n lake-dev create secret generic lake-secrets "${secret_args[@]}" >/dev/null
        info "Secrets synced (${#secret_args[@]} keys)."
    fi
}

# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------
cmd_up() {
    check_prereqs
    ensure_env
    ensure_geoip

    # Detect port conflicts before creating the cluster, so we don't
    # mistake our own Tilt port-forwards for external conflicts.
    local offset=0
    if cluster_exists; then
        # Cluster already running — Tilt will reclaim its existing port-forwards.
        offset=0
    else
        offset=$(detect_port_offset)
    fi

    ensure_cluster
    sync_secrets
    export LAKE_PORT_OFFSET="$offset"

    if [ "$offset" -ne 0 ]; then
        warn "Default ports are in use — shifting all ports by +$offset"
    fi
    echo ""
    echo "  web:         http://localhost:$((5173 + offset))"
    echo "  api:         http://localhost:$((8080 + offset))"
    echo "  clickhouse:  localhost:$((8123 + offset)) (HTTP), localhost:$((9100 + offset)) (TCP)"
    echo "  postgres:    localhost:$((5432 + offset))"
    echo "  neo4j:       localhost:$((7474 + offset)) (browser), localhost:$((7687 + offset)) (bolt)"
    echo "  temporal-ui: http://localhost:$((8233 + offset))"
    echo "  tilt:        http://localhost:$((10350 + offset))"
    echo ""

    export LAKE_CLUSTER_NAME="$CLUSTER_NAME"

    info "Starting Tilt (cluster: $CLUSTER_NAME)..."
    cd "$LAKE_ROOT"
    exec tilt up --port "$((10350 + offset))"
}

cmd_down() {
    check_prereqs

    if ! cluster_exists; then
        info "Cluster '$CLUSTER_NAME' does not exist — nothing to do."
        return
    fi

    if [ "${CLEAN:-}" = "true" ]; then
        if [ "${YES:-}" != "true" ]; then
            warn "This will delete cluster '$CLUSTER_NAME' and ALL data (volumes, PVCs)."
            printf "Are you sure? [y/N] "
            read -r confirm
            if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
                info "Aborted."
                return
            fi
        fi
        info "Deleting cluster '$CLUSTER_NAME' (including all data)..."
        k3d cluster delete "$CLUSTER_NAME"
        rm -f "$KUBECONFIG_DIR/$CLUSTER_NAME.kubeconfig"
        info "Cluster deleted."
    else
        if [ "${YES:-}" != "true" ]; then
            printf "Stop cluster '$CLUSTER_NAME'? [y/N] "
            read -r confirm
            if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
                info "Aborted."
                return
            fi
        fi
        info "Stopping cluster '$CLUSTER_NAME' (data preserved)..."
        k3d cluster stop "$CLUSTER_NAME"
        info "Cluster stopped. Run 'up' to restart, or 'down --clean' to delete."
    fi
}

cmd_status() {
    if ! cluster_exists; then
        info "Cluster '$CLUSTER_NAME' is not running."
        exit 0
    fi

    # Ensure we have a valid kubeconfig for this cluster
    k3d kubeconfig get "$CLUSTER_NAME" > "$KUBECONFIG" 2>/dev/null
    echo ""
    info "Cluster: $CLUSTER_NAME"
    echo ""
    kubectl -n lake-dev get pods -o wide 2>/dev/null || echo "  No pods found."
    echo ""
    kubectl -n lake-dev get svc 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# Cluster naming
# ---------------------------------------------------------------------------
resolve_cluster_name() {
    local base="lake-${USER:-dev}"
    local suffix="${1:-}"
    if [ -n "$suffix" ]; then
        echo "${base}-${suffix}"
    else
        echo "$base"
    fi
}

# ---------------------------------------------------------------------------
# List all lake clusters
# ---------------------------------------------------------------------------
cmd_list() {
    check_prereqs
    info "Lake clusters:"
    k3d cluster list 2>/dev/null | grep "^lake-" || echo "  (none)"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
ACTION="${1:-}"
shift || true

# Parse remaining args: flags (--clean) and positional (name)
CLEAN=false
YES=false
NAME_ARG=""
for arg in "$@"; do
    case "$arg" in
        --clean) CLEAN=true ;;
        -y|--yes) YES=true ;;
        *)       NAME_ARG="$arg" ;;
    esac
done
export CLEAN YES

CLUSTER_NAME="$(resolve_cluster_name "$NAME_ARG")"
use_isolated_kubeconfig

case "$ACTION" in
    up)     cmd_up ;;
    down)   cmd_down ;;
    status) cmd_status ;;
    list)   cmd_list ;;
    *)
        echo "Usage: $0 {up|down|status|list} [name] [--clean] [-y|--yes]"
        echo ""
        echo "  up [name]           Create cluster and start Tilt"
        echo "  down [name]         Stop cluster (preserves data)"
        echo "  down --clean        Delete cluster and all data"
        echo "  down -y|--yes       Skip confirmation prompt"
        echo "  status [name]       Show cluster and pod status"
        echo "  list                List all lake clusters"
        echo ""
        echo "  [name] is optional — lets you run multiple clusters:"
        echo "    $0 up              # lake-${USER:-dev}"
        echo "    $0 up feature-x    # lake-${USER:-dev}-feature-x"
        exit 1
        ;;
esac
