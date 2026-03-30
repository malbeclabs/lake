# -*- mode: Python -*-
# Lake local K8s development environment
#
# Prerequisites: k3d, tilt, kubectl
# Usage: ./scripts/k8s.sh up

load('ext://restart_process', 'docker_build_with_restart')

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Port offset — set by k8s.sh to avoid conflicts with host services.
# When offset is 0, ports match the standard dev setup (8080, 5173, etc.).
# When offset is e.g. 100, ports become 8180, 5273, etc.
PORT_OFFSET = int(os.environ.get('LAKE_PORT_OFFSET', '0'))

def p(local_port, remote_port=None):
    """Apply port offset to local port. Remote (container) port is unchanged."""
    if remote_port == None:
        remote_port = local_port
    return '%d:%d' % (local_port + PORT_OFFSET, remote_port)

if PORT_OFFSET != 0:
    print('Port offset: +%d' % PORT_OFFSET)

cluster_name = os.environ.get('LAKE_CLUSTER_NAME', 'lake-' + os.environ.get('USER', 'dev'))
allow_k8s_contexts(['k3d-' + cluster_name])
default_registry('localhost:5050')

# ---------------------------------------------------------------------------
# Infrastructure services
# ---------------------------------------------------------------------------

k8s_yaml(kustomize('k8s/base'))

k8s_resource('postgres', labels=['infra'],
    port_forwards=[p(5432)])
k8s_resource('clickhouse', labels=['infra'],
    port_forwards=[p(8123), p(9100, 9000)])
k8s_resource('neo4j', labels=['infra'],
    port_forwards=[p(7474), p(7687)])
k8s_resource('temporal', labels=['infra'],
    resource_deps=['postgres'],
    port_forwards=[p(7233)])
k8s_resource('temporal-ui', labels=['infra'],
    resource_deps=['temporal'],
    port_forwards=[p(8233, 8080)])

# ---------------------------------------------------------------------------
# GeoIP databases — mounted from host into k3d node at /data/geoip
# by the k8s.sh setup script.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# API service
# ---------------------------------------------------------------------------

docker_build_with_restart(
    'lake-api',
    context='.',
    dockerfile='k8s/docker/api.Dockerfile',
    only=[
        'go.mod', 'go.sum',
        'api/', 'agent/', 'admin/',
        'indexer/', 'slack/', 'utils/',
    ],
    entrypoint=['/usr/local/bin/lake-api'],
    live_update=[
        sync('.', '/lake'),
        run(
            'CGO_ENABLED=0 go build -o /usr/local/bin/lake-api ./api/main.go',
            trigger=[
                'api/', 'agent/',
                'go.mod', 'go.sum',
            ],
        ),
    ],
)

k8s_resource('api', labels=['app'],
    resource_deps=['indexer', 'postgres', 'neo4j', 'temporal'],
    port_forwards=[p(8080)])

# ---------------------------------------------------------------------------
# Indexer service
# ---------------------------------------------------------------------------

docker_build_with_restart(
    'lake-indexer',
    context='.',
    dockerfile='k8s/docker/indexer.Dockerfile',
    only=[
        'go.mod', 'go.sum',
        'indexer/', 'admin/',
        'api/', 'agent/', 'slack/', 'utils/',
    ],
    entrypoint=['/usr/local/bin/lake-indexer', '--verbose', '--migrations-enable', '--setup-remote-tables'],
    live_update=[
        sync('.', '/lake'),
        run(
            'CGO_ENABLED=0 go build -o /usr/local/bin/lake-indexer ./indexer/cmd/indexer/main.go',
            trigger=[
                'indexer/',
                'go.mod', 'go.sum',
            ],
        ),
    ],
)

k8s_resource('indexer', labels=['app'],
    resource_deps=['clickhouse', 'postgres'],
    port_forwards=[p(3010)])

# ---------------------------------------------------------------------------
# Web frontend
# ---------------------------------------------------------------------------

docker_build(
    'lake-web',
    context='.',
    dockerfile='k8s/docker/web-dev.Dockerfile',
    only=['web/'],
    live_update=[
        sync('./web/src', '/app/src'),
        sync('./web/public', '/app/public'),
        sync('./web/index.html', '/app/index.html'),
    ],
)

k8s_resource('web', labels=['app'],
    port_forwards=[p(5173, 5173)])

# ---------------------------------------------------------------------------
# Print port mapping on startup
# ---------------------------------------------------------------------------

if PORT_OFFSET != 0:
    print('')
    print('Port mapping (offset +%d):' % PORT_OFFSET)
    print('  web:         http://localhost:%d' % (5173 + PORT_OFFSET))
    print('  api:         http://localhost:%d' % (8080 + PORT_OFFSET))
    print('  clickhouse:  localhost:%d (HTTP), localhost:%d (TCP)' % (8123 + PORT_OFFSET, 9100 + PORT_OFFSET))
    print('  postgres:    localhost:%d' % (5432 + PORT_OFFSET))
    print('  neo4j:       localhost:%d (browser), localhost:%d (bolt)' % (7474 + PORT_OFFSET, 7687 + PORT_OFFSET))
    print('  temporal-ui: http://localhost:%d' % (8233 + PORT_OFFSET))
    print('')

# ---------------------------------------------------------------------------
# Tilt settings
# ---------------------------------------------------------------------------

update_settings(max_parallel_updates=3, k8s_upsert_timeout_secs=120)
