#!/bin/bash
set -euo pipefail

# Verify multicast delivery-state endpoints against a running local Lake API.
#
# The script discovers a multicast group with mroute data unless --group is
# provided, then exercises the split endpoints backed by the enriched views:
#   /mroutes, /oifs, /msdp, and /delivery-tree.
#
# Usage:
#   bash scripts/verify-multicast-delivery.sh
#   bash scripts/verify-multicast-delivery.sh --env testnet
#   bash scripts/verify-multicast-delivery.sh --group rocksteady
#   bash scripts/verify-multicast-delivery.sh --allow-empty

BASE_URL="${BASE_URL:-http://localhost:8080}"
DZ_ENV="${DZ_ENV:-mainnet-beta}"
GROUP="${GROUP:-}"
GROUP_LIMIT="${GROUP_LIMIT:-50}"
LIMIT="${LIMIT:-20}"
CURL_MAX_TIME="${CURL_MAX_TIME:-60}"
REQUIRE_DATA="${REQUIRE_DATA:-true}"
REQUIRE_MSDP="${REQUIRE_MSDP:-false}"
VERBOSE="${VERBOSE:-false}"
DATABASE="${DATABASE:-}"

usage() {
  cat <<EOF
Usage: bash scripts/verify-multicast-delivery.sh [options]

Options:
  --base-url URL       Lake API base URL (default: ${BASE_URL})
  --env ENV            DZ env header: mainnet-beta, devnet, testnet, or none
                       (default: ${DZ_ENV})
  --group PK_OR_CODE   Skip discovery and test this multicast group pk/code
  --group-limit N      Number of groups to inspect during discovery
                       (default: ${GROUP_LIMIT})
  --limit N            Endpoint item limit for smoke calls (default: ${LIMIT})
  --allow-empty        Do not fail when discovered endpoints have zero rows
  --require-msdp       Fail unless /msdp returns at least one item
  --verbose            Print full JSON responses
  --database DB        Database name for failure diagnostics via /api/sql/query
                       (default inferred from --env: lake/lake_devnet/lake_testnet)
  -h, --help           Show this help

Environment overrides: BASE_URL, DZ_ENV, GROUP, GROUP_LIMIT, LIMIT,
CURL_MAX_TIME, REQUIRE_DATA, REQUIRE_MSDP, VERBOSE, DATABASE
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base-url)
      BASE_URL="$2"
      shift 2
      ;;
    --env)
      DZ_ENV="$2"
      shift 2
      ;;
    --group)
      GROUP="$2"
      shift 2
      ;;
    --group-limit)
      GROUP_LIMIT="$2"
      shift 2
      ;;
    --limit)
      LIMIT="$2"
      shift 2
      ;;
    --allow-empty)
      REQUIRE_DATA="false"
      shift
      ;;
    --require-msdp)
      REQUIRE_MSDP="true"
      shift
      ;;
    --verbose)
      VERBOSE="true"
      shift
      ;;
    --database)
      DATABASE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

BASE_URL="${BASE_URL%/}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

need curl
need jq

for value_name in GROUP_LIMIT LIMIT CURL_MAX_TIME; do
  value="${!value_name}"
  if ! [[ "$value" =~ ^[0-9]+$ ]] || [[ "$value" -lt 1 ]]; then
    echo "$value_name must be a positive integer; got: $value" >&2
    exit 2
  fi
done

cleanup_files=()
cleanup() {
  if [[ ${#cleanup_files[@]} -gt 0 ]]; then
    rm -f "${cleanup_files[@]}"
  fi
}
trap cleanup EXIT

mktemp_tracked() {
  local tmp
  tmp=$(mktemp)
  cleanup_files+=("$tmp")
  printf '%s' "$tmp"
}

urlencode() {
  jq -rn --arg v "$1" '$v|@uri'
}

curl_args=(-sS --max-time "$CURL_MAX_TIME")
if [[ -n "$DZ_ENV" && "$DZ_ENV" != "none" ]]; then
  curl_args+=(-H "X-DZ-Env: $DZ_ENV")
fi

raw_get() {
  local path="$1"
  local out_file="$2"
  curl "${curl_args[@]}" -o "$out_file" -w "%{http_code}" "$BASE_URL$path"
}

raw_post_sql() {
  local query="$1"
  local out_file="$2"
  local body_file
  body_file=$(mktemp_tracked)
  jq -nc --arg query "$query" '{query: $query}' >"$body_file"
  curl -sS --max-time "$CURL_MAX_TIME" \
    -H "Content-Type: application/json" \
    -o "$out_file" \
    -w "%{http_code}" \
    -d @"$body_file" \
    "$BASE_URL/api/sql/query"
}

database_for_env() {
  if [[ -n "$DATABASE" ]]; then
    printf '%s' "$DATABASE"
    return
  fi
  case "$DZ_ENV" in
    mainnet-beta|none|"")
      printf 'lake'
      ;;
    devnet)
      printf 'lake_devnet'
      ;;
    testnet)
      printf 'lake_testnet'
      ;;
    *)
      printf 'lake_%s' "$DZ_ENV"
      ;;
  esac
}

assert_json() {
  local file="$1"
  local label="$2"
  if ! jq -e . "$file" >/dev/null 2>&1; then
    echo "FAIL: $label returned non-JSON:" >&2
    cat "$file" >&2
    echo >&2
    exit 1
  fi
}

api_get() {
  local path="$1"
  local out_file="$2"
  local label="$3"
  local http_code

  if ! http_code=$(raw_get "$path" "$out_file"); then
    echo "FAIL: $label request failed: $BASE_URL$path" >&2
    return 1
  fi
  if [[ ! "$http_code" =~ ^2 ]]; then
    echo "FAIL: $label returned HTTP $http_code: $BASE_URL$path" >&2
    cat "$out_file" >&2
    echo >&2
    return 1
  fi
  assert_json "$out_file" "$label"
}

jq_require() {
  local file="$1"
  local expr="$2"
  local message="$3"
  if ! jq -e "$expr" "$file" >/dev/null; then
    echo "FAIL: $message" >&2
    echo "Response excerpt:" >&2
    jq . "$file" >&2 || cat "$file" >&2
    exit 1
  fi
}

print_if_verbose() {
  local title="$1"
  local file="$2"
  if [[ "$VERBOSE" == "true" ]]; then
    echo
    echo "--- $title full response ---"
    jq . "$file"
  fi
}

diagnose_multicast_schema() {
  local db sql_file http_code sql_query enriched_count source_count
  db=$(database_for_env)
  sql_file=$(mktemp_tracked)
  sql_query="SELECT database, name, engine FROM system.tables WHERE database = '$db' AND name IN ('dz_ip_mroute_entries_current','dz_ip_msdp_peers_current','dz_ip_msdp_pim_sa_cache_current','dz_ip_msdp_sa_cache_current','enriched_ip_mroute','enriched_ip_mroute_oifs','enriched_ip_msdp_peers','enriched_ip_msdp_pim_sa_cache','enriched_ip_msdp_sa_cache') ORDER BY name"

  echo
  echo "Schema diagnostic for database '$db' via /api/sql/query:"
  if ! http_code=$(raw_post_sql "$sql_query" "$sql_file"); then
    echo "  diagnostic request failed"
    return
  fi
  if [[ ! "$http_code" =~ ^2 ]] || ! jq -e . "$sql_file" >/dev/null 2>&1; then
    echo "  diagnostic failed with HTTP $http_code:"
    cat "$sql_file"
    echo
    return
  fi
  if jq -e '.error? // empty' "$sql_file" >/dev/null; then
    echo "  diagnostic query error:"
    jq -r '.error' "$sql_file"
    return
  fi

  jq -r '.rows[]? | "  \(.[0]).\(.[1]) (\(.[2]))"' "$sql_file"
  enriched_count=$(jq '[.rows[]? | select(.[1] | startswith("enriched_ip_"))] | length' "$sql_file")
  source_count=$(jq '[.rows[]? | select((.[1] | startswith("dz_ip_")) and (.[1] | endswith("_current")))] | length' "$sql_file")

  if [[ "$enriched_count" -eq 0 && "$source_count" -gt 0 ]]; then
    echo "  => old source views exist, but enriched_ip_* views are missing."
    echo "     The API refactor intentionally gates on the enriched views now."
    echo "     Run the ClickHouse migrations that create Ben's enriched multicast views"
    echo "     in this database, or smoke-test against a DB where those migrations ran."
  fi
}

selected_group=""
selected_group_code=""
selected_group_ip=""

choose_group() {
  if [[ -n "$GROUP" ]]; then
    selected_group="$GROUP"
    selected_group_code="$GROUP"
    return
  fi

  local groups_file candidate_file http_code count
  local fallback_pk=""
  local fallback_code=""
  local fallback_ip=""
  local fallback_total="0"
  local fallback_source="false"
  groups_file=$(mktemp_tracked)
  candidate_file=$(mktemp_tracked)

  echo "Discovering multicast groups from /api/dz/multicast-groups?limit=$GROUP_LIMIT ..."
  api_get "/api/dz/multicast-groups?limit=$GROUP_LIMIT" "$groups_file" "multicast group discovery"

  count=$(jq '.items | length' "$groups_file")
  if [[ "$count" -eq 0 ]]; then
    echo "FAIL: /api/dz/multicast-groups returned no groups for env=$DZ_ENV" >&2
    exit 1
  fi

  echo "Trying up to $count group(s); preferring one with mroute rows ..."
  while IFS=$'\t' read -r pk code multicast_ip; do
    [[ -z "$pk" ]] && continue
    local encoded_pk path source_available total
    encoded_pk=$(urlencode "$pk")
    path="/api/dz/multicast-groups/$encoded_pk/mroutes?limit=1"

    if ! http_code=$(raw_get "$path" "$candidate_file"); then
      echo "  skip ${code:-$pk}: curl failed"
      continue
    fi
    if [[ ! "$http_code" =~ ^2 ]]; then
      echo "  skip ${code:-$pk}: HTTP $http_code"
      continue
    fi
    if ! jq -e . "$candidate_file" >/dev/null 2>&1; then
      echo "  skip ${code:-$pk}: non-JSON response"
      continue
    fi

    source_available=$(jq -r '.source_available // false' "$candidate_file")
    total=$(jq -r '.total // 0' "$candidate_file")
    echo "  ${code:-$pk} ($pk): source_available=$source_available mroutes_total=$total multicast_ip=$multicast_ip"

    if [[ -z "$fallback_pk" || "$source_available" == "true" ]]; then
      fallback_pk="$pk"
      fallback_code="$code"
      fallback_ip="$multicast_ip"
      fallback_total="$total"
      fallback_source="$source_available"
    fi

    if [[ "$source_available" == "true" && "$total" -gt 0 ]]; then
      selected_group="$pk"
      selected_group_code="$code"
      selected_group_ip="$multicast_ip"
      return
    fi
  done < <(jq -r '.items[] | [.pk, .code, .multicast_ip] | @tsv' "$groups_file")

  if [[ -n "$fallback_pk" && "$REQUIRE_DATA" != "true" ]]; then
    selected_group="$fallback_pk"
    selected_group_code="$fallback_code"
    selected_group_ip="$fallback_ip"
    echo "No group with mroute rows found; falling back to ${fallback_code:-$fallback_pk} (source_available=$fallback_source total=$fallback_total)."
    return
  fi

  diagnose_multicast_schema >&2
  echo >&2
  echo "FAIL: no discovered multicast group had mroute rows." >&2
  echo "This usually means the local API is pointed at an empty/mismapped database, or the enriched multicast views are missing in that database." >&2
  echo "For remote-table local smoke, start API with something like:" >&2
  echo "  CLICKHOUSE_DATABASE=lake CLICKHOUSE_DATABASE_DEVNET=lake_devnet CLICKHOUSE_DATABASE_TESTNET=lake_testnet go run ./api/main.go --use-remote" >&2
  echo "Or rerun this script with --allow-empty to only check endpoint shape." >&2
  exit 1
}

summarize_mroutes() {
  local file="$1"
  jq '{
    group: (.group | {pk, code, multicast_ip}),
    source_available,
    total,
    freshness: .freshness.mroute,
    sample: (.items[0]? | {
      mroute_id,
      device_code,
      group_address,
      source_address,
      publisher_device_code,
      source_match_status
    })
  }' "$file"
}

summarize_oifs() {
  local file="$1"
  jq '{
    group: (.group | {pk, code, multicast_ip}),
    source_available,
    total,
    freshness: .freshness.mroute,
    oif_kinds: ([.items[]?.oif_kind] | unique),
    sample: (.items[0]? | {
      mroute_id,
      device_code,
      oif_name,
      oif_kind,
      observed_delivery_role,
      link_code,
      peer_device_code,
      subscriber_device_code
    })
  }' "$file"
}

summarize_msdp() {
  local file="$1"
  jq '{
    group: (.group | {pk, code, multicast_ip}),
    kind,
    total,
    freshness: {
      peers: .freshness.msdp_peers,
      pim_sa_cache: .freshness.msdp_pim_sa_cache,
      sa_cache: .freshness.msdp_sa_cache
    },
    item_kinds: ([.items[]?.kind] | unique),
    peer_sample: (first(.items[]? | select(.kind == "peers") | .peer | {
      device_code,
      peer_address,
      peer_device_code,
      peer_interface_name,
      state
    }) // null),
    sa_sample: (first(.items[]? | select(.kind == "sa_cache") | .sa | {
      device_code,
      source_address,
      remote_address,
      remote_device_code,
      remote_interface_name,
      accept_status,
      source_match_status
    }) // null)
  }' "$file"
}

summarize_tree() {
  local file="$1"
  jq '{
    group: (.group | {pk, code, multicast_ip}),
    source_available,
    mode,
    freshness: .freshness.mroute,
    observed_segments: (.observed_segments | length),
    expected_segments: (.expected_segments | length),
    subscriber_outcomes: (.subscriber_outcomes | length),
    anomalies: (.anomalies | length),
    anomaly_kinds: ([.anomalies[]?.kind] | unique)
  }' "$file"
}

check_total_if_required() {
  local file="$1"
  local label="$2"
  local total
  total=$(jq -r '.total // 0' "$file")
  if [[ "$REQUIRE_DATA" == "true" && "$total" -le 0 ]]; then
    echo "FAIL: $label returned total=$total. Use --allow-empty if this env/group legitimately has no rows." >&2
    exit 1
  fi
}

echo "Lake API:       $BASE_URL"
echo "Environment:    $DZ_ENV"
echo "Group:          ${GROUP:-<auto-discover>}"
echo "Require data:   $REQUIRE_DATA"
echo "Require MSDP:   $REQUIRE_MSDP"

choose_group
encoded_group=$(urlencode "$selected_group")
base_path="/api/dz/multicast-groups/$encoded_group"

echo
echo "Selected group: ${selected_group_code:-$selected_group}"
echo "Group PK/code:  $selected_group"
[[ -n "$selected_group_ip" ]] && echo "Multicast IP:   $selected_group_ip"

mroutes_file=$(mktemp_tracked)
oifs_file=$(mktemp_tracked)
msdp_file=$(mktemp_tracked)
tree_file=$(mktemp_tracked)

echo
echo "Checking mroutes ..."
api_get "$base_path/mroutes?limit=$LIMIT" "$mroutes_file" "mroutes"
jq_require "$mroutes_file" 'has("group") and has("freshness") and (.items | type == "array")' "mroutes response shape is invalid"
jq_require "$mroutes_file" '(.items | length == 0) or (.items[0] | has("mroute_id") and has("publisher_device_code") and has("source_match_status"))' "mroutes items are missing enriched fields"
check_total_if_required "$mroutes_file" "mroutes"
summarize_mroutes "$mroutes_file"
print_if_verbose "mroutes" "$mroutes_file"

echo
echo "Checking oifs ..."
api_get "$base_path/oifs?limit=$LIMIT" "$oifs_file" "oifs"
jq_require "$oifs_file" 'has("group") and has("freshness") and (.items | type == "array")' "oifs response shape is invalid"
jq_require "$oifs_file" '(.items | length == 0) or (.items[0] | has("oif_kind") and has("observed_delivery_role"))' "oif items are missing enriched classification fields"
check_total_if_required "$oifs_file" "oifs"
summarize_oifs "$oifs_file"
print_if_verbose "oifs" "$oifs_file"

echo
echo "Checking msdp ..."
api_get "$base_path/msdp?kind=all&limit=$LIMIT" "$msdp_file" "msdp"
jq_require "$msdp_file" 'has("group") and has("freshness") and (.items | type == "array")' "msdp response shape is invalid"
jq_require "$msdp_file" 'all(.items[]?; ((.kind == "peers" and (.peer | type == "object")) or ((.kind == "pim_sa_cache" or .kind == "sa_cache") and (.sa | type == "object"))))' "msdp items are not shaped as peer/SA wrappers"
jq_require "$msdp_file" 'all(.items[]?; (.kind != "sa_cache") or ((.sa | has("status")) | not))' "msdp sa_cache still exposes old .status field"
msdp_total=$(jq -r '.total // 0' "$msdp_file")
if [[ "$REQUIRE_MSDP" == "true" && "$msdp_total" -le 0 ]]; then
  echo "FAIL: msdp returned total=$msdp_total. Omit --require-msdp if this group/env has no MSDP rows." >&2
  exit 1
fi
summarize_msdp "$msdp_file"
print_if_verbose "msdp" "$msdp_file"

echo
echo "Checking delivery tree ..."
api_get "$base_path/delivery-tree?mode=all" "$tree_file" "delivery tree"
jq_require "$tree_file" 'has("group") and has("freshness") and (.observed_segments | type == "array") and (.expected_segments | type == "array") and (.subscriber_outcomes | type == "array") and (.anomalies | type == "array")' "delivery-tree response shape is invalid"
summarize_tree "$tree_file"
print_if_verbose "delivery-tree" "$tree_file"

echo
echo "OK: multicast delivery endpoints returned JSON with enriched-view fields."
