#!/bin/bash
set -euo pipefail

# Verify the Lake controller-calls endpoint against a running local API.
#
# This intentionally avoids /api/sql/query because that endpoint uses the
# restricted public query ClickHouse user, which may not have grants on the
# controller databases (devnet/testnet/mainnet-beta). Instead it discovers
# valid device PKs through /api/dz/devices and then calls the real product
# endpoint with X-DZ-Env.
#
# Usage:
#   bash scripts/verify-controller-calls.sh
#   bash scripts/verify-controller-calls.sh --env testnet
#   bash scripts/verify-controller-calls.sh --pk <device_pubkey>
#   bash scripts/verify-controller-calls.sh --range 7d --device-limit 100

BASE_URL="${BASE_URL:-http://localhost:8080}"
DZ_ENV="${DZ_ENV:-devnet}"
RANGE="${RANGE:-24h}"
DEVICE_LIMIT="${DEVICE_LIMIT:-50}"
PK="${PK:-}"
REQUIRE_CALLS="${REQUIRE_CALLS:-false}"

usage() {
  cat <<EOF
Usage: bash scripts/verify-controller-calls.sh [options]

Options:
  --base-url URL       Lake API base URL (default: ${BASE_URL})
  --env ENV            DZ env: devnet, testnet, mainnet-beta (default: ${DZ_ENV})
  --range RANGE        Endpoint range, e.g. 24h, 7d (default: ${RANGE})
  --device-limit N     Number of /api/dz/devices rows to try (default: ${DEVICE_LIMIT})
  --pk PUBKEY          Skip discovery and use this device pubkey
  --require-calls      Fail unless selected response has total_calls > 0
  -h, --help           Show this help

Environment overrides: BASE_URL, DZ_ENV, RANGE, DEVICE_LIMIT, PK, REQUIRE_CALLS
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
    --range)
      RANGE="$2"
      shift 2
      ;;
    --device-limit)
      DEVICE_LIMIT="$2"
      shift 2
      ;;
    --pk)
      PK="$2"
      shift 2
      ;;
    --require-calls)
      REQUIRE_CALLS="true"
      shift
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

if ! [[ "$DEVICE_LIMIT" =~ ^[0-9]+$ ]] || [[ "$DEVICE_LIMIT" -lt 1 ]]; then
  echo "--device-limit must be a positive integer" >&2
  exit 2
fi

case "$DZ_ENV" in
  devnet)
    CONTROLLER_DB="devnet"
    ;;
  testnet)
    CONTROLLER_DB="testnet"
    ;;
  mainnet-beta)
    CONTROLLER_DB="mainnet-beta"
    ;;
  *)
    echo "Unsupported env: $DZ_ENV" >&2
    echo "Expected one of: devnet, testnet, mainnet-beta" >&2
    exit 2
    ;;
esac

assert_json() {
  local file="$1"
  if ! jq -e . "$file" >/dev/null 2>&1; then
    echo "Response was not JSON:" >&2
    cat "$file" >&2
    exit 1
  fi
}

get_with_env() {
  local url="$1"
  local out_file="$2"
  curl -sS -o "$out_file" -w "%{http_code}" \
    -H "X-DZ-Env: $DZ_ENV" \
    "$url"
}

call_controller_calls() {
  local pk="$1"
  local out_file="$2"
  local encoded_pk
  encoded_pk=$(jq -rn --arg v "$pk" '$v|@uri')
  get_with_env "$BASE_URL/api/dz/devices/$encoded_pk/controller-calls?range=$RANGE" "$out_file"
}

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

echo "Lake API:       $BASE_URL"
echo "Environment:    $DZ_ENV"
echo "Controller DB:  $CONTROLLER_DB"
echo "Endpoint range: $RANGE"

response_file=$(mktemp_tracked)
selected_pk=""
selected_code=""
selected_status=""
selected_calls="0"

if [[ -n "$PK" ]]; then
  echo "Using provided PK: $PK"
  http_code=$(call_controller_calls "$PK" "$response_file")
  if [[ ! "$http_code" =~ ^2 ]]; then
    echo "Controller-calls endpoint failed with HTTP $http_code:" >&2
    cat "$response_file" >&2
    echo >&2
    exit 1
  fi
  assert_json "$response_file"
  selected_pk="$PK"
else
  echo "Discovering devices from /api/dz/devices?limit=$DEVICE_LIMIT ..."
  devices_file=$(mktemp_tracked)
  http_code=$(get_with_env "$BASE_URL/api/dz/devices?limit=$DEVICE_LIMIT" "$devices_file")
  if [[ ! "$http_code" =~ ^2 ]]; then
    echo "Device discovery failed with HTTP $http_code:" >&2
    cat "$devices_file" >&2
    exit 1
  fi
  assert_json "$devices_file"

  count=$(jq '.items | length' "$devices_file")
  if [[ "$count" -eq 0 ]]; then
    echo "No devices returned by /api/dz/devices for env $DZ_ENV" >&2
    exit 1
  fi

  echo "Trying up to $count device(s); preferring one with total_calls > 0 ..."

  candidate_file=$(mktemp_tracked)
  while IFS=$'\t' read -r candidate_pk candidate_code candidate_status; do
    [[ -z "$candidate_pk" ]] && continue

    http_code=$(call_controller_calls "$candidate_pk" "$candidate_file")
    if [[ ! "$http_code" =~ ^2 ]]; then
      echo "  skip $candidate_code ($candidate_pk): HTTP $http_code"
      continue
    fi
    if ! jq -e . "$candidate_file" >/dev/null 2>&1; then
      echo "  skip $candidate_code ($candidate_pk): non-JSON response"
      continue
    fi

    source_available=$(jq -r '.source_available' "$candidate_file")
    total_calls=$(jq -r '.total_calls // 0' "$candidate_file")
    last_status=$(jq -r '.last_status // ""' "$candidate_file")
    echo "  $candidate_code ($candidate_pk): source_available=$source_available total_calls=$total_calls last_status=$last_status"

    if [[ "$source_available" == "true" ]]; then
      if [[ -z "$selected_pk" || "$total_calls" -gt 0 ]]; then
        cp "$candidate_file" "$response_file"
        selected_pk="$candidate_pk"
        selected_code="$candidate_code"
        selected_status="$candidate_status"
        selected_calls="$total_calls"
      fi
      if [[ "$total_calls" -gt 0 ]]; then
        break
      fi
    fi
  done < <(jq -r '.items[] | [.pk, .code, .status] | @tsv' "$devices_file")

  if [[ -z "$selected_pk" ]]; then
    echo >&2
    echo "FAIL: none of the discovered devices returned source_available=true." >&2
    echo "If ${CONTROLLER_DB}.controller_grpc_getconfig_success exists, make sure:" >&2
    echo "  1. the API was rebuilt/restarted after the ControllerCallsDatabaseForEnv change" >&2
    echo "  2. the API ClickHouse user has SHOW TABLES and SELECT on ${CONTROLLER_DB}.controller_grpc_getconfig_success" >&2
    exit 1
  fi

  echo "Selected PK:     $selected_pk"
  [[ -n "$selected_code" ]] && echo "Device code:     $selected_code"
  [[ -n "$selected_status" ]] && echo "Device status:   $selected_status"
fi

assert_json "$response_file"

echo
echo "Controller-calls response summary:"
jq '{
  device_pk,
  device_code,
  device_status,
  source_available,
  total_calls,
  minutes_with_calls,
  last_call_at,
  current_gap_seconds,
  last_status,
  from,
  to,
  bucket_count
}' "$response_file"

source_available=$(jq -r '.source_available' "$response_file")
total_calls=$(jq -r '.total_calls // 0' "$response_file")
if [[ "$source_available" != "true" ]]; then
  echo >&2
  echo "FAIL: source_available is false." >&2
  echo "If the table exists in ${CONTROLLER_DB}.controller_grpc_getconfig_success," >&2
  echo "make sure the API was rebuilt/restarted with ControllerCallsDatabaseForEnv()." >&2
  exit 1
fi

if [[ "$REQUIRE_CALLS" == "true" && "$total_calls" -le 0 ]]; then
  echo >&2
  echo "FAIL: source is available but total_calls is 0 for the selected device/range." >&2
  echo "Try a larger device sample or wider range, for example:" >&2
  echo "  bash scripts/verify-controller-calls.sh --env $DZ_ENV --range 7d --device-limit 200 --require-calls" >&2
  exit 1
fi

echo
if [[ "$total_calls" -gt 0 ]]; then
  echo "OK: source_available=true and total_calls=$total_calls"
else
  echo "OK: source_available=true (selected device has total_calls=0 for range=$RANGE)"
fi
