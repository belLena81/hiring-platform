#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
[[ -r .env ]] || { printf 'Ignored root .env is required for local analytics credentials.\n' >&2; exit 1; }
. ./.env
export -n KAFKA_BROKER_PASSWORD KAFKA_PUBLISHER_V2_PASSWORD KAFKA_READER_PASSWORD KAFKA_FENCER_PASSWORD HIRING_ANALYTICS_HMAC_SECRET_BASE64
for variable in KAFKA_BROKER_PASSWORD KAFKA_PUBLISHER_V2_PASSWORD KAFKA_READER_PASSWORD KAFKA_FENCER_PASSWORD HIRING_ANALYTICS_HMAC_SECRET_BASE64; do
  [[ -n "${!variable:-}" ]] || { printf 'Required local setting %s is missing from .env.\n' "$variable" >&2; exit 1; }
done

check_state_directory() {
  local create="${1:-false}" current="$repo_root" component permissions
  for component in .local config; do
    current="$current/$component"
    if [[ ! -e "$current" && ! -L "$current" && "$create" == true ]]; then
      mkdir -m 700 -- "$current"
    fi
    [[ ! -L "$current" && -d "$current" ]] || { printf 'Smoke state path must use real directories.\n' >&2; exit 1; }
    permissions="$(stat -c '%a' "$current")"
    [[ "$permissions" =~ ^[0-7]{3,4}$ ]] && (( (8#$permissions & 0022) == 0 )) || {
      printf 'Smoke state path must not be group- or world-writable.\n' >&2; exit 1;
    }
  done
  [[ "$(stat -c '%u:%a' "$current")" == "$(id -u):700" ]] || {
    printf 'Smoke state directory must be owned by this user with mode 700.\n' >&2; exit 1;
  }
}

mode="${1:-start}"
requested_horizon_seconds="${HIRING_ANALYTICS_ERASURE_SMOKE_HORIZON_SECONDS:-}"
if [[ "$mode" == start && $# -le 1 ]]; then
  nonce="$(openssl rand -hex 8)"
  subject_id="$(python3 -c 'import uuid; print(uuid.uuid4())')"
elif [[ "$mode" == resume && $# -eq 2 && "$2" =~ ^[a-f0-9]{16}$ ]]; then
  nonce="$2"
  state_file=".local/config/analytics-erasure-smoke-$nonce.state"
  check_state_directory
  [[ -f "$state_file" && ! -L "$state_file" ]] || { printf 'Smoke state is missing: %s\n' "$state_file" >&2; exit 1; }
  [[ "$(stat -c '%u:%a' "$state_file")" == "$(id -u):600" ]] || {
    printf 'Smoke state must be owned by this user with mode 600.\n' >&2; exit 1;
  }
  read -r stored_nonce subject_id stored_horizon_seconds < "$state_file"
  [[ "$stored_nonce" == "$nonce" && "$subject_id" =~ ^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$ ]] || {
    printf 'Smoke state has invalid identity.\n' >&2; exit 1;
  }
  [[ "$stored_horizon_seconds" =~ ^[1-9][0-9]*$ ]] || {
    printf 'Smoke state does not contain a valid pinned retention horizon; start a fresh proof.\n' >&2
    exit 1
  }
  if [[ -n "$requested_horizon_seconds" && "$requested_horizon_seconds" != "$stored_horizon_seconds" ]]; then
    printf 'Smoke horizon is fixed at %s seconds for this proof.\n' "$stored_horizon_seconds" >&2
    exit 2
  fi
else
  printf 'Usage: %s [start | resume <16-hex-nonce>]\n' "$0" >&2
  exit 2
fi
project="hiring-analytics-erasure-smoke-$nonce"
export HIRING_ANALYTICS_ERASURE_SMOKE_DATABASE="hiring_erasure_smoke_$nonce"
export HIRING_ANALYTICS_ERASURE_SMOKE_TOPIC="hiring.erasure.smoke.$nonce"
export HIRING_ANALYTICS_RETENTION_PROOF_NONCE="$nonce"
export HIRING_ANALYTICS_RETENTION_PROOF_SUBJECT_ID="$subject_id"
if [[ "$mode" == resume ]]; then
  HIRING_ANALYTICS_ERASURE_SMOKE_HORIZON_SECONDS="$stored_horizon_seconds"
else
  HIRING_ANALYTICS_ERASURE_SMOKE_HORIZON_SECONDS="${requested_horizon_seconds:-60}"
fi
[[ "$HIRING_ANALYTICS_ERASURE_SMOKE_HORIZON_SECONDS" =~ ^[1-9][0-9]*$ ]] &&
  (( HIRING_ANALYTICS_ERASURE_SMOKE_HORIZON_SECONDS <= 2592000 )) || {
  printf 'HIRING_ANALYTICS_ERASURE_SMOKE_HORIZON_SECONDS must be an integer from 1 to 2592000.\n' >&2
  exit 2
}
export HIRING_ANALYTICS_ERASURE_SMOKE_HORIZON_SECONDS
export HIRING_ANALYTICS_ERASURE_SMOKE_KAFKA_RETENTION_HOURS="${HIRING_ANALYTICS_ERASURE_SMOKE_KAFKA_RETENTION_HOURS:-1}"
[[ "$HIRING_ANALYTICS_ERASURE_SMOKE_KAFKA_RETENTION_HOURS" =~ ^[1-9][0-9]*$ ]] &&
  (( HIRING_ANALYTICS_ERASURE_SMOKE_KAFKA_RETENTION_HOURS <= 720 )) || {
  printf 'HIRING_ANALYTICS_ERASURE_SMOKE_KAFKA_RETENTION_HOURS must be an integer from 1 to 720.\n' >&2
  exit 2
}
export KAFKA_TOPIC="$HIRING_ANALYTICS_ERASURE_SMOKE_TOPIC"
export KAFKA_BROKER_PASSWORD KAFKA_PUBLISHER_V2_PASSWORD KAFKA_READER_PASSWORD KAFKA_FENCER_PASSWORD HIRING_ANALYTICS_HMAC_SECRET_BASE64

if [[ "$mode" == start ]]; then
  state_file=".local/config/analytics-erasure-smoke-$nonce.state"
  check_state_directory true
  git check-ignore -q "$state_file" || { printf 'Smoke state path is not ignored by Git.\n' >&2; exit 1; }
  [[ ! -e "$state_file" && ! -L "$state_file" ]] || { printf 'Smoke state already exists: %s\n' "$state_file" >&2; exit 1; }
fi

compose() {
  docker compose --profile analytics --profile analytics-erasure \
    -f compose.yaml -f compose.analytics-erasure-smoke.yaml -p "$project" "$@"
}

cleanup() {
  compose stop >/dev/null 2>&1 || true
}
trap cleanup EXIT

run_fixture() {
  local fixture_mode="$1"
  compose run --rm --no-deps \
    -e HIRING_ANALYTICS_RETENTION_PROOF_ENABLED=true \
    -e HIRING_ANALYTICS_RETENTION_PROOF_SHORT_HORIZON=true \
    -e HIRING_ANALYTICS_RETENTION_PROOF_HORIZON_SECONDS="$HIRING_ANALYTICS_ERASURE_SMOKE_HORIZON_SECONDS" \
    -e HIRING_ANALYTICS_RETENTION_PROOF_MODE="$fixture_mode" \
    -e HIRING_ANALYTICS_RETENTION_PROOF_NONCE="$nonce" \
    -e HIRING_ANALYTICS_RETENTION_PROOF_SUBJECT_ID="$subject_id" \
    -e KAFKA_BROKER_PASSWORD -e KAFKA_PUBLISHER_V2_PASSWORD -e KAFKA_READER_PASSWORD \
    -e HIRING_ANALYTICS_HMAC_SECRET_BASE64 \
    --entrypoint /bin/sh analytics-batch \
    -ec 'exec sbt "IntegrationTest / testOnly *AnalyticsRetentionProofIntegrationSpec"'
}

wait_for_phase() {
  local phase="$1" limit_seconds="$2" elapsed=0 actual
  while (( elapsed < limit_seconds )); do
    actual="$(compose exec -T mongodb mongosh --quiet --eval \
      "const r=db.getSiblingDB('$HIRING_ANALYTICS_ERASURE_SMOKE_DATABASE').analytics_erasure_requests.findOne({_id:'$subject_id'}); print(r ? r.state+':'+r.phase : 'MISSING')" \
      2>/dev/null | tail -n 1 || true)"
    if [[ "$phase" == ReportPublished && "$actual" == "Complete:ReportPublished" ||
          "$phase" != ReportPublished && "$actual" == "Processing:$phase" ]]; then
      printf 'Smoke worker checkpoint: %s\n' "$actual"
      return 0
    fi
    sleep 2
    elapsed=$((elapsed + 2))
  done
  printf 'Timed out waiting for %s; current checkpoint is %s. Project %s retains evidence volumes.\n' \
    "$phase" "${actual:-unknown}" "$project" >&2
  return 1
}

if [[ "$mode" == start ]]; then
  printf 'Starting isolated analytics erasure smoke project %s.\n' "$project"
  compose build analytics-batch analytics-erasure-worker
  compose up -d mongodb kafka kafka-acl-init
  run_fixture prepare
  compose up -d analytics-erasure-worker
  wait_for_phase DeltaPurged 600
  run_fixture smoke-pre-horizon
  compose stop analytics-erasure-worker
  run_fixture append-retention-tail
  state_file=".local/config/analytics-erasure-smoke-$nonce.state"
  check_state_directory
  git check-ignore -q "$state_file" || { printf 'Smoke state path is not ignored by Git.\n' >&2; exit 1; }
  [[ ! -e "$state_file" && ! -L "$state_file" ]] || { printf 'Smoke state already exists: %s\n' "$state_file" >&2; exit 1; }
  printf '%s %s %s\n' "$nonce" "$subject_id" "$HIRING_ANALYTICS_ERASURE_SMOKE_HORIZON_SECONDS" > "$state_file"
  chmod 600 "$state_file"
  printf 'Smoke staged on preserved volumes. Resume after the next UTC day plus %s seconds: %s resume %s\n' \
    "$HIRING_ANALYTICS_ERASURE_SMOKE_HORIZON_SECONDS" "$0" "$nonce"
else
  compose up -d mongodb kafka kafka-acl-init
  deadline_ms=""
  for attempt in {1..60}; do
    deadline_ms="$(compose exec -T mongodb mongosh --quiet --eval \
      "const r=db.getSiblingDB('$HIRING_ANALYTICS_ERASURE_SMOKE_DATABASE').analytics_erasure_requests.findOne({_id:'$subject_id'}); if (!r || !r.deltaPurgedAt || r.repairRequired) { print('INVALID'); } else { const d=r.deltaPurgedAt; print(Date.UTC(d.getUTCFullYear(),d.getUTCMonth(),d.getUTCDate()+1)+$((HIRING_ANALYTICS_ERASURE_SMOKE_HORIZON_SECONDS * 1000))); }" \
      2>/dev/null | tail -n 1 || true)"
    [[ "$deadline_ms" == INVALID || "$deadline_ms" =~ ^[0-9]+$ ]] && break
    sleep 2
  done
  [[ "$deadline_ms" =~ ^[0-9]+$ ]] || { printf 'Smoke request is missing, invalid, or requires repair.\n' >&2; exit 1; }
  now_ms="$(date -u +%s%3N)"
  if (( now_ms < deadline_ms )); then
    printf 'Delta log cleanup is pending until %s UTC. Resume with: %s resume %s\n' \
      "$(date -u -d "@$((deadline_ms / 1000))" '+%Y-%m-%d %H:%M:%S')" "$0" "$nonce"
    exit 0
  fi
  printf 'Resuming smoke worker after the physical Delta log-cleanup horizon.\n'
  compose up -d analytics-erasure-worker
  wait_for_phase ReportPublished 1200
  run_fixture smoke-verify
  printf 'Smoke proof passed: populated synthetic Delta rows, worker restart, actual Kafka earliest offsets, completion receipt, report publication, and captured file absence. Project-scoped named volumes remain available as local evidence. This supports local implementation closure; full-horizon retention remains a production rollout gate.\n'
fi
