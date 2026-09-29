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

nonce="$(openssl rand -hex 8)"
subject_id="$(python3 -c 'import uuid; print(uuid.uuid4())')"
project="hiring-analytics-erasure-smoke-$nonce"
export HIRING_ANALYTICS_ERASURE_SMOKE_DATABASE="hiring_erasure_smoke_$nonce"
export HIRING_ANALYTICS_ERASURE_SMOKE_TOPIC="hiring.erasure.smoke.$nonce"
export HIRING_ANALYTICS_RETENTION_PROOF_NONCE="$nonce"
export HIRING_ANALYTICS_RETENTION_PROOF_SUBJECT_ID="$subject_id"
export KAFKA_TOPIC="$HIRING_ANALYTICS_ERASURE_SMOKE_TOPIC"
export KAFKA_BROKER_PASSWORD KAFKA_PUBLISHER_V2_PASSWORD KAFKA_READER_PASSWORD KAFKA_FENCER_PASSWORD HIRING_ANALYTICS_HMAC_SECRET_BASE64

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
    if [[ "$actual" == "Processing:$phase" || "$actual" == "Complete:ReportPublished" ]]; then
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

printf 'Starting isolated analytics erasure smoke project %s.\n' "$project"
compose build analytics-batch analytics-erasure-worker
compose up -d mongodb kafka kafka-acl-init
run_fixture prepare
compose up -d analytics-erasure-worker
wait_for_phase DeltaPurged 180
run_fixture append-retention-tail
compose stop analytics-erasure-worker
printf 'Worker stopped; waiting for the configured one-minute Delta and Kafka retention periods.\n'
sleep 75
compose up -d analytics-erasure-worker
wait_for_phase ReportPublished 600
run_fixture smoke-verify
compose stop
trap - EXIT
printf 'Smoke proof passed: populated synthetic Delta rows, worker restart, actual Kafka earliest offsets, completion receipt, report publication, and captured file absence. Project-scoped named volumes remain available as local evidence. This is flow evidence only; the full-horizon retention gate remains open.\n'
