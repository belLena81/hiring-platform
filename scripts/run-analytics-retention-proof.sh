#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

state_file="$repo_root/.local/config/analytics-retention-proof.state"
log_dir="$repo_root/.local/logs/analytics-retention-proof"

load_local_environment() {
  [[ -r .env ]] || { printf 'Ignored root .env is required for local analytics credentials.\n' >&2; exit 1; }
  . ./.env
  export -n KAFKA_BROKER_PASSWORD KAFKA_PUBLISHER_V2_PASSWORD KAFKA_READER_PASSWORD KAFKA_FENCER_PASSWORD HIRING_ANALYTICS_HMAC_SECRET_BASE64
  for variable in KAFKA_BROKER_PASSWORD KAFKA_PUBLISHER_V2_PASSWORD KAFKA_READER_PASSWORD KAFKA_FENCER_PASSWORD HIRING_ANALYTICS_HMAC_SECRET_BASE64; do
    [[ -n "${!variable:-}" ]] || { printf 'Required local setting %s is missing from .env.\n' "$variable" >&2; exit 1; }
  done
}

compose() {
  docker compose \
    --profile analytics \
    --profile analytics-erasure \
    -f compose.yaml \
    -f compose.analytics-retention-proof.yaml \
    -p "$HIRING_ANALYTICS_RETENTION_PROOF_PROJECT" \
    "$@"
}

check_local_directory() {
  local path="$1"
  local create="${2:-false}"
  local relative component current="$repo_root" permissions owner
  [[ "$path" == "$repo_root/.local" || "$path" == "$repo_root/.local/"* ]] || {
    printf 'Retention proof path is outside ignored local storage.\n' >&2
    exit 1
  }
  relative="${path#"$repo_root"/}"
  local -a components
  IFS='/' read -r -a components <<< "$relative"
  for component in "${components[@]}"; do
    current="$current/$component"
    [[ ! -L "$current" ]] || { printf 'Refusing a symlink in retention proof storage paths.\n' >&2; exit 1; }
    if [[ ! -e "$current" ]]; then
      [[ "$create" == true ]] || { printf 'Required retention proof directory is missing.\n' >&2; exit 1; }
      mkdir -m 700 -- "$current"
    fi
    [[ -d "$current" ]] || {
      printf 'Retention proof path component must be a real directory.\n' >&2
      exit 1
    }
    permissions="$(stat -c '%a' "$current")"
    owner="$(stat -c '%u' "$current")"
    [[ "$permissions" =~ ^[0-7]{3,4}$ ]] && (( (8#$permissions & 0022) == 0 )) || {
      printf 'Retention proof path ancestors must not be group- or world-writable.\n' >&2
      exit 1
    }
    if [[ "$current" == "$path" ]]; then
      [[ "$owner" == "$(id -u)" ]] || {
        printf 'Retention proof directory must be owned by this user.\n' >&2
        exit 1
      }
      [[ "$permissions" == 700 ]] || {
        printf 'Retention proof directory must have mode 700.\n' >&2
        exit 1
      }
    fi
  done
  if [[ "$create" == true ]]; then
    chmod 700 -- "$path"
  fi
}

ensure_log_directory() {
  check_local_directory "$repo_root/.local/logs" true
  check_local_directory "$log_dir" true
}

capture_log() {
  local name="$1"
  shift
  [[ "$name" =~ ^[a-z][a-z0-9-]*\.log$ ]] || { printf 'Invalid retention proof log name.\n' >&2; return 1; }
  ensure_log_directory
  local temporary result
  temporary="$(mktemp "$log_dir/.${name}.XXXXXX")"
  if "$@" | tee "$temporary"; then
    mv -fT -- "$temporary" "$log_dir/$name"
  else
    result=$?
    rm -f -- "$temporary"
    return "$result"
  fi
}

load_state() {
  check_local_directory "$repo_root/.local/config"
  [[ -r "$state_file" ]] || { printf 'No retention proof state exists; start with: %s start\n' "$0" >&2; exit 1; }
  [[ -f "$state_file" && ! -L "$state_file" ]] || {
    printf 'Retention proof state must be a regular, non-symlink file.\n' >&2
    exit 1
  }
  [[ "$(stat -c '%u:%a' "$state_file")" == "$(id -u):600" ]] || {
    printf 'Retention proof state must be owned by this user with mode 600.\n' >&2
    exit 1
  }
  local key value line
  declare -A state=()
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ "$line" =~ ^([A-Z_]+)=([A-Za-z0-9_./:-]+)$ ]] || {
      printf 'Invalid retention proof state entry.\n' >&2
      exit 1
    }
    key="${BASH_REMATCH[1]}"
    value="${BASH_REMATCH[2]}"
    case "$key" in
      HIRING_ANALYTICS_RETENTION_PROOF_NONCE|HIRING_ANALYTICS_RETENTION_PROOF_PROJECT|HIRING_ANALYTICS_RETENTION_PROOF_SUBJECT_ID|HIRING_ANALYTICS_RETENTION_PROOF_DATABASE|HIRING_ANALYTICS_RETENTION_PROOF_TOPIC|HIRING_ANALYTICS_RETENTION_PROOF_COMPLETED_AT) ;;
      *) printf 'Unknown retention proof state field.\n' >&2; exit 1 ;;
    esac
    [[ -z "${state[$key]:-}" ]] || { printf 'Duplicate retention proof state field.\n' >&2; exit 1; }
    state[$key]="$value"
  done < "$state_file"

  for key in HIRING_ANALYTICS_RETENTION_PROOF_NONCE HIRING_ANALYTICS_RETENTION_PROOF_PROJECT HIRING_ANALYTICS_RETENTION_PROOF_SUBJECT_ID HIRING_ANALYTICS_RETENTION_PROOF_DATABASE HIRING_ANALYTICS_RETENTION_PROOF_TOPIC; do
    [[ -n "${state[$key]:-}" ]] || { printf 'Required retention proof state field is missing.\n' >&2; exit 1; }
  done
  HIRING_ANALYTICS_RETENTION_PROOF_NONCE="${state[HIRING_ANALYTICS_RETENTION_PROOF_NONCE]}"
  HIRING_ANALYTICS_RETENTION_PROOF_PROJECT="${state[HIRING_ANALYTICS_RETENTION_PROOF_PROJECT]}"
  HIRING_ANALYTICS_RETENTION_PROOF_SUBJECT_ID="${state[HIRING_ANALYTICS_RETENTION_PROOF_SUBJECT_ID]}"
  HIRING_ANALYTICS_RETENTION_PROOF_DATABASE="${state[HIRING_ANALYTICS_RETENTION_PROOF_DATABASE]}"
  HIRING_ANALYTICS_RETENTION_PROOF_TOPIC="${state[HIRING_ANALYTICS_RETENTION_PROOF_TOPIC]}"
  HIRING_ANALYTICS_RETENTION_PROOF_COMPLETED_AT="${state[HIRING_ANALYTICS_RETENTION_PROOF_COMPLETED_AT]:-}"
  [[ "$HIRING_ANALYTICS_RETENTION_PROOF_NONCE" =~ ^[a-f0-9]{16}$ &&
    "$HIRING_ANALYTICS_RETENTION_PROOF_PROJECT" == "hiring-analytics-retention-$HIRING_ANALYTICS_RETENTION_PROOF_NONCE" &&
    "$HIRING_ANALYTICS_RETENTION_PROOF_SUBJECT_ID" =~ ^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$ &&
    "$HIRING_ANALYTICS_RETENTION_PROOF_DATABASE" == "hiring_retention_$HIRING_ANALYTICS_RETENTION_PROOF_NONCE" &&
    "$HIRING_ANALYTICS_RETENTION_PROOF_TOPIC" == "hiring.retention.$HIRING_ANALYTICS_RETENTION_PROOF_NONCE" ]] || {
      printf 'Retention proof state does not match its generated project identifiers.\n' >&2
      exit 1
    }
  if [[ -n "$HIRING_ANALYTICS_RETENTION_PROOF_COMPLETED_AT" ]]; then
    [[ "$HIRING_ANALYTICS_RETENTION_PROOF_COMPLETED_AT" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$ ]] || {
      printf 'Invalid retention proof completion timestamp.\n' >&2
      exit 1
    }
  fi
  export HIRING_ANALYTICS_RETENTION_PROOF_DATABASE
  export HIRING_ANALYTICS_RETENTION_PROOF_TOPIC
  export HIRING_ANALYTICS_RETENTION_PROOF_SUBJECT_ID
  export HIRING_ANALYTICS_RETENTION_PROOF_NONCE
  export KAFKA_TOPIC="$HIRING_ANALYTICS_RETENTION_PROOF_TOPIC"
  chmod 600 "$state_file"
  ensure_log_directory
  git check-ignore -q "$log_dir/probe" || {
    printf 'Retention proof log path is not ignored by Git; refusing to use it.\n' >&2
    exit 1
  }
}

run_fixture() {
  local mode="$1"
  local result=0
  export KAFKA_BROKER_PASSWORD KAFKA_PUBLISHER_V2_PASSWORD KAFKA_READER_PASSWORD HIRING_ANALYTICS_HMAC_SECRET_BASE64
  if compose run --rm --no-deps \
    -e HIRING_ANALYTICS_RETENTION_PROOF_ENABLED=true \
    -e HIRING_ANALYTICS_RETENTION_PROOF_MODE="$mode" \
    -e HIRING_ANALYTICS_RETENTION_PROOF_NONCE="$HIRING_ANALYTICS_RETENTION_PROOF_NONCE" \
    -e HIRING_ANALYTICS_RETENTION_PROOF_SUBJECT_ID="$HIRING_ANALYTICS_RETENTION_PROOF_SUBJECT_ID" \
    -e KAFKA_BROKER_PASSWORD \
    -e KAFKA_PUBLISHER_V2_PASSWORD \
    -e KAFKA_READER_PASSWORD \
    -e HIRING_ANALYTICS_HMAC_SECRET_BASE64 \
    --entrypoint /bin/sh analytics-batch \
    -ec 'exec sbt "IntegrationTest / testOnly *AnalyticsRetentionProofIntegrationSpec"'; then
    result=0
  else
    result=$?
  fi
  unset KAFKA_BROKER_PASSWORD KAFKA_PUBLISHER_V2_PASSWORD KAFKA_READER_PASSWORD HIRING_ANALYTICS_HMAC_SECRET_BASE64
  return "$result"
}

wait_for_request_phase() {
  local expected_phase="$1"
  local elapsed=0
  local actual=''
  while (( elapsed < 1200 )); do
    actual="$(compose exec -T mongodb mongosh --quiet --eval \
      "const r=db.getSiblingDB('$HIRING_ANALYTICS_RETENTION_PROOF_DATABASE').analytics_erasure_requests.findOne({_id:'$HIRING_ANALYTICS_RETENTION_PROOF_SUBJECT_ID'}); print(r ? r.state+':'+r.phase : 'MISSING')" 2>/dev/null | tail -n 1 || true)"
    if [[ "$actual" == "Processing:$expected_phase" || "$actual" == "Complete:ReportPublished" ]]; then
      printf 'Worker checkpoint: %s\n' "$actual"
      return 0
    fi
    if [[ "$actual" == MISSING ]]; then
      printf 'Erasure proof request has not been staged yet.\n' >&2
      return 1
    fi
    sleep 2
    elapsed=$((elapsed + 2))
  done
  printf 'Timed out waiting for %s; current checkpoint is %s. Inspect %s.\n' \
    "$expected_phase" "${actual:-unknown}" "$log_dir" >&2
  return 1
}

start_proof() {
  load_local_environment
  local nonce subject_id project database topic
  if [[ -e "$state_file" || -L "$state_file" ]]; then
    load_state
    nonce="$HIRING_ANALYTICS_RETENTION_PROOF_NONCE"
    subject_id="$HIRING_ANALYTICS_RETENTION_PROOF_SUBJECT_ID"
    project="$HIRING_ANALYTICS_RETENTION_PROOF_PROJECT"
    database="$HIRING_ANALYTICS_RETENTION_PROOF_DATABASE"
    topic="$HIRING_ANALYTICS_RETENTION_PROOF_TOPIC"
    printf 'Resuming isolated Compose project %s.\n' "$project"
  else
    nonce="$(openssl rand -hex 8)"
    subject_id="$(python3 -c 'import uuid; print(uuid.uuid4())')"
    project="hiring-analytics-retention-$nonce"
    database="hiring_retention_$nonce"
    topic="hiring.retention.$nonce"

    check_local_directory "$repo_root/.local/config" true
    ensure_log_directory
    (
      set -o noclobber
      cat > "$state_file" <<EOF
HIRING_ANALYTICS_RETENTION_PROOF_NONCE=$nonce
HIRING_ANALYTICS_RETENTION_PROOF_PROJECT=$project
HIRING_ANALYTICS_RETENTION_PROOF_SUBJECT_ID=$subject_id
HIRING_ANALYTICS_RETENTION_PROOF_DATABASE=$database
HIRING_ANALYTICS_RETENTION_PROOF_TOPIC=$topic
EOF
    )
    chmod 600 "$state_file"
    load_state
  fi

  printf 'Starting isolated Compose project %s with task-scoped persistent volumes.\n' "$project"
  compose build analytics-batch analytics-erasure-worker
  compose up -d mongodb kafka kafka-acl-init
  compose ps -a
  local staged_request
  staged_request="$(compose exec -T mongodb mongosh --quiet --eval \
    "const d=db.getSiblingDB('$database'); const f=d.analytics_retention_proof.findOne({_id:'$nonce',stage:'Prepared'}); print(f && d.analytics_erasure_requests.countDocuments({_id:'$subject_id'}) === 1 ? 'PRESENT' : 'ABSENT')" 2>/dev/null | tail -n 1 || true)"
  if [[ "$staged_request" == PRESENT ]]; then
    printf 'Resuming from the complete durable fixture; preparation will not be repeated.\n'
  else
    capture_log prepare.log run_fixture prepare
  fi

  compose up -d analytics-erasure-worker
  wait_for_request_phase DeltaPurged
  capture_log retention-tail.log run_fixture append-retention-tail
  capture_log initial-status.log run_fixture inspect
  printf 'Proof is staged. Keep Kafka available for its 168-hour retention; use %s status to inspect progress.\n' "$0"
}

status_proof() {
  load_state
  local running_services
  if ! running_services="$(compose ps --status running --services 2>/dev/null)"; then
    printf 'Could not inspect the retention proof Compose project; Docker status is unavailable.\n' >&2
    return 1
  fi
  if [[ -n "$running_services" ]]; then
    run_fixture inspect
  else
    printf 'No Compose services are running. This status check did not stop or remove proof containers or volumes.\n'
    printf 'Use %s finish after the recorded 30-day Delta horizon has elapsed.\n' "$0"
  fi
}

tail_proof() {
  load_local_environment
  load_state
  compose build analytics-batch analytics-erasure-worker
  compose up -d mongodb kafka kafka-acl-init analytics-erasure-worker
  wait_for_request_phase DeltaPurged
  capture_log retention-tail.log run_fixture append-retention-tail
  capture_log initial-status.log run_fixture inspect
  printf 'Proof is staged. Keep Kafka available for its 168-hour retention; use %s status to inspect progress.\n' "$0"
}

pause_proof() {
  load_state
  run_fixture require-kafka-retention
  compose stop
  printf 'Kafka retention barrier passed; isolated containers stopped and task-scoped volumes remain attached.\n'
  printf 'Delta log retention continues from its persisted DeltaPurged timestamp.\n'
}

finish_proof() {
  load_state
  compose up -d mongodb kafka kafka-acl-init
  run_fixture require-all-retention
  compose up -d analytics-erasure-worker
  wait_for_request_phase ReportPublished
  capture_log final-verification.log run_fixture verify
  compose stop
  cat >> "$state_file" <<EOF
HIRING_ANALYTICS_RETENTION_PROOF_COMPLETED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
EOF
  chmod 600 "$state_file"
  printf 'Retention proof passed. Persisted evidence remains in project-scoped Docker volumes.\n'
}

usage() {
  printf 'Usage: %s {start|tail|status|pause|finish}\n' "$0" >&2
}

case "${1:-}" in
  start)
    start_proof
    ;;
  status)
    load_local_environment
    load_state
    status_proof
    ;;
  tail)
    tail_proof
    ;;
  pause)
    load_local_environment
    pause_proof
    ;;
  finish)
    load_local_environment
    finish_proof
    ;;
  *)
    usage
    exit 2
    ;;
esac
