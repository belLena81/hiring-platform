#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
[[ -r .env ]] || { printf 'Ignored root .env is required for local Kafka credentials.\n' >&2; exit 1; }
git check-ignore -q .local/logs/account-deletion-compose-proof/probe
mkdir -p .local/config .local/logs/account-deletion-compose-proof
chmod 700 .local/config .local/logs/account-deletion-compose-proof
. ./.env

nonce="$(openssl rand -hex 8)"
project="hiring-account-deletion-$nonce"
database="account_deletion_$nonce"
topic="hiring.deletion.$nonce"
proof_parent="$repo_root/.local/data/account-deletion-compose-proof"
for parent in "$repo_root/.local" "$repo_root/.local/data" "$proof_parent"; do
  [[ ! -L "$parent" ]] || { printf 'Refusing symlinked proof-data parent: %s\n' "$parent" >&2; exit 1; }
done
mkdir -p "$proof_parent"
canonical_proof_parent="$(cd "$proof_parent" && pwd -P)"
[[ "$canonical_proof_parent" == "$proof_parent" ]] || { printf 'Proof-data parent resolves outside its task-scoped repository path.\n' >&2; exit 1; }
proof_data_dir="$proof_parent/$project"
git check-ignore -q .local/data/account-deletion-compose-proof/probe
[[ ! -e "$proof_data_dir" && ! -L "$proof_data_dir" ]] || { printf 'Nonce-scoped proof data path already exists; refusing to reuse it.\n' >&2; exit 1; }
mkdir -p "$proof_data_dir"
chmod 700 "$proof_data_dir"
read -r mongo_port kafka_port api_port < <(python3 -c 'import socket; ss=[]
for _ in range(3):
 s=socket.socket(); s.bind(("127.0.0.1",0)); ss.append(s)
print(*(s.getsockname()[1] for s in ss))
for s in ss:s.close()')
config="$repo_root/.local/config/$project.conf"
log="$repo_root/.local/logs/account-deletion-compose-proof/$project.log"
compose=(docker compose --profile analytics-erasure -f compose.yaml -f compose.analytics-account-deletion-proof.yaml -p "$project")
api_pid=""
cleanup() {
  local result=$?
  trap - EXIT
  if [[ -n "$api_pid" ]]; then kill -- "-$api_pid" 2>/dev/null || true; wait "$api_pid" 2>/dev/null || true; fi
  if (( result == 0 )); then
    "${compose[@]}" down --remove-orphans >/dev/null 2>&1 || true
    if [[ ! -L "$repo_root/.local" && ! -L "$repo_root/.local/data" && ! -L "$proof_parent" &&
          "$(cd "$proof_parent" && pwd -P)" == "$proof_parent" &&
          ! -L "$proof_data_dir" && "$(dirname "$proof_data_dir")" == "$proof_parent" ]]; then
      rm -rf -- "$proof_data_dir"
    else
      printf 'Proof-data path changed ownership or resolved through a symlink; retained it for inspection.\n' >&2
      result=1
    fi
  else
    "${compose[@]}" stop >/dev/null 2>&1 || true
    printf 'Proof failed; isolated containers and the task-scoped Delta directory for %s were stopped and retained for inspection.\n' "$project" >&2
  fi
  rm -f -- "$config"
  exit "$result"
}
trap cleanup EXIT

export HIRING_ACCOUNT_DELETION_PROOF_MONGO_PORT="$mongo_port"
export HIRING_ACCOUNT_DELETION_PROOF_KAFKA_PORT="$kafka_port"
export HIRING_ACCOUNT_DELETION_PROOF_DATABASE="$database"
export HIRING_ACCOUNT_DELETION_PROOF_TOPIC="$topic"
export HIRING_ACCOUNT_DELETION_PROOF_ANALYTICS_DIR="$proof_data_dir"
export HIRING_ANALYTICS_HMAC_SECRET_BASE64
export HIRING_ANALYTICS_HMAC_KEY_ID
export HIRING_ANALYTICS_HMAC_PREVIOUS_KEY_ID
export HIRING_ANALYTICS_HMAC_PREVIOUS_SECRET_BASE64
export KAFKA_TOPIC="$topic"
"${compose[@]}" up -d --wait mongodb kafka kafka-acl-init

cat > "$config" <<EOF
include classpath("application.conf")
mongo.database = "$database"
kafka.topic = "$topic"
EOF
chmod 600 "$config"
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
export MONGODB_URI="mongodb://127.0.0.1:$mongo_port/?replicaSet=rs0&directConnection=true"
export MONGODB_RESET_ON_START=false
export AUTH_JWT_HS256_SECRET="$(openssl rand -hex 32)"
export HTTP_HOST=127.0.0.1
export HTTP_PORT="$api_port"
export KAFKA_ENABLED=true
export KAFKA_BOOTSTRAP_SERVERS="127.0.0.1:$kafka_port"
export KAFKA_PUBLISHER_USERNAME=hiring_publisher_v2
export KAFKA_PUBLISHER_V2_PASSWORD
export JAVA_TOOL_OPTIONS="-Dconfig.file=$config"
setsid sbt run >"$log" 2>&1 &
api_pid=$!
api_url="http://127.0.0.1:$api_port"
ready=false
for _ in $(seq 1 120); do
  if curl -fsS "$api_url/ready" 2>/dev/null | rg -q READY; then ready=true; break; fi
  kill -0 "$api_pid" 2>/dev/null || { printf 'API exited; inspect %s\n' "$log" >&2; exit 1; }
  sleep 1
done
[[ "$ready" == true ]] || { printf 'API readiness timed out; inspect %s\n' "$log" >&2; exit 1; }

# API startup owns Mongo collection creation and versioned setup migrations.
# Start the worker only after that preflight has completed against this database.
"${compose[@]}" up -d --no-deps analytics-erasure-worker

export HIRING_ACCOUNT_DELETION_COMPOSE_PROOF_ENABLED=true
export HIRING_ACCOUNT_DELETION_COMPOSE_PROOF_DATABASE="$database"
export HIRING_ACCOUNT_DELETION_COMPOSE_PROOF_TOPIC="$topic"
export HIRING_ACCOUNT_DELETION_COMPOSE_PROOF_API_URL="$api_url"
export HIRING_ACCOUNT_DELETION_COMPOSE_PROOF_MONGO_URI="$MONGODB_URI"
export HIRING_ACCOUNT_DELETION_COMPOSE_PROOF_KAFKA="127.0.0.1:$kafka_port"
export HIRING_ACCOUNT_DELETION_COMPOSE_PROOF_ANALYTICS_DIR="$proof_data_dir"
(cd analytics && unset JAVA_TOOL_OPTIONS && sbt 'IntegrationTest / testOnly *AccountDeletionComposeIntegrationSpec')
printf 'Proof stack %s removed its task-scoped containers/network; named volumes remain for inspection. Active retention/shared Compose projects were not targeted. API log: %s\n' "$project" "$log"
