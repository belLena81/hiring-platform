#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

test -f .env || { printf 'Ignored root .env is required for local Kafka credentials.\n' >&2; exit 1; }
git check-ignore -q .local/config/account-deletion-recovery.conf
git check-ignore -q .local/logs/account-deletion-recovery.log
mkdir -p .local/config .local/logs
chmod 700 .local/config .local/logs

set -a
# The file is local and ignored; do not print its values.
. ./.env
set +a

recovery_database="account_deletion_recovery_$(python3 -c 'import uuid; print(uuid.uuid4().hex)')"
recovery_port="${HIRING_ACCOUNT_DELETION_RECOVERY_PORT:-18080}"
recovery_config="$repo_root/.local/config/$recovery_database.conf"
recovery_log="$repo_root/.local/logs/$recovery_database.log"
python3 -c 'import socket,sys; sock=socket.socket(); sock.bind(("127.0.0.1",int(sys.argv[1]))); sock.close()' "$recovery_port"
existing_collections="$(docker exec hiringplatform-mongodb-1 mongosh --quiet --eval "db.getSiblingDB('$recovery_database').getCollectionNames().length" | tail -1)"
if [[ "$existing_collections" != 0 ]]; then
  printf 'Recovery database already contains collections; refusing to use it.\n' >&2
  exit 1
fi
recovery_nonce="$(openssl rand -hex 16)"
docker exec hiringplatform-mongodb-1 mongosh --quiet --eval "db.getSiblingDB('$recovery_database').getCollection('account_deletion_recovery_fixture').insertOne({_id:'$recovery_database',nonce:'$recovery_nonce',state:'Prepared'})" > /dev/null
api_pid=""
cleanup() {
  if [[ -n "$api_pid" ]]; then
    kill -- "-$api_pid" 2>/dev/null || true
    wait "$api_pid" 2>/dev/null || true
  fi
  docker exec hiringplatform-mongodb-1 mongosh --quiet --eval "const database=db.getSiblingDB('$recovery_database');const marker=database.getCollection('account_deletion_recovery_fixture').findOne({_id:'$recovery_database',nonce:'$recovery_nonce'});if(marker)database.dropDatabase()" > /dev/null || true
}
trap cleanup EXIT

cat > "$recovery_config" <<EOF
include classpath("application.conf")
mongo.database = "$recovery_database"
EOF
chmod 600 "$recovery_config"

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
export MONGODB_URI="mongodb://127.0.0.1:27017/?replicaSet=rs0&directConnection=true"
export MONGODB_RESET_ON_START=false
export AUTH_JWT_HS256_SECRET="${AUTH_JWT_HS256_SECRET:-$(openssl rand -hex 32)}"
export HTTP_HOST=127.0.0.1
export HTTP_PORT="$recovery_port"
export KAFKA_ENABLED=false
export JAVA_TOOL_OPTIONS="-Dconfig.file=$recovery_config"

setsid sbt run > "$recovery_log" 2>&1 &
api_pid=$!

api_url="http://127.0.0.1:$recovery_port"
ready=false
for _ in $(seq 1 120); do
  if curl -fsS "$api_url/ready" 2>/dev/null | rg -q 'READY'; then
    ready=true
    break
  fi
  if ! kill -0 "$api_pid" 2>/dev/null; then
    printf 'The API exited before readiness; inspect %s.\n' "$recovery_log" >&2
    exit 1
  fi
  sleep 1
done
if [[ "$ready" != true ]]; then
  printf 'The API did not become ready; inspect %s.\n' "$recovery_log" >&2
  exit 1
fi

export HIRING_ACCOUNT_DELETION_RECOVERY_EVIDENCE=true
export HIRING_ACCOUNT_DELETION_RECOVERY_DATABASE="$recovery_database"
export HIRING_ACCOUNT_DELETION_RECOVERY_NONCE="$recovery_nonce"
export HIRING_ACCOUNT_DELETION_RECOVERY_API_URL="$api_url"

(
  cd analytics
  unset JAVA_TOOL_OPTIONS
  sbt 'IntegrationTest / testOnly *AccountDeletionRecoveryIntegrationSpec'
)
