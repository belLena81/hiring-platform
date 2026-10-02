#!/usr/bin/env bash
# Coordinator only: ordinary production Main startup, nonce database, reset/events disabled.
set -euo pipefail
umask 077
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$repo_root"
nonce="${1:?Supply the authorized isolated HMAC fixture nonce}"
cp_file="${2:-}"
[[ $# -le 2 && "$nonce" =~ ^[a-f0-9]{16}$  ]] || exit 2
state="$repo_root/.local/config/hmac-key-retirement-isolated-$nonce.state"
[[ -f "$state" && ! -L "$state" && "$(stat -c '%u:%a' "$state")" == "$(id -u):600" ]] || exit 2
mapfile -t values < <(python3 - "$state" "$nonce" <<'STATE'
import pathlib,re,sys
rows=[line.split('=',1) for line in pathlib.Path(sys.argv[1]).read_text().splitlines()]
assert all(len(row)==2 for row in rows)
s=dict(rows)
assert len(s)==len(rows) and s['NONCE']==sys.argv[2]
assert re.fullmatch(r'[0-9]{4,5}',s['MONGO_PORT'])
for key in ('OLD_IMAGE_ID','NEW_IMAGE_ID'): assert re.fullmatch(r'sha256:[a-f0-9]{64}',s[key])
for key in ('MONGO_PORT','OLD_IMAGE_ID','NEW_IMAGE_ID'): print(s[key])
STATE
)
[[ ${#values[@]} -eq 3 ]] || exit 2
project="hiring-hmac-rotation-test-$nonce"
database="hiring_hmac_rotation_test_$nonce"
volume="${project}_hmac-rotation-analytics"
data="$repo_root/.local/data/hmac-key-retirement/isolated-$nonce"
[[ "$(docker inspect --format '{{ index .Config.Labels "com.docker.compose.project" }}:{{ .State.Running }}' "$project-mongodb-1")" == "$project:true" ]] || exit 2
[[ "$(docker image inspect --format '{{.Id}}' "$project-old")" == "${values[1]}" ]] || exit 2
[[ "$(docker image inspect --format '{{.Id}}' "$project-new")" == "${values[2]}" ]] || exit 2
[[ "$(docker volume inspect --format '{{index .Options "device"}}' "$volume")" == "$data" ]] || exit 2
[[ -z "$(docker ps -q --filter "volume=$volume")" ]] || exit 2
for path in "$state" "$data"; do
  ancestor="$path"
  while [[ "$ancestor" != "$repo_root" ]]; do
    [[ -e "$ancestor" && ! -L "$ancestor" ]] || exit 2
    ancestor="$(dirname "$ancestor")"
  done
done
stamp="$(python3 -c 'import uuid; print(uuid.uuid4())')"
config_dir="$repo_root/.local/config/hmac-operational-bootstrap-$nonce-$stamp"
log_dir="$repo_root/.local/logs/hmac-operational-bootstrap-$nonce-$stamp"
for path in "$config_dir" "$log_dir"; do
  ancestor="$(dirname "$path")"
  while [[ "$ancestor" != "$repo_root" ]]; do
    [[ -d "$ancestor" && ! -L "$ancestor" ]] || exit 2
    ancestor="$(dirname "$ancestor")"
  done
  [[ ! -e "$path" && ! -L "$path" ]] || exit 2
  mkdir -m 700 "$path"
done
if [[ -z "$cp_file" ]]; then
  # Export the current root build serially before starting its single owned runtime.
  sbt -java-home /usr/lib/jvm/java-17-openjdk-amd64 'export Runtime / fullClasspath' > "$log_dir/root-classpath-export.log" 2>&1
  cp_file="$config_dir/root.classpath"
  rg '^/.*\.jar' "$log_dir/root-classpath-export.log" | tail -n 1 > "$cp_file"
fi
[[ "$cp_file" == "$repo_root/.local/config/"* && ! -L "$cp_file" ]] || exit 2
ancestor="$cp_file"
while [[ "$ancestor" != "$repo_root" ]]; do
  [[ -e "$ancestor" && ! -L "$ancestor" ]] || exit 2
  ancestor="$(dirname "$ancestor")"
done
[[ -f "$cp_file" && "$(stat -c '%u' "$cp_file")" == "$(id -u)" ]] || exit 2
api_cp="$(cat "$cp_file")"
python3 - "$api_cp" "$repo_root" <<'CLASSPATH'
import pathlib,sys
entries=sys.argv[1].split(':')
assert entries and all(pathlib.Path(path).is_absolute() and pathlib.Path(path).exists() for path in entries)
root=pathlib.Path(sys.argv[2])
assert any(pathlib.Path(path).is_relative_to(root/'target') and (pathlib.Path(path)/'com/example/graphQL/cats/Main.class').is_file() for path in entries)
CLASSPATH
port="$(python3 - <<'PORT'
import socket
with socket.socket() as s:
 s.bind(('127.0.0.1',0)); print(s.getsockname()[1])
PORT
)"
python3 - "$config_dir/application.conf" "${values[0]}" "$database" "$port" <<'CONFIG'
import json,pathlib,secrets,sys
q=json.dumps
uri='mongodb://127.0.0.1:'+sys.argv[2]+'/?replicaSet=rs0&directConnection=true'
conf='include classpath("application.conf")\n'+f'mongo.uri={q(uri)}\nmongo.database={q(sys.argv[3])}\nmongo.reset-on-start=false\nhttp.host="127.0.0.1"\nhttp.port={sys.argv[4]}\nauth.jwt.hs256-secret={q(secrets.token_hex(32))}\n'
conf+='kafka.enabled=false\nkafka.consumer.enabled=false\nvector-search.enabled=false\nlogging.mask-sensitive=true\n'
p=pathlib.Path(sys.argv[1]); p.write_text(conf); p.chmod(0o600)
CONFIG
observe() {
  docker exec "$project-mongodb-1" mongosh --quiet --eval "
    const d=db.getSiblingDB('$database');
    const a=d.analytics_hmac_key_retirements.find({keyId:'rotation-old-$nonce'}).sort({_id:1}).toArray();
    if(a.length>1) throw new Error('Ambiguous isolated old-key authorization');
    print('AUTH_COUNT='+a.length);
    print('AUTH_SHA256='+require('crypto').createHash('sha256').update(EJSON.stringify(a)).digest('hex'));
    print('USER_COUNT='+d.users.countDocuments({}));
    print('CONTROL_COUNT='+d.analytics_report_control.countDocuments({_id:'analytics-report'}));
    print('CONTROL_MIGRATION_COMPLETE='+d.hiring_migration_ledger.countDocuments({_id:'004_analytics_report_control',state:'Complete'}));
  "
}
observe > "$log_dir/before.log"
rg -q '^USER_COUNT=0$' "$log_dir/before.log" || exit 2
api_pid=
stop_owned() {
  [[ -n "$api_pid" ]] || return 0
  if kill -0 "$api_pid" 2>/dev/null; then
    kill -TERM "$api_pid" 2>/dev/null || true
    for ((attempt=0; attempt<30; attempt++)); do
      if ! kill -0 "$api_pid" 2>/dev/null; then break; fi
      sleep 1
    done
    if kill -0 "$api_pid" 2>/dev/null; then
      kill -KILL "$api_pid" 2>/dev/null || true
      wait "$api_pid" 2>/dev/null || true
      api_pid=
      printf 'Owned runtime required forced termination; bootstrap evidence is incomplete.\n' >&2
      return 1
    fi
  fi
  wait "$api_pid" 2>/dev/null || true
  api_pid=
}
trap 'stop_owned' EXIT
unset JAVA_TOOL_OPTIONS
/usr/lib/jvm/java-17-openjdk-amd64/bin/java -Dcats.effect.trackFiberContext=true -Dconfig.file="$config_dir/application.conf" \
  -cp "$api_cp" com.example.graphQL.cats.Main > "$log_dir/runtime.log" 2>&1 &
api_pid=$!
printf '%s\n' "$api_pid" > "$config_dir/owned-api.pid"
ready=false
for ((attempt=0; attempt<90; attempt++)); do
  kill -0 "$api_pid" 2>/dev/null || { printf 'Owned runtime exited before readiness: %s\n' "$log_dir/runtime.log" >&2; exit 1; }
  status="$(curl --silent --max-time 2 --output "$log_dir/readiness.json" --write-out '%{http_code}' "http://127.0.0.1:$port/ready" || true)"
  if [[ "$status" == 200 ]] && python3 - "$log_dir/readiness.json" <<'READY'
import json,pathlib,sys
assert json.loads(pathlib.Path(sys.argv[1]).read_text())=={'status':'READY'}
READY
  then ready=true; break; fi
  sleep 2
done
[[ "$ready" == true ]] || { printf 'Owned runtime did not become ready: %s\n' "$log_dir/runtime.log" >&2; exit 1; }
stop_owned
observe > "$log_dir/after.log"
rg -q '^USER_COUNT=0$' "$log_dir/after.log" || exit 1
rg -q '^CONTROL_COUNT=1$' "$log_dir/after.log" || exit 1
rg -q '^CONTROL_MIGRATION_COMPLETE=1$' "$log_dir/after.log" || exit 1
[[ "$(rg '^AUTH_SHA256=' "$log_dir/before.log")" == "$(rg '^AUTH_SHA256=' "$log_dir/after.log")" ]] || exit 1
[[ "$(docker image inspect --format '{{.Id}}' "$project-old")" == "${values[1]}" ]] || exit 1
[[ "$(docker image inspect --format '{{.Id}}' "$project-new")" == "${values[2]}" ]] || exit 1
[[ -z "$(docker ps -q --filter "volume=$volume")" ]] || exit 1
printf 'STANDARD_HIRING_BOOTSTRAP_PASS database=%s reset=false events=false users=0 authorizationUnchanged=true logs=%s\n' "$database" "$log_dir"
