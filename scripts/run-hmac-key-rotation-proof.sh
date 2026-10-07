#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$repo_root"

state_file="$repo_root/.local/config/hmac-key-retirement.state"
data_parent="$repo_root/.local/data/hmac-key-retirement"
log_dir="$repo_root/.local/logs/hmac-key-retirement"
old_uid=20001
new_uid=20002
isolated_test=false
isolated_test_resume=false
if [[ "${1:-}" == isolated-test ]]; then
  isolated_test=true
  if [[ -n "${2:-}" ]]; then
    [[ "$2" =~ ^[a-f0-9]{16}$ && $# -eq 2 ]] || { printf 'Resume requires exactly one 16-character lowercase hexadecimal nonce.\n' >&2; exit 2; }
    nonce="$2"
    isolated_test_resume=true
  else
    [[ $# -eq 1 ]] || exit 2
    nonce="$(openssl rand -hex 8)"
  fi
  state_file="$repo_root/.local/config/hmac-key-retirement-isolated-$nonce.state"
  log_dir="$repo_root/.local/logs/hmac-key-retirement-isolated-$nonce"
  if [[ "$isolated_test_resume" == true && ( ! -f "$state_file" || -L "$state_file" ) ]]; then
    printf 'The requested isolated rotation state does not exist.\n' >&2; exit 2
  fi
  export HIRING_HMAC_ROTATION_ISOLATED_TEST_NONCE="$nonce"
  export ANALYTICS_RETENTION_DELTA_VACUUM_SAFETY='60 seconds'
  export ANALYTICS_RETENTION_DELTA_LOG_RETENTION='60 seconds'
fi

die() { printf '%s\n' "$*" >&2; exit 1; }

local_directory() {
  local path="$1" create="${2:-false}" part current="$repo_root" mode
  local -a parts
  [[ "$path" == "$repo_root/.local/"* ]] || die 'Proof path is outside the ignored local directory.'
  IFS='/' read -r -a parts <<< "${path#"$repo_root"/}"
  for part in "${parts[@]}"; do
    current="$current/$part"
    [[ ! -L "$current" ]] || die 'Refusing a symlink in proof storage.'
    if [[ ! -e "$current" ]]; then
      [[ "$create" == true ]] || die "Required proof directory is absent: $current"
      mkdir -m 700 -- "$current" || die "Cannot create $current; resolve its ownership through the normal approval path."
    fi
    [[ -d "$current" ]] || die 'Proof storage component is not a directory.'
    mode="$(stat -c '%a' "$current")"
    (( (8#$mode & 0022) == 0 )) || die 'Proof storage ancestor is group or world writable.'
  done
}

load_credentials() {
  [[ -f .env && ! -L .env ]] || die 'Ignored root .env with local Kafka credentials is required.'
  # The operator owns this ignored local configuration file.
  set -a
  . ./.env
  set +a
  local name
  for name in KAFKA_BROKER_PASSWORD KAFKA_PUBLISHER_V2_PASSWORD KAFKA_READER_PASSWORD KAFKA_FENCER_PASSWORD; do
    [[ -n "${!name:-}" ]] || die "Missing local Kafka credential: $name"
  done
}

save_state() {
  local temporary
  temporary="$(mktemp "$repo_root/.local/config/.hmac-key-retirement.XXXXXX")"
  {
    printf 'NONCE=%s\n' "$nonce"
    printf 'MONGO_PORT=%s\n' "$mongo_port"
    printf 'KAFKA_PORT=%s\n' "$kafka_port"
    printf 'OLD_SECRET=%s\n' "$old_secret"
    printf 'NEW_SECRET=%s\n' "$new_secret"
    printf 'OLD_IMAGE_ID=%s\n' "$old_image_id"
    printf 'NEW_IMAGE_ID=%s\n' "$new_image_id"
    printf 'STAGED_AT=%s\n' "$staged_at"
    printf 'TAIL_AT=%s\n' "$tail_at"
    printf 'CUTOVER_AT=%s\n' "$cutover_at"
    printf 'AUTHORIZED_AT=%s\n' "$authorized_at"
    printf 'RESTARTED_AT=%s\n' "$restarted_at"
  } > "$temporary"
  chmod 600 "$temporary"
  mv -fT -- "$temporary" "$state_file"
}

load_state() {
  local_directory "$repo_root/.local/config"
  [[ -f "$state_file" && ! -L "$state_file" ]] || die 'No rotation state exists; run start first.'
  [[ "$(stat -c '%u:%a' "$state_file")" == "$(id -u):600" ]] || die 'Rotation state must be owned by this user with mode 600.'
  local line key value
  declare -A seen=()
  nonce= mongo_port= kafka_port= old_secret= new_secret= old_image_id= new_image_id=
  staged_at= tail_at= cutover_at= authorized_at= restarted_at=
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ "$line" =~ ^([A-Z_]+)=(.*)$ ]] || die 'Malformed rotation state.'
    key="${BASH_REMATCH[1]}"; value="${BASH_REMATCH[2]}"
    [[ ! -v seen[$key] ]] || die 'Duplicate rotation state key.'
    seen[$key]=1
    case "$key" in
      NONCE) nonce="$value" ;; MONGO_PORT) mongo_port="$value" ;; KAFKA_PORT) kafka_port="$value" ;;
      OLD_SECRET) old_secret="$value" ;; NEW_SECRET) new_secret="$value" ;;
      OLD_IMAGE_ID) old_image_id="$value" ;; NEW_IMAGE_ID) new_image_id="$value" ;;
      STAGED_AT) staged_at="$value" ;; CUTOVER_AT) cutover_at="$value" ;;
      TAIL_AT) tail_at="$value" ;;
      AUTHORIZED_AT) authorized_at="$value" ;; RESTARTED_AT) restarted_at="$value" ;;
      *) die 'Unknown rotation state key.' ;;
    esac
  done < "$state_file"
  if [[ "$isolated_test" == true ]]; then
    [[ "$nonce" == "$HIRING_HMAC_ROTATION_ISOLATED_TEST_NONCE" ]] || die 'Isolated state nonce differs from its requested fixture.'
  fi
  [[ "$nonce" =~ ^[a-f0-9]{16}$ && "$mongo_port" =~ ^[0-9]{4,5}$ && "$kafka_port" =~ ^[0-9]{4,5}$ &&
    "$new_secret" =~ ^[A-Za-z0-9+/]{43}=$ ]] || die 'Invalid rotation state identifiers or secrets.'
  [[ "$old_secret" =~ ^[A-Za-z0-9+/]{43}=$ || ( -z "$old_secret" && -n "$restarted_at" ) ]] ||
    die 'Old-key material is missing before the new-key-only restart.'
  local stamp
  for stamp in "$staged_at" "$tail_at" "$cutover_at" "$authorized_at" "$restarted_at"; do
    [[ -z "$stamp" || "$stamp" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$ ]] || die 'Invalid rotation state timestamp.'
  done
  for key in old_image_id new_image_id; do
    [[ -z "${!key}" || "${!key}" =~ ^sha256:[a-f0-9]{64}$ ]] || die 'Invalid recorded image identity.'
  done
  configure_names
  local_directory "$data_dir"
  [[ "$(cd "$data_dir" && pwd -P)" == "$data_dir" ]] || die 'Rotation volume path is not canonical.'
}

configure_names() {
  project="hiring-hmac-rotation-$nonce"
  database="hiring_hmac_rotation_$nonce"
  topic="hiring.hmac.rotation.$nonce"
  data_dir="$data_parent/$nonce"
  if [[ "$isolated_test" == true ]]; then
    project="hiring-hmac-rotation-test-$nonce"
    database="hiring_hmac_rotation_test_$nonce"
    topic="hiring.hmac.rotation.test.$nonce"
    data_dir="$data_parent/isolated-$nonce"
  fi
  volume_name="${project}_hmac-rotation-analytics"
  base_image="${project}-base"
  # Existing proof state already pins immutable IDs; avoid retagging its images.
  old_image="${old_image_id:-${project}-old}"
  new_image="${new_image_id:-${project}-new}"
  export HIRING_HMAC_ROTATION_MONGO_PORT="$mongo_port"
  export HIRING_HMAC_ROTATION_KAFKA_PORT="$kafka_port"
  export HIRING_HMAC_ROTATION_DATABASE="$database"
  export HIRING_HMAC_ROTATION_TOPIC="$topic"
  export HIRING_HMAC_ROTATION_DATA_DIR="$data_dir"
  export HIRING_HMAC_ROTATION_LAKEHOUSE_URI="$(python3 "$repo_root/scripts/canonical-local-file-uri.py" "$data_dir/lakehouse")"
  export HIRING_HMAC_ROTATION_WRITER_IMAGE="$new_image"
  export KAFKA_TOPIC="$topic"
  export HIRING_ANALYTICS_HMAC_SECRET_BASE64="$new_secret"
}

compose() {
  local -a profile_files=(-f compose.yaml -f compose.hmac-key-rotation-proof.yaml)
  if [[ "$isolated_test" == true ]]; then profile_files+=(-f compose.hmac-key-rotation-calendar-proof.yaml); fi
  docker compose --profile analytics --profile analytics-erasure "${profile_files[@]}" -p "$project" "$@"
}

capture() {
  local name="$1"; shift
  [[ "$name" =~ ^[a-z][a-z0-9-]*\.log$ ]] || die 'Invalid proof log name.'
  local_directory "$repo_root/.local/logs" true
  local_directory "$log_dir" true
  git check-ignore -q "$log_dir/$name" || die 'Proof log path is not ignored by Git.'
  "$@" > "$log_dir/$name" 2>&1 || {
    printf 'Proof command failed; inspect %s.\n' "$log_dir/$name" >&2
    return 1
  }
}

free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'
}

build_images() {
  local fingerprint
  fingerprint="$(python3 - "$repo_root" "$old_uid" "$new_uid" <<'PY_IMAGE'
from pathlib import Path
import hashlib,sys
root=Path(sys.argv[1]);digest=hashlib.sha256()
paths=[root/'.dockerignore',root/'analytics/build.sbt',root/'analytics/Dockerfile',root/'analytics/Dockerfile.hmac-key-rotation-proof']
for directory in ['analytics/src','analytics/project','test-support']:
    paths += [path for path in (root/directory).rglob('*') if path.is_file() and not any(part in ('target','__pycache__') for part in path.parts)]
for path in sorted(set(paths)):
    digest.update(str(path.relative_to(root)).encode());digest.update(b'\0');digest.update(path.read_bytes());digest.update(b'\0')
digest.update(('writer-uids:'+sys.argv[2]+','+sys.argv[3]).encode())
print(digest.hexdigest())
PY_IMAGE
)"
  base_image="hiring-analytics-proof-cache:base-$fingerprint"
  old_image="hiring-analytics-proof-cache:writer-$old_uid-$fingerprint"
  new_image="hiring-analytics-proof-cache:writer-$new_uid-$fingerprint"
  if ! docker image inspect "$base_image" >/dev/null 2>&1; then
    docker build -f analytics/Dockerfile -t "$base_image" .
  fi
  if ! docker image inspect "$old_image" >/dev/null 2>&1; then
    docker build -f analytics/Dockerfile.hmac-key-rotation-proof \
      --build-arg "ANALYTICS_BASE_IMAGE=$base_image" --build-arg "ANALYTICS_WRITER_UID=$old_uid" \
      -t "$old_image" .
  fi
  if ! docker image inspect "$new_image" >/dev/null 2>&1; then
    docker build -f analytics/Dockerfile.hmac-key-rotation-proof \
      --build-arg "ANALYTICS_BASE_IMAGE=$base_image" --build-arg "ANALYTICS_WRITER_UID=$new_uid" \
      -t "$new_image" .
  fi
  old_image_id="$(docker image inspect --format '{{.Id}}' "$old_image")"
  new_image_id="$(docker image inspect --format '{{.Id}}' "$new_image")"
  [[ "$old_image_id" =~ ^sha256:[a-f0-9]{64}$ && "$new_image_id" =~ ^sha256:[a-f0-9]{64}$ ]] || die 'Writer image identity is unavailable.'
  save_state
}

verify_images() {
  [[ "$(docker image inspect --format '{{.Id}}' "$old_image")" == "$old_image_id" &&
    "$(docker image inspect --format '{{.Id}}' "$new_image")" == "$new_image_id" ]] || die 'Recorded writer image identity changed.'
}

old_fixture() {
  local action="$1"
  (
    export HIRING_HMAC_ROTATION_OLD_KEY_ID="rotation-old-$nonce"
    export HIRING_HMAC_ROTATION_NEW_KEY_ID="rotation-new-$nonce"
    export HIRING_HMAC_ROTATION_OLD_SECRET_BASE64="$old_secret"
    export HIRING_HMAC_ROTATION_NEW_SECRET_BASE64="$new_secret"
    export ANALYTICS_KAFKA_USERNAME=hiring_publisher_v2
    export ANALYTICS_KAFKA_PASSWORD="$KAFKA_PUBLISHER_V2_PASSWORD"
    export HIRING_HMAC_ROTATION_WRITER_IMAGE="$old_image"
    compose run --rm --no-deps \
      -e HIRING_HMAC_ROTATION_OLD_KEY_ID -e HIRING_HMAC_ROTATION_NEW_KEY_ID \
      -e HIRING_HMAC_ROTATION_OLD_SECRET_BASE64 -e HIRING_HMAC_ROTATION_NEW_SECRET_BASE64 \
      -e ANALYTICS_KAFKA_USERNAME -e ANALYTICS_KAFKA_PASSWORD \
      --entrypoint sbt analytics-batch \
      "Test / runMain com.example.hiring.analytics.cli.HmacKeyRetirementFixtureMain $action"
  )
}

new_fixture() {
  local action="$1"
  (
    export HIRING_HMAC_ROTATION_OLD_KEY_ID="rotation-old-$nonce"
    export HIRING_HMAC_ROTATION_NEW_KEY_ID="rotation-new-$nonce"
    export HIRING_HMAC_ROTATION_OLD_SECRET_BASE64="$old_secret"
    export HIRING_HMAC_ROTATION_NEW_SECRET_BASE64="$new_secret"
    if [[ "$action" == publish-new-event ]]; then
      export ANALYTICS_KAFKA_USERNAME=hiring_publisher_v2
      export ANALYTICS_KAFKA_PASSWORD="$KAFKA_PUBLISHER_V2_PASSWORD"
    fi
    compose run --rm --no-deps \
      -e HIRING_HMAC_ROTATION_OLD_KEY_ID -e HIRING_HMAC_ROTATION_NEW_KEY_ID \
      -e HIRING_HMAC_ROTATION_OLD_SECRET_BASE64 -e HIRING_HMAC_ROTATION_NEW_SECRET_BASE64 \
      -e ANALYTICS_KAFKA_USERNAME -e ANALYTICS_KAFKA_PASSWORD \
      --entrypoint sbt analytics-batch \
      "Test / runMain com.example.hiring.analytics.cli.HmacKeyRetirementFixtureMain $action"
  )
}

host_authorization() {
  local action="$1"
  local host_scratch="$repo_root/.local/data/analytics/spark-temp/hmac-key-retirement-$nonce"
  if [[ "$isolated_test" == true ]]; then host_scratch="$repo_root/.local/data/analytics/spark-temp/hmac-key-retirement-isolated-$nonce"; fi
  # This host-owned scratch root is separate from the writer volume's read-only host ACL.
  local_directory "$host_scratch" true
  [[ "$(stat -c '%u:%a' "$host_scratch")" == "$(id -u):700" ]] || die 'Host Spark scratch must be owned by this user with mode 700.'
  [[ -z "$(find "$host_scratch" -mindepth 1 \( -type f -o -type l \) -print -quit)" ]] || die 'Prior host Spark spill files remain; preserve them for inspection.'
  verify_images
  (
    export MONGODB_URI="mongodb://127.0.0.1:$mongo_port/?directConnection=true"
    export MONGODB_DATABASE="$database" SPARK_MASTER='local[2]'
    export ANALYTICS_SPARK_LOCAL_DIRECTORY="$host_scratch"
    export ANALYTICS_LAKEHOUSE_ROOT="$(python3 "$repo_root/scripts/canonical-local-file-uri.py" "$data_dir/lakehouse")"
    export ANALYTICS_BOOTSTRAP_SERVERS="127.0.0.1:$kafka_port"
    export ANALYTICS_KAFKA_USERNAME=analytics_reader ANALYTICS_KAFKA_PASSWORD="$KAFKA_READER_PASSWORD"
    export ANALYTICS_KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT ANALYTICS_KAFKA_ALLOW_PLAINTEXT=true
    export ANALYTICS_TOPIC="$topic" ANALYTICS_RETIRING_KEY_ID="rotation-old-$nonce"
    export HIRING_HMAC_ROTATION_VOLUME_NAME="$volume_name"
    export HIRING_HMAC_ROTATION_OLD_IMAGE="$old_image" HIRING_HMAC_ROTATION_OLD_IMAGE_ID="$old_image_id"
    export HIRING_HMAC_ROTATION_OLD_UID="$old_uid" HIRING_HMAC_ROTATION_NEW_IMAGE="$new_image"
    export HIRING_HMAC_ROTATION_NEW_IMAGE_ID="$new_image_id" HIRING_HMAC_ROTATION_NEW_UID="$new_uid"
    cd "$repo_root/analytics"
    sbt -java-home /usr/lib/jvm/java-17-openjdk-amd64 \
      "Test / runMain com.example.hiring.analytics.cli.HmacKeyRetirementAuthorizationMain $action"
  ) || return
  [[ -z "$(find "$host_scratch" -mindepth 1 \( -type f -o -type l \) -print -quit)" ]] || die 'Host Spark spill files remain after the operator resource closed.'
  printf 'Host operator Spark scratch is clean after resource release.\n'
}

assert_quiescent() {
  [[ -z "$(docker ps -q --filter "volume=$volume_name")" ]] || die 'A writer still has the rotation volume mounted.'
}

switch_writer() {
  compose stop analytics-batch analytics-erasure-worker >/dev/null
  assert_quiescent
  # This scoped ownership change revokes the old nonroot image's storage write access.
  docker run --rm --user 0:0 --network none \
    --mount "type=volume,src=$volume_name,dst=/retirement-volume" \
    --entrypoint /bin/sh "$new_image" -ec \
    "chown -R $new_uid:$new_uid /retirement-volume"
  assert_quiescent
}

prepare_old_writer_volume() {
  HIRING_HMAC_ROTATION_WRITER_IMAGE="$old_image" compose run --rm --no-deps \
    --user 0:0 --entrypoint /bin/sh analytics-batch -ec \
    "chown -R $old_uid:$old_uid '$data_dir'"
  assert_quiescent
}

volume_owner() {
  if ! docker volume inspect "$volume_name" >/dev/null 2>&1; then
    printf 'uncreated'
  else
    docker run --rm --network none --user 0:0 \
      --mount "type=volume,src=$volume_name,dst=/retirement-volume,readonly" \
      --entrypoint stat "$new_image" -c %u /retirement-volume
  fi
}

declare -A stage_status_cache=()

stage_flag() {
  local mode="$1" flag="$2" value
  [[ "$flag" =~ ^(OLD_ROW_STAGED|OLD_EVENT_PUBLISHED|NEW_CONTROL_STAGED|NEW_EVENT_PUBLISHED)$ ]] || die 'Invalid fixture stage flag.'
  if [[ ! -v stage_status_cache[$mode] ]]; then
    capture stage-status.log "${mode}_fixture" stage-status
    stage_status_cache[$mode]="$(cat "$log_dir/stage-status.log")"
  fi
  value="$(printf '%s\n' "${stage_status_cache[$mode]}" | sed -nE "s/^(\[info\] )?$flag=//p" | tail -n 1)"
  [[ "$value" == true || "$value" == false ]] || die 'Fixture stage status is unavailable.'
  [[ "$value" == true ]]
}

prepared_at() {
  local observed
  observed="$(compose exec -T mongodb mongosh --quiet --eval \
    "const d=db.getSiblingDB('$database'); const r=d.analytics_hmac_key_retirement_preparations.find({keyId:'rotation-old-$nonce'}).limit(2).toArray(); print(r.length===0?'ABSENT':r.length===1?r[0].capturedAt.toISOString():'MULTIPLE')" | tail -n 1)"
  [[ "$observed" == ABSENT || "$observed" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}([.][0-9]{3})?Z$ ]] || die 'Retirement preparation state is ambiguous.'
  printf '%s' "$observed"
}

initialize_empty_mongo_contract() {
  # This nonce database has no hiring application or outbox writers. The empty collections
  # and completed subject-reference ledger represent that exact synthetic fixture state.
  compose exec -T mongodb mongosh --quiet --eval "
    const d=db.getSiblingDB('$database');
    const required=['analytics_report_snapshots','analytics_report_runs','analytics_report_control',
      'analytics_erasure_requests','analytics_erasure_completions','analytics_erasure_delta_files',
      'event_outbox','hiring_migration_ledger','outbox_subject_fences'];
    if (d.event_outbox.countDocuments({}) !== 0) throw new Error('isolated outbox is not empty');
    const names=new Set(d.getCollectionNames());
    for (const name of required) if (!names.has(name)) d.createCollection(name);
    const ledger=d.hiring_migration_ledger.findOne({_id:'003_event_outbox_subject_references'});
    if (!ledger) d.hiring_migration_ledger.insertOne({
      _id:'003_event_outbox_subject_references',state:'Complete',
      completedAt:new Date(),scope:'empty isolated HMAC rotation fixture'});
    else if (ledger.state !== 'Complete') throw new Error('subject-reference ledger is incomplete');
    print('Synthetic empty Mongo contract is ready');
  "
}

start_kafka_acl() {
  compose up -d kafka-acl-init
  local container result
  container="$(compose ps -a -q kafka-acl-init)"
  [[ -n "$container" ]] || die 'Kafka ACL initializer container is missing.'
  result="$(docker wait "$container")"
  [[ "$result" == 0 ]] || die 'Kafka ACL initializer did not complete successfully.'
  # The guarded local lineage proof reads the broker cluster ID. Grant this
  # metadata permission only on the isolated proof broker.
  compose run --rm --no-deps --entrypoint /bin/bash kafka-acl-init -ec '
    umask 077
    trap '\''rm -f /tmp/rotation-admin.properties'\'' EXIT
    cat > /tmp/rotation-admin.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="broker" password="${KAFKA_BROKER_PASSWORD}";
EOF
    /opt/kafka/bin/kafka-acls.sh --bootstrap-server kafka:9092 \
      --command-config /tmp/rotation-admin.properties --add \
      --allow-principal User:analytics_reader --operation DESCRIBE --cluster
  '
}

persist_preparation() {
  local observed
  observed="$(prepared_at)"
  if [[ "$observed" == ABSENT ]]; then
    capture prepare.log host_authorization prepare
    observed="$(prepared_at)"
    [[ "$observed" != ABSENT ]] || die 'Guarded retirement preparation was not durably persisted.'
  fi
  staged_at="${observed%%.*}"
  staged_at="${staged_at%Z}Z"
  save_state
}

start_proof() {
  load_credentials
  if [[ -e "$state_file" || -L "$state_file" ]]; then
    load_state
    [[ -z "$staged_at" ]] || { printf 'Rotation is already prepared in %s.\n' "$project"; return; }
  else
    local_directory "$repo_root/.local/config" true
    local_directory "$repo_root/.local/logs" true
    local_directory "$data_parent" true
    git check-ignore -q "$data_parent/probe" || die 'Rotation data path is not ignored by Git.'
    if [[ "$isolated_test" != true ]]; then nonce="$(openssl rand -hex 8)"; fi
    mongo_port="$(free_port)"; kafka_port="$(free_port)"
    [[ "$mongo_port" != "$kafka_port" ]] || kafka_port="$(free_port)"
    old_secret="$(openssl rand -base64 32 | tr -d '\n')"
    new_secret="$(openssl rand -base64 32 | tr -d '\n')"
    old_image_id= new_image_id= staged_at= tail_at= cutover_at= authorized_at= restarted_at=
    configure_names
    local_directory "$data_dir" true
    [[ "$(cd "$data_dir" && pwd -P)" == "$data_dir" ]] || die 'Rotation data path is not canonical.'
    # A host read ACL lets the host-only authorization inspect Delta without giving the old UID write access.
    setfacl -m "u:$(id -u):r-x,d:u:$(id -u):r-x" "$data_dir"
    save_state
  fi
  # Before durable preparation, rebuild from the current reviewed source on resume.
  if [[ -z "$staged_at" && "$isolated_test_resume" != true ]]; then build_images; else verify_images; fi
  capture infrastructure.log compose up -d --wait mongodb kafka
  capture kafka-acl-init.log start_kafka_acl
  if [[ "$isolated_test" != true ]]; then
    capture empty-mongo-contract.log initialize_empty_mongo_contract
  fi
  local owner
  owner="$(volume_owner)"
  if [[ "$owner" == uncreated ]]; then
    capture old-writer-volume.log prepare_old_writer_volume
    owner="$old_uid"
  fi
  if [[ "$isolated_test" == true ]]; then
    capture hiring-operational-bootstrap.log host_hiring_bootstrap
  fi
  if [[ "$owner" == "$old_uid" ]]; then
    if ! stage_flag old OLD_ROW_STAGED; then
      capture stage-old.log old_fixture stage-old
      unset 'stage_status_cache[old]'
    fi
    if ! stage_flag old OLD_EVENT_PUBLISHED; then
      capture publish-old-event.log old_fixture publish-old-event
      unset 'stage_status_cache[old]'
    fi
    switch_writer
  elif [[ "$owner" != "$new_uid" ]]; then
    die 'Rotation volume owner is neither the old nor new isolated writer.'
  fi
  stage_flag new OLD_ROW_STAGED || die 'Old Silver fixture is missing after writer exclusion.'
  stage_flag new OLD_EVENT_PUBLISHED || die 'Old Kafka fixture is missing after writer exclusion.'
  persist_preparation
  if [[ "$isolated_test" == true ]]; then
    printf 'Old-key fixture staged and writer excluded in %s. Actual isolated Kafka/data retention is 60 seconds; calendar ages are simulated +32 days.\n' "$project"
    printf 'The isolated runner continues through observed Kafka offsets, physical Delta cleanup and guarded retirement.\n'
  else
    printf 'Old-key fixture staged and writer excluded in %s. Keep its Kafka volume through the real 168-hour horizon.\n' "$project"
    printf 'After the old Silver row reaches 30 days, run %s maintain.\n' "$0"
  fi
}

retry_prepare_proof() {
  load_credentials
  load_state
  [[ -z "$staged_at" ]] || die 'Rotation preparation is already recorded.'
  verify_images
  [[ "$(volume_owner)" == "$new_uid" ]] || die 'Old writer exclusion has not been staged.'
  persist_preparation
  printf 'Guarded preparation persisted for %s. Keep its Kafka volume through the real 168-hour horizon.\n' "$project"
}

status_proof() {
  load_credentials
  load_state
  printf 'Project: %s\n' "$project"
  printf 'Prepared at: %s\n' "${staged_at:-pending}"
  printf 'Kafka rollover tail at: %s\n' "${tail_at:-pending}"
  printf 'New-key control and cleanup at: %s\n' "${cutover_at:-pending}"
  printf 'Authorized at: %s\n' "${authorized_at:-pending}"
  printf 'Restarted without old key at: %s\n' "${restarted_at:-pending}"
  compose ps -a
  printf 'Elapsed horizons and broker earliest offsets are checked by the guarded prepare/authorize commands.\n'
}

kafka_admin() {
  compose run --rm -T --no-deps --entrypoint /bin/bash kafka-acl-init -ec '
    umask 077
    trap '\''rm -f /tmp/rotation-admin.properties'\'' EXIT
    cat > /tmp/rotation-admin.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="broker" password="${KAFKA_BROKER_PASSWORD}";
EOF
    command="$1"; shift
    "/opt/kafka/bin/$command" --bootstrap-server kafka:9092 \
      --command-config /tmp/rotation-admin.properties "$@"
  ' rotation-admin "$@"
}

kafka_offset() {
  local when="$1" output value
  output="$(kafka_admin kafka-get-offsets.sh --topic "$topic" --time "$when")"
  value="$(printf '%s\n' "$output" | awk -F: -v name="$topic" \
    '$1 == name && $2 == "0" && $3 ~ /^[0-9]+$/ {print $3}')"
  [[ "$value" =~ ^[0-9]+$ ]] || die "Kafka partition 0 $when offset is unavailable."
  printf '%s' "$value"
}

prepared_tail_barrier() {
  local observed
  observed="$(compose exec -T mongodb mongosh --quiet --eval "
    const d=db.getSiblingDB('$database');
    const r=d.analytics_hmac_key_retirement_preparations.find({keyId:'rotation-old-$nonce'}).limit(2).toArray();
    print(r.length===1 && r[0].topic==='$topic' && r[0].kafkaVolumeName==='${project}_hmac-rotation-kafka' &&
      r[0].partitions.length===1 && r[0].partitions[0].number===0 &&
      Number(r[0].partitions[0].endOffsetExclusive)===1 ? 'BARRIER_ONE' : 'INVALID_BARRIER');
  " | tail -n 1)"
  [[ "$observed" == BARRIER_ONE ]] || die 'Persisted old-event Kafka barrier or volume lineage is not the expected single partition at offset 1.'
}

configure_rollover_segment() {
  local before after
  before="$(kafka_admin kafka-configs.sh --entity-type topics --entity-name "$topic" --describe --all)"
  printf '%s\n' "$before" | rg -q '(^|[[:space:]])retention\.ms=604800000([[:space:]]|$)' ||
    die 'Effective topic retention is not the unchanged 168 hours.'
  kafka_admin kafka-configs.sh --entity-type topics --entity-name "$topic" \
    --alter --add-config segment.bytes=16384
  after="$(kafka_admin kafka-configs.sh --entity-type topics --entity-name "$topic" --describe --all)"
  printf '%s\n' "$after" | rg -q '(^|[[:space:]])retention\.ms=604800000([[:space:]]|$)' ||
    die 'Effective topic retention changed during rollover configuration.'
  printf '%s\n' "$after" | rg -q '(^|[[:space:]])segment\.bytes=16384([[:space:]]|$)' ||
    die 'Isolated topic segment size was not applied.'
  printf 'Effective retention.ms=604800000; topic segment.bytes=16384.\n'
}

old_segment_closed() {
  local segments segment found_old=false found_new=false
  segments="$(compose exec -T kafka find "/var/lib/kafka/data/$topic-0" \
    -maxdepth 1 -type f -name '[0-9]*.log' -print)"
  while IFS= read -r segment; do
    segment="${segment##*/}"
    [[ "$segment" =~ ^[0-9]{20}\.log$ ]] || continue
    if [[ "$segment" == 00000000000000000000.log ]]; then found_old=true
    else found_new=true
    fi
  done <<< "$segments"
  [[ "$found_old" == true ]] || die 'The original Kafka segment is absent; preserve the elapsed-retention evidence.'
  [[ "$found_new" == true ]]
}

publish_rollover_tail() {
  local padding batch number
  printf -v padding '%1024s' ''
  padding="${padding// /x}"
  batch="$(openssl rand -hex 8)"
  for ((number=0; number<32; number++)); do
    printf 'hmac-retention-%s-%02d:{"kind":"hmac-retention-rollover","batch":"%s","ordinal":%d,"padding":"%s"}\n' \
      "$batch" "$number" "$batch" "$number" "$padding"
  done | compose run --rm -T --no-deps -e KAFKA_PUBLISHER_V2_PASSWORD \
    --entrypoint /bin/bash kafka-acl-init -ec '
      umask 077
      trap '\''rm -f /tmp/rotation-publisher.properties'\'' EXIT
      cat > /tmp/rotation-publisher.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="hiring_publisher_v2" password="${KAFKA_PUBLISHER_V2_PASSWORD}";
EOF
      /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 \
        --producer.config /tmp/rotation-publisher.properties --topic "$KAFKA_TOPIC" \
        --sync --property parse.key=true --property key.separator=: \
        --producer-property acks=all --producer-property enable.idempotence=true \
        --producer-property compression.type=none
    '
}

tail_proof() {
  load_credentials; load_state
  [[ -n "$staged_at" && -z "$authorized_at" ]] || die 'Rollover tail requires a prepared, not yet authorized fixture.'
  verify_images
  assert_quiescent
  prepared_tail_barrier
  [[ "$(compose exec -T kafka printenv KAFKA_LOG_RETENTION_HOURS | tr -d '\r')" == 168 ]] ||
    die 'Isolated broker log retention is not 168 hours.'
  local before after earliest
  before="$(kafka_offset latest)"
  earliest="$(kafka_offset earliest)"
  (( before >= 1 )) || die 'The old event is missing from the prepared Kafka topic.'
  (( earliest <= 1 )) || die 'The old-event barrier has already passed; no rollover tail is needed.'
  capture tail-config.log configure_rollover_segment
  if [[ -z "$tail_at" ]]; then
    if (( before == 1 )); then
      capture tail-publish.log publish_rollover_tail
      after="$(kafka_offset latest)"
      (( after == 33 )) || die 'Kafka tail did not advance the isolated partition by exactly 32 records.'
    elif (( before == 33 )); then
      # The previous publish may have completed before local state was saved.
      after="$before"
    else
      die 'Unrecorded Kafka tail has an unexpected offset; preserve the proof for inspection.'
    fi
    old_segment_closed || die 'Bounded tail did not close the old-event Kafka segment.'
  else
    old_segment_closed || die 'Recorded rollover tail has no closed old-event segment.'
    after="$before"
    (( after == 33 )) || die 'Recorded Kafka tail no longer has the exact bounded latest offset.'
  fi
  earliest="$(kafka_offset earliest)"
  (( earliest <= 1 && after > 1 )) || die 'Kafka offset metadata is inconsistent with the captured barrier.'
  if [[ -z "$tail_at" ]]; then
    tail_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    save_state
  fi
  printf 'Old-event segment closed; Kafka partition 0 earliest=%s latest=%s, retention.ms=604800000.\n' "$earliest" "$after"
  printf 'Keep the isolated broker and volumes through the real 168-hour retention horizon.\n'
}

maintain_proof() {
  load_credentials; load_state
  [[ -n "$staged_at" && -z "$cutover_at" ]] || die 'Maintenance requires a prepared fixture and no prior cutover.'
  verify_images
  assert_quiescent
  if ! stage_flag new NEW_CONTROL_STAGED; then capture seed-new-control.log new_fixture seed-new-control; fi
  capture maintain.log new_fixture maintain
  cutover_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  save_state
  if [[ "$isolated_test" == true ]]; then
    printf 'New-key control and calendar-aged old-row cleanup recorded. Keep isolated volumes through the actual 60-second Delta data horizon; UTC log calendar is simulated +32 days.\n'
  else
    printf 'New-key control and elapsed old-row cleanup recorded. Preserve Kafka and Delta volumes for the real log horizon.\n'
  fi
}

authorize_proof() {
  load_credentials; load_state
  [[ -n "$cutover_at" && -z "$authorized_at" ]] || die 'Authorization requires completed cutover and no prior authorization.'
  assert_quiescent
  capture maintain-final.log new_fixture maintain
  capture authorize.log host_authorization authorize
  authorized_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  save_state
  printf 'Guarded retirement authorization persisted. Run restart to prove omission of the old key.\n'
}

host_hiring_bootstrap() {
  [[ "$isolated_test" == true ]] || die 'Standard hiring bootstrap requires the isolated nonce fixture.'
  verify_images
  assert_quiescent
  if [[ -n "${HIRING_HMAC_ROTATION_ROOT_CLASSPATH:-}" ]]; then
    bash "$repo_root/scripts/bootstrap-isolated-hmac-hiring-runtime.sh" "$nonce" "$HIRING_HMAC_ROTATION_ROOT_CLASSPATH" || return
  else
    bash "$repo_root/scripts/bootstrap-isolated-hmac-hiring-runtime.sh" "$nonce" || return
  fi
  assert_quiescent
}

host_post_authorization_control() {
  local partition="$1" start="$2" end="$3"
  local host_scratch="$repo_root/.local/data/analytics/spark-temp/hmac-key-retirement-isolated-$nonce"
  [[ "$isolated_test" == true && -n "$authorized_at" && -z "$restarted_at" ]] ||
    die 'Host control refresh requires an authorized isolated fixture awaiting restart.'
  verify_images
  assert_quiescent
  local_directory "$host_scratch"
  [[ "$(stat -c '%u:%a' "$host_scratch")" == "$(id -u):700" ]] ||
    die 'Host control configuration requires its existing private operator scratch directory.'
  (
    unset JAVA_TOOL_OPTIONS
    export MONGODB_URI="mongodb://127.0.0.1:$mongo_port/?directConnection=true"
    export MONGODB_DATABASE="$database"
    export ANALYTICS_LAKEHOUSE_ROOT="$(python3 "$repo_root/scripts/canonical-local-file-uri.py" "$data_dir/lakehouse")"
    export ANALYTICS_BOOTSTRAP_SERVERS="127.0.0.1:$kafka_port"
    export ANALYTICS_KAFKA_USERNAME=hiring_publisher_v2 ANALYTICS_KAFKA_PASSWORD="$KAFKA_PUBLISHER_V2_PASSWORD"
    export ANALYTICS_KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT ANALYTICS_KAFKA_ALLOW_PLAINTEXT=true
    export ANALYTICS_TOPIC="$topic" ANALYTICS_PARTITION="$partition"
    export ANALYTICS_START_OFFSET="$start" ANALYTICS_END_OFFSET_EXCLUSIVE="$end"
    export ANALYTICS_RUN_ID="$(python3 -c 'import uuid; print(uuid.uuid4())')"
    export SPARK_MASTER='local[2]' ANALYTICS_SPARK_LOCAL_DIRECTORY="$host_scratch"
    export HIRING_HMAC_ROTATION_OLD_KEY_ID="rotation-old-$nonce"
    export HIRING_HMAC_ROTATION_NEW_KEY_ID="rotation-new-$nonce"
    export HIRING_HMAC_ROTATION_OLD_SECRET_BASE64="$old_secret"
    export HIRING_HMAC_ROTATION_NEW_SECRET_BASE64="$new_secret"
    export HIRING_ANALYTICS_HMAC_KEY_ID="rotation-new-$nonce" HIRING_ANALYTICS_HMAC_SECRET_BASE64="$new_secret"
    export HIRING_ANALYTICS_HMAC_PREVIOUS_KEY_ID="rotation-old-$nonce"
    export HIRING_ANALYTICS_HMAC_PREVIOUS_SECRET_BASE64="$old_secret"
    export SPARK_LOCAL_IP=127.0.0.1
    # This test CLI owns only Mongo control evidence and Kafka publication; it never starts Spark
    # or writes Delta, and executes fresh test bytecode without rebinding the authorized images.
    cd "$repo_root/analytics"
    sbt -java-home /usr/lib/jvm/java-17-openjdk-amd64 \
      'Test / runMain com.example.hiring.analytics.cli.PostAuthorizationHmacControlEventMain'
  )
}

isolated_mount_equivalence() {
  [[ "$isolated_test" == true ]] || die 'Mount equivalence probe requires the isolated fixture.'
  verify_images
  assert_quiescent
  [[ "$(docker volume inspect --format '{{ index .Options "device" }}' "$volume_name")" == "$data_dir" ]] ||
    die 'Isolated volume device does not match its nonce-owned root.'
  # Read-only probe in the exact authorized new image: both aliases must resolve to the
  # same physical parent/lakehouse, and the same scratch directory if present.
  compose run --rm --no-deps --entrypoint /bin/sh analytics-batch -ec '
    test "$ANALYTICS_SPARK_LOCAL_DIRECTORY" = /var/lib/hiring-analytics/spark-temp
    host_root="$HIRING_HMAC_ROTATION_DATA_DIR/lakehouse"
    host_volume="${host_root%/lakehouse}"
    for directory in "$host_volume" /var/lib/hiring-analytics "$host_root" /var/lib/hiring-analytics/lakehouse; do
      test -d "$directory"
      test ! -L "$directory"
    done
    host_parent_id="$(stat -c "%d:%i" "$host_volume")"
    alias_parent_id="$(stat -c "%d:%i" /var/lib/hiring-analytics)"
    test -n "$host_parent_id"
    test "$host_parent_id" = "$alias_parent_id"
    host_lakehouse_id="$(stat -c "%d:%i" "$host_root")"
    alias_lakehouse_id="$(stat -c "%d:%i" /var/lib/hiring-analytics/lakehouse)"
    test -n "$host_lakehouse_id"
    test "$host_lakehouse_id" = "$alias_lakehouse_id"
    if test -e "$host_volume/spark-temp" || test -L "$host_volume/spark-temp" ||
      test -e /var/lib/hiring-analytics/spark-temp || test -L /var/lib/hiring-analytics/spark-temp; then
      for directory in "$host_volume/spark-temp" /var/lib/hiring-analytics/spark-temp; do
        test -d "$directory"
        test ! -L "$directory"
      done
      host_scratch_id="$(stat -c "%d:%i" "$host_volume/spark-temp")"
      alias_scratch_id="$(stat -c "%d:%i" /var/lib/hiring-analytics/spark-temp)"
      test -n "$host_scratch_id"
      test "$host_scratch_id" = "$alias_scratch_id"
      scratch_state=PRESENT
    else
      scratch_state=ABSENT
    fi
    printf "ISOLATED_MOUNT_EQUIVALENCE lakehouse=%s alias=/var/lib/hiring-analytics/lakehouse parentIdentity=%s lakehouseIdentity=%s scratch=%s/spark-temp alias=/var/lib/hiring-analytics/spark-temp scratchState=%s\n" "$host_root" "$host_parent_id" "$host_lakehouse_id" "$host_volume" "$scratch_state"
  '
  assert_quiescent
}

restart_proof() {
  load_credentials; load_state
  [[ -n "$authorized_at" && -z "$restarted_at" ]] || die 'Restart requires a persisted authorization.'
  verify_images
  assert_quiescent
  if [[ "$isolated_test" == true ]]; then
    capture hiring-operational-bootstrap-restart.log host_hiring_bootstrap
    capture isolated-mount-equivalence.log isolated_mount_equivalence
    # The captured old barrier already passed before durable authorization. This isolated-only
    # window preserves the subsequent control event across fresh JVM compilation and startup.
    capture new-control-retention.log kafka_admin kafka-configs.sh --entity-type topics --entity-name "$topic" \
      --alter --add-config retention.ms=86400000
    capture new-control-retention-verified.log kafka_admin kafka-configs.sh --entity-type topics --entity-name "$topic" \
      --describe --all
    rg -q '(^|[[:space:]])retention\.ms=86400000([[:space:]]|$)' "$log_dir/new-control-retention-verified.log" ||
      die 'Isolated post-authorization control retention was not applied.'
  fi
  if ! stage_flag new NEW_EVENT_PUBLISHED; then capture publish-new-event.log new_fixture publish-new-event; fi
  if [[ "$isolated_test" == true ]]; then
    [[ ! -L "$log_dir/original-new-event-range.log" && ! -L "$log_dir/new-event-range.log" ]] ||
      die 'Refusing a symlink in the post-authorization control-range evidence.'
  fi
  if [[ "$isolated_test" == true && -e "$log_dir/new-event-range.log" &&
    ! -e "$log_dir/original-new-event-range.log" ]]; then
    [[ ! -L "$log_dir/new-event-range.log" && ! -L "$log_dir/original-new-event-range.log" ]] ||
      die 'Refusing a symlink in the original control-range evidence.'
    cp -- "$log_dir/new-event-range.log" "$log_dir/original-new-event-range.log"
  fi
  capture new-event-range.log new_fixture new-event-range
  local reference event_topic partition start_offset end_offset remainder
  reference="$(sed -nE 's/^(\[info\] )?NEW_EVENT_RANGE=//p' "$log_dir/new-event-range.log" | tail -n 1)"
  IFS=: read -r event_topic partition start_offset end_offset remainder <<< "$reference"
  [[ "$event_topic" == "$topic" && "$partition" =~ ^[0-9]+$ &&
    "$start_offset" =~ ^[0-9]+$ && "$end_offset" =~ ^[0-9]+$ &&
    "$end_offset" -eq $((start_offset + 1)) && -z "$remainder" ]] || die 'New event range is malformed.'
  if [[ "$isolated_test" == true ]]; then
    [[ "$partition" == 0 ]] || die 'Isolated control must remain in its captured partition zero.'
    if [[ ! -e "$log_dir/original-new-event-range.log" && ! -L "$log_dir/original-new-event-range.log" ]]; then
      cp -- "$log_dir/new-event-range.log" "$log_dir/original-new-event-range.log"
    fi
    local earliest latest refresh_log
    earliest="$(kafka_offset earliest)"; latest="$(kafka_offset latest)"
    (( end_offset <= latest )) || die 'Recorded control range exceeds the actual Kafka latest offset.'
    if (( start_offset < earliest )); then
      printf 'Original post-authorization control expired: start=%s end=%s earliest=%s latest=%s.\n' \
        "$start_offset" "$end_offset" "$earliest" "$latest"
      refresh_log="post-authorization-control-$(python3 -c 'import uuid; print(uuid.uuid4())').log"
      capture "$refresh_log" host_post_authorization_control "$partition" "$start_offset" "$end_offset"
      reference="$(sed -nE 's/^(\[info\] )?NEW_EVENT_RANGE=//p' "$log_dir/$refresh_log" | tail -n 1)"
      IFS=: read -r event_topic partition start_offset end_offset remainder <<< "$reference"
      [[ "$event_topic" == "$topic" && "$partition" == 0 &&
        "$start_offset" =~ ^[0-9]+$ && "$end_offset" =~ ^[0-9]+$ &&
        "$end_offset" -eq $((start_offset + 1)) && -z "$remainder" ]] || die 'Refreshed control range is malformed.'
      earliest="$(kafka_offset earliest)"; latest="$(kafka_offset latest)"
    fi
    (( earliest <= start_offset && end_offset <= latest )) || die 'Post-authorization control is not currently available.'
    printf 'Verified post-authorization control: start=%s end=%s earliest=%s latest=%s.\n' \
      "$start_offset" "$end_offset" "$earliest" "$latest"
  fi
  local run_id
  run_id="$(python3 -c 'import uuid; print(uuid.uuid4())')"
  if [[ "$isolated_test" == true ]]; then
    HIRING_ANALYTICS_HMAC_KEY_ID="rotation-old-$nonce" \
       HIRING_ANALYTICS_HMAC_SECRET_BASE64="$old_secret" \
       HIRING_ANALYTICS_HMAC_PREVIOUS_KEY_ID="rotation-new-$nonce" \
       HIRING_ANALYTICS_HMAC_PREVIOUS_SECRET_BASE64="$new_secret" \
       ANALYTICS_RUN_ID="$run_id" ANALYTICS_PARTITION="$partition" \
       ANALYTICS_START_OFFSET="$start_offset" ANALYTICS_END_OFFSET_EXCLUSIVE="$end_offset" \
       compose run --rm --no-deps --entrypoint sbt analytics-batch \
       'Test / runMain com.example.hiring.analytics.cli.RetiredHmacPrimaryStartupProofMain' > "$log_dir/retired-primary-rejected.log" 2>&1 ||
      die 'Retired old-primary startup did not produce the required rejection.'
    rg -q 'RETIRED_PRIMARY_STARTUP_REJECTED' "$log_dir/retired-primary-rejected.log" ||
      die 'Old-primary startup failed without the required retirement rejection.'
    run_id="$(python3 -c 'import uuid; print(uuid.uuid4())')"
    earliest="$(kafka_offset earliest)"; latest="$(kafka_offset latest)"
    (( earliest <= start_offset && end_offset <= latest )) ||
      die 'Post-authorization control expired before the new-key-only batch restart.'
  fi
  HIRING_ANALYTICS_HMAC_KEY_ID="rotation-new-$nonce" \
  HIRING_ANALYTICS_HMAC_SECRET_BASE64="$new_secret" \
  HIRING_ANALYTICS_HMAC_PREVIOUS_KEY_ID='' \
  HIRING_ANALYTICS_HMAC_PREVIOUS_SECRET_BASE64='' \
  ANALYTICS_RUN_ID="$run_id" ANALYTICS_PARTITION="$partition" \
  ANALYTICS_START_OFFSET="$start_offset" ANALYTICS_END_OFFSET_EXCLUSIVE="$end_offset" \
    capture restart.log compose run --rm --no-deps --entrypoint sbt analytics-batch \
      'runMain com.example.hiring.analytics.cli.HiringAnalyticsBatchMain'
  restarted_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  old_secret=''
  save_state
  printf 'New-key-only batch restart completed; preserved proof volumes and state remain available.\n'
}

isolated_test_proof() {
  printf 'Isolated fixture nonce=%s; actual Kafka and Delta data retention=60 seconds; calendar ages simulated +32 days.\n' "$nonce"
  start_proof
  verify_images
  assert_quiescent
  if [[ -z "$cutover_at" ]]; then
    prepared_tail_barrier
    capture isolated-topic-retention.log kafka_admin kafka-configs.sh --entity-type topics --entity-name "$topic" \
      --alter --add-config retention.ms=60000,segment.ms=1000,segment.bytes=16384,file.delete.delay.ms=1000
    capture isolated-topic-config.log kafka_admin kafka-configs.sh --entity-type topics --entity-name "$topic" --describe --all
    rg -q 'retention.ms=60000([[:space:]]|$)' "$log_dir/isolated-topic-config.log" || die 'Isolated topic retention was not applied.'
    local latest
    latest="$(kafka_offset latest)"
    if [[ -z "$tail_at" ]]; then
      if (( latest == 1 )); then capture isolated-rollover.log publish_rollover_tail
      elif (( latest != 33 )); then die 'Isolated rollover has an unexpected durable latest offset.'
      fi
      [[ "$(kafka_offset latest)" == 33 ]] || die 'Isolated rollover did not retain its exact latest offset.'
      tail_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
      save_state
    fi
    local earliest attempt
    for ((attempt=0; attempt<40; attempt++)); do
      earliest="$(kafka_offset earliest)"
      if (( earliest >= 1 )); then break; fi
      printf 'Waiting for actual isolated Kafka retention: earliest=%s, barrier=1.\n' "$earliest"
      sleep 10
    done
    (( earliest >= 1 )) || die 'Actual Kafka earliest offset did not pass the captured barrier.'
    printf 'Actual Kafka retention passed: earliest=%s barrier=1; preserved same broker/topic/volume lineage.\n' "$earliest"
    maintain_proof
  fi
  # Delta VACUUM uses its real system clock, independently of the substituted calendar.
  local deadline remaining
  if [[ -z "$authorized_at" ]]; then
    deadline=$(( $(date -u -d "$cutover_at" +%s) + 65 ))
    while (( $(date -u +%s) < deadline )); do
      remaining=$(( deadline - $(date -u +%s) ))
      printf 'Waiting for actual Delta data-file retention: %s seconds remain.\n' "$remaining"
      if (( remaining > 10 )); then sleep 10; else sleep "$remaining"; fi
    done
    authorize_proof
  fi
  if [[ -z "$restarted_at" ]]; then restart_proof; fi
  printf 'ISOLATED_HMAC_RETIREMENT_PASS nonce=%s state=%s logs=%s\n' "$nonce" "$state_file" "$log_dir"
  printf 'Actual: guarded writer access denial, broker offsets/lineage, physical Delta files/logs, durable authorization, old-primary rejection/new-only restart. Simulated: Silver/report calendar horizons and Delta log calendar.\n'
}

case "${1:-}" in
  isolated-test) isolated_test_proof ;;
  start) start_proof ;; prepare) retry_prepare_proof ;; status) status_proof ;; tail) tail_proof ;;
  maintain) maintain_proof ;;
  authorize) authorize_proof ;; restart) restart_proof ;;
  *) printf 'Usage: %s {start|prepare|status|tail|maintain|authorize|restart|isolated-test [nonce]}\n' "$0" >&2; exit 2 ;;
esac
