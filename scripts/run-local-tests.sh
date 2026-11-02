#!/usr/bin/env bash
# Runs the local test suites against this workspace's isolated test services (compose.test.yaml).
# Contract: see "Operator scripts" in docs/engineering-quality.md. Run with --help for usage and exit codes.
set -Eeuo pipefail
umask 077
# Test services must only ever run on the local default Docker engine.
unset DOCKER_HOST DOCKER_CONTEXT DOCKER_CONFIG DOCKER_TLS_VERIFY DOCKER_CERT_PATH COMPOSE_FILE COMPOSE_PROJECT_NAME COMPOSE_PROFILES COMPOSE_ENV_FILES
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$repo_root"
# shellcheck source=scripts/operator-lib.sh
source "$repo_root/scripts/operator-lib.sh"

exec 3>&1 4>&2 # terminal descriptors, restored by the traps (see on_interrupt)
mode="${1:-}"
phase_limit="${HIRING_TEST_TIMEOUT_SECONDS:-14400}"
lock_wait="${HIRING_TEST_LOCK_WAIT_SECONDS:-300}"
compose_timeout="${COMPOSE_TIMEOUT_SECONDS:-300}"
sbt_phase_active=0
run_log_display=".local/logs/test-services/run.log"
log_root="$repo_root/.local/logs/test-services"
current_phase="preparing"
result_done=0
err_line=""
err_cmd=""

help_text() {
  cat <<'HELP'
Usage: scripts/run-local-tests.sh MODE [suite-glob ...]
       scripts/run-local-tests.sh --help

Starts (or reuses) this workspace's isolated test services, then runs sbt against them.
Every sbt invocation is bounded; its output goes to .local/logs/test-services/run.log and a status line
(phase, elapsed time, last log line) is printed every 10 s.

Modes:
  start                      start and verify the isolated test services only (up to ~5 min; first start may pull images)
  test [suite-glob ...]      root build. No glob: unit + integration under JaCoCo with the 85% merged coverage gate.
                             With globs: all unit tests (about 1 min), then only the matching integration suites.
  analytics [suite-glob ...] analytics build. No glob: unit + integration under JaCoCo. With globs: matching integration suites only.

Recorded durations: a root integration run took 566 s plus units on 2026-10-08 (docs/specs/candidate-discovery-and-interview-integrity.md);
a whole scripts/check-local.sh (both builds, formatting, coverage) took 1h48m-2h18m. Full test/analytics runs therefore
need well over an hour; the default per-invocation limit is 4 h.
  interview                  the three interview Kafka integration suites (root build)
  retention                  KafkaTopicRetentionCheck (shortened Kafka topic retention)
  recovery                   AccountDeletionRecoveryIntegrationSpec (compiles the API classpath first)
  status                     show the test services (docker compose ps)
  stop                       remove only this workspace's test services (needs human approval for agents)

Suite globs may contain letters, digits, '_', '.', '*' and '$' (for example '*SafeDiagnosticsSpec').

Environment:
  HIRING_TEST_TIMEOUT_SECONDS    limit for each sbt invocation, 60-86400 (default 14400 = 4 h)
  HIRING_TEST_LOCK_WAIT_SECONDS  how long to wait for another run's lock, 0-3600 (default 300)
  COMPOSE_TIMEOUT_SECONDS        limit for each docker compose call, 30-3600 (default 300)
  HIRING_TEST_JAVA_HOME          JDK passed to sbt with -java-home

Exit codes: 0 PASS, 1 FAIL, 2 usage error or precondition not met (BLOCKED), 130 interrupted (Ctrl+C, SIGTERM or SIGHUP).
The last line is "RESULT: PASS|FAIL|BLOCKED ..." with counts, duration and the log path.

Examples:
  scripts/run-local-tests.sh test '*SafeDiagnosticsSpec'
  scripts/run-local-tests.sh analytics '*HiringAnalyticsStreamingBoundaryRecoveryIntegrationSpec'
  scripts/run-local-tests.sh status
HELP
}

# finish RESULT EXIT_CODE DETAIL: the single summary line (stdout for PASS, stderr otherwise).
finish() {
  local result="$1" code="$2" detail="$3" line
  line="RESULT: $result mode=${mode:-none} $detail duration=$(op_elapsed)"
  result_done=1
  if [[ "$result" == PASS ]]; then printf '%s\n' "$line"; else printf '%s\n' "$line" >&2; fi
  exit "$code"
}

blocked() { # message, next action
  printf 'run-local-tests: %s\n' "$1" >&2
  printf 'run-local-tests: next: %s\n' "${2:-fix the precondition above and run again}" >&2
  finish BLOCKED 2 "reason=precondition"
}

usage_error() {
  printf 'run-local-tests: %s\n' "$1" >&2
  printf 'run-local-tests: run scripts/run-local-tests.sh --help for usage\n' >&2
  finish BLOCKED 2 "reason=usage"
}

on_error() { err_line="${BASH_LINENO[0]:-?}"; err_cmd="$BASH_COMMAND"; }
on_exit() {
  local code=$?
  exec 1>&3 2>&4
  op_ticker_stop
  if (( code != 0 && result_done == 0 )); then
    printf 'run-local-tests: unexpected failure in phase "%s" at line %s (%s), exit %s\n' "$current_phase" "$err_line" "$err_cmd" "$code" >&2
    printf 'run-local-tests: next: read the message above; sbt output is in %s\n' "$run_log_display" >&2
    printf 'RESULT: FAIL mode=%s phase=%s duration=%s\n' "${mode:-none}" "$current_phase" "$(op_elapsed)" >&2
  fi
}
on_interrupt() {
  # A trap runs under the redirections of the command it interrupted (for example "verify_service_identity >log"),
  # so restore the terminal descriptors saved at startup before printing anything.
  exec 1>&3 2>&4
  trap '' INT TERM HUP
  op_ticker_stop
  printf '\n' >&2
  if [[ -n "$OP_CHILD_PID" ]]; then
    op_say "interrupted during phase \"$current_phase\"; stopping the sbt process this script started (up to 20 s)" >&2
  else
    op_say "interrupted during phase \"$current_phase\"; no sbt process was running; stopping only this script's own helpers" >&2
  fi
  op_stop_child 20
  local partial="no finished test summary in the log yet" total=0 passed=0 failed=0 skipped=0
  if (( sbt_phase_active )) && [[ -s "$log_root/run.log" ]]; then
    read -r total passed failed skipped <<<"$(op_count_tests "$log_root/run.log")" || true
    if (( total > 0 )); then partial="finished test groups so far: total=$total passed=$passed failed=$failed skipped=$skipped"; fi
  fi
  op_say "interrupted: phase \"$current_phase\" did not finish ($partial); test services stay running for reuse (scripts/run-local-tests.sh status); log: $run_log_display" >&2
  result_done=1
  exit 130
}
trap on_error ERR
trap on_exit EXIT
trap on_interrupt INT TERM HUP

case "$mode" in
  -h|--help|help) help_text; result_done=1; exit 0 ;;
  start|test|analytics|interview|retention|recovery|status|stop) ;;
  "") usage_error "missing MODE" ;;
  *) usage_error "unknown mode or option: $mode" ;;
esac
if [[ "$mode" == test || "$mode" == analytics ]]; then
  for suite_glob in "${@:2}"; do
    [[ "$suite_glob" =~ ^[A-Za-z0-9_.*$]+$ ]] || usage_error "invalid integration suite glob (allowed: letters, digits, _ . * \$)"
  done
elif (( $# != 1 )); then
  usage_error "mode $mode takes no further arguments"
fi
bounded_integer() { # name value min max
  [[ "$2" =~ ^[0-9]{1,6}$ ]] && (( 10#$2 >= $3 && 10#$2 <= $4 )) || usage_error "$1 must be an integer from $3 to $4, got: $2"
}
bounded_integer HIRING_TEST_TIMEOUT_SECONDS "$phase_limit" 60 86400
bounded_integer HIRING_TEST_LOCK_WAIT_SECONDS "$lock_wait" 0 3600
bounded_integer COMPOSE_TIMEOUT_SECONDS "$compose_timeout" 30 3600
phase_limit=$((10#$phase_limit)); lock_wait=$((10#$lock_wait)); compose_timeout=$((10#$compose_timeout))
for tool in python3 flock setsid timeout git docker; do
  command -v "$tool" >/dev/null 2>&1 || blocked "required tool not found: $tool" "install $tool and put it on PATH"
done
case "$mode" in
  test|analytics|interview|retention|recovery)
    command -v sbt >/dev/null 2>&1 || blocked "required tool not found: sbt" "install sbt and put it on PATH" ;;
esac

globs_text="${*:2}"
describe_banner() {
  case "$mode" in
    start) printf 'starts and verifies the isolated test services; expected 10 s with services up, up to 5 min on first start' ;;
    test) if [[ -n "$globs_text" ]]; then printf 'root build: all unit tests, then integration suites [%s]; recorded: units about 1 min plus the suites (a small suite took 3 s)' "$globs_text"
          else printf 'root build: full unit + integration under JaCoCo with the 85%% coverage gate; expected well over 1 h (a whole check-local took 1h48m-2h18m)'; fi ;;
    analytics) if [[ -n "$globs_text" ]]; then printf 'analytics build: integration suites [%s]; no recorded duration, depends on the suites' "$globs_text"
               else printf 'analytics build: full unit + integration under JaCoCo with the 85%% coverage gate; expected about 1 h (a whole check-local took 1h48m-2h18m)'; fi ;;
    interview) printf 'three interview Kafka integration suites; no recorded duration' ;;
    retention) printf 'KafkaTopicRetentionCheck; no recorded duration' ;;
    recovery) printf 'compiles the API classpath, then AccountDeletionRecoveryIntegrationSpec; no recorded duration' ;;
    status) printf 'shows the test services; expected under 30 s' ;;
    stop) printf 'removes only this workspace test services and their data; expected under 2 min' ;;
  esac
}
printf 'run-local-tests: %s. Parameters: mode=%s, limit per sbt invocation %s, lock wait %ss, compose call limit %ss. Log: %s\n' \
  "$(describe_banner)" "$mode" "$(op_duration "$phase_limit")" "$lock_wait" "$compose_timeout" "$run_log_display"
timeout --foreground 30 docker info >/dev/null 2>&1 || blocked "the Docker daemon is not reachable within 30 s" "start Docker, then run again"

# acquire_lock FD LABEL: wait for an advisory lock for at most lock_wait seconds, reporting every 10 s.
acquire_lock() {
  local fd="$1" label="$2" waited=0
  flock -n "$fd" && return 0
  op_say "waiting for $label (at most ${lock_wait}s)"
  while (( waited < lock_wait )); do
    flock -w 10 "$fd" && return 0
    waited=$((waited + 10))
    op_say "still waiting for $label (${waited}s of ${lock_wait}s)"
  done
  blocked "gave up waiting for $label after ${lock_wait}s" "wait for the other run to finish, or raise HIRING_TEST_LOCK_WAIT_SECONDS"
}

config_root="$repo_root/.local/config/test-services"
registry_root="$repo_root/.local/data/test-services/runs"
log_root="$repo_root/.local/logs/test-services"
build_root="$repo_root/.local/data/test-builds"
for path in "$config_root/manifest.json" "$registry_root/owner" "$log_root/run.log" "$build_root/root/owner.lock" "$build_root/analytics/owner.lock"; do
  git check-ignore -q "$path" || blocked "test output path is not git-ignored: ${path#"$repo_root"/}" "add it to .gitignore, then run again"
done
python3 - "$repo_root" "$config_root" "$registry_root" "$log_root" "$build_root" "$build_root/root" "$build_root/analytics" "$build_root/root/target" "$build_root/analytics/target" <<'PY_PATHS' || blocked "unsafe test output directory (see the message above)" "fix ownership, permissions or symlinks of the named path under .local/, then run again"
from pathlib import Path
import os,stat,sys
repo=Path(sys.argv[1])
for raw in sys.argv[2:]:
    path=Path(raw)
    for part in [path,*path.parents]:
        if part==repo:break
        if part.is_symlink():raise SystemExit('Test output ancestry must not contain symlinks')
        if part.exists() and part.stat().st_uid!=os.getuid():raise SystemExit('Test output ancestry must be owned by the current user')
    if path.exists() and path.stat().st_mode & 0o077:raise SystemExit('Test service output directories require owner-only permissions')
config=Path(sys.argv[2])
for name in ('manifest.json','compose.env','project','current.json','initialized.json','setup.lock'):
    path=config/name
    if path.is_symlink():raise SystemExit('Test configuration output must not be a symlink')
for raw in sys.argv[6:8]:
    lock=Path(raw)/'owner.lock'
    if lock.is_symlink():raise SystemExit('Test build ownership lock must not be a symlink')
    if lock.exists() and (not lock.is_file() or lock.stat().st_uid!=os.getuid() or lock.stat().st_mode & 0o077):raise SystemExit('Test build lock requires a private regular file owned by the current user')
PY_PATHS
mkdir -p "$config_root" "$registry_root" "$log_root" "$build_root/root/target" "$build_root/analytics/target"
for path in "$config_root" "$registry_root" "$log_root" "$build_root" "$build_root/root" "$build_root/analytics" "$build_root/root/target" "$build_root/analytics/target"; do
  [[ ! -L "$path" ]] || blocked "test output directory must not be a symlink: ${path#"$repo_root"/}" "replace the symlink with a real directory under .local/"
  chmod 700 "$path"
done
# One setup operation per workspace. Runs themselves have separate ownership locks.
exec 9>"$config_root/setup.lock"
acquire_lock 9 "another run-local-tests invocation to finish test service setup"
initialize_manifest() {
  python3 - "$repo_root" "$config_root" <<'PY'
from pathlib import Path
import base64,hashlib,json,re,secrets,socket,sys
repo=Path(sys.argv[1]).resolve(); root=Path(sys.argv[2]); file=root/'manifest.json'
fingerprint=hashlib.sha256((repo/'compose.test.yaml').read_bytes()).hexdigest()
if file.exists():
    if file.is_symlink(): raise SystemExit('Refusing a symlink test manifest')
    value=json.loads(file.read_text())
    if value.get('workspace')!=str(repo):raise SystemExit('Test manifest belongs to another workspace')
    if value.get('composeFingerprint')!=fingerprint and not __import__('os').environ.get('HIRING_TEST_STOP'):
        raise SystemExit('Test service configuration changed; stop the owned stack before recreating its manifest')
else:
    nonce=secrets.token_hex(16)
    def port():
        with socket.socket() as sock:
            sock.bind(('127.0.0.1',0)); value=sock.getsockname()[1]
        if value in (27017,9092):return port()
        return value
    mongo=port(); kafka=port()
    while kafka==mongo:kafka=port()
    value=dict(schema=1,workspace=str(repo),nonce=nonce,project='hiring-tests-'+hashlib.sha256(str(repo).encode()).hexdigest()[:12]+'-'+nonce[:8],
        mongoUri=f'mongodb://127.0.0.1:{mongo}/?replicaSet=hiring_test_{nonce}&directConnection=true',replicaSet='hiring_test_'+nonce,
        kafkaBootstrap=f'127.0.0.1:{kafka}',kafkaClusterId=base64.urlsafe_b64encode(secrets.token_bytes(16)).decode().rstrip('='),
        composeFingerprint=fingerprint)
    for key in ('brokerPassword','publisherPassword','readerPassword','fencerPassword','orchestratorPassword','workerPassword','interviewFencerPassword'):
        value[key]=secrets.token_hex(24)
    file.write_text(json.dumps(value));file.chmod(0o600)
nonce=value.get('nonce',''); expected='hiring-tests-'+hashlib.sha256(str(repo).encode()).hexdigest()[:12]+'-'+nonce[:8]
if value.get('schema')!=1 or not re.fullmatch('[a-f0-9]{32}',nonce) or value.get('project')!=expected or value.get('replicaSet')!='hiring_test_'+nonce:raise SystemExit('Invalid test service manifest identity')
mongo=re.fullmatch(r'mongodb://127\.0\.0\.1:([0-9]+)/\?replicaSet=hiring_test_'+nonce+r'&directConnection=true',value.get('mongoUri',''))
kafka=re.fullmatch(r'127\.0\.0\.1:([0-9]+)',value.get('kafkaBootstrap',''))
if not mongo or not kafka or not 1024<int(mongo[1])<=65535 or not 1024<int(kafka[1])<=65535 or int(mongo[1])==27017 or int(kafka[1])==9092 or mongo[1]==kafka[1]:raise SystemExit('Invalid isolated test endpoints')
if not re.fullmatch('[A-Za-z0-9_-]{22}',value.get('kafkaClusterId','')):raise SystemExit('Invalid test broker identity')
for key in ('brokerPassword','publisherPassword','readerPassword','fencerPassword','orchestratorPassword','workerPassword','interviewFencerPassword'):
    if not re.fullmatch('[a-f0-9]{48}',value.get(key,'')):raise SystemExit('Invalid test credentials')
fields={'HIRING_TEST_NONCE':value['nonce'],'HIRING_TEST_PROJECT':value['project'],'HIRING_TEST_REPLICA_SET':value['replicaSet'],
 'HIRING_TEST_MONGO_PORT':value['mongoUri'].split(':')[2].split('/')[0],'HIRING_TEST_KAFKA_PORT':value['kafkaBootstrap'].split(':')[1],
 'HIRING_TEST_KAFKA_CLUSTER_ID':value['kafkaClusterId']}
for field,key in [('BROKER','brokerPassword'),('PUBLISHER','publisherPassword'),('READER','readerPassword'),('FENCER','fencerPassword'),('ORCHESTRATOR','orchestratorPassword'),('WORKER','workerPassword'),('INTERVIEW_FENCER','interviewFencerPassword')]:fields['HIRING_TEST_'+field+'_PASSWORD']=value[key]
(root/'compose.env').write_text('\n'.join(k+'='+v for k,v in fields.items())+'\n');(root/'compose.env').chmod(0o600)
(root/'project').write_text(value['project'])
PY
}
compose() { timeout --foreground -k 10 "$compose_timeout" docker compose --env-file "$config_root/compose.env" -f compose.test.yaml -p "$test_project" "$@"; }
require_manifest() {
  [[ -f "$config_root/manifest.json" && ! -L "$config_root/manifest.json" ]] || blocked "no test services exist for this workspace" "run scripts/run-local-tests.sh start"
  initialize_manifest || blocked "the test service manifest is invalid or changed (see the message above)" "stop the owned stack with scripts/run-local-tests.sh stop (needs human approval), then start again"
  test_project="$(cat "$config_root/project")"
}
verify_service_identity() {
  python3 - "$repo_root" "$config_root" <<'PY_IDENTITY'
from pathlib import Path
import json,re,subprocess,sys,time
repo=Path(sys.argv[1]);config=Path(sys.argv[2]);m=json.loads((config/'manifest.json').read_text())
compose=['docker','compose','--env-file',str(config/'compose.env'),'-f',str(repo/'compose.test.yaml'),'-p',m['project']]
def run(args,stdin=None):return subprocess.run(args,input=stdin,text=True,capture_output=True,check=True,timeout=120).stdout
identities=[]
for service,internal,endpoint in [('mongodb','27017/tcp',m['mongoUri'].split(':')[2].split('/')[0]),('kafka','29092/tcp',m['kafkaBootstrap'].split(':')[1])]:
    ids=run(compose+['ps','-q',service]).split()
    if len(ids)!=1:raise SystemExit('Expected one isolated '+service+' service')
    info=json.loads(run(['docker','inspect',ids[0]]))[0];labels=info['Config']['Labels']
    identities.append({'service':service,'id':ids[0],'startedAt':info['State']['StartedAt']})
    if labels.get('hiring.test.identity')!=m['nonce'] or labels.get('com.docker.compose.project')!=m['project'] or labels.get('com.docker.compose.service')!=service:raise SystemExit('Isolated service labels mismatch')
    bindings=info['NetworkSettings']['Ports'].get(internal)
    if bindings!=[{'HostIp':'127.0.0.1','HostPort':endpoint}]:raise SystemExit('Isolated service binding mismatch')
probe="const h=db.hello();const d=db.getSiblingDB('hiring_test_control');const rows=d.identity.find().toArray();print(h.setName==='"+m['replicaSet']+"'&&(rows.length===0||(rows.length===1&&rows[0]._id==='"+m['nonce']+"'&&rows[0].project==='"+m['project']+"'))?'MATCH':'MISMATCH')"
if run(compose+['exec','-T','mongodb','mongosh','--quiet','--eval',probe]).strip()!='MATCH':raise SystemExit('Existing Mongo test identity mismatch')
properties='security.protocol=SASL_PLAINTEXT\nsasl.mechanism=PLAIN\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="broker" password="'+m['brokerPassword']+'";\n'
script="umask 077; f=$(mktemp); trap 'rm -f \"$f\"' EXIT; cat > \"$f\"; /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka:9092 --command-config \"$f\" describe --status"
deadline=time.monotonic()+180
for attempt in range(90):
    try:
        result=run(compose+['exec','-T','kafka','/bin/bash','-ec',script],properties);break
    except (subprocess.CalledProcessError,subprocess.TimeoutExpired):
        if attempt==89 or time.monotonic()>deadline:raise SystemExit('Test broker identity unavailable')
        time.sleep(1)
if not re.search(r'ClusterId:\s*'+re.escape(m['kafkaClusterId'])+r'(?:\s|$)',result):raise SystemExit('Existing Kafka test identity mismatch')
(config/'current.json').write_text(json.dumps(identities))
PY_IDENTITY
}
cleanup_orphans() {
  python3 - "$repo_root" "$config_root" "$registry_root" <<'PY_CLEAN'
from pathlib import Path
import fcntl,json,re,subprocess,sys
repo=Path(sys.argv[1]);config=Path(sys.argv[2]);runs=Path(sys.argv[3]);m=json.loads((config/'manifest.json').read_text())
compose=['docker','compose','--env-file',str(config/'compose.env'),'-f',str(repo/'compose.test.yaml'),'-p',m['project']]
def run(args,stdin=None):return subprocess.run(args,input=stdin,text=True,capture_output=True,check=True,timeout=120).stdout
ids=run(compose+['ps','-q','mongodb','kafka']).split()
if len(ids)!=2:raise SystemExit('Test service identity unavailable for cleanup')
for id in ids:
    info=json.loads(run(['docker','inspect',id]))[0]
    if info['Config']['Labels'].get('hiring.test.identity')!=m['nonce']:raise SystemExit('Test container identity mismatch')
probe="const h=db.hello();const d=db.getSiblingDB('hiring_test_control');const x=d.identity.findOne({_id:'"+m['nonce']+"'});print(h.setName==='"+m['replicaSet']+"'&&x&&x.project==='"+m['project']+"'?'MATCH':'MISMATCH')"
if run(compose+['exec','-T','mongodb','mongosh','--quiet','--eval',probe]).strip()!='MATCH':raise SystemExit('Test Mongo identity mismatch')
properties='security.protocol=SASL_PLAINTEXT\nsasl.mechanism=PLAIN\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="broker" password="'+m['brokerPassword']+'";\n'
def kafka(command):
    script="umask 077; f=$(mktemp); trap 'rm -f \"$f\"' EXIT; cat > \"$f\"; "+command
    return run(compose+['exec','-T','kafka','/bin/bash','-ec',script],properties)
metadata=kafka('/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka:9092 --command-config "$f" describe --status')
if not re.search(r'ClusterId:\s*'+re.escape(m['kafkaClusterId'])+r'(?:\s|$)',metadata):raise SystemExit('Test Kafka identity mismatch')
for root in runs.iterdir():
    if root.is_symlink() or not root.is_dir() or not re.fullmatch('[a-f0-9]{32}',root.name):continue
    lock=root/'owner.lock'
    if lock.is_symlink():raise SystemExit('Orphan ownership lock must not be a symlink')
    with lock.open('a+') as owner:
        try:fcntl.flock(owner,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:continue
        process_lock=root/'process.lock'
        if process_lock.is_symlink():raise SystemExit('Process ownership lock must not be a symlink')
        process_owner=None
        if process_lock.exists():
            process_owner=process_lock.open('a+')
            try:fcntl.flock(process_owner,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:
                process_owner.close()
                continue
        identity=root/'service.json'
        if not identity.exists():continue
        if identity.is_symlink() or json.loads(identity.read_text())!={'nonce':m['nonce'],'project':m['project']}:continue
        for marker in root.iterdir():
            if marker.is_symlink():raise SystemExit('Test ownership marker must not be a symlink')
            if marker.suffix=='.database':
                name=marker.read_text()
                if not re.fullmatch('hiring_test_[a-f0-9]{32}',name) or marker.name!=name+'.database':raise SystemExit('Invalid owned database marker')
                run(compose+['exec','-T','mongodb','mongosh','--quiet','--eval',"db.getSiblingDB('"+name+"').dropDatabase()"])
                marker.unlink()
            elif marker.suffix=='.kafka':
                value=json.loads(marker.read_text())
                if set(value)!= {'topics','groups'}:raise SystemExit('Invalid owned Kafka marker')
                for name in value['groups']:
                    if not re.fullmatch(r'hiring\.test\.(workers|orchestrator|events-group)\.[a-f0-9]{32}',name):raise SystemExit('Invalid owned group')
                    existing=kafka('/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --command-config "$f" --list').splitlines()
                    if name in existing:kafka('/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --command-config "$f" --delete --group '+name)
                    kafka('/opt/kafka/bin/kafka-acls.sh --bootstrap-server kafka:9092 --command-config "$f" --remove --force --group '+name)
                for name in value['topics']:
                    if not re.fullmatch(r'hiring\.test\.(commands|results|events)\.[a-f0-9]{32}',name):raise SystemExit('Invalid owned topic')
                    kafka('/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --command-config "$f" --delete --if-exists --topic '+name)
                    kafka('/opt/kafka/bin/kafka-acls.sh --bootstrap-server kafka:9092 --command-config "$f" --remove --force --topic '+name)
                marker.unlink()
        remaining={entry.name for entry in root.iterdir()}
        if remaining <= {'service.json','owner.lock','process.lock'}:
            identity.unlink();lock.unlink()
            process_lock.unlink(missing_ok=True)
            if process_owner is not None:process_owner.close()
            root.rmdir()
PY_CLEAN
}
fail_step() { # what, log file
  printf 'run-local-tests: %s failed; details are in %s\n' "$1" "${2#"$repo_root"/}" >&2
  printf 'run-local-tests: next: read that log, check that Docker is healthy (docker ps), then run again\n' >&2
  finish FAIL 1 "phase=$current_phase log=${2#"$repo_root"/}"
}
start() {
  current_phase="test services"
  op_say "starting the isolated test services (compose up --wait, at most 120 s; the first start may pull images)"
  op_ticker_start "test services"
  initialize_manifest || blocked "the test service manifest is invalid or changed (see the message above)" "stop the owned stack with scripts/run-local-tests.sh stop (needs human approval), then start again"
  test_project="$(cat "$config_root/project")"
  compose up -d --wait --wait-timeout 120 mongodb kafka >"$log_root/start.log" 2>&1 || fail_step "service startup" "$log_root/start.log"
  verify_service_identity >"$log_root/identity.log" 2>&1 || fail_step "service identity verification" "$log_root/identity.log"
  if ! cmp -s "$config_root/current.json" "$config_root/initialized.json"; then
    compose run --rm --no-deps initialize >"$log_root/initialize.log" 2>&1 || fail_step "service initialization" "$log_root/initialize.log"
  fi
  nonce="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["nonce"])' "$config_root/manifest.json")"
  compose exec -T mongodb mongosh --quiet --eval "db.getSiblingDB('hiring_test_control').identity.updateOne({_id:'$nonce'},{\$setOnInsert:{project:'$test_project'}},{upsert:true})" >"$log_root/mongo-initialize.log" 2>&1 || fail_step "recording the Mongo test identity" "$log_root/mongo-initialize.log"
  cp "$config_root/current.json" "$config_root/initialized.json"
  cleanup_orphans >"$log_root/orphan-cleanup.log" 2>&1 || fail_step "cleanup of leftover test resources" "$log_root/orphan-cleanup.log"
  op_ticker_stop
  op_say "reusable isolated test services ready: $test_project"
}

# Parses and reports the outcome of the sbt phase that wrote $run_log_path.
OP_PHASE_DESCRIBE() { # logfile
  local summaries
  summaries="$(sed -e 's/\x1b\[[0-9;?]*[A-Za-z]//g' "$1" | grep -cE '(Passed|Failed): Total [0-9]+' || true)"
  case "$mode" in
    test)
      if [[ -n "$globs_text" ]]; then
        if (( summaries == 0 )); then printf 'phase 1/2 unit tests (sbt test, includes compilation)'
        else printf 'phase 2/2 integration suites (it:testOnly %s)' "$globs_text"; fi
      else
        if (( summaries == 0 )); then printf 'phase 1/2 unit tests under JaCoCo (includes compilation)'
        else printf 'phase 2/2 integration suites and merged coverage (it:jacoco)'; fi
      fi ;;
    analytics)
      if [[ -n "$globs_text" ]]; then printf 'integration suites (it:testOnly %s)' "$globs_text"
      elif (( summaries == 0 )); then printf 'phase 1/2 analytics unit tests under JaCoCo (includes compilation)'
      else printf 'phase 2/2 analytics integration suites and merged coverage (it:jacoco)'; fi ;;
    interview) printf 'interview Kafka integration suites (it:testOnly)' ;;
    retention) printf 'Kafka topic retention check (runMain)' ;;
    recovery) printf 'account-deletion recovery suite (it:testOnly)' ;;
  esac
}

# run_sbt LABEL SBT_ARGS...: runs sbt bounded, with heartbeat; reports PASS/FAIL and exits.
run_sbt() {
  local label="$1" rc=0 total passed failed skipped counts
  shift
  current_phase="$label"
  op_say "phase: $label (log: $run_log_display; follow it with tail -f)"
  sbt_phase_active=1 # op_run_logged truncates run.log first, so counts below are never stale
  op_run_logged "$label" "$phase_limit" "$log_root/run.log" yes sbt "${java_args[@]}" "-Dhiring.test.buildRoot=$build_target" "$@" || rc=$?
  counts="$(op_count_tests "$log_root/run.log")"
  read -r total passed failed skipped <<<"$counts"
  local detail="tests_total=$total passed=$passed failed=$failed skipped=$skipped log=$run_log_display"
  if (( rc == 0 )); then
    if [[ "$mode" != retention && -z "$globs_text" ]] && (( total == 0 )); then
      printf 'run-local-tests: sbt exited 0 but no test ran in a mode that always runs fixed suites\n' >&2
      printf 'run-local-tests: next: read %s; the suite may have been renamed or removed\n' "$run_log_display" >&2
      finish FAIL 1 "reason=no-tests-ran $detail"
    fi
    if [[ -n "$globs_text" ]]; then
      local summaries needed=1 last_total
      summaries="$(sed -e 's/\x1b\[[0-9;?]*[A-Za-z]//g' "$log_root/run.log" | grep -cE '(Passed|Failed): Total [0-9]+' || true)"
      last_total="$(sed -e 's/\x1b\[[0-9;?]*[A-Za-z]//g' "$log_root/run.log" | grep -E '(Passed|Failed): Total [0-9]+' | tail -n 1 | sed -E 's/.*Total ([0-9]+).*/\1/')"
      if [[ "$mode" == test ]]; then needed=2; fi
      if (( summaries < needed )) || (( ${last_total:-0} == 0 )); then
        op_say "warning: no integration suite summary found for [$globs_text]; sbt passes when a glob matches nothing, so check the glob spelling"
        detail="$detail warning=no-integration-suite-matched"
      fi
    fi
    finish PASS 0 "$detail"
  elif (( rc == 124 )); then
    printf 'run-local-tests: phase "%s" exceeded %s and was stopped\n' "$label" "$(op_duration "$phase_limit")" >&2
    printf 'run-local-tests: next: inspect %s, or raise HIRING_TEST_TIMEOUT_SECONDS\n' "$run_log_display" >&2
    finish FAIL 1 "reason=timeout $detail"
  else
    op_show_failure_tail "$log_root/run.log" 40
    printf 'run-local-tests: sbt failed (exit %s) in phase "%s"\n' "$rc" "$label" >&2
    printf 'run-local-tests: next: read the excerpt above or the full log %s; failing tests are marked "==> X"\n' "$run_log_display" >&2
    finish FAIL 1 "$detail"
  fi
}

case "$mode" in
  start) start; finish PASS 0 "services=ready" ;;
  status)
    require_manifest
    op_ticker_start "status"
    compose ps
    op_ticker_stop
    finish PASS 0 "services=listed"
    ;;
  stop)
    current_phase="stopping test services"
    export HIRING_TEST_STOP=true
    require_manifest
    python3 - "$registry_root" "$config_root/manifest.json" <<'PY_STOP' || blocked "refusing to stop the services (see the message above)" "wait for the active test run to finish, then run stop again"
from pathlib import Path
import fcntl,json,sys
runs=Path(sys.argv[1]);m=json.loads(Path(sys.argv[2]).read_text())
for root in runs.iterdir():
    identity=root/'service.json'
    if root.is_symlink() or not identity.is_file() or identity.is_symlink():continue
    if json.loads(identity.read_text())!={'nonce':m['nonce'],'project':m['project']}:continue
    lock=root/'owner.lock'
    if lock.is_symlink():raise SystemExit('Invalid ownership lock')
    with lock.open('a+') as handle:
        try:fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:raise SystemExit('An owned test run is active; refuse to stop its services')
        process_lock=root/'process.lock'
        if process_lock.is_symlink():raise SystemExit('Invalid process ownership lock')
        if process_lock.exists():
            with process_lock.open('a+') as process_owner:
                try:fcntl.flock(process_owner,fcntl.LOCK_EX|fcntl.LOCK_NB)
                except BlockingIOError:raise SystemExit('An owned API process is active; refuse to stop its services')
PY_STOP
    op_say "stopping this workspace's test services (compose down, at most ${compose_timeout}s)"
    op_ticker_start "stop"
    compose down --volumes >"$log_root/stop.log" 2>&1 || fail_step "compose down" "$log_root/stop.log"
    python3 - "$config_root" "$registry_root" <<'PY_DELETE' || fail_step "removing leftover run registrations" "$log_root/stop.log"
from pathlib import Path
import fcntl,json,re,sys
root=Path(sys.argv[1]);runs=Path(sys.argv[2]);m=json.loads((root/'manifest.json').read_text())
for directory in runs.iterdir():
    identity=directory/'service.json'
    if directory.is_symlink() or not directory.is_dir() or not re.fullmatch('[a-f0-9]{32}',directory.name) or not identity.is_file() or identity.is_symlink():continue
    if json.loads(identity.read_text())!={'nonce':m['nonce'],'project':m['project']}:continue
    lock=directory/'owner.lock'
    if lock.is_symlink():raise SystemExit('Invalid completed run ownership lock')
    with lock.open('a+') as handle:
        try:fcntl.flock(handle,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:raise SystemExit('Owned run is active after service stop')
        entries=list(directory.iterdir())
        def owned(entry):
            if entry.is_symlink() or not entry.is_file():return False
            if entry.name in ('owner.lock','service.json','process.lock'):return True
            if entry.suffix=='.database':return bool(re.fullmatch('hiring_test_[a-f0-9]{32}',entry.read_text())) and entry.name==entry.read_text()+'.database'
            if entry.suffix=='.kafka':
                data=json.loads(entry.read_text())
                return set(data)=={'topics','groups'} and all(re.fullmatch(r'hiring\.test\.(commands|results|events)\.[a-f0-9]{32}',name) for name in data['topics']) and all(re.fullmatch(r'hiring\.test\.(workers|orchestrator|events-group)\.[a-f0-9]{32}',name) for name in data['groups'])
            return False
        if all(owned(entry) for entry in entries):
            for entry in entries:entry.unlink()
            directory.rmdir()
for name in ('manifest.json','compose.env','project','current.json','initialized.json'):(root/name).unlink(missing_ok=True)
PY_DELETE
    op_ticker_stop
    op_say "removed only $test_project; cached images remain"
    finish PASS 0 "services=removed"
    ;;
  test|analytics|interview|retention|recovery)
    start
    run_id="$(python3 -c 'import secrets;print(secrets.token_hex(16))')"
    run_root="$registry_root/$run_id"
    mkdir -p "$run_root"
    exec 8>"$run_root/owner.lock"
    acquire_lock 8 "this run's ownership lock"
    python3 - "$config_root/manifest.json" "$run_root/service.json" <<'PY_RUN'
import json,sys
from pathlib import Path
m=json.loads(Path(sys.argv[1]).read_text());Path(sys.argv[2]).write_text(json.dumps({'nonce':m['nonce'],'project':m['project']}))
PY_RUN
    export HIRING_TEST_MANIFEST="$config_root/manifest.json" HIRING_TEST_WORKSPACE="$repo_root" HIRING_TEST_RUN_REGISTRY="$run_root"
    # Never delete unregistered resources. Java Resource finalizers remove successful registrations.
    flock -u 9
    java_args=()
    if [[ -n "${HIRING_TEST_JAVA_HOME:-}" ]]; then java_args=(-java-home "$HIRING_TEST_JAVA_HOME"); fi
    if [[ "$1" == recovery ]]; then
      # The API classpath and analytics process share this run's root ownership lock.
      # Acquire root before analytics consistently; no nested sbt builds in the test JVM.
      exec 6>"$build_root/root/owner.lock"
      acquire_lock 6 "the root build lock held by another test run"
      current_phase="recovery classpath"
      op_say "phase: compiling the API and exporting its classpath (sbt compile; log: .local/logs/test-services/recovery-classpath.log)"
      rc=0
      op_run_logged "recovery classpath (sbt compile)" "$phase_limit" "$log_root/recovery-classpath.log" no \
        sbt "${java_args[@]}" "-Dhiring.test.buildRoot=$build_root/root" compile 'export Compile / fullClasspath' || rc=$?
      if (( rc != 0 )); then
        printf 'run-local-tests: compiling the API classpath failed (exit %s)\n' "$rc" >&2
        printf 'run-local-tests: next: read .local/logs/test-services/recovery-classpath.log (a status of 124 means the %s limit was reached)\n' "$(op_duration "$phase_limit")" >&2
        finish FAIL 1 "phase=recovery-classpath log=.local/logs/test-services/recovery-classpath.log"
      fi
      export HIRING_RECOVERY_CLASSPATH_FILE="$log_root/recovery-classpath.log"
      export HIRING_ACCOUNT_DELETION_RECOVERY_EVIDENCE=true
    fi
    build_target="$build_root/root"
    if [[ "$1" == analytics || "$1" == recovery ]]; then build_target="$build_root/analytics"; fi
    exec 7>"$build_target/owner.lock"
    acquire_lock 7 "the $(basename "$build_target") build lock held by another test run"
    case "$1" in
      recovery)
        cd analytics
        run_sbt "recovery suite" 'it:testOnly *AccountDeletionRecoveryIntegrationSpec'
        ;;
      analytics)
        cd analytics
        if (( $# > 1 )); then
          run_sbt "analytics integration suites" "it:testOnly ${*:2}"
        else
          # Full runs merge unit and integration coverage and enforce the 85% line threshold.
          run_sbt "analytics full run" jacoco 'it:jacoco'
        fi
        ;;
      interview) run_sbt "interview suites" 'it:testOnly *InterviewKafkaRestartIntegrationSpec *InterviewPublicationFencingIntegrationSpec *InterviewSchedulingWorkerIntegrationSpec' ;;
      retention) run_sbt "topic retention check" 'Test / runMain com.example.hiring.testing.KafkaTopicRetentionCheck' ;;
      test)
        if (( $# > 1 )); then
          run_sbt "root unit tests and integration suites" test "it:testOnly ${*:2}"
        else
          # Full runs merge unit and integration coverage and enforce the 85% line threshold.
          run_sbt "root full run" jacoco 'it:jacoco'
        fi
        ;;
    esac
    ;;
esac
