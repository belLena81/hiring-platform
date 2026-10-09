#!/usr/bin/env bash
set -euo pipefail
umask 077
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$repo_root"
usage() { printf 'Usage: %s start|test [suite-glob ...]|analytics [suite-glob ...]|interview|retention|recovery|status|stop\n' "$0" >&2; }
case "${1:-}" in
  start|test|analytics|interview|retention|recovery|status|stop) ;;
  *) usage; exit 2 ;;
esac
if [[ "${1:-}" == test || "${1:-}" == analytics ]]; then
  for suite_glob in "${@:2}"; do
    [[ "$suite_glob" =~ ^[A-Za-z0-9_.*$]+$ ]] || { printf 'Invalid integration suite glob.\n' >&2; exit 2; }
  done
elif (( $# != 1 )); then
  usage
  exit 2
fi
config_root="$repo_root/.local/config/test-services"
registry_root="$repo_root/.local/data/test-services/runs"
log_root="$repo_root/.local/logs/test-services"
build_root="$repo_root/.local/data/test-builds"
for path in "$config_root/manifest.json" "$registry_root/owner" "$log_root/run.log" "$build_root/root/owner.lock" "$build_root/analytics/owner.lock"; do
  git check-ignore -q "$path" || { printf 'Test output path must be ignored.\n' >&2; exit 1; }
done
python3 - "$repo_root" "$config_root" "$registry_root" "$log_root" "$build_root" "$build_root/root" "$build_root/analytics" "$build_root/root/target" "$build_root/analytics/target" <<'PY_PATHS'
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
  [[ ! -L "$path" ]] || { printf 'Test output directory must not be a symlink.\n' >&2; exit 1; }
  chmod 700 "$path"
done
# One setup operation per workspace. Runs themselves have separate ownership locks.
exec 9>"$config_root/setup.lock"
flock 9
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
compose() { docker compose --env-file "$config_root/compose.env" -f compose.test.yaml -p "$test_project" "$@"; }
require_manifest() {
  [[ -f "$config_root/manifest.json" && ! -L "$config_root/manifest.json" ]] || { printf 'Start test services first.\n' >&2; exit 1; }
  initialize_manifest
  test_project="$(cat "$config_root/project")"
}
verify_service_identity() {
  python3 - "$repo_root" "$config_root" <<'PY_IDENTITY'
from pathlib import Path
import json,re,subprocess,sys,time
repo=Path(sys.argv[1]);config=Path(sys.argv[2]);m=json.loads((config/'manifest.json').read_text())
compose=['docker','compose','--env-file',str(config/'compose.env'),'-f',str(repo/'compose.test.yaml'),'-p',m['project']]
def run(args,stdin=None):return subprocess.run(args,input=stdin,text=True,capture_output=True,check=True).stdout
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
for attempt in range(90):
    try:
        result=run(compose+['exec','-T','kafka','/bin/bash','-ec',script],properties);break
    except subprocess.CalledProcessError:
        if attempt==89:raise SystemExit('Test broker identity unavailable')
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
def run(args,stdin=None):return subprocess.run(args,input=stdin,text=True,capture_output=True,check=True).stdout
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
start() {
  initialize_manifest
  test_project="$(cat "$config_root/project")"
  if ! compose up -d --wait --wait-timeout 120 mongodb kafka >"$log_root/start.log" 2>&1; then
    printf 'Test service startup failed; see .local/logs/test-services/start.log.\n' >&2
    return 1
  fi
  if ! verify_service_identity >"$log_root/identity.log" 2>&1; then
    printf 'Test service identity verification failed; see .local/logs/test-services/identity.log.\n' >&2
    return 1
  fi
  if ! cmp -s "$config_root/current.json" "$config_root/initialized.json"; then
    compose run --rm --no-deps initialize >"$log_root/initialize.log" 2>&1
  fi
  nonce="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["nonce"])' "$config_root/manifest.json")"
  compose exec -T mongodb mongosh --quiet --eval "db.getSiblingDB('hiring_test_control').identity.updateOne({_id:'$nonce'},{\$setOnInsert:{project:'$test_project'}},{upsert:true})" >"$log_root/mongo-initialize.log" 2>&1
  cp "$config_root/current.json" "$config_root/initialized.json"
  cleanup_orphans >"$log_root/orphan-cleanup.log" 2>&1
  printf 'Reusable isolated test services ready: %s\n' "$test_project"
}
case "${1:-}" in
  start) start ;;
  status) require_manifest; compose ps ;;
  stop)
    export HIRING_TEST_STOP=true
    require_manifest
    python3 - "$registry_root" "$config_root/manifest.json" <<'PY_STOP'
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
    compose down --volumes >"$log_root/stop.log" 2>&1
    python3 - "$config_root" "$registry_root" <<'PY_DELETE'
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
    printf 'Removed only %s; cached images remain.\n' "$test_project"
    ;;
  test|analytics|interview|retention|recovery)
    start
    run_id="$(python3 -c 'import secrets;print(secrets.token_hex(16))')"
    run_root="$registry_root/$run_id"
    mkdir -p "$run_root"
    exec 8>"$run_root/owner.lock"
    flock 8
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
      flock 6
      sbt "${java_args[@]}" "-Dhiring.test.buildRoot=$build_root/root" compile 'export Compile / fullClasspath' >"$log_root/recovery-classpath.log" 2>&1
      export HIRING_RECOVERY_CLASSPATH_FILE="$log_root/recovery-classpath.log"
      export HIRING_ACCOUNT_DELETION_RECOVERY_EVIDENCE=true
    fi
    build_target="$build_root/root"
    if [[ "$1" == analytics || "$1" == recovery ]]; then build_target="$build_root/analytics"; fi
    exec 7>"$build_target/owner.lock"
    flock 7
    case "$1" in
      recovery)
        cd analytics
        sbt "${java_args[@]}" "-Dhiring.test.buildRoot=$build_target" 'it:testOnly *AccountDeletionRecoveryIntegrationSpec'
        ;;
      analytics)
        cd analytics
        if (( $# > 1 )); then
          sbt "${java_args[@]}" "-Dhiring.test.buildRoot=$build_target" "it:testOnly ${*:2}"
        else
          # Full runs merge unit and integration coverage and enforce the 60% line threshold.
          sbt "${java_args[@]}" "-Dhiring.test.buildRoot=$build_target" jacoco 'it:jacoco'
        fi
        ;;
      interview) sbt "${java_args[@]}" "-Dhiring.test.buildRoot=$build_target" 'it:testOnly *InterviewKafkaRestartIntegrationSpec *InterviewPublicationFencingIntegrationSpec *InterviewSchedulingWorkerIntegrationSpec' ;;
      retention) sbt "${java_args[@]}" "-Dhiring.test.buildRoot=$build_target" 'Test / runMain com.example.hiring.testing.KafkaRetentionProof' ;;
      test)
        if (( $# > 1 )); then
          sbt "${java_args[@]}" "-Dhiring.test.buildRoot=$build_target" test "it:testOnly ${*:2}"
        else
          # Full runs merge unit and integration coverage and enforce the 60% line threshold.
          sbt "${java_args[@]}" "-Dhiring.test.buildRoot=$build_target" jacoco 'it:jacoco'
        fi
        ;;
    esac
    ;;
  *) usage; exit 2 ;;
esac
