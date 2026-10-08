#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
mode="${1:-stage}"
nonce="${2:-$(openssl rand -hex 8)}"
[[ "$nonce" =~ ^[a-f0-9]{16}$ && ( "$mode" == stage || "$mode" == stage-maintenance || "$mode" == run || "$mode" == scenarios || "$mode" == diagnose || "$mode" == maintenance || "$mode" == stop ) ]] || {
  printf 'Usage: %s {stage|stage-maintenance} [nonce] | {run|scenarios|diagnose|maintenance} nonce acceptance.json review-directory | stop nonce\n' "$0" >&2; exit 2;
}
[[ -r .env ]] || { printf 'Ignored local .env credentials are required.\n' >&2; exit 1; }
. ./.env
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
export SPARK_LOCAL_IP=127.0.0.1
unset JAVA_TOOL_OPTIONS JDK_JAVA_OPTIONS _JAVA_OPTIONS
project="hiring-streaming-proof-$nonce"
config_dir="$repo_root/.local/config/$project"
log_dir="$repo_root/.local/logs/$project"
for parent in "$repo_root/.local" "$repo_root/.local/config" "$repo_root/.local/logs"; do
  [[ ! -L "$parent" ]] || { printf 'Proof parents must not use symbolic links.\n' >&2; exit 1; }
done
git check-ignore -q ".local/config/$project/state.json"
git check-ignore -q ".local/logs/$project/runtime.log"
export HIRING_STREAMING_PROOF_CONFIG="$config_dir"
export HIRING_STREAMING_PROOF_NONCE="$nonce"
export HIRING_STREAMING_PROOF_REPO="$repo_root"
export KAFKA_TOPIC="hiring.streaming.proof.$nonce"

compose=(docker compose -f compose.yaml -f compose.analytics-streaming-proof.yaml -p "$project")
network_overlay() {
  local overlay="$config_dir/network.yaml"
  if [[ -e "$overlay" || -L "$overlay" ]]; then
    [[ -f "$overlay" && ! -L "$overlay" && "$(stat -c '%u:%a' "$overlay")" == "$(id -u):600" ]] || {
      printf 'Private proof network override is unsafe.\n' >&2; exit 1;
    }
    compose+=(-f "$overlay")
  elif [[ "$mode" != stop ]]; then
    printf 'New proof requires its private network override.\n' >&2; exit 1;
  fi
}
java_run() {
  local conf="$1" main="$2"; shift 2
  local logging_options=()
  if [[ "$main" == com.example.hiring.analytics.cli.HiringAnalyticsLateFactReplayMain ]]; then
    local replay_logging="$config_dir/replay-outcome-log4j2.properties"
    [[ -f "$replay_logging" && ! -L "$replay_logging" && "$(stat -c '%u:%a' "$replay_logging")" == "$(id -u):600" ]] || {
      printf 'Private replay outcome logging configuration is absent or unsafe.\n' >&2
      return 1
    }
    logging_options=("-Dlog4j.configurationFile=$replay_logging")
  fi
  (cd analytics && java -Dspark.sql.shuffle.partitions=2 -Dspark.databricks.delta.snapshotPartitions=2 --add-opens=java.base/sun.security.action=ALL-UNNAMED \
    "${logging_options[@]}" -Dconfig.file="$conf" -cp "$analytics_cp" "$main" "$@")
}
replay_run() {
  local conf="$1" output="$2" actual_exit
  if java_run "$conf" com.example.hiring.analytics.cli.HiringAnalyticsLateFactReplayMain >"$output" 2>&1; then
    actual_exit=0
  else
    actual_exit=$?
  fi
  printf 'STREAMING_REPLAY_CLI_EXIT actualExit=%d\n' "$actual_exit" >>"$output"
  return "$actual_exit"
}
read_state() {
  [[ -d "$config_dir" && ! -L "$config_dir" && "$(stat -c '%u:%a' "$config_dir")" == "$(id -u):700" &&
    -d "$log_dir" && ! -L "$log_dir" && "$(stat -c '%u:%a' "$log_dir")" == "$(id -u):700" ]] || {
    printf 'Proof config and log directories must be private and owned by this user.\n' >&2; exit 1;
  }
  [[ -f "$config_dir/state.json" && ! -L "$config_dir/state.json" &&
    "$(stat -c '%u:%a' "$config_dir/state.json")" == "$(id -u):600" ]] || {
    printf 'Private task state is absent or unsafe.\n' >&2; exit 1;
  }
  network_overlay
  mapfile -t values < <(python3 - "$config_dir" <<'PY'
import json, os, pathlib, sys
p=pathlib.Path(sys.argv[1]); state=json.loads((p/'state.json').read_text()); credentials=json.loads((p/'credentials.json').read_text()); assert state['nonce']==os.environ['HIRING_STREAMING_PROOF_NONCE']
for k in ('mongoPort','kafkaPort','apiPort','apiPid'): print(state[k])
print(credentials['operatorPassword'])
PY
  )
  export HIRING_STREAMING_PROOF_MONGO_PORT="${values[0]}"
  export HIRING_STREAMING_PROOF_KAFKA_PORT="${values[1]}"
  export HIRING_STREAMING_PROOF_MONGO_PASSWORD="${values[4]}"
  if [[ "$mode" == run || "$mode" == scenarios || "$mode" == diagnose || "$mode" == maintenance ]]; then analytics_cp="$(cat "$config_dir/analytics.classpath")"; fi
}

if [[ "$mode" == stage || "$mode" == stage-maintenance ]]; then
  export HIRING_STREAMING_PROOF_PROFILE="$mode"
  [[ ! -e "$config_dir" && ! -L "$config_dir" && ! -e "$log_dir" && ! -L "$log_dir" ]] || {
    printf 'This nonce is already in use.\n' >&2; exit 1;
  }
  api_pid=""
  compose_started=false
  stage_exit() {
    local result=$?
    trap - EXIT
    if (( result != 0 )); then
      if [[ -n "$api_pid" ]]; then kill -- "-$api_pid" 2>/dev/null || true; fi
      if [[ "$compose_started" == true ]]; then "${compose[@]}" stop >/dev/null 2>&1 || true; fi
      printf 'Staging failed; isolated resources stopped and private evidence retained: %s\n' "$log_dir" >&2
    fi
    exit "$result"
  }
  trap stage_exit EXIT
  mkdir -p "$config_dir" "$log_dir"
  chmod 700 "$config_dir" "$log_dir"
  printf 'ISOLATED_SPARK_LOCAL_PROFILE master=local[2] shufflePartitions=2 deltaSnapshotPartitions=2 sourceOffsetCap=1000 maintenanceSeconds=60 lockWaitSeconds=120 healthyRecords=7500 healthyWallSeconds=900..905 bronzeP95LimitMs=30000 reportP95LimitMs=120000\n' >"$log_dir/spark-local-profile.log"
  read -r mongo_port kafka_port api_port < <(python3 - <<'PY'
import socket
ss=[socket.socket() for _ in range(3)]
for s in ss: s.bind(('127.0.0.1',0))
print(*(s.getsockname()[1] for s in ss))
for s in ss: s.close()
PY
  )
  export HIRING_STREAMING_PROOF_MONGO_PORT="$mongo_port"
  export HIRING_STREAMING_PROOF_KAFKA_PORT="$kafka_port"
  export HIRING_STREAMING_PROOF_API_PORT="$api_port"
  export HIRING_STREAMING_PROOF_MONGO_PASSWORD="$(openssl rand -hex 32)"
  export KAFKA_READER_PASSWORD KAFKA_PUBLISHER_V2_PASSWORD HIRING_ANALYTICS_HMAC_SECRET_BASE64
  export KAFKA_READER_USERNAME=analytics_reader
  export KAFKA_CONSUMER_ENABLED=false
  python3 - <<'PY'
import base64, ipaddress, json, os, pathlib, secrets, subprocess
root=pathlib.Path(os.environ['HIRING_STREAMING_PROOF_REPO']); nonce=os.environ['HIRING_STREAMING_PROOF_NONCE']; p=pathlib.Path(os.environ['HIRING_STREAMING_PROOF_CONFIG'])
credentials={'operatorPassword':os.environ['HIRING_STREAMING_PROOF_MONGO_PASSWORD'],'runtimePassword':secrets.token_hex(32),'apiPassword':secrets.token_hex(32),'jwtSecret':secrets.token_hex(32)}
(p/'credentials.json').write_text(json.dumps(credentials)); (p/'mongo-keyfile').write_text(base64.b64encode(secrets.token_bytes(756)).decode())
mongo=int(os.environ['HIRING_STREAMING_PROOF_MONGO_PORT']); kafka=int(os.environ['HIRING_STREAMING_PROOF_KAFKA_PORT']); api=int(os.environ['HIRING_STREAMING_PROOF_API_PORT']); database='hiring_streaming_proof_'+nonce; topic='hiring.streaming.proof.'+nonce
state={'nonce':nonce,'mongoPort':mongo,'kafkaPort':kafka,'apiPort':api,'apiPid':0}; (p/'state.json').write_text(json.dumps(state))
# Use one small task-owned subnet; preserved proof networks may exhaust Docker's default pools.
network_ids=subprocess.check_output(['docker','network','ls','-q'],text=True).split()
if len(network_ids)>256: raise RuntimeError('Docker network inventory bound exceeded')
networks=json.loads(subprocess.check_output(['docker','network','inspect',*network_ids],text=True))
used=[ipaddress.ip_network(entry['Subnet'],strict=False) for network in networks for entry in ((network.get('IPAM') or {}).get('Config') or []) if entry.get('Subnet')]
routes=json.loads(subprocess.check_output(['ip','-j','route'],text=True))
used.extend(ipaddress.ip_network(route['dst'],strict=False) for route in routes if route.get('dst') not in (None,'default','0.0.0.0/0'))
subnet=next((ipaddress.ip_network(f'10.253.{(int(nonce[:2],16)+offset)%256}.0/24') for offset in range(256) if not any(ipaddress.ip_network(f'10.253.{(int(nonce[:2],16)+offset)%256}.0/24').overlaps(existing) for existing in used if existing.version==4)),None)
if subnet is None: raise RuntimeError('No isolated proof subnet available')
(p/'network.yaml').write_text('networks:\n  default:\n    ipam:\n      config:\n        - subnet: '+str(subnet)+'\n')
q=json.dumps
def mongo_uri(operator): return f"mongodb://{'proof_operator' if operator else 'proof_api'}:{credentials['operatorPassword' if operator else 'apiPassword']}@127.0.0.1:{mongo}/?authSource={'admin' if operator else database}&replicaSet=rs0&directConnection=true"
for operator in (True,False):
 conf='include classpath("application.conf")\n'+f'mongo.uri={q(mongo_uri(operator))}\nmongo.database={q(database)}\nmongo.reset-on-start=false\nhttp.host="127.0.0.1"\nhttp.port={api}\nauth.jwt.hs256-secret={q(credentials["jwtSecret"])}\nkafka.enabled=true\nkafka.bootstrap-servers={q("127.0.0.1:"+str(kafka))}\nkafka.sasl-security-protocol="SASL_PLAINTEXT"\nkafka.topic={q(topic)}\nkafka.publisher.sasl-username="hiring_publisher_v2"\nkafka.publisher.sasl-password={q(os.environ["KAFKA_PUBLISHER_V2_PASSWORD"])}\n'
 conf+=f'auth.admin-seed.enabled=true\nauth.admin-seed.name={q("Proof Admin "+nonce)}\nauth.admin-seed.password={q("proof-password-"+nonce)}\n'
 (p/('api-operator.conf' if operator else 'api-runtime.conf')).write_text(conf)
with (p/'replay-outcome-log4j2.properties').open('x',encoding='utf-8') as replay_logging:
 os.fchmod(replay_logging.fileno(),0o600)
 replay_logging.write('rootLogger.level = error\nrootLogger.appenderRef.stdout.ref = console\nappender.console.type = Console\nappender.console.name = console\nappender.console.target = SYSTEM_ERR\nappender.console.layout.type = PatternLayout\nappender.console.layout.pattern = %d{HH:mm:ss} %-5p %c{1}: %m%n\nlogger.replay.name = com.example.hiring.analytics.cli.HiringAnalyticsLateFactReplayMain\nlogger.replay.level = info\nlogger.replay.additivity = false\nlogger.replay.appenderRef.console.ref = console\nlogger.replayObject.name = com.example.hiring.analytics.cli.HiringAnalyticsLateFactReplayMain$\nlogger.replayObject.level = info\nlogger.replayObject.additivity = false\nlogger.replayObject.appenderRef.console.ref = console\n')
with (p/'streaming-progress-log4j2.properties').open('x',encoding='utf-8') as progress_logging:
 os.fchmod(progress_logging.fileno(),0o600)
 progress_logging.write('rootLogger.level = error\nrootLogger.appenderRef.stdout.ref = console\nappender.console.type = Console\nappender.console.name = console\nappender.console.target = SYSTEM_ERR\nappender.console.layout.type = PatternLayout\nappender.console.layout.pattern = %d{HH:mm:ss} %-5p %c{1}: %m%n\nlogger.maintenance.name = com.example.hiring.analytics.streaming.maintenance\nlogger.maintenance.level = info\nlogger.maintenance.additivity = false\nlogger.maintenance.appenderRef.console.ref = console\n')
for file in p.iterdir(): file.chmod(0o600)
PY
  network_overlay
  compose_started=true
  "${compose[@]}" up -d --wait mongodb kafka kafka-acl-init
  # Source identity uses Admin.describeCluster; permit metadata reads on this disposable broker.
  "${compose[@]}" run --rm --no-deps --entrypoint /bin/bash kafka-acl-init -ec '
    umask 077
    trap '\''rm -f /tmp/streaming-identity-admin.properties'\'' EXIT
    cat > /tmp/streaming-identity-admin.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="broker" password="${KAFKA_BROKER_PASSWORD}";
EOF
    /opt/kafka/bin/kafka-acls.sh --bootstrap-server kafka:9092 \
      --command-config /tmp/streaming-identity-admin.properties --add \
      --allow-principal User:analytics_reader --operation DESCRIBE --cluster
    printf "STREAMING_IDENTITY_METADATA_ACCESS_GRANTED principal=analytics_reader cluster=Describe writes=unchanged\n"
  ' >"$log_dir/source-identity-acl.log" 2>&1
  sbt 'export Runtime / fullClasspath' >"$log_dir/api-classpath.log" 2>&1
  (cd analytics && sbt 'export Test / fullClasspath') >"$log_dir/analytics-classpath.log" 2>&1
  rg '^/.*\.jar' "$log_dir/api-classpath.log" | tail -n 1 >"$config_dir/api.classpath"
  rg '^/.*\.jar' "$log_dir/analytics-classpath.log" | tail -n 1 >"$config_dir/analytics.classpath"
  api_cp="$(cat "$config_dir/api.classpath")"
  analytics_cp="$(cat "$config_dir/analytics.classpath")"
  cat >"$config_dir/logback.xml" <<'XML'
<configuration>
  <appender name="PROOF" class="ch.qos.logback.core.ConsoleAppender">
    <encoder><pattern>%msg%n</pattern></encoder>
  </appender>
  <logger name="hiring.foundation" level="INFO" additivity="false"><appender-ref ref="PROOF"/></logger>
  <root level="OFF"/>
</configuration>
XML
  setsid java -Dcats.effect.trackFiberContext=true -Dlogback.configurationFile="$config_dir/logback.xml" -Dconfig.file="$config_dir/api-operator.conf" -cp "$api_cp" com.example.graphQL.cats.Main >"$log_dir/api.log" 2>&1 &
  api_pid=$!
  for _ in $(seq 1 120); do
    curl -fsS "http://127.0.0.1:$api_port/ready" 2>/dev/null | rg -q READY && break
    kill -0 "$api_pid" 2>/dev/null || { printf 'API startup failed; inspect private proof logs.\n' >&2; exit 1; }
    sleep 1
  done
  curl -fsS "http://127.0.0.1:$api_port/ready" >/dev/null
  python3 - <<'PY'
import json, os, pathlib, re, sys, urllib.error, urllib.request, uuid
p=pathlib.Path(os.environ['HIRING_STREAMING_PROOF_CONFIG']); state=json.loads((p/'state.json').read_text()); url=f'http://127.0.0.1:{state["apiPort"]}'; nonce=state['nonce']; tokens={}
def safe_code(value):
 return value if isinstance(value,str) and re.fullmatch(r'[A-Z][A-Z0-9_]{0,63}',value) else 'UNKNOWN'
def provisioning_failure(field,role,body):
 data=body.get('data') if isinstance(body,dict) else None
 result=data.get(field) if isinstance(data,dict) else None
 result=result if isinstance(result,dict) else {}
 typename=result.get('__typename'); typename=typename if typename in ('AuthSuccess','ValidationError','DomainError') else 'UNKNOWN'
 codes=[safe_code(result.get('code'))] if result.get('code') is not None else []
 for error in body.get('errors',[]) if isinstance(body,dict) and isinstance(body.get('errors',[]),list) else []:
  extensions=error.get('extensions',{}) if isinstance(error,dict) else {}
  if isinstance(extensions,dict): codes.append(safe_code(extensions.get('code')))
 print('STREAMING_PROOF_ROLE_PROVISIONING_FAILED '+json.dumps({'field':field,'role':role,'typename':typename,'codes':sorted(set(codes))}),file=sys.stderr)
 raise RuntimeError('authenticated synthetic role provisioning failed')
for field,role in [('login','Admin'),('signUp','Candidate'),('signUp','Recruiter')]:
 fields=f'idempotencyKey: "{uuid.uuid4()}", name: "Proof {role} {nonce}", password: "proof-password-{nonce}"'
 if field=='signUp':
  fields+=f', role: {role.upper()}'
  fields+=', skills: ["Scala"]' if role=='Candidate' else ', organizationName: "Synthetic proof organization", jobTitle: "Proof recruiter"'
 operation=f'mutation {{ {field}(input: {{{fields}}}) {{ __typename ... on AuthSuccess {{ accessToken }} ... on ValidationError {{ code }} ... on DomainError {{ code }} }} }}'
 request=urllib.request.Request(url+'/graphql',data=json.dumps({'query':operation}).encode(),headers={'Content-Type':'application/json'})
 try:
  with urllib.request.urlopen(request,timeout=15) as response: body=json.load(response)
 except urllib.error.HTTPError as error:
  try: body=json.loads(error.read(65536))
  except (ValueError,UnicodeError): body={}
  provisioning_failure(field,role,body)
 data=body.get('data'); result=data.get(field,{}) if isinstance(data,dict) else {}
 if not isinstance(result,dict) or result.get('__typename')!='AuthSuccess': provisioning_failure(field,role,body)
 tokens[role]=result['accessToken']
(p/'tokens.json').write_text(json.dumps(tokens))
PY
  cat >"$config_dir/roles.js" <<'JS'
const c=JSON.parse(require('fs').readFileSync('/proof/credentials.json','utf8'));
db.getSiblingDB('admin').auth({user:'proof_operator',pwd:c.operatorPassword});
const s=JSON.parse(require('fs').readFileSync('/proof/state.json','utf8'));
const d=db.getSiblingDB('hiring_streaming_proof_'+s.nonce);
for(const name of ['analytics_streaming_activation','analytics_streaming_lakehouses','analytics_lakehouse_mutexes','analytics_late_fact_replay_requests']) if(!d.getCollectionNames().includes(name)) d.createCollection(name);
const protectedNames=new Set(['analytics_streaming_activation','analytics_streaming_lakehouses']);
const operational=d.getCollectionNames().filter(n=>!n.startsWith('system.')&&!protectedNames.has(n));
const apiPrivileges=operational.map(n=>({resource:{db:d.getName(),collection:n},actions:['find','insert','update','remove','createIndex','listIndexes']}));
// Normal Root Main setup reapplies validators to exactly these owned operational collections.
for(const name of ['users','jobs']) apiPrivileges.push({resource:{db:d.getName(),collection:name},actions:['createCollection','collMod']});
apiPrivileges.push({resource:{db:d.getName(),collection:'event_outbox'},actions:['collMod']});
apiPrivileges.push({resource:{db:d.getName(),collection:''},actions:['listCollections']});
for(const name of protectedNames) apiPrivileges.push({resource:{db:d.getName(),collection:name},actions:['find']});
d.createRole({role:'proof_api',privileges:apiPrivileges,roles:[]});
d.createUser({user:'proof_api',pwd:c.apiPassword,roles:[{role:'proof_api',db:d.getName()}]});
const analyticsWrites=['analytics_report_snapshots','analytics_report_runs','analytics_report_control','analytics_lakehouse_mutexes','analytics_late_fact_replay_requests'];
const analyticsReads=['users','analytics_erasure_requests','analytics_erasure_completions','outbox_subject_fences','hiring_migration_ledger','analytics_hmac_key_retirements'];
const privileges=analyticsWrites.map(n=>({resource:{db:d.getName(),collection:n},actions:['find','insert','update','remove','createIndex','listIndexes']}));
for(const name of analyticsReads) privileges.push({resource:{db:d.getName(),collection:name},actions:['find']});
privileges.push({resource:{db:d.getName(),collection:''},actions:['listCollections']});
privileges.push({resource:{db:d.getName(),collection:'analytics_streaming_activation'},actions:['find']});
privileges.push({resource:{db:d.getName(),collection:'analytics_streaming_lakehouses'},actions:['find','insert']});
d.createRole({role:'analytics_runtime',privileges:privileges,roles:[]});
d.createUser({user:'analytics_runtime',pwd:c.runtimePassword,roles:[{role:'analytics_runtime',db:d.getName()}]});
print('STREAMING_PROOF_RUNTIME_ROLE_CREATED grant=find hmacRetirements=find registration=find,insert roleManagement=denied');
print('STREAMING_PROOF_API_SETUP_ROLE_CREATED usersJobs=createCollection,collMod outbox=collMod grant=find registration=find roleManagement=denied');
JS
  "${compose[@]}" exec -T mongodb mongosh --quiet --file /proof/roles.js >"$log_dir/permissions.log" 2>&1
  cat >"$config_dir/permission-denial.js" <<'JS'
const fs=require('fs'); const c=JSON.parse(fs.readFileSync('/proof/credentials.json','utf8')); const s=JSON.parse(fs.readFileSync('/proof/state.json','utf8'));
const d=db.getSiblingDB('hiring_streaming_proof_'+s.nonce); d.auth({user:'analytics_runtime',pwd:c.runtimePassword});
function denied(f) { let blocked=false; try { const r=f(); blocked=!!r&&r.ok===0&&r.code===13; } catch(e) { if(e.code!==13) throw e; blocked=true; } if(!blocked) throw Error('runtime permission exceeded'); }
d.analytics_streaming_activation.findOne({_id:'ungranted'});
d.analytics_hmac_key_retirements.findOne({_id:'absent-retirement'});
denied(()=>d.analytics_hmac_key_retirements.insertOne({_id:'forged-retirement'}));
denied(()=>d.analytics_hmac_key_retirements.updateOne({_id:'absent-retirement'},{$set:{keyId:'forged'}}));
denied(()=>d.analytics_hmac_key_retirements.deleteOne({_id:'absent-retirement'}));
denied(()=>d.analytics_streaming_activation.insertOne({_id:'forged'}));
denied(()=>d.analytics_streaming_activation.updateOne({_id:'ungranted'},{$set:{expiresAt:new Date()}}));
denied(()=>d.analytics_streaming_activation.deleteOne({_id:'ungranted'}));
const id='permission-probe-'+s.nonce; d.analytics_streaming_lakehouses.insertOne({_id:id,lakehouseId:id});
denied(()=>d.analytics_streaming_lakehouses.updateOne({_id:id},{$set:{lakehouseId:'changed'}}));
denied(()=>d.analytics_streaming_lakehouses.deleteOne({_id:id}));
denied(()=>d.users.updateOne({_id:'nonexistent-synthetic-user'},{$set:{role:'Admin'}}));
denied(()=>d.runCommand({createRole:'forged',privileges:[],roles:[]}));
denied(()=>d.runCommand({grantRolesToUser:'analytics_runtime',roles:[{role:'root',db:'admin'}]}));
d.logout(); d.auth({user:'proof_api',pwd:c.apiPassword});
d.analytics_streaming_activation.findOne({_id:'ungranted'});
denied(()=>d.analytics_streaming_activation.insertOne({_id:'forged-api-grant'}));
denied(()=>d.analytics_streaming_activation.updateOne({_id:'ungranted'},{$set:{expiresAt:new Date()}}));
denied(()=>d.analytics_streaming_activation.deleteOne({_id:'ungranted'}));
denied(()=>d.analytics_streaming_lakehouses.insertOne({_id:'forged-api-registration'}));
denied(()=>d.runCommand({create:'unauthorized_api_ddl_probe'}));
denied(()=>d.runCommand({collMod:'analytics_streaming_activation',validationLevel:'off'}));
denied(()=>d.runCommand({createRole:'forged-api-role',privileges:[],roles:[]}));
denied(()=>d.runCommand({grantRolesToUser:'proof_api',roles:[{role:'root',db:'admin'}]}));
print('STREAMING_API_PERMISSION_DENIAL_VERIFIED grantMutation=false registrationMutation=false unrelatedDDL=false roleManagement=false');
d.logout();
db.getSiblingDB('admin').auth({user:'proof_operator',pwd:c.operatorPassword}); d.analytics_streaming_lakehouses.deleteOne({_id:id});
print('STREAMING_RUNTIME_PERMISSION_DENIAL_VERIFIED grantMutation=false hmacRetirementMutation=false ownershipRewrite=false roleManagement=false');
JS
  "${compose[@]}" exec -T mongodb mongosh --quiet --file /proof/permission-denial.js >>"$log_dir/permissions.log" 2>&1
  kill -- "-$api_pid"
  wait "$api_pid" || true
  setsid java -Dcats.effect.trackFiberContext=true -Dlogback.configurationFile="$config_dir/logback.xml" -Dconfig.file="$config_dir/api-runtime.conf" -cp "$api_cp" com.example.graphQL.cats.Main >>"$log_dir/api.log" 2>&1 &
  api_pid=$!
  export HIRING_STREAMING_PROOF_API_PID="$api_pid"
  for _ in $(seq 1 120); do
    curl -fsS "http://127.0.0.1:$api_port/ready" 2>/dev/null | rg -q READY && break
    kill -0 "$api_pid" 2>/dev/null || { printf 'Restricted API startup failed; inspect private proof logs.\n' >&2; exit 1; }
    sleep 1
  done
  curl -fsS "http://127.0.0.1:$api_port/ready" >/dev/null
  python3 - <<'PY'
import json, os, pathlib, subprocess, sys
p=pathlib.Path(os.environ['HIRING_STREAMING_PROOF_CONFIG']); root=pathlib.Path(os.environ['HIRING_STREAMING_PROOF_REPO']); s=json.loads((p/'state.json').read_text()); c=json.loads((p/'credentials.json').read_text()); tokens=json.loads((p/'tokens.json').read_text()); nonce=s['nonce']; database='hiring_streaming_proof_'+nonce; q=json.dumps
def file_uri(path):
 return subprocess.check_output([sys.executable,str(root/'scripts/canonical-local-file-uri.py'),str(path)],text=True).strip()
s['apiPid']=int(os.environ['HIRING_STREAMING_PROOF_API_PID']); (p/'state.json').write_text(json.dumps(s))
for operator in (True,False):
 uri=f"mongodb://{'proof_operator' if operator else 'analytics_runtime'}:{c['operatorPassword' if operator else 'runtimePassword']}@127.0.0.1:{s['mongoPort']}/?authSource={'admin' if operator else database}&replicaSet=rs0&directConnection=true"
 conf='include classpath("application.conf")\n'+f'analytics.mongo.uri={q(uri)}\nanalytics.mongo.database={q(database)}\nanalytics.spark.master="local[2]"\nanalytics.spark.local-directory={q(str(root/".local/data/analytics/spark-temp"/f"hiring-streaming-proof-{nonce}"/"driver"))}\nanalytics.kafka.bootstrap-servers={q("127.0.0.1:"+str(s["kafkaPort"]))}\nanalytics.kafka.username="analytics_reader"\nanalytics.kafka.password={q(os.environ["KAFKA_READER_PASSWORD"])}\nanalytics.kafka.security-protocol="SASL_PLAINTEXT"\nanalytics.kafka.allow-plaintext=true\nanalytics.kafka.topic={q("hiring.streaming.proof."+nonce)}\nanalytics.lakehouse.root={q(file_uri(root/".local/data"/f"hiring-streaming-proof-{nonce}"/"lakehouse"))}\nanalytics.hmac.secret-base64={q(os.environ["HIRING_ANALYTICS_HMAC_SECRET_BASE64"])}\nanalytics.streaming.stream-id={q("hiring-streaming-proof-"+nonce)}\nanalytics.streaming.activation-grant-id={q("local-reviewed-"+nonce)}\nanalytics.streaming.checkpoint-location={q(file_uri(root/".local/data/analytics/checkpoints"/f"hiring-streaming-proof-{nonce}"/"query"))}\nanalytics.streaming.trigger-interval=10 seconds\nanalytics.streaming.maintenance-interval=60 seconds\nanalytics.streaming.max-offsets-per-trigger=1000\nanalytics.streaming.maximum-replay-records=1000\nanalytics.streaming.initial-offsets=[{{partition=0,offset=0}},{{partition=1,offset=0}},{{partition=2,offset=0}}]\nanalytics.workload-producer.username="hiring_publisher_v2"\nanalytics.workload-producer.password={q(os.environ["KAFKA_PUBLISHER_V2_PASSWORD"])}\nanalytics.workload-api.url={q("http://127.0.0.1:"+str(s["apiPort"]))}\nanalytics.workload-api.nonce={q(nonce)}\nanalytics.workload-api.password={q("proof-password-"+nonce)}\n'
 if os.environ['HIRING_STREAMING_PROOF_PROFILE']=='stage-maintenance':
  conf+='analytics.streaming.progress-retention=60 seconds\n'
 (p/('analytics-operator.conf' if operator else 'analytics-runtime.conf')).write_text(conf)
PY
  # Native erasure worker is a required account-deletion dependency, not a fabricated readiness record.
  export KAFKA_FENCER_PASSWORD
  python3 - "$repo_root" "$nonce" <<'PYWORKER'
import json, os, pathlib, re, secrets, sys, stat

def safe(path, mode):
    for parent in (path, *path.parents):
        if parent.is_symlink(): raise RuntimeError('unsafe worker path')
    info=path.stat()
    if info.st_uid!=os.getuid() or stat.S_IMODE(info.st_mode)!=mode: raise RuntimeError('unsafe worker ownership')

def create(path, content):
    with path.open('x') as out:
        os.fchmod(out.fileno(),0o600); out.write(content)

def prepare(repo,nonce):
    if not re.fullmatch('[a-f0-9]{16}',nonce): raise RuntimeError('invalid worker nonce')
    repo=pathlib.Path(repo).absolute(); p=repo/'.local/config'/('hiring-streaming-proof-'+nonce)
    safe(p,0o700)
    for name in ('state.json','credentials.json','analytics-runtime.conf'):
        safe(p/name,0o600)
        if (p/name).stat().st_size>65536: raise RuntimeError('oversized worker input')
    state=json.loads((p/'state.json').read_text()); assert state['nonce']==nonce
    assert 1024<=state['mongoPort']<=65535
    password=secrets.token_hex(32); create(p/'worker-credentials.json',json.dumps({'nonce':nonce,'workerPassword':password}))
    database='hiring_streaming_proof_'+nonce
    uri=f'mongodb://analytics_worker:{password}@127.0.0.1:{state["mongoPort"]}/?authSource={database}&replicaSet=rs0&directConnection=true'
    fencer=os.environ['KAFKA_FENCER_PASSWORD']; assert fencer and len(fencer)<=4096
    conf='include '+json.dumps(str(p/'analytics-runtime.conf'))+'\n'
    conf+='analytics.mongo.uri='+json.dumps(uri)+'\n'
    conf+='analytics.spark.local-directory='+json.dumps(str(repo/'.local/data/analytics/spark-temp'/('hiring-streaming-proof-'+nonce)/'worker'))+'\n'
    conf+='analytics.kafka.fencer.username="analytics_fencer"\nanalytics.kafka.fencer.password='+json.dumps(fencer)+'\n'
    create(p/'analytics-worker.conf',conf)
    create(p/'worker-role.js',ROLE)
    create(p/'worker-ready.js',READY)
    print('STREAMING_WORKER_FIXTURE_PREPARED privateCredentials=true leastPrivilegeRole=true nativeReadyRequired=true')

ROLE=r'''const fs=require('fs');
const c=JSON.parse(fs.readFileSync('/proof/credentials.json','utf8'));
const s=JSON.parse(fs.readFileSync('/proof/state.json','utf8'));
const w=JSON.parse(fs.readFileSync('/proof/worker-credentials.json','utf8'));
if(!/^[a-f0-9]{16}$/.test(s.nonce)||w.nonce!==s.nonce) throw new Error('worker nonce mismatch');
db.getSiblingDB('admin').auth({user:'proof_operator',pwd:c.operatorPassword});
const d=db.getSiblingDB('hiring_streaming_proof_'+s.nonce);
if(d.getUser('analytics_worker')||d.getRole('analytics_worker')) throw new Error('worker role already exists');
const actions={
 analytics_erasure_requests:['find','update'],
 analytics_worker_heartbeats:['find','insert','update'],
 analytics_erasure_completions:['find','insert','update'],
 analytics_erasure_delta_files:['find','insert','update'],
 analytics_report_snapshots:['find','insert','update'],
 analytics_report_runs:['find','insert','update'],
 analytics_report_control:['find','update'],
 analytics_lakehouse_mutexes:['find','insert','remove']
};
const privileges=Object.entries(actions).map(([n,a])=>({resource:{db:d.getName(),collection:n},actions:a}));
for(const n of ['users','outbox_subject_fences','hiring_migration_ledger','analytics_hmac_key_retirements','analytics_late_fact_replay_requests']) privileges.push({resource:{db:d.getName(),collection:n},actions:['find']});
privileges.push({resource:{db:d.getName(),collection:'event_outbox'},actions:['find','remove']});
privileges.push({resource:{db:d.getName(),collection:''},actions:['listCollections']});
d.createRole({role:'analytics_worker',privileges:privileges,roles:[]});
d.createUser({user:'analytics_worker',pwd:w.workerPassword,roles:[{role:'analytics_worker',db:d.getName()}]});
print('STREAMING_WORKER_ROLE_CREATED activationWrite=false registryWrite=false roleManagement=false outbox=find,remove');
'''
READY=r'''try {
const fs=require('fs'); const s=JSON.parse(fs.readFileSync('/proof/state.json','utf8'));
const w=JSON.parse(fs.readFileSync('/proof/worker-credentials.json','utf8'));
const owner=JSON.parse(fs.readFileSync('/proof/erasure-worker-owner.json','utf8'));
if(!/^[a-f0-9]{16}$/.test(s.nonce)||w.nonce!==s.nonce||owner.nonce!==s.nonce) throw new Error('nonce');
const d=db.getSiblingDB('hiring_streaming_proof_'+s.nonce);
if(!d.auth({user:'analytics_worker',pwd:w.workerPassword})) throw new Error('auth');
const result=d.runCommand({find:'analytics_worker_heartbeats',filter:{_id:'analytics-erasure'},projection:{state:1,updatedAt:1,leaseUntil:1},limit:1,singleBatch:true,readConcern:{level:'majority'},maxTimeMS:5000});
if(result.ok!==1) throw new Error('query');
const row=result.cursor.firstBatch[0]; const now=new Date(); const start=new Date(owner.launchedAt);
const ready=!!row && row.state==='Ready' && row.updatedAt instanceof Date && row.leaseUntil instanceof Date && row.updatedAt>=start && row.updatedAt<=now && row.leaseUntil>now;
print(ready?'STREAMING_ERASURE_WORKER_READY nativeReady=true freshLease=true':'STREAMING_ERASURE_WORKER_WAIT');
} catch (_) { print('STREAMING_ERASURE_WORKER_READINESS_FAILED'); quit(2); }
'''

prepare(sys.argv[1],sys.argv[2])
create(pathlib.Path(sys.argv[1])/".local/config"/("hiring-streaming-proof-"+sys.argv[2])/"worker-supervisor.py", r'''
import datetime, hashlib, json, os, pathlib, re, signal, stat, subprocess, sys, threading, time
MAIN='com.example.hiring.analytics.cli.AnalyticsErasureWorkerMain'
CAP=1048576

def safe(path,mode):
    for entry in (path,*path.parents):
        if entry.is_symlink(): raise RuntimeError('unsafe path')
    info=path.stat()
    if info.st_uid!=os.getuid() or stat.S_IMODE(info.st_mode)!=mode: raise RuntimeError('unsafe owner')

def read(path):
    safe(path,0o600)
    if path.stat().st_size>65536: raise RuntimeError('input bound')
    return path.read_text()

def write(path,value):
    with path.open('x') as out:
        os.fchmod(out.fileno(),0o600);out.write(json.dumps(value))

def identity(pid):
    path=pathlib.Path('/proc')/str(pid)
    if path.stat().st_uid!=os.getuid(): raise RuntimeError('process owner')
    raw=(path/'stat').read_text(); start=raw[raw.rfind(')')+2:].split()[19]
    args=(path/'cmdline').read_bytes().split(b'\0')
    return start,args

def owned_zombie(pid,expected_start):
    path=pathlib.Path('/proc')/str(pid)
    if path.stat().st_uid!=os.getuid():raise RuntimeError('process owner')
    raw=(path/'stat').read_text();fields=raw[raw.rfind(')')+2:].split()
    if fields[19]!=expected_start:raise RuntimeError('process identity changed')
    return fields[0]=='Z'

def run(repo,nonce):
    if not re.fullmatch('[a-f0-9]{16}',nonce): raise RuntimeError('nonce')
    repo=pathlib.Path(repo).absolute(); config=repo/'.local/config'/('hiring-streaming-proof-'+nonce)
    logs=repo/'.local/logs'/('hiring-streaming-proof-'+nonce)
    safe(config,0o700);safe(logs,0o700)
    state=json.loads(read(config/'state.json'));assert state['nonce']==nonce
    names=('analytics-runtime.conf','analytics-worker.conf','worker-credentials.json','worker-ready.js','state.json')
    inputs={name:read(config/name) for name in names}
    hashes={name:hashlib.sha256(value.encode()).hexdigest() for name,value in inputs.items()}
    credentials=json.loads(inputs['worker-credentials.json'])
    if credentials['nonce']!=nonce or not re.fullmatch('[a-f0-9]{64}',credentials['workerPassword']):raise RuntimeError('worker credential binding')
    worker_lines=inputs['analytics-worker.conf'].splitlines()
    uri='mongodb://analytics_worker:'+credentials['workerPassword']+'@127.0.0.1:'+str(state['mongoPort'])+'/?authSource=hiring_streaming_proof_'+nonce+'&replicaSet=rs0&directConnection=true'
    expected=['include '+json.dumps(str(config/'analytics-runtime.conf')),
      'analytics.mongo.uri='+json.dumps(uri),
      'analytics.spark.local-directory='+json.dumps(str(repo/'.local/data/analytics/spark-temp'/('hiring-streaming-proof-'+nonce)/'worker')),
      'analytics.kafka.fencer.username="analytics_fencer"']
    if len(worker_lines)!=5 or worker_lines[:4]!=expected or not worker_lines[4].startswith('analytics.kafka.fencer.password='):raise RuntimeError('worker config binding')
    if not isinstance(json.loads(worker_lines[4].split('=',1)[1]),str):raise RuntimeError('fencer binding')
    def unchanged():
        if any(hashlib.sha256(read(config/name).encode()).hexdigest()!=value for name,value in hashes.items()):raise RuntimeError('worker input changed')
    cp=read(config/'analytics.classpath').strip()
    if not cp or '\n' in cp: raise RuntimeError('classpath')
    cp_log=logs/'analytics-classpath.log';safe(cp_log,0o600)
    if cp_log.stat().st_size>1048576 or cp not in cp_log.read_text().splitlines(): raise RuntimeError('staged classpath')
    conf=config/'analytics-worker.conf'; target=logs/'erasure-worker.log'
    stop=threading.Event();overflow=threading.Event();worker=None;started=None
    def signal_stop(_signum,_frame):stop.set()
    signal.signal(signal.SIGTERM,signal_stop);signal.signal(signal.SIGINT,signal_stop)
    output=target.open('xb');os.fchmod(output.fileno(),0o600)
    try:
        launched=datetime.datetime.now(datetime.timezone.utc).isoformat()
        worker_env=os.environ.copy()
        for name in ('JAVA_TOOL_OPTIONS','JDK_JAVA_OPTIONS','_JAVA_OPTIONS'):worker_env.pop(name,None)
        worker_env['SPARK_LOCAL_IP']='127.0.0.1'
        worker=subprocess.Popen(['/usr/lib/jvm/java-17-openjdk-amd64/bin/java','-Dspark.sql.shuffle.partitions=2','-Dspark.databricks.delta.snapshotPartitions=2','--add-opens=java.base/sun.security.action=ALL-UNNAMED','-Dconfig.file='+str(conf),'-cp',cp,MAIN],cwd=repo/'analytics',stdout=subprocess.PIPE,stderr=subprocess.STDOUT,start_new_session=True,env=worker_env)
        # The Popen handle is owned before every fallible identity/ledger check.
        started,args=identity(worker.pid)
        if MAIN.encode() not in args or ('-Dconfig.file='+str(conf)).encode() not in args:raise RuntimeError('process identity')
        write(config/'erasure-worker-owner.json',{'nonce':nonce,'pid':worker.pid,'uid':os.getuid(),'startTicks':started,'main':MAIN,'config':str(conf),'launchedAt':launched,'supervisorPid':os.getpid(),'supervisorStartTicks':identity(os.getpid())[0],'pidNamespace':os.readlink('/proc/self/ns/pid')})
        def pump():
            total=0
            while True:
                chunk=worker.stdout.read(8192)
                if not chunk:return
                total+=len(chunk)
                if total>CAP:overflow.set();return
                output.write(chunk);output.flush()
        pump_thread=threading.Thread(target=pump,daemon=True);pump_thread.start()
        deadline=time.monotonic()+90;ready=False
        compose=['docker','compose','-f','compose.yaml','-f','compose.analytics-streaming-proof.yaml','-p','hiring-streaming-proof-'+nonce]
        while not stop.is_set() and time.monotonic()<deadline:
            unchanged()
            if worker.poll() is not None or overflow.is_set():raise RuntimeError('worker ended')
            current,args=identity(worker.pid)
            if current!=started or MAIN.encode() not in args or ('-Dconfig.file='+str(conf)).encode() not in args:raise RuntimeError('owner changed')
            result=subprocess.run(compose+['exec','-T','mongodb','mongosh','--quiet','--file','/proof/worker-ready.js'],cwd=repo,capture_output=True,timeout=12)
            if len(result.stdout)+len(result.stderr)>65536:raise RuntimeError('readiness output bound')
            if result.returncode:raise RuntimeError('readiness failed')
            if b'STREAMING_ERASURE_WORKER_READY nativeReady=true freshLease=true' in result.stdout.splitlines():
                current,args=identity(worker.pid)
                if current!=started or worker.poll() is not None:raise RuntimeError('owner ended')
                write(logs/'erasure-worker-ready.json',{'nonce':nonce,'nativeReady':True,'freshLease':True,'pid':worker.pid,'startTicks':started})
                print('STREAMING_ERASURE_WORKER_READY nativeReady=true freshLease=true',flush=True);ready=True;break
            stop.wait(.5)
        if not ready:raise RuntimeError('readiness deadline')
        while not stop.wait(.2):
            unchanged()
            if worker.poll() is not None or overflow.is_set():raise RuntimeError('worker ended')
            current,args=identity(worker.pid)
            if current!=started:raise RuntimeError('owner changed')
    finally:
        if worker is not None and worker.poll() is None:
            if started is None:worker.terminate()
            else:
                current,args=identity(worker.pid)
                if current!=started or MAIN.encode() not in args or ('-Dconfig.file='+str(conf)).encode() not in args:raise RuntimeError('cleanup audit required')
                worker.terminate()
            try:worker.wait(timeout=120)
            except subprocess.TimeoutExpired:raise RuntimeError('cleanup audit required')
        if worker is not None and 'pump_thread' in locals():
            pump_thread.join(timeout=5)
            if pump_thread.is_alive():raise RuntimeError('log pump still alive')
        output.close()
        if worker is not None and worker.poll() is not None:
            if target.stat().st_size>CAP:raise RuntimeError('worker log bound')
            raw=target.read_bytes()
            text=raw.decode('utf8',errors='strict')
            clean=not re.search(r'\bERROR\b|Exception|Caused by:|Dropping event from queue|numDroppedEvents',text)
            write(logs/'erasure-worker-stopped.json',{'nonce':nonce,'actualExit':worker.returncode,'joined':True,'cleanLog':clean,'logClassification':'CLEAN' if clean else 'ERROR_OR_LISTENER_DROP'})
            if not clean:raise RuntimeError('worker error log')
    if worker.returncode!=0 or overflow.is_set():raise RuntimeError('unclean worker closure')
    print('STREAMING_ERASURE_WORKER_STOPPED joined=true',flush=True)

def stop_existing(repo,nonce):
    if not re.fullmatch('[a-f0-9]{16}',nonce):raise RuntimeError('nonce')
    repo=pathlib.Path(repo).absolute();config=repo/'.local/config'/('hiring-streaming-proof-'+nonce)
    safe(config,0o700);owner_path=config/'erasure-worker-owner.json'
    if not owner_path.exists():return
    owner=json.loads(read(owner_path));assert owner['nonce']==nonce and owner['uid']==os.getuid()
    if owner.get('pidNamespace')!=os.readlink('/proc/self/ns/pid'):raise RuntimeError('process namespace mismatch')
    pid=owner['supervisorPid'];worker=owner['pid'];conf=config/'analytics-worker.conf'
    if pathlib.Path('/proc',str(pid)).exists():
        try:
            if not owned_zombie(pid,owner['supervisorStartTicks']):
                ticks,args=identity(pid)
                cwd=pathlib.Path(os.readlink('/proc/'+str(pid)+'/cwd'))
                expected=config/'worker-supervisor.py'
                scripts=[pathlib.Path(os.fsdecode(arg)) for arg in args if arg.endswith(b'worker-supervisor.py')]
                canonical=[(path if path.is_absolute() else cwd/path).absolute() for path in scripts]
                if ticks!=owner['supervisorStartTicks'] or cwd!=repo or expected not in canonical or nonce.encode() not in args:raise RuntimeError('supervisor identity changed')
                os.kill(pid,signal.SIGTERM)
        except (FileNotFoundError, ProcessLookupError):
            if pathlib.Path('/proc',str(pid)).exists() and not owned_zombie(pid,owner['supervisorStartTicks']):raise
            # The supervisor may have joined naturally. Still audit the worker below.
    deadline=time.monotonic()+130
    while time.monotonic()<deadline:
        if not pathlib.Path('/proc',str(worker)).exists():return
        try:
            if owned_zombie(worker,owner['startTicks']):
                time.sleep(.2);continue
            ticks,args=identity(worker)
        except FileNotFoundError:
            if not pathlib.Path('/proc',str(worker)).exists():return
            if owned_zombie(worker,owner['startTicks']):
                time.sleep(.2);continue
            raise
        if ticks!=owner['startTicks'] or MAIN.encode() not in args or ('-Dconfig.file='+str(conf)).encode() not in args:
            if owned_zombie(worker,owner['startTicks']):
                time.sleep(.2);continue
            raise RuntimeError('worker identity changed')
        time.sleep(.2)
    raise RuntimeError('cleanup audit required')

if __name__=='__main__':
    try:
        if sys.argv[1]=='run':run(sys.argv[2],sys.argv[3])
        elif sys.argv[1]=='stop':stop_existing(sys.argv[2],sys.argv[3])
        else:raise RuntimeError('mode')
    except BaseException:print('STREAMING_WORKER_FIXTURE_FAILED cleanupAuditRequired=true',file=sys.stderr);sys.exit(1)
''')
PYWORKER
  "${compose[@]}" exec -T mongodb mongosh --quiet --file /proof/worker-role.js >"$log_dir/worker-permissions.log" 2>&1
  java_run "$config_dir/analytics-operator.conf" com.example.hiring.analytics.cli.StreamingActivationProofMain fingerprint >"$log_dir/source-fingerprint.log" 2>&1
  java_run "$config_dir/analytics-operator.conf" com.example.hiring.analytics.cli.StreamingActivationProofMain identity >"$log_dir/runtime-identity.log" 2>&1
  printf 'Isolated authenticated proof staged: %s. Runtime remains gated. Evidence metadata: %s\n' "$nonce" "$log_dir"
elif [[ "$mode" == run || "$mode" == scenarios || "$mode" == diagnose || "$mode" == maintenance ]]; then
  [[ $# -eq 4 ]] || { printf 'Run requires acceptance JSON and independent review directory.\n' >&2; exit 2; }
  read_state
  acceptance_path="$3"
  reviews_directory="$4"
  [[ "$acceptance_path" == /* ]] || acceptance_path="$repo_root/$acceptance_path"
  [[ "$reviews_directory" == /* ]] || reviews_directory="$repo_root/$reviews_directory"
  java_run "$config_dir/analytics-operator.conf" com.example.hiring.analytics.cli.StreamingActivationProofMain \
    provision "$acceptance_path" "$reviews_directory" proof_operator 3600 >"$log_dir/provision.log" 2>&1
  run_started="$(date -u +%s)"
  stream_main=com.example.hiring.analytics.cli.HiringAnalyticsStreamingMain
  if [[ "$mode" == diagnose ]]; then stream_main=com.example.hiring.analytics.cli.StreamingCostProofMain; fi
  if [[ "$mode" == maintenance ]]; then stream_main=com.example.hiring.analytics.cli.StreamingMaintenanceProofMain; fi
  setsid bash -c 'cd "$1/analytics"; exec java -Dspark.sql.shuffle.partitions=2 -Dspark.databricks.delta.snapshotPartitions=2 --add-opens=java.base/sun.security.action=ALL-UNNAMED -Dconfig.file="$2" -Dlog4j.configurationFile="$4" -cp "$3" "$5"' \
    _ "$repo_root" "$config_dir/analytics-runtime.conf" "$analytics_cp" "$config_dir/streaming-progress-log4j2.properties" "$stream_main" >"$log_dir/stream.log" 2>&1 &
  stream_pid=$!
  worker_supervisor_pid="${worker_supervisor_pid:-}"
  run_cleanup() {
    local result=$?
    trap - EXIT
    kill -- "-$stream_pid" 2>/dev/null || true
    wait "$stream_pid" 2>/dev/null || true
    if [[ -n "$worker_supervisor_pid" ]]; then
      python3 "$config_dir/worker-supervisor.py" stop "$repo_root" "$nonce" || result=1
      wait "$worker_supervisor_pid" || result=1
    fi
    exit "$result"
  }
  trap run_cleanup EXIT
  (while kill -0 "$stream_pid" 2>/dev/null; do
    date -u '+%Y-%m-%dT%H:%M:%SZ'
    ps -p "$stream_pid" -o pid=,pcpu=,pmem=,rss=,vsz=,etime=
    sleep 5
  done) >"$log_dir/stream-resources.log" &
  resource_pid=$!
  if [[ "$mode" == maintenance ]]; then
    wait "$stream_pid"
    wait "$resource_pid" || true
    trap - EXIT
    if rg -q '\bERROR\b|Exception|Caused by:|Dropping event from queue|numDroppedEvents' "$log_dir/stream.log"; then
      printf 'Maintenance proof has an unclean runtime log.\n' >&2; exit 1;
    fi
    python3 scripts/verify-streaming-maintenance-progress.py "$log_dir/stream.log" >"$log_dir/maintenance-progress.log"
    rg -q '^STREAMING_MAINTENANCE_RETENTION_PASS ' "$log_dir/stream.log"
    printf 'Isolated streaming maintenance component passed; healthy freshness remains unmeasured: %s\n' "$log_dir"
    exit 0
  fi
  if [[ "$mode" == run ]]; then
    java_run "$config_dir/analytics-runtime.conf" com.example.hiring.analytics.cli.HiringAnalyticsStreamingWorkloadMain >"$log_dir/healthy-workload.log" 2>&1
  else
    if [[ "$mode" == scenarios ]]; then
      printf 'STREAMING_SCENARIO_SCOPE healthyFreshness=DEFERRED_BY_USER_AFTER_DEPLOYMENT healthyCadence=NOT_MEASURED burstDrainQualification=DEFERRED_BY_USER_AFTER_DEPLOYMENT functionalSeedRecords=12 functionalRestartRecords=12 overallAcceptance=OPEN\n' >"$log_dir/scenario-scope.log"
      java_run "$config_dir/analytics-runtime.conf" com.example.hiring.analytics.cli.HiringAnalyticsStreamingWorkloadMain functional >"$log_dir/scenario-seed-functional.log" 2>&1
      rg -q '^STREAMING_FUNCTIONAL_COHORT_VERIFIED records=12 partitions=3 bronzeSamples=12 reportSamples=12 privacyDenials=true latencyQualification=DEFERRED burstDrainQualification=DEFERRED$' "$log_dir/scenario-seed-functional.log"
    else
      printf 'STREAMING_SCENARIO_SCOPE healthyFreshness=NOT_MEASURED healthyCadence=NOT_MEASURED overallAcceptance=OPEN\n' >"$log_dir/scenario-scope.log"
      java_run "$config_dir/analytics-runtime.conf" com.example.hiring.analytics.cli.HiringAnalyticsStreamingWorkloadMain burst >"$log_dir/scenario-seed-burst.log" 2>&1
    fi
  fi
  if [[ "$mode" == diagnose ]]; then
    kill -- "-$stream_pid"
    wait "$stream_pid"
    wait "$resource_pid" || true
    trap - EXIT
    printf 'Streaming cost diagnostic complete; healthy freshness and overall acceptance remain OPEN: %s\n' "$log_dir"
    exit 0
  fi
  kill -0 "$stream_pid"
  python3 - "$config_dir" "$log_dir/restart-baseline.json" <<'PY'
import datetime, json, pathlib, sys, urllib.request, uuid
p=pathlib.Path(sys.argv[1]); s=json.loads((p/'state.json').read_text()); url=f'http://127.0.0.1:{s["apiPort"]}/graphql'; login={'query':'mutation($input: LoginInput!) { login(input: $input) { __typename ... on AuthSuccess { accessToken } } }','variables':{'input':{'idempotencyKey':str(uuid.uuid4()),'name':'Proof Admin '+s['nonce'],'password':'proof-password-'+s['nonce']}}}; request=urllib.request.Request(url,data=json.dumps(login).encode(),headers={'Content-Type':'application/json'})
with urllib.request.urlopen(request,timeout=15) as response: session=json.load(response)
result=session.get('data',{}).get('login',{})
if result.get('__typename')!='AuthSuccess': raise RuntimeError('Admin proof login failed')
t=result['accessToken']; day=datetime.datetime.now(datetime.timezone.utc).date(); start=str(day-datetime.timedelta(days=1))+'T00:00:00Z'; end=str(day+datetime.timedelta(days=1))+'T00:00:00Z'
query='{ analyticsReport(from: "'+start+'", to: "'+end+'") { skillPostingActivity { skill postings } } }'
request=urllib.request.Request(f'http://127.0.0.1:{s["apiPort"]}/graphql',data=json.dumps({'query':query}).encode(),headers={'Content-Type':'application/json','Authorization':'Bearer '+t})
with urllib.request.urlopen(request,timeout=15) as response: body=json.load(response)
if body.get('errors') or not body.get('data'): raise RuntimeError('Admin restart baseline unavailable')
counts={}
for row in body['data']['analyticsReport']['skillPostingActivity']: counts[row['skill']]=counts.get(row['skill'],0)+row['postings']
pathlib.Path(sys.argv[2]).write_text(json.dumps(counts))
PY
  # Graceful process reconstruction preserves the exact checkpoint and permanent registration.
  kill -- "-$stream_pid"
  wait "$stream_pid"
  wait "$resource_pid" || true
  setsid bash -c 'cd "$1/analytics"; exec java -Dspark.sql.shuffle.partitions=2 -Dspark.databricks.delta.snapshotPartitions=2 --add-opens=java.base/sun.security.action=ALL-UNNAMED -Dconfig.file="$2" -Dlog4j.configurationFile="$4" -cp "$3" com.example.hiring.analytics.cli.HiringAnalyticsStreamingMain' \
    _ "$repo_root" "$config_dir/analytics-runtime.conf" "$analytics_cp" "$config_dir/streaming-progress-log4j2.properties" >>"$log_dir/stream.log" 2>&1 &
  stream_pid=$!
  (while kill -0 "$stream_pid" 2>/dev/null; do
    date -u '+%Y-%m-%dT%H:%M:%SZ'
    ps -p "$stream_pid" -o pid=,pcpu=,pmem=,rss=,vsz=,etime=
    sleep 5
  done) >>"$log_dir/stream-resources.log" &
  resource_pid=$!
  if [[ "$mode" == scenarios ]]; then
    java_run "$config_dir/analytics-runtime.conf" com.example.hiring.analytics.cli.HiringAnalyticsStreamingWorkloadMain functional >"$log_dir/restart-functional-workload.log" 2>&1
    rg -q '^STREAMING_FUNCTIONAL_COHORT_VERIFIED records=12 partitions=3 bronzeSamples=12 reportSamples=12 privacyDenials=true latencyQualification=DEFERRED burstDrainQualification=DEFERRED$' "$log_dir/restart-functional-workload.log"
  else
    java_run "$config_dir/analytics-runtime.conf" com.example.hiring.analytics.cli.HiringAnalyticsStreamingWorkloadMain burst >"$log_dir/burst-workload.log" 2>&1
  fi
  python3 - "$config_dir" "$log_dir/restart-baseline.json" "$mode" <<'PY'
import datetime, json, pathlib, sys, urllib.request, uuid
p=pathlib.Path(sys.argv[1]); s=json.loads((p/'state.json').read_text()); url=f'http://127.0.0.1:{s["apiPort"]}/graphql'; login={'query':'mutation($input: LoginInput!) { login(input: $input) { __typename ... on AuthSuccess { accessToken } } }','variables':{'input':{'idempotencyKey':str(uuid.uuid4()),'name':'Proof Admin '+s['nonce'],'password':'proof-password-'+s['nonce']}}}; request=urllib.request.Request(url,data=json.dumps(login).encode(),headers={'Content-Type':'application/json'})
with urllib.request.urlopen(request,timeout=15) as response: session=json.load(response)
result=session.get('data',{}).get('login',{})
if result.get('__typename')!='AuthSuccess': raise RuntimeError('Admin proof login failed')
t=result['accessToken']; day=datetime.datetime.now(datetime.timezone.utc).date(); start=str(day-datetime.timedelta(days=1))+'T00:00:00Z'; end=str(day+datetime.timedelta(days=1))+'T00:00:00Z'
query='{ analyticsReport(from: "'+start+'", to: "'+end+'") { skillPostingActivity { skill postings } } }'
request=urllib.request.Request(f'http://127.0.0.1:{s["apiPort"]}/graphql',data=json.dumps({'query':query}).encode(),headers={'Content-Type':'application/json','Authorization':'Bearer '+t})
with urllib.request.urlopen(request,timeout=15) as response: body=json.load(response)
if body.get('errors') or not body.get('data'): raise RuntimeError('Admin restart verification unavailable')
counts={}
for row in body['data']['analyticsReport']['skillPostingActivity']: counts[row['skill']]=counts.get(row['skill'],0)+row['postings']
baseline=json.loads(pathlib.Path(sys.argv[2]).read_text())
if not baseline or any(counts.get(skill)!=count for skill,count in baseline.items()): raise RuntimeError('restart changed prior aggregate facts')
if sys.argv[3]=='scenarios':
 print('STREAMING_PROCESS_RESTART_VERIFIED preservedCheckpoint=true priorVisibleFactsUnchanged=true newFunctionalCohortPublished=true actualNewRecords=12 benchmarkQualification=DEFERRED')
else:
 print('STREAMING_PROCESS_RESTART_VERIFIED preservedCheckpoint=true priorVisibleFactsUnchanged=true newBurstPublished=true')
PY
  # Live admission/replay follows verified functional cohorts or the measured healthy/burst runs.
  java_run "$config_dir/analytics-runtime.conf" com.example.hiring.analytics.cli.HiringAnalyticsStreamingScenariosMain prepare >"$log_dir/live-admission.log" 2>&1
  replay_run "$config_dir/scenarios-replay.conf" "$log_dir/live-replay-first.log"
  rg -q 'late replay outcome: Published' "$log_dir/live-replay-first.log"
  replay_run "$config_dir/scenarios-replay.conf" "$log_dir/live-replay-retry.log"
  rg -q 'late replay outcome: AlreadyPublished' "$log_dir/live-replay-retry.log"
  java_run "$config_dir/analytics-runtime.conf" com.example.hiring.analytics.cli.HiringAnalyticsStreamingScenariosMain verify-replay >"$log_dir/live-replay-readback.log" 2>&1
  java_run "$config_dir/analytics-runtime.conf" com.example.hiring.analytics.cli.HiringAnalyticsStreamingScenariosMain contention >"$log_dir/live-contention.log" 2>&1
  kill -- "-$stream_pid"
  wait "$stream_pid"
  wait "$resource_pid" || true
  trap - EXIT
  [[ $(( $(date -u +%s) - run_started )) -lt 3000 ]] || {
    printf 'Checkpoint fault proof cannot run near grant expiry.\n' >&2; exit 1;
  }
  checkpoint_dir="$repo_root/.local/data/analytics/checkpoints/$project/query"
  preserved_checkpoint="$repo_root/.local/data/analytics/checkpoints/$project/query-preserved"
  [[ -d "$checkpoint_dir" && ! -L "$checkpoint_dir" && ! -e "$preserved_checkpoint" ]] || {
    printf 'Checkpoint fault proof requires the exact established task checkpoint.\n' >&2; exit 1;
  }
  checkpoint_inventory() {
    python3 - "$checkpoint_dir" "$log_dir/checkpoint-before-faults.json" "$1" <<'PY'
import hashlib, json, os, pathlib, stat, sys
root=pathlib.Path(sys.argv[1]); output=pathlib.Path(sys.argv[2]); mode=sys.argv[3]
if root.is_symlink() or not root.is_dir(): raise RuntimeError('owned checkpoint directory required')
files={}; entries=0; total=0; directories=[root]
while directories:
 with os.scandir(directories.pop()) as children:
  for child in children:
   entries+=1
   if entries>10000: raise RuntimeError('checkpoint inventory entry bound exceeded')
   path=pathlib.Path(child.path); metadata=child.stat(follow_symlinks=False)
   if stat.S_ISLNK(metadata.st_mode): raise RuntimeError('checkpoint inventory must remain literal')
   if stat.S_ISDIR(metadata.st_mode):
    directories.append(path); continue
   if not stat.S_ISREG(metadata.st_mode) or metadata.st_size>1048576: raise RuntimeError('checkpoint file must be bounded and regular')
   total+=metadata.st_size
   if total>33554432: raise RuntimeError('checkpoint inventory byte bound exceeded')
   with path.open('rb') as stream: data=stream.read(1048577)
   if len(data)!=metadata.st_size or len(data)>1048576: raise RuntimeError('checkpoint file changed during inventory')
   files[path.relative_to(root).as_posix()]=hashlib.sha256(data).hexdigest()
if mode=='capture':
 with output.open('x') as stream: json.dump(files,stream,sort_keys=True)
elif mode=='verify':
 if output.is_symlink() or not output.is_file() or output.stat().st_size>2097152: raise RuntimeError('original checkpoint inventory required')
 if json.loads(output.read_text())!=files: raise RuntimeError('checkpoint fault proof changed durable checkpoint bytes')
 print('STREAMING_CHECKPOINT_BYTES_RESTORED files='+str(len(files))+' exactHashes=true')
else: raise RuntimeError('invalid checkpoint inventory operation')
PY
  }
  checkpoint_inventory capture
  reject_runtime() {
    local output="$1" fault_mode="$2"
    (cd analytics && timeout --kill-after=10s 110s java -Dspark.sql.shuffle.partitions=2 -Dspark.databricks.delta.snapshotPartitions=2 --add-opens=java.base/sun.security.action=ALL-UNNAMED \
      -Dconfig.file="$config_dir/analytics-runtime.conf" -cp "$analytics_cp" \
      com.example.hiring.analytics.cli.StreamingCheckpointStartupProofMain "$fault_mode") >"$output" 2>&1 || return 1
    rg -q "^STREAMING_CHECKPOINT_STARTUP_REJECTED mode=$fault_mode category=CHECKPOINT_IDENTITY$" "$output"
  }
  mv -- "$checkpoint_dir" "$preserved_checkpoint"
  loss_result=0
  reject_runtime "$log_dir/checkpoint-loss.log" missing || loss_result=$?
  [[ ! -e "$checkpoint_dir" ]] || { printf 'Rejected runtime wrote a replacement checkpoint; original retained in %s\n' "$preserved_checkpoint" >&2; exit 1; }
  mv -- "$preserved_checkpoint" "$checkpoint_dir"
  [[ "$loss_result" -eq 0 ]] || { printf 'Checkpoint loss did not produce the expected fail-closed result.\n' >&2; exit 1; }
  identity_file="$checkpoint_dir/_hiring_stream_identity"
  [[ -f "$identity_file" && ! -L "$identity_file" ]] || { printf 'Established checkpoint identity is absent.\n' >&2; exit 1; }
  cp -- "$identity_file" "$config_dir/checkpoint-identity.backup"
  identity_checksum="$checkpoint_dir/._hiring_stream_identity.crc"
  [[ -f "$identity_checksum" && ! -L "$identity_checksum" ]] || { printf 'Native local checkpoint identity checksum is absent.\n' >&2; exit 1; }
  cp -- "$identity_checksum" "$config_dir/checkpoint-identity.crc.backup"
  printf 'corrupted synthetic checkpoint identity\n' >"$identity_file"
  corruption_result=0
  reject_runtime "$log_dir/checkpoint-corruption.log" corrupt-identity || corruption_result=$?
  cp -- "$config_dir/checkpoint-identity.backup" "$identity_file"
  [[ "$corruption_result" -eq 0 ]] || { printf 'Checkpoint corruption did not produce the expected fail-closed result.\n' >&2; exit 1; }
  foreign_result=0
  reject_runtime "$log_dir/checkpoint-foreign-identity.log" foreign-identity || foreign_result=$?
  cp -- "$config_dir/checkpoint-identity.backup" "$identity_file"
  cp -- "$config_dir/checkpoint-identity.crc.backup" "$identity_checksum"
  [[ "$foreign_result" -eq 0 ]] || { printf 'Foreign checkpoint identity did not produce the expected fail-closed result.\n' >&2; exit 1; }
  checkpoint_inventory verify >"$log_dir/checkpoint-byte-restoration.log"
  setsid bash -c 'cd "$1/analytics"; exec java -Dspark.sql.shuffle.partitions=2 -Dspark.databricks.delta.snapshotPartitions=2 --add-opens=java.base/sun.security.action=ALL-UNNAMED -Dconfig.file="$2" -Dlog4j.configurationFile="$4" -cp "$3" com.example.hiring.analytics.cli.HiringAnalyticsStreamingMain' \
    _ "$repo_root" "$config_dir/analytics-runtime.conf" "$analytics_cp" "$config_dir/streaming-progress-log4j2.properties" >>"$log_dir/stream.log" 2>&1 &
  stream_pid=$!
  worker_supervisor_pid="${worker_supervisor_pid:-}"
  run_cleanup() {
    local result=$?
    trap - EXIT
    kill -- "-$stream_pid" 2>/dev/null || true
    wait "$stream_pid" 2>/dev/null || true
    if [[ -n "$worker_supervisor_pid" ]]; then
      python3 "$config_dir/worker-supervisor.py" stop "$repo_root" "$nonce" || result=1
      wait "$worker_supervisor_pid" || result=1
    fi
    exit "$result"
  }
  trap run_cleanup EXIT
  java_run "$config_dir/analytics-runtime.conf" com.example.hiring.analytics.cli.HiringAnalyticsStreamingScenariosMain continuation >"$log_dir/checkpoint-restored-continuation.log" 2>&1
  rg -q '^STREAMING_CHECKPOINT_RESTORED_CONTINUATION_PASS actualNewRecords=12 adminVisible=true$' "$log_dir/checkpoint-restored-continuation.log"
  printf 'STREAMING_CHECKPOINT_FAULTS_VERIFIED missing=exactCheckpointIdentityRejection corruptedIdentity=exactCheckpointIdentityRejection foreignIdentity=exactCheckpointIdentityRejection originalCheckpointRestored=true exactCheckpointHashesRestored=true actualNewSourceAndAdminContinuation=true\n' >"$log_dir/checkpoint-faults.log"
  python3 "$config_dir/worker-supervisor.py" run "$repo_root" "$nonce" >"$log_dir/worker-supervisor.log" 2>&1 &
  worker_supervisor_pid=$!
  worker_ready_deadline=$((SECONDS + 100))
  until [[ -f "$log_dir/erasure-worker-ready.json" ]]; do
    kill -0 "$worker_supervisor_pid" 2>/dev/null || { printf 'Native erasure worker readiness failed.\n' >&2; exit 1; }
    (( SECONDS < worker_ready_deadline )) || { printf 'Native erasure worker readiness deadline exceeded.\n' >&2; exit 1; }
    sleep 0.2
  done
  # Owner deletion runs last: its durable pending marker deliberately fences later report publication.
  race_result=0
  java_run "$config_dir/scenarios-race.conf" com.example.hiring.analytics.cli.HiringAnalyticsStreamingScenariosMain race-replay >"$log_dir/live-replay-deletion-race.log" 2>&1 || race_result=$?
  rg '^STREAMING_SUPPRESSION_STAGE mode=worker-coexistence|^STREAMING_WORKER_COEXISTENCE_VERIFIED' "$log_dir/live-replay-deletion-race.log" >"$log_dir/live-worker-coexistence.log" || true
  printf 'STREAMING_REPLAY_COEXISTENCE_PROCESS_EXIT actualExit=%s\n' "$race_result" >>"$log_dir/live-worker-coexistence.log"
  [[ "$race_result" -eq 0 ]] || { printf 'Warm replay/deletion coexistence proof failed.\n' >&2; exit 1; }
  rg -q '^STREAMING_REPLAY_HTTP_DELETION_BARRIER_PASSED actualMergeBeforeDelete=true oldReservationPinned=true$' "$log_dir/live-replay-deletion-race.log"
  rg -q '^STREAMING_REPLAY_DELETION_RACE_REJECTED category=(ERASURE_PENDING|DELETION_SELECTION_REJECTED)$' "$log_dir/live-replay-deletion-race.log"
  rg -q '^STREAMING_WORKER_COEXISTENCE_VERIFIED actualRecords=12 workerRootOwnedAfterCommit=true workerLive=true ownerNaturallyReleased=true sourceAck=true terminal=ErasurePending maintenanceSucceeded=true maximumDelayMillis=300000 publicationHiddenUnchanged=true$' "$log_dir/live-worker-coexistence.log"
  java_run "$config_dir/analytics-runtime.conf" com.example.hiring.analytics.cli.HiringAnalyticsStreamingScenariosMain delete-race >"$log_dir/live-deletion-readback.log" 2>&1
  python3 "$config_dir/worker-supervisor.py" stop "$repo_root" "$nonce"
  wait "$worker_supervisor_pid"
  worker_supervisor_pid=""
  java_run "$config_dir/analytics-runtime.conf" com.example.hiring.analytics.cli.HiringAnalyticsStreamingScenariosMain suppressed-only >"$log_dir/live-suppressed-only.log" 2>&1
  rg -q '^STREAMING_SUPPRESSED_ONLY_VERIFIED actualRecords=12 sourceAck=true terminal=ErasurePending bronzeAbsent=true silverAbsent=true lateAndQuarantineAbsent=true candidateWatermarkAbsent=true publishedWatermarkUnchanged=true reportPublicationUnchanged=true receiptReserved=true adminUnavailable=true$' "$log_dir/live-suppressed-only.log"
  kill -- "-$stream_pid"
  wait "$stream_pid"
  trap - EXIT
  if rg -q '\bERROR\b|Exception|Caused by:|Dropping event from queue|numDroppedEvents' "$log_dir/stream.log"; then
    printf 'Continuous proof has an unclean runtime log.\n' >&2; exit 1;
  fi
  python3 scripts/verify-streaming-maintenance-progress.py "$log_dir/stream.log" >"$log_dir/maintenance-progress.log"
  if [[ "$mode" == run ]]; then
    printf 'Real-clock healthy and bounded burst workload passed. State and resource evidence retained: %s\n' "$log_dir"
  else
    printf 'Isolated continuous functional scenarios passed; healthy and burst-drain qualification remain deferred until after deployment. Evidence retained: %s\n' "$log_dir"
  fi
else
  read_state
  if [[ -f "$config_dir/worker-supervisor.py" ]]; then
    python3 "$config_dir/worker-supervisor.py" stop "$repo_root" "$nonce"
  fi
  api_pid="${values[3]}"
  [[ "$api_pid" =~ ^(0|[1-9][0-9]*)$ ]] || { printf 'Invalid task API process identity.\n' >&2; exit 1; }
  if [[ "$api_pid" != 0 && -r "/proc/$api_pid/cmdline" ]] && tr '\0' '\n' <"/proc/$api_pid/cmdline" | rg -Fq -- "$config_dir/api-runtime.conf"; then
    kill -- "-$api_pid" 2>/dev/null || true
  fi
  "${compose[@]}" stop
  "${compose[@]}" logs --no-color >"$log_dir/infrastructure-final.log" 2>&1
  "${compose[@]}" down --remove-orphans
  printf 'Isolated proof processes stopped and network released; task data, named volumes and private evidence preserved.\n'
fi
