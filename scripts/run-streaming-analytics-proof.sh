#!/usr/bin/env bash
set -euo pipefail
umask 077

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
mode="${1:-stage}"
nonce="${2:-$(openssl rand -hex 8)}"
[[ "$nonce" =~ ^[a-f0-9]{16}$ && ( "$mode" == stage || "$mode" == run || "$mode" == scenarios || "$mode" == stop ) ]] || {
  printf 'Usage: %s stage [nonce] | run nonce acceptance.json review-directory | scenarios nonce acceptance.json review-directory | stop nonce\n' "$0" >&2; exit 2;
}
[[ -r .env ]] || { printf 'Ignored local .env credentials are required.\n' >&2; exit 1; }
. ./.env
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
export SPARK_LOCAL_IP=127.0.0.1
unset JAVA_TOOL_OPTIONS
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
java_run() {
  local conf="$1" main="$2"; shift 2
  (cd analytics && java -Dspark.sql.shuffle.partitions=2 -Dspark.databricks.delta.snapshotPartitions=2 --add-opens=java.base/sun.security.action=ALL-UNNAMED \
    -Dconfig.file="$conf" -cp "$analytics_cp" "$main" "$@")
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
  if [[ "$mode" == run || "$mode" == scenarios ]]; then analytics_cp="$(cat "$config_dir/analytics.classpath")"; fi
}

if [[ "$mode" == stage ]]; then
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
import base64, json, os, pathlib, secrets
root=pathlib.Path(os.environ['HIRING_STREAMING_PROOF_REPO']); nonce=os.environ['HIRING_STREAMING_PROOF_NONCE']; p=pathlib.Path(os.environ['HIRING_STREAMING_PROOF_CONFIG'])
credentials={'operatorPassword':os.environ['HIRING_STREAMING_PROOF_MONGO_PASSWORD'],'runtimePassword':secrets.token_hex(32),'apiPassword':secrets.token_hex(32),'jwtSecret':secrets.token_hex(32)}
(p/'credentials.json').write_text(json.dumps(credentials)); (p/'mongo-keyfile').write_text(base64.b64encode(secrets.token_bytes(756)).decode())
mongo=int(os.environ['HIRING_STREAMING_PROOF_MONGO_PORT']); kafka=int(os.environ['HIRING_STREAMING_PROOF_KAFKA_PORT']); api=int(os.environ['HIRING_STREAMING_PROOF_API_PORT']); database='hiring_streaming_proof_'+nonce; topic='hiring.streaming.proof.'+nonce
state={'nonce':nonce,'mongoPort':mongo,'kafkaPort':kafka,'apiPort':api,'apiPid':0}; (p/'state.json').write_text(json.dumps(state))
q=json.dumps
def mongo_uri(operator): return f"mongodb://{'proof_operator' if operator else 'proof_api'}:{credentials['operatorPassword' if operator else 'apiPassword']}@127.0.0.1:{mongo}/?authSource={'admin' if operator else database}&replicaSet=rs0&directConnection=true"
for operator in (True,False):
 conf='include classpath("application.conf")\n'+f'mongo.uri={q(mongo_uri(operator))}\nmongo.database={q(database)}\nmongo.reset-on-start=false\nhttp.host="127.0.0.1"\nhttp.port={api}\nauth.jwt.hs256-secret={q(credentials["jwtSecret"])}\nkafka.enabled=true\nkafka.bootstrap-servers={q("127.0.0.1:"+str(kafka))}\nkafka.sasl-security-protocol="SASL_PLAINTEXT"\nkafka.topic={q(topic)}\nkafka.publisher.sasl-username="hiring_publisher_v2"\nkafka.publisher.sasl-password={q(os.environ["KAFKA_PUBLISHER_V2_PASSWORD"])}\n'
 (p/('api-operator.conf' if operator else 'api-runtime.conf')).write_text(conf)
for file in p.iterdir(): file.chmod(0o600)
PY
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
for field,role in [('bootstrapAdmin','Admin'),('signUp','Candidate'),('signUp','Recruiter')]:
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
 (p/('analytics-operator.conf' if operator else 'analytics-runtime.conf')).write_text(conf)
PY
  java_run "$config_dir/analytics-operator.conf" com.example.hiring.analytics.cli.StreamingActivationProofMain fingerprint >"$log_dir/source-fingerprint.log" 2>&1
  java_run "$config_dir/analytics-operator.conf" com.example.hiring.analytics.cli.StreamingActivationProofMain identity >"$log_dir/runtime-identity.log" 2>&1
  printf 'Isolated authenticated proof staged: %s. Runtime remains gated. Evidence metadata: %s\n' "$nonce" "$log_dir"
elif [[ "$mode" == run || "$mode" == scenarios ]]; then
  [[ $# -eq 4 ]] || { printf 'Run requires acceptance JSON and independent review directory.\n' >&2; exit 2; }
  read_state
  java_run "$config_dir/analytics-operator.conf" com.example.hiring.analytics.cli.StreamingActivationProofMain \
    provision "$3" "$4" proof_operator 3600 >"$log_dir/provision.log" 2>&1
  run_started="$(date -u +%s)"
  setsid bash -c 'cd "$1/analytics"; exec java -Dspark.sql.shuffle.partitions=2 -Dspark.databricks.delta.snapshotPartitions=2 --add-opens=java.base/sun.security.action=ALL-UNNAMED -Dconfig.file="$2" -cp "$3" com.example.hiring.analytics.cli.HiringAnalyticsStreamingMain' \
    _ "$repo_root" "$config_dir/analytics-runtime.conf" "$analytics_cp" >"$log_dir/stream.log" 2>&1 &
  stream_pid=$!
  trap 'kill -- "-$stream_pid" 2>/dev/null || true; wait "$stream_pid" 2>/dev/null || true' EXIT
  (while kill -0 "$stream_pid" 2>/dev/null; do
    date -u '+%Y-%m-%dT%H:%M:%SZ'
    ps -p "$stream_pid" -o pid=,pcpu=,pmem=,rss=,vsz=,etime=
    sleep 5
  done) >"$log_dir/stream-resources.log" &
  resource_pid=$!
  if [[ "$mode" == run ]]; then
    java_run "$config_dir/analytics-runtime.conf" com.example.hiring.analytics.cli.HiringAnalyticsStreamingWorkloadMain >"$log_dir/healthy-workload.log" 2>&1
  else
    printf 'STREAMING_SCENARIO_SCOPE healthyFreshness=NOT_MEASURED healthyCadence=NOT_MEASURED overallAcceptance=OPEN\n' >"$log_dir/scenario-scope.log"
    java_run "$config_dir/analytics-runtime.conf" com.example.hiring.analytics.cli.HiringAnalyticsStreamingWorkloadMain burst >"$log_dir/scenario-seed-burst.log" 2>&1
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
  wait "$stream_pid" || true
  wait "$resource_pid" || true
  setsid bash -c 'cd "$1/analytics"; exec java -Dspark.sql.shuffle.partitions=2 -Dspark.databricks.delta.snapshotPartitions=2 --add-opens=java.base/sun.security.action=ALL-UNNAMED -Dconfig.file="$2" -cp "$3" com.example.hiring.analytics.cli.HiringAnalyticsStreamingMain' \
    _ "$repo_root" "$config_dir/analytics-runtime.conf" "$analytics_cp" >>"$log_dir/stream.log" 2>&1 &
  stream_pid=$!
  (while kill -0 "$stream_pid" 2>/dev/null; do
    date -u '+%Y-%m-%dT%H:%M:%SZ'
    ps -p "$stream_pid" -o pid=,pcpu=,pmem=,rss=,vsz=,etime=
    sleep 5
  done) >>"$log_dir/stream-resources.log" &
  resource_pid=$!
  java_run "$config_dir/analytics-runtime.conf" com.example.hiring.analytics.cli.HiringAnalyticsStreamingWorkloadMain burst >"$log_dir/burst-workload.log" 2>&1
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
if body.get('errors') or not body.get('data'): raise RuntimeError('Admin restart verification unavailable')
counts={}
for row in body['data']['analyticsReport']['skillPostingActivity']: counts[row['skill']]=counts.get(row['skill'],0)+row['postings']
baseline=json.loads(pathlib.Path(sys.argv[2]).read_text())
if not baseline or any(counts.get(skill)!=count for skill,count in baseline.items()): raise RuntimeError('restart changed prior aggregate facts')
print('STREAMING_PROCESS_RESTART_VERIFIED preservedCheckpoint=true priorVisibleFactsUnchanged=true newBurstPublished=true')
PY
  # Live admission/replay scenarios follow the separately measured healthy/burst runs.
  java_run "$config_dir/analytics-runtime.conf" com.example.hiring.analytics.cli.HiringAnalyticsStreamingScenariosMain prepare >"$log_dir/live-admission.log" 2>&1
  java_run "$config_dir/scenarios-replay.conf" com.example.hiring.analytics.cli.HiringAnalyticsLateFactReplayMain >"$log_dir/live-replay-first.log" 2>&1
  rg -q 'late replay outcome: Published' "$log_dir/live-replay-first.log"
  java_run "$config_dir/scenarios-replay.conf" com.example.hiring.analytics.cli.HiringAnalyticsLateFactReplayMain >"$log_dir/live-replay-retry.log" 2>&1
  rg -q 'late replay outcome: AlreadyPublished' "$log_dir/live-replay-retry.log"
  java_run "$config_dir/analytics-runtime.conf" com.example.hiring.analytics.cli.HiringAnalyticsStreamingScenariosMain verify-replay >"$log_dir/live-replay-readback.log" 2>&1
  java_run "$config_dir/analytics-runtime.conf" com.example.hiring.analytics.cli.HiringAnalyticsStreamingScenariosMain contention >"$log_dir/live-contention.log" 2>&1
  kill -- "-$stream_pid"
  wait "$stream_pid" || true
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
  setsid bash -c 'cd "$1/analytics"; exec java -Dspark.sql.shuffle.partitions=2 -Dspark.databricks.delta.snapshotPartitions=2 --add-opens=java.base/sun.security.action=ALL-UNNAMED -Dconfig.file="$2" -cp "$3" com.example.hiring.analytics.cli.HiringAnalyticsStreamingMain' \
    _ "$repo_root" "$config_dir/analytics-runtime.conf" "$analytics_cp" >>"$log_dir/stream.log" 2>&1 &
  stream_pid=$!
  trap 'kill -- "-$stream_pid" 2>/dev/null || true; wait "$stream_pid" 2>/dev/null || true' EXIT
  java_run "$config_dir/analytics-runtime.conf" com.example.hiring.analytics.cli.HiringAnalyticsStreamingScenariosMain continuation >"$log_dir/checkpoint-restored-continuation.log" 2>&1
  rg -q '^STREAMING_CHECKPOINT_RESTORED_CONTINUATION_PASS actualNewRecords=12 adminVisible=true$' "$log_dir/checkpoint-restored-continuation.log"
  printf 'STREAMING_CHECKPOINT_FAULTS_VERIFIED missing=exactCheckpointIdentityRejection corruptedIdentity=exactCheckpointIdentityRejection foreignIdentity=exactCheckpointIdentityRejection originalCheckpointRestored=true exactCheckpointHashesRestored=true actualNewSourceAndAdminContinuation=true\n' >"$log_dir/checkpoint-faults.log"
  # Owner deletion runs last: its durable pending marker deliberately fences later report publication.
  java_run "$config_dir/scenarios-race.conf" com.example.hiring.analytics.cli.HiringAnalyticsStreamingScenariosMain race-replay >"$log_dir/live-replay-deletion-race.log" 2>&1
  rg -q '^STREAMING_REPLAY_HTTP_DELETION_BARRIER_PASSED actualMergeBeforeDelete=true oldReservationPinned=true$' "$log_dir/live-replay-deletion-race.log"
  rg -q '^STREAMING_REPLAY_DELETION_RACE_REJECTED category=(ERASURE_PENDING|DELETION_SELECTION_REJECTED)$' "$log_dir/live-replay-deletion-race.log"
  java_run "$config_dir/analytics-runtime.conf" com.example.hiring.analytics.cli.HiringAnalyticsStreamingScenariosMain delete-race >"$log_dir/live-deletion-readback.log" 2>&1
  kill -- "-$stream_pid"
  wait "$stream_pid" || true
  trap - EXIT
  if [[ "$mode" == run ]]; then
    printf 'Real-clock healthy and bounded burst workload passed. State and resource evidence retained: %s\n' "$log_dir"
  else
    printf 'Isolated continuous scenarios passed; healthy-load acceptance was NOT MEASURED. Evidence retained: %s\n' "$log_dir"
  fi
else
  read_state
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
