#!/usr/bin/env bash
set -euo pipefail
# Isolated broker; no hiring Kafka or MongoDB volume is mounted.
proof_root="${INTERVIEW_KAFKA_PROOF_ROOT:-.local/data/interview-kafka-proof}"
proof_project="${INTERVIEW_KAFKA_PROOF_PROJECT:-hiring-interview-proof}"
mkdir -p "$proof_root"
case "${1:-}" in
  start)
    git check-ignore "$proof_root/compose.yaml" >/dev/null
    python3 - "$proof_root" <<'PY'
from pathlib import Path
import os,re,secrets,socket,sys
root=Path(sys.argv[1]); source=Path('compose.yaml').read_text()
with socket.socket() as sock:
    sock.bind(('127.0.0.1',0)); port=sock.getsockname()[1]
passwords={name:secrets.token_hex(24) for name in ['KAFKA_BROKER_PASSWORD','KAFKA_PUBLISHER_V2_PASSWORD','KAFKA_READER_PASSWORD','KAFKA_FENCER_PASSWORD','KAFKA_INTERVIEW_ORCHESTRATOR_PASSWORD','KAFKA_INTERVIEW_WORKER_PASSWORD','KAFKA_INTERVIEW_FENCER_PASSWORD']}
broker=source[source.index('  kafka:\n'):source.index('  kafka-acl-init:\n')]
acl=source[source.index('  kafka-acl-init:\n'):source.index('  analytics-',source.index('  kafka-acl-init:\n'))]
# Remove original host persistence and publish only the isolated loopback listener.
broker=re.sub(r'    volumes:\n(?:      .*\n)+','',broker)
broker=broker.replace('127.0.0.1:9092',f'127.0.0.1:{port}')
broker=broker.replace('      KAFKA_LOG_RETENTION_HOURS: "168"','      KAFKA_LOG_RETENTION_HOURS: "168"\n      KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS: "1000"\n      KAFKA_LOG_SEGMENT_DELETE_DELAY_MS: "1000"')
subnet=os.environ.get('INTERVIEW_KAFKA_PROOF_SUBNET','10.249.102.0/24')
config='services:\n'+broker+acl+'networks:\n  default:\n    ipam:\n      config:\n        - subnet: '+subnet+'\n'
for name,value in passwords.items():
    config=re.sub(r'(?<!\$)\$\{'+name+r'[^}]*\}',value,config)
(root/'compose.yaml').write_text(config)
(root/'compose.yaml').chmod(0o600)
(root/'runtime.env').write_text('\n'.join(f'{key}={value}' for key,value in passwords.items())+f'\nINTERVIEW_KAFKA_BOOTSTRAP=127.0.0.1:{port}\nINTERVIEW_KAFKA_EVIDENCE=true\n')
(root/'runtime.env').chmod(0o600)
PY
    docker compose -p "$proof_project" -f "$proof_root/compose.yaml" up -d kafka kafka-acl-init
    ;;
  retention)
    docker compose -p "$proof_project" -f "$proof_root/compose.yaml" run --rm --no-deps --entrypoint /bin/bash kafka-acl-init -ec '
      printf "security.protocol=SASL_PLAINTEXT\nsasl.mechanism=PLAIN\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"broker\" password=\"%s\";\n" "$KAFKA_BROKER_PASSWORD" > /tmp/admin.properties
      for topic in hiring.interview-commands hiring.interview-results; do
        /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:9092 --command-config /tmp/admin.properties --entity-type topics --entity-name "$topic" --alter --add-config retention.ms=1500,segment.ms=1000,file.delete.delay.ms=1000
        printf "retention-probe-before\n" | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --producer.config /tmp/admin.properties --topic "$topic"
        sleep 2
        printf "retention-probe-after\n" | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --producer.config /tmp/admin.properties --topic "$topic"
        barrier=$(/opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --command-config /tmp/admin.properties --topic "$topic" --time -1 | cut -d: -f3)
        sleep 2
        printf "retention-probe-roll\n" | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --producer.config /tmp/admin.properties --topic "$topic"
        passed=false
        for attempt in $(seq 1 30); do
          beginning=$(/opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --command-config /tmp/admin.properties --topic "$topic" --time -2 | cut -d: -f3)
          if [ "$beginning" -ge "$barrier" ]; then passed=true; break; fi
          sleep 1
        done
        [ "$passed" = true ] || exit 1
        printf "%s barrier=%s beginning=%s physical-retention=PASS\n" "$topic" "$barrier" "$beginning"
        /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:9092 --command-config /tmp/admin.properties --entity-type topics --entity-name "$topic" --alter --add-config retention.ms=604800000,segment.ms=604800000,file.delete.delay.ms=60000
      done
    ' > "$proof_root/physical-retention.log" 2>&1
    cat "$proof_root/physical-retention.log"
    ;;
  stop)
    docker compose -p "$proof_project" -f "$proof_root/compose.yaml" down
    ;;
  *) printf 'Usage: %s start|retention|stop\nAfter start: source ignored runtime.env and run InterviewKafkaRestartIntegrationSpec before retention.\n' "$0"; exit 2 ;;
esac
