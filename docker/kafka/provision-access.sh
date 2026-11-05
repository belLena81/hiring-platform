#!/bin/bash
# Local Compose broker provisioning: creates the SCRAM client users and the hiring topics, and grants each principal
# only the access it needs. Idempotent; rerun on every stack start. Passwords are read from Compose secret files and
# never printed. Admin traffic uses the broker principal's PLAIN login on the INTERNAL listener (see compose.yaml).
set -euo pipefail

umask 077
admin_properties="$(mktemp)"
trap 'rm -f "$admin_properties"' EXIT
broker_password="$(</run/secrets/kafka_broker_password)"
printf 'security.protocol=SASL_PLAINTEXT\nsasl.mechanism=PLAIN\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="broker" password="%s";\n' \
  "$broker_password" >"$admin_properties"
unset broker_password

bootstrap="kafka:9092"
partitions="${KAFKA_TOPIC_PARTITIONS:-1}"
topic="${KAFKA_TOPIC:-hiring.operational-events}"
# Keep these aligned with ANALYTICS_RETENTION_KAFKA_RETENTION and ANALYTICS_RETENTION_KAFKA_SEGMENT_ROLL (milliseconds).
topic_retention_ms="${KAFKA_TOPIC_RETENTION_MS:-604800000}"
topic_segment_ms="${KAFKA_TOPIC_SEGMENT_MS:-86400000}"
bin=/opt/kafka/bin

topics() { "$bin/kafka-topics.sh" --bootstrap-server "$bootstrap" --command-config "$admin_properties" "$@"; }
configs() { "$bin/kafka-configs.sh" --bootstrap-server "$bootstrap" --command-config "$admin_properties" "$@"; }
acls() { "$bin/kafka-acls.sh" --bootstrap-server "$bootstrap" --command-config "$admin_properties" "$@"; }

# SCRAM-SHA-512 client users (the ACLs below grant them access). alter replaces the credential, so reruns are
# idempotent and a rotated secret file takes effect. The retry loop also waits for the broker to accept requests. The
# password never appears in a process argument list: it goes to the tool through an owner-only (umask 077) properties
# file via --add-config-file, removed on exit; stdout is discarded and stderr is shown, with the password redacted, if
# the wait is exhausted. Passwords must be printable ASCII (0x21-0x7E) without , = ] or \ : the tool reads the file as
# ISO-8859-1 while clients send UTF-8, so a non-ASCII password would never match. (Assumes Kafka 3.9 ConfigCommand
# accepts the SCRAM entry from --add-config-file; the file form has no brackets, unlike --add-config, and is unverified
# until the next stack start.)
scram_properties="$(mktemp)"
scram_errors="$(mktemp)"
trap 'rm -f "$admin_properties" "$scram_properties" "$scram_errors"' EXIT
scram_user() {
  local user="$1" secret_file="$2" password
  password="$(<"/run/secrets/${secret_file}")"
  # Byte-wise C locale, so [:graph:] is exactly 0x21-0x7E (no space, control, newline or non-ASCII byte).
  local LC_ALL=C
  if [[ -z "$password" || "$password" =~ [^[:graph:]] || "$password" == *,* || "$password" == *=* ||
    "$password" == *']'* || "$password" == *'\'* ]]; then
    echo "provision-access: secret ${secret_file} is empty, not printable ASCII, or contains , = ] or a backslash" >&2
    exit 65
  fi
  printf 'SCRAM-SHA-512=iterations=8192,password=%s\n' "$password" >"$scram_properties"
  # Bounded wait (60 x 2 s) for the broker; stderr is kept in an owner-only file and printed, redacted, on exhaustion
  # (the tool may echo the credential string it rejected).
  local attempt errors
  for attempt in $(seq 1 60); do
    if configs --alter --entity-type users --entity-name "$user" --add-config-file "$scram_properties" \
      >/dev/null 2>"$scram_errors"; then
      : >"$scram_properties"
      return 0
    fi
    sleep 2
  done
  echo "provision-access: creating the SCRAM credential for ${user} failed after 60 attempts; last error:" >&2
  errors="$(<"$scram_errors")"
  printf '%s\n' "${errors//"$password"/<redacted>}" >&2
  exit 70
}
scram_user hiring_publisher_v2 kafka_publisher_password
scram_user analytics_reader kafka_reader_password
scram_user analytics_fencer kafka_fencer_password
scram_user interview_command_publisher kafka_interview_orchestrator_password
scram_user interview_result_publisher kafka_interview_worker_password
scram_user interview_fencer kafka_interview_fencer_password

# Topic contract: partitions, retention, segment, max bytes and owner per topic are tabulated in
# docs/big-data-architecture.md (topic contract table). KAFKA_TOPIC_PARTITIONS applies to every topic created here;
# consumer parallelism is bounded by it.
topics --create --if-not-exists --topic "$topic" --partitions "$partitions" --replication-factor 1
# Explicit retention and segment roll: the analytics erasure budget assumes kafka-retention + kafka-segment-roll
# (application.conf analytics.operational.retention), so the topic must not inherit silent broker defaults.
configs --entity-type topics --entity-name "$topic" --alter \
  --add-config max.message.bytes=1048576,cleanup.policy=delete,retention.ms=${topic_retention_ms},segment.ms=${topic_segment_ms}

acls --add --allow-principal User:hiring_publisher_v2 --operation WRITE --operation DESCRIBE --topic "$topic"
acls --add --allow-principal User:hiring_publisher_v2 --operation IdempotentWrite --cluster
acls --add --allow-principal User:hiring_publisher_v2 --operation WRITE --operation DESCRIBE --transactional-id hiring-publisher- --resource-pattern-type prefixed

# Credential cutover: the retired nontransactional publisher keeps no grants on brokers provisioned before it.
acls --remove --force --allow-principal User:publisher --operation WRITE --operation DESCRIBE --topic "$topic"
acls --remove --force --allow-principal User:publisher --operation WRITE --operation DESCRIBE --topic hiring.phase6.runtime
acls --remove --force --allow-principal User:publisher --operation IdempotentWrite --cluster
acls --remove --force --allow-principal User:publisher --operation WRITE --operation DESCRIBE --transactional-id hiring-publisher- --resource-pattern-type prefixed

acls --add --allow-principal User:analytics_fencer --operation WRITE --operation DESCRIBE --transactional-id hiring-publisher- --resource-pattern-type prefixed
acls --add --allow-principal User:analytics_fencer --operation IdempotentWrite --cluster
acls --add --allow-principal User:analytics_reader --operation READ --operation DESCRIBE --topic "$topic"
acls --add --allow-principal User:analytics_reader --operation DESCRIBE --cluster
# The application consumer reads retention.ms and segment.ms of this one topic before it consumes (DDC-02).
acls --add --allow-principal User:analytics_reader --operation DESCRIBE_CONFIGS --topic "$topic"
for group in hiring-analytics-batch hiring-analytics-streaming-v1 hiring-operational-events-local hiring-operational-events-integration; do
  acls --add --allow-principal User:analytics_reader --operation READ --group "$group"
done

# Credential cutover: previous nontransactional publishers cannot retain topic-write authority.
for principal in interview_orchestrator interview_worker; do
  for interview_topic in hiring.interview-commands hiring.interview-results; do
    acls --remove --force --allow-principal "User:${principal}" --operation WRITE --topic "$interview_topic"
  done
done

# Interview principals can only send commands/read results or read commands/send results.
for interview_topic in hiring.interview-commands hiring.interview-results; do
  topics --create --if-not-exists --topic "$interview_topic" --partitions "$partitions" --replication-factor 1 \
    --config min.insync.replicas=1 --config cleanup.policy=delete --config retention.ms=604800000 --config max.message.bytes=65536
  configs --entity-type topics --entity-name "$interview_topic" --alter \
    --add-config min.insync.replicas=1,cleanup.policy=delete,retention.ms=604800000,max.message.bytes=65536
done
acls --add --allow-principal User:interview_command_publisher --operation WRITE --operation DESCRIBE --topic hiring.interview-commands
acls --add --allow-principal User:interview_command_publisher --operation READ --operation DESCRIBE --topic hiring.interview-results
acls --add --allow-principal User:interview_command_publisher --operation READ --group hiring-interview-orchestrator
acls --add --allow-principal User:interview_result_publisher --operation READ --operation DESCRIBE --topic hiring.interview-commands
acls --add --allow-principal User:interview_result_publisher --operation WRITE --operation DESCRIBE --topic hiring.interview-results
acls --add --allow-principal User:interview_result_publisher --operation READ --group hiring-interview-workers
acls --add --allow-principal User:interview_command_publisher --operation WRITE --operation DESCRIBE --transactional-id hiring-interview-orchestrator- --resource-pattern-type prefixed
acls --add --allow-principal User:interview_result_publisher --operation WRITE --operation DESCRIBE --transactional-id hiring-interview-worker- --resource-pattern-type prefixed
for prefix in hiring-interview-orchestrator- hiring-interview-worker-; do
  acls --add --allow-principal User:interview_fencer --operation WRITE --operation DESCRIBE --transactional-id "$prefix" --resource-pattern-type prefixed
done
for principal in interview_command_publisher interview_result_publisher interview_fencer; do
  acls --add --allow-principal "User:${principal}" --operation IdempotentWrite --cluster
done
