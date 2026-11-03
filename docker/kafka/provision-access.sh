#!/bin/bash
# Local Compose broker provisioning: creates the hiring topics and grants each principal only the access it needs.
# Idempotent; rerun on every stack start. The broker password is read from its Compose secret file.
set -euo pipefail

umask 077
admin_properties="$(mktemp)"
trap 'rm -f "$admin_properties"' EXIT
broker_password="$(</run/secrets/kafka_broker_password)"
printf 'security.protocol=SASL_PLAINTEXT\nsasl.mechanism=PLAIN\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="broker" password="%s";\n' \
  "$broker_password" >"$admin_properties"
unset broker_password

bootstrap="kafka:9092"
topic="${KAFKA_TOPIC:-hiring.operational-events}"
# Keep these aligned with ANALYTICS_RETENTION_KAFKA_RETENTION and ANALYTICS_RETENTION_KAFKA_SEGMENT_ROLL (milliseconds).
topic_retention_ms="${KAFKA_TOPIC_RETENTION_MS:-604800000}"
topic_segment_ms="${KAFKA_TOPIC_SEGMENT_MS:-86400000}"
bin=/opt/kafka/bin

topics() { "$bin/kafka-topics.sh" --bootstrap-server "$bootstrap" --command-config "$admin_properties" "$@"; }
configs() { "$bin/kafka-configs.sh" --bootstrap-server "$bootstrap" --command-config "$admin_properties" "$@"; }
acls() { "$bin/kafka-acls.sh" --bootstrap-server "$bootstrap" --command-config "$admin_properties" "$@"; }

until topics --create --if-not-exists --topic "$topic" --partitions "${KAFKA_TOPIC_PARTITIONS:-1}" --replication-factor 1 >/dev/null 2>&1; do
  sleep 2
done
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
  topics --create --if-not-exists --topic "$interview_topic" --partitions 1 --replication-factor 1 \
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
