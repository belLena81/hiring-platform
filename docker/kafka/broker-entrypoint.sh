#!/bin/bash
# Local Compose broker entrypoint: builds the SASL/PLAIN JAAS settings from Compose secret files inside the container,
# so broker and client passwords never appear in container metadata, then hands over to the image's own entrypoint.
set -euo pipefail

read_secret() {
  local path="/run/secrets/$1"
  if [[ ! -r "$path" ]]; then
    echo "broker-entrypoint: secret $1 is not readable" >&2
    exit 66
  fi
  local value
  value="$(<"$path")"
  if [[ -z "$value" || "$value" == *'"'* || "$value" == *'\'* ]]; then
    echo "broker-entrypoint: secret $1 is empty or contains a quote or backslash" >&2
    exit 65
  fi
  printf '%s' "$value"
}

broker="$(read_secret kafka_broker_password)"
publisher="$(read_secret kafka_publisher_password)"
reader="$(read_secret kafka_reader_password)"
fencer="$(read_secret kafka_fencer_password)"
orchestrator="$(read_secret kafka_interview_orchestrator_password)"
worker="$(read_secret kafka_interview_worker_password)"
interview_fencer="$(read_secret kafka_interview_fencer_password)"

login_module="org.apache.kafka.common.security.plain.PlainLoginModule required"
users="user_broker=\"${broker}\" user_hiring_publisher_v2=\"${publisher}\" user_analytics_reader=\"${reader}\""
users+=" user_analytics_fencer=\"${fencer}\" user_interview_command_publisher=\"${orchestrator}\""
users+=" user_interview_result_publisher=\"${worker}\" user_interview_fencer=\"${interview_fencer}\""
server_jaas="${login_module} username=\"broker\" password=\"${broker}\" ${users};"

export KAFKA_SASL_JAAS_CONFIG="${login_module} username=\"broker\" password=\"${broker}\";"
export KAFKA_LISTENER_NAME_INTERNAL_PLAIN_SASL_JAAS_CONFIG="$server_jaas"
export KAFKA_LISTENER_NAME_EXTERNAL_PLAIN_SASL_JAAS_CONFIG="$server_jaas"
export KAFKA_LISTENER_NAME_CONTROLLER_PLAIN_SASL_JAAS_CONFIG="$server_jaas"
unset broker publisher reader fencer orchestrator worker interview_fencer users server_jaas

exec /__cacert_entrypoint.sh /etc/kafka/docker/run
