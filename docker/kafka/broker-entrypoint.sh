#!/bin/bash
# Local Compose broker entrypoint: builds the broker-only SASL/PLAIN JAAS settings from the broker Compose secret file
# inside the container, so the password never appears in container metadata, then hands over to the image's own
# entrypoint. Client users authenticate with SCRAM-SHA-512; their credentials live in the cluster metadata and are
# created by provision-access.sh, so no client password is read or embedded here.
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

login_module="org.apache.kafka.common.security.plain.PlainLoginModule required"
# The only PLAIN identity: the broker principal (controller listener, inter-broker and provisioning admin on INTERNAL).
server_jaas="${login_module} username=\"broker\" password=\"${broker}\" user_broker=\"${broker}\";"

# With several enabled mechanisms the broker ignores an unprefixed sasl.jaas.config, so every entry is per listener and
# mechanism. The PLAIN entries also supply the broker's own client credentials (inter-broker and controller).
export KAFKA_LISTENER_NAME_INTERNAL_PLAIN_SASL_JAAS_CONFIG="$server_jaas"
export KAFKA_LISTENER_NAME_CONTROLLER_PLAIN_SASL_JAAS_CONFIG="$server_jaas"
unset broker server_jaas

exec /__cacert_entrypoint.sh /etc/kafka/docker/run
