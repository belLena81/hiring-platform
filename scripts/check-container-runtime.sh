#!/usr/bin/env bash
# Offline checks for the container runtime scripts: shell syntax and the secret-file entrypoint contract (AC-03).
# The entrypoint only reads under /run/secrets/; the checks run a copy whose secret root is a private temporary directory.
set -euo pipefail

project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
entrypoint="$project_root/docker/load-secret-files.sh"

sh -n "$entrypoint"
bash -n "$project_root/docker/kafka/broker-entrypoint.sh"
bash -n "$project_root/docker/kafka/provision-access.sh"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
chmod 700 "$work"
secret_root="$work/secrets/"
mkdir "$secret_root"
printf 's3cret-value\n' >"$secret_root/reader_password"

if [[ "$(grep -c '^secret_root=/run/secrets/$' "$entrypoint")" != 1 ]]; then
  echo "check-container-runtime: entrypoint secret root line not found" >&2
  exit 1
fi
sed "s|^secret_root=/run/secrets/\$|secret_root=$secret_root|" "$entrypoint" >"$work/load-secret-files.sh"

fail() {
  echo "check-container-runtime: $1" >&2
  exit 1
}

# AC-03.1: NAME_FILE under the secret root becomes NAME without its trailing newline; NAME_FILE is removed.
output="$(env -i PATH="$PATH" ANALYTICS_KAFKA_PASSWORD_FILE="$secret_root/reader_password" \
  sh "$work/load-secret-files.sh" sh -c 'printf "%s|%s" "$ANALYTICS_KAFKA_PASSWORD" "${ANALYTICS_KAFKA_PASSWORD_FILE-unset}"' 2>&1)" ||
  fail "AC-03.1 exited non-zero"
[[ "$output" == "s3cret-value|unset" ]] || fail "AC-03.1 unexpected result"

# AC-03.2: a *_FILE variable outside the secret root is left untouched.
output="$(env -i PATH="$PATH" HIRING_RECOVERY_CLASSPATH_FILE=/etc/hostname sh "$work/load-secret-files.sh" \
  sh -c 'printf "%s|%s" "${HIRING_RECOVERY_CLASSPATH-unset}" "$HIRING_RECOVERY_CLASSPATH_FILE"')" ||
  fail "AC-03.2 exited non-zero"
[[ "$output" == "unset|/etc/hostname" ]] || fail "AC-03.2 unexpected result"

# AC-03.3: NAME and NAME_FILE together are rejected without echoing either value.
set +e
output="$(env -i PATH="$PATH" ANALYTICS_KAFKA_PASSWORD=inline-value \
  ANALYTICS_KAFKA_PASSWORD_FILE="$secret_root/reader_password" sh "$work/load-secret-files.sh" true 2>&1)"
status=$?
set -e
[[ $status == 64 ]] || fail "AC-03.3 expected exit 64, got $status"
[[ "$output" != *inline-value* && "$output" != *s3cret-value* ]] || fail "AC-03.3 leaked a value"

# AC-03.4: an unreadable or missing file is rejected.
set +e
env -i PATH="$PATH" ANALYTICS_KAFKA_PASSWORD_FILE="$secret_root/missing" sh "$work/load-secret-files.sh" true 2>/dev/null
status=$?
set -e
[[ $status == 66 ]] || fail "AC-03.4 expected exit 66, got $status"
if [[ "$(id -u)" != 0 ]]; then
  printf 'locked\n' >"$secret_root/locked_password"
  chmod 000 "$secret_root/locked_password"
  set +e
  env -i PATH="$PATH" ANALYTICS_KAFKA_PASSWORD_FILE="$secret_root/locked_password" sh "$work/load-secret-files.sh" true 2>/dev/null
  status=$?
  set -e
  [[ $status == 66 ]] || fail "AC-03.4 expected exit 66 for an unreadable file, got $status"
fi

# AC-03.5: an empty secret file is rejected, so a missing cloud secret fails at startup rather than in the job.
: >"$secret_root/empty_password"
set +e
env -i PATH="$PATH" ANALYTICS_KAFKA_PASSWORD_FILE="$secret_root/empty_password" sh "$work/load-secret-files.sh" true 2>/dev/null
status=$?
set -e
[[ $status == 65 ]] || fail "AC-03.5 expected exit 65, got $status"

# AC-03.6: a path that leaves the secret root through ".." is not treated as a secret.
output="$(env -i PATH="$PATH" ANALYTICS_KAFKA_PASSWORD_FILE="$secret_root../secrets/reader_password" \
  sh "$work/load-secret-files.sh" sh -c 'printf "%s" "${ANALYTICS_KAFKA_PASSWORD-unset}"')" ||
  fail "AC-03.6 exited non-zero"
[[ "$output" == "unset" ]] || fail "AC-03.6 followed a path outside the secret root"

echo "Container runtime script checks passed."