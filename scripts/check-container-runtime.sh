#!/usr/bin/env bash
# Offline checks for the container runtime scripts: shell syntax and the secret-file entrypoint contract (AC-03).
# The entrypoint only reads under /run/secrets/; the checks run a copy whose secret root is a private temporary directory.
# Contract: see "Operator scripts" in docs/engineering-quality.md. Run with --help for usage and exit codes.
set -Eeuo pipefail

readonly total_checks=10
project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
entrypoint="$project_root/docker/load-secret-files.sh"
started="$SECONDS"
checks_passed=0
work=""
result_done=0
current_check="setup"

case "${1:-}" in
  -h|--help)
    cat <<'HELP'
Usage: scripts/check-container-runtime.sh
       scripts/check-container-runtime.sh --help

Offline checks (no Docker, no network): shell syntax of the docker/ entrypoints and the secret-file loader contract
AC-03.1 to AC-03.6 (runs a copy of docker/load-secret-files.sh against a private temporary directory that this script
creates and removes). Takes about 1-3 seconds, so it prints no progress lines; each check is printed as it passes.

Exit codes: 0 PASS, 1 FAIL, 2 usage error or precondition not met (BLOCKED), 130 interrupted.
The last line is "RESULT: PASS|FAIL|BLOCKED checks=<passed>/<total> duration=<s>".

Example:
  scripts/check-container-runtime.sh
HELP
    exit 0 ;;
  "") ;;
  *)
    printf 'check-container-runtime: unexpected argument: %s\n' "$1" >&2
    printf 'check-container-runtime: run scripts/check-container-runtime.sh --help for usage\n' >&2
    printf 'RESULT: BLOCKED reason=usage checks=0/%s duration=0s\n' "$total_checks" >&2
    exit 2 ;;
esac

cleanup() { if [[ -n "$work" ]]; then rm -rf "$work"; fi; }
on_interrupt() {
  trap '' INT TERM HUP
  printf '\ncheck-container-runtime: interrupted during "%s" after %ss; %s of %s checks had passed; removing only its own temporary directory\n' \
    "$current_check" "$((SECONDS - started))" "$checks_passed" "$total_checks" >&2
  result_done=1
  exit 130
}
on_exit() {
  local code=$?
  cleanup
  if (( code != 0 && result_done == 0 )); then
    printf 'check-container-runtime: unexpected failure during "%s" (exit %s)\n' "$current_check" "$code" >&2
    printf 'check-container-runtime: next: run the failing command by hand (sh -n docker/load-secret-files.sh, bash -n docker/kafka/*.sh)\n' >&2
    printf 'RESULT: FAIL checks=%s/%s duration=%ss\n' "$checks_passed" "$total_checks" "$((SECONDS - started))" >&2
  fi
}
trap on_interrupt INT TERM HUP
trap on_exit EXIT

printf 'check-container-runtime: offline syntax and secret-loader checks (%s checks, no parameters); expected 1-3 s\n' "$total_checks"

fail() {
  printf 'check-container-runtime: failed: %s\n' "$1" >&2
  printf 'check-container-runtime: next: compare docker/load-secret-files.sh with the AC-03 criteria in the container runtime specification\n' >&2
  printf 'RESULT: FAIL checks=%s/%s failed="%s" duration=%ss\n' "$checks_passed" "$total_checks" "$current_check" "$((SECONDS - started))" >&2
  result_done=1
  exit 1
}
check() { current_check="$1"; }
passed() { checks_passed=$((checks_passed + 1)); printf '  ok  %s\n' "$current_check"; }

check "syntax of docker/load-secret-files.sh"
sh -n "$entrypoint" || fail "syntax error in docker/load-secret-files.sh"
passed
check "syntax of docker/kafka/broker-entrypoint.sh"
bash -n "$project_root/docker/kafka/broker-entrypoint.sh" || fail "syntax error in docker/kafka/broker-entrypoint.sh"
passed
check "syntax of docker/kafka/provision-access.sh"
bash -n "$project_root/docker/kafka/provision-access.sh" || fail "syntax error in docker/kafka/provision-access.sh"
passed

work="$(mktemp -d)"
chmod 700 "$work"
secret_root="$work/secrets/"
mkdir "$secret_root"
printf 's3cret-value\n' >"$secret_root/reader_password"

check "entrypoint secret root line"
if [[ "$(grep -c '^secret_root=/run/secrets/$' "$entrypoint")" != 1 ]]; then
  fail "entrypoint secret root line not found"
fi
sed "s|^secret_root=/run/secrets/\$|secret_root=$secret_root|" "$entrypoint" >"$work/load-secret-files.sh"
passed

# AC-03.1: NAME_FILE under the secret root becomes NAME without its trailing newline; NAME_FILE is removed.
check "AC-03.1 NAME_FILE is loaded into NAME"
output="$(env -i PATH="$PATH" ANALYTICS_KAFKA_PASSWORD_FILE="$secret_root/reader_password" \
  sh "$work/load-secret-files.sh" sh -c 'printf "%s|%s" "$ANALYTICS_KAFKA_PASSWORD" "${ANALYTICS_KAFKA_PASSWORD_FILE-unset}"' 2>&1)" ||
  fail "AC-03.1 exited non-zero"
[[ "$output" == "s3cret-value|unset" ]] || fail "AC-03.1 unexpected result"
passed

# AC-03.2: a *_FILE variable outside the secret root is left untouched.
check "AC-03.2 files outside the secret root are untouched"
output="$(env -i PATH="$PATH" HIRING_RECOVERY_CLASSPATH_FILE=/etc/hostname sh "$work/load-secret-files.sh" \
  sh -c 'printf "%s|%s" "${HIRING_RECOVERY_CLASSPATH-unset}" "$HIRING_RECOVERY_CLASSPATH_FILE"')" ||
  fail "AC-03.2 exited non-zero"
[[ "$output" == "unset|/etc/hostname" ]] || fail "AC-03.2 unexpected result"
passed

# AC-03.3: NAME and NAME_FILE together are rejected without echoing either value.
check "AC-03.3 NAME with NAME_FILE is rejected without echoing values"
set +e
output="$(env -i PATH="$PATH" ANALYTICS_KAFKA_PASSWORD=inline-value \
  ANALYTICS_KAFKA_PASSWORD_FILE="$secret_root/reader_password" sh "$work/load-secret-files.sh" true 2>&1)"
status=$?
set -e
[[ $status == 64 ]] || fail "AC-03.3 expected exit 64, got $status"
[[ "$output" != *inline-value* && "$output" != *s3cret-value* ]] || fail "AC-03.3 leaked a value"
passed

# AC-03.4: an unreadable or missing file is rejected.
check "AC-03.4 missing or unreadable file is rejected"
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
passed

# AC-03.5: an empty secret file is rejected, so a missing cloud secret fails at startup rather than in the job.
check "AC-03.5 empty secret file is rejected"
: >"$secret_root/empty_password"
set +e
env -i PATH="$PATH" ANALYTICS_KAFKA_PASSWORD_FILE="$secret_root/empty_password" sh "$work/load-secret-files.sh" true 2>/dev/null
status=$?
set -e
[[ $status == 65 ]] || fail "AC-03.5 expected exit 65, got $status"
passed

# AC-03.6: a path that leaves the secret root through ".." is not treated as a secret.
check "AC-03.6 path leaving the secret root is not a secret"
output="$(env -i PATH="$PATH" ANALYTICS_KAFKA_PASSWORD_FILE="$secret_root../secrets/reader_password" \
  sh "$work/load-secret-files.sh" sh -c 'printf "%s" "${ANALYTICS_KAFKA_PASSWORD-unset}"')" ||
  fail "AC-03.6 exited non-zero"
[[ "$output" == "unset" ]] || fail "AC-03.6 followed a path outside the secret root"
passed

result_done=1
printf 'RESULT: PASS checks=%s/%s duration=%ss\n' "$checks_passed" "$total_checks" "$((SECONDS - started))"
