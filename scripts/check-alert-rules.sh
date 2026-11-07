#!/usr/bin/env bash
# Checks and unit-tests the local Prometheus alert rules with promtool (spec: docs/specs/operational-alert-verification.md, OAV-01).
# promtool comes from the pinned prom/prometheus image through the alert-rules service (profile alert-tests) in
# compose.observability.yaml: no network, read-only mounts of docker/observability/alerts.yaml and alerts.test.yaml.
# Needs Docker, so a human runs it outside the agent sandbox. It never starts the observability stack and never commits.
# Exit codes: 0 PASS, 1 FAIL (rules or tests rejected), 2 BLOCKED (Docker or a file is missing, or usage error).
set -Eeuo pipefail
umask 077
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$repo_root"

if [[ $# -gt 0 ]]; then
  case "$1" in
    -h|--help) sed -n '2,6p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) printf 'check-alert-rules: unexpected argument: %s (no arguments are accepted; try --help)\n' "$1" >&2; exit 2 ;;
  esac
fi

for f in compose.yaml compose.observability.yaml docker/observability/alerts.yaml docker/observability/alerts.test.yaml; do
  [[ -f "$f" ]] || { printf 'check-alert-rules: missing file %s\nRESULT: BLOCKED\n' "$f" >&2; exit 2; }
done
command -v docker >/dev/null 2>&1 || { printf 'check-alert-rules: docker not found on PATH\nRESULT: BLOCKED\n' >&2; exit 2; }

compose=(docker compose -f compose.yaml -f compose.observability.yaml --profile alert-tests)
run_promtool() { "${compose[@]}" run --rm --no-deps alert-rules "$@"; }

status=PASS
printf 'check-alert-rules: promtool check rules\n'
run_promtool check rules /rules/alerts.yaml || status=FAIL
printf 'check-alert-rules: promtool test rules\n'
run_promtool test rules /rules/alerts.test.yaml || status=FAIL

if [[ "$status" == PASS ]]; then
  printf 'RESULT: PASS promtool check rules and promtool test rules\n'
else
  printf 'RESULT: FAIL read the promtool output above (a failing test names its OAV-01 case)\n' >&2
  exit 1
fi