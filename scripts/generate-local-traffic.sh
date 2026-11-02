#!/usr/bin/env bash
# Sends unauthenticated, read-only requests to a local API so the Grafana dashboard has data.
# Mix: health checks, a trivial GraphQL query, and an invalid query (expected 4xx). No credentials or personal data are sent.
# Contract: see "Operator scripts" in docs/engineering-quality.md. Run with --help for usage and exit codes.
set -euo pipefail

readonly default_seconds=60
readonly default_base="http://127.0.0.1:8080"
readonly max_seconds=3600
readonly request_timeout=3 # seconds per request, connect included
readonly heartbeat=7       # seconds between progress lines; plus one in-flight request this keeps every gap <= 10 s

help_text() {
  cat <<HELP
Usage: scripts/generate-local-traffic.sh [seconds] [base-url]
       scripts/generate-local-traffic.sh --help

Repeats a cycle of three read-only requests (health, a trivial GraphQL query, an invalid query) against a local API.
It first checks that /health answers; if not, it stops with BLOCKED.
Runs for exactly [seconds] (default $default_seconds, 1-$max_seconds) and always ends within about 1 s of that, even if a request hangs
(each request is limited to $request_timeout s). Prints progress every ${heartbeat}s.

Parameters:
  seconds   duration of the run in seconds (default $default_seconds)
  base-url  API root without credentials (default $default_base); http or https only

Result categories: ok (2xx/3xx), 4xx (the invalid query is expected to return one per cycle), 5xx, failed (no HTTP answer: refused, timeout).
Exit codes: 0 PASS (no 5xx, no failed, at least one ok), 1 FAIL, 2 usage error or API unreachable (BLOCKED), 130 interrupted.
The last line is "RESULT: PASS|FAIL|BLOCKED ..." with request counts and duration.

Examples:
  scripts/generate-local-traffic.sh 120
  scripts/generate-local-traffic.sh 15 http://127.0.0.1:8080
HELP
}

case "${1:-}" in
  -h|--help) help_text; exit 0 ;;
esac

duration="${1:-$default_seconds}"
base="${2:-$default_base}"

usage_error() {
  printf 'generate-local-traffic: %s\n' "$1" >&2
  printf 'generate-local-traffic: run scripts/generate-local-traffic.sh --help for usage\n' >&2
  printf 'RESULT: BLOCKED reason=usage ok=0 4xx=0 5xx=0 failed=0 duration=0s\n' >&2
  exit 2
}

(( $# <= 2 )) || usage_error "expected at most two arguments, got $#"
[[ "$duration" =~ ^[0-9]{1,5}$ ]] && (( 10#$duration >= 1 && 10#$duration <= max_seconds )) || usage_error "seconds must be an integer from 1 to $max_seconds, got: $duration"
duration=$((10#$duration))
[[ "$base" =~ ^https?://[A-Za-z0-9._:-]+(/[A-Za-z0-9._~/-]*)?$ ]] || usage_error "base-url must be http(s)://host[:port][/path] without credentials, query or spaces"
base="${base%/}"

ok=0 c4xx=0 c5xx=0 failed=0
started="$SECONDS"
last_code="none"

counts() { printf 'ok=%d 4xx=%d 5xx=%d failed=%d' "$ok" "$c4xx" "$c5xx" "$failed"; }
elapsed() { printf '%ds' $((SECONDS - started)); }

on_interrupt() {
  trap '' INT TERM HUP
  printf '\ngenerate-local-traffic: interrupted after %s of %ss; completed requests: %s (nothing to clean up)\n' "$(elapsed)" "$duration" "$(counts)" >&2
  exit 130
}
trap on_interrupt INT TERM HUP

next_beat=0
# Progress line at most every $heartbeat seconds; checked after every request so a slow API cannot silence it.
maybe_beat() {
  if (( SECONDS >= next_beat )); then
    printf '[%3ds/%ds] %s (last status %s)\n' $((SECONDS - started)) "$duration" "$(counts)" "$last_code"
    next_beat=$((SECONDS + heartbeat))
  fi
}

# request METHOD-ARGS...: one bounded curl; classifies the HTTP status into the counters.
request() {
  local remaining limit code
  remaining=$((started + duration - SECONDS))
  limit=$(( remaining < request_timeout ? remaining : request_timeout ))
  (( limit >= 1 )) || limit=1
  code="$(curl -sS -o /dev/null -w '%{http_code}' --connect-timeout 2 --max-time "$limit" "$@" 2>/dev/null)" || code=000
  last_code="$code"
  case "$code" in
    2??|3??) ok=$((ok + 1)) ;;
    4??) c4xx=$((c4xx + 1)) ;;
    5??) c5xx=$((c5xx + 1)) ;;
    *) failed=$((failed + 1)) ;;
  esac
  maybe_beat
}

finish() { # RESULT code detail
  local line="RESULT: $1 $(counts) $3 duration=$(elapsed) base=$base"
  if [[ "$1" == PASS ]]; then printf '%s\n' "$line"; else printf '%s\n' "$line" >&2; fi
  exit "$2"
}

printf 'generate-local-traffic: sending read-only requests to %s for %ss (defaults: %ss, %s); progress every %ss; each request limited to %ss\n' \
  "$base" "$duration" "$default_seconds" "$default_base" "$heartbeat" "$request_timeout"

# Precondition: the API must answer on /health, otherwise nothing useful can be generated.
health_code="$(curl -sS -o /dev/null -w '%{http_code}' --connect-timeout 2 --max-time "$request_timeout" "$base/health" 2>/dev/null)" || health_code=000
if [[ "$health_code" == 000 ]]; then
  printf 'generate-local-traffic: no HTTP answer from %s/health within %ss\n' "$base" "$request_timeout" >&2
  printf 'generate-local-traffic: next: start the API (docker compose -f compose.yaml -f compose.observability.yaml --profile api up -d --build, or sbt run) and check the base-url and port\n' >&2
  finish BLOCKED 2 "reason=api-unreachable"
fi

next_beat=$((started + heartbeat))
end=$((started + duration))
while (( SECONDS < end )); do
  request "$base/health"
  (( SECONDS < end )) || break
  request -H 'Content-Type: application/json' -d '{"query":"{ __typename }"}' "$base/graphql"
  (( SECONDS < end )) || break
  request -H 'Content-Type: application/json' -d '{"query":"{ notAField }"}' "$base/graphql"
  sleep 0.2
done

total=$((ok + c4xx + c5xx + failed))
if (( c5xx > 0 || failed > 0 )); then
  printf 'generate-local-traffic: %d of %d requests got a 5xx answer or no answer; check the API and its logs (docker compose ... logs api)\n' $((c5xx + failed)) "$total" >&2
  finish FAIL 1 "reason=server-errors-or-failed-requests"
elif (( ok == 0 )); then
  printf 'generate-local-traffic: no request succeeded; check the base-url and the API\n' >&2
  finish FAIL 1 "reason=no-successful-request"
fi
finish PASS 0 "requests=$total"
