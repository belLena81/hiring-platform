#!/usr/bin/env bash
# Sends read-only requests to a local API so the Grafana dashboard has data, or (opt-in) measures request latency.
# Default mix: unauthenticated health checks, a trivial GraphQL query, and an invalid query (expected 4xx). No credentials
# or personal data are sent.
# Opt-in `--mix search` (telemetry overhead measurement): authenticated job search and job detail requests. The client
# sends queries only (no mutation, no personal data), but the server records a search session for every `jobs` call, so
# the database grows slightly. This is the one documented exception to "no credentials" (see "Operator scripts" in
# docs/engineering-quality.md): the bearer token(s) of synthetic candidates are read only from the environment
# (TRAFFIC_BEARER_TOKEN or TRAFFIC_BEARER_TOKENS), are never printed, never appear on a command line (curl reads them from a
# 0600 header file in a 0700 temporary directory that is removed on exit), and the base URL must be loopback.
# Contract: see "Operator scripts" in docs/engineering-quality.md. Run with --help for usage and exit codes.
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
repo_root="$(cd -- "$script_dir/.." && pwd -P)"
# shellcheck source=scripts/traffic-lib.sh
source "$script_dir/traffic-lib.sh"

readonly default_seconds=60
readonly default_base="http://127.0.0.1:8080"
readonly max_seconds=3600
readonly request_timeout=3 # seconds per request, connect included
readonly heartbeat=7       # seconds between progress lines; plus one in-flight request this keeps every gap <= 10 s
readonly default_concurrency=4
readonly max_concurrency=16
readonly max_search_ids=20  # job ids collected from the first search page for the detail requests

help_text() {
  cat <<HELP
Usage: scripts/generate-local-traffic.sh [seconds] [base-url]
       scripts/generate-local-traffic.sh [seconds] [base-url] --mix search [--concurrency N] [--latency-file PATH]
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

Measurement mode (opt-in, authenticated; queries only):
  --mix search          instead of the default cycle, send only the Mongo-backed queries jobs(first: 20, filter: skills
                        [perfseed]) and job(id:), alternating, from --concurrency parallel workers for [seconds].
                        Needs a seeded dataset (scripts/seed-local-jobs.sh) and a bearer token of a seeded candidate in the
                        environment: TRAFFIC_BEARER_TOKEN, or TRAFFIC_BEARER_TOKENS (comma-separated, workers use them
                        round-robin). The token is never printed or put on a command line; source the seed file in a
                        subshell. The base-url must be loopback (127.0.0.1, localhost or [::1]). The persisted-query
                        allowlist must be off (the default). Without a token the run stops with BLOCKED (exit 2).
                        The client sends no mutation, but the server records a search session per jobs call (database growth).
  --concurrency N       parallel workers, integer 1-$max_concurrency (default $default_concurrency)
  --latency-file PATH   also write one "kind class seconds" line per measured request; PATH must resolve below .local/logs/
                        (relative paths are relative to the repository root); a refused or failed write fails the run
Procedure: one warm-up search and one detail request (not counted) read the job ids and extensions.cost; the clock of
[seconds] starts after them. Pacing: the per-actor query cost budget (packaged 10000 per 60 s) would answer HTTP 429 at full
speed, so each worker sleeps (staggered at start) so that the busiest token, serving ceil(workers / tokens) workers, spends at
most 80 percent of the budget. Throughput is therefore bounded by the budget and is NOT an overhead signal; the signals are the
latency percentiles and the API container CPU/memory. Aim for at least 1000 successful requests per run (the start line prints
the pace and the minimum seconds; use TRAFFIC_BEARER_TOKENS with 8 tokens and --concurrency 8 or more). Fewer samples leave
the RESULT PASS but add low-sample=yes. Any 429 fails the run (reason=cost-budget-exhausted). Each request opens a new
connection (no keep-alive); connect time is included, identically in every run. The script never calls docker.
Search-mix result: ok/4xx/429/5xx/failed/gqlerr counts (gqlerr = HTTP 200 with a GraphQL errors entry), requests, throughput
(req/s over the measured run), p50/p95/p99 in ms (nearest rank over the successful measured requests; warm-up excluded).
PASS needs every measured request ok. A request still in flight at the end is allowed to finish (up to $request_timeout s past [seconds]).

Examples:
  scripts/generate-local-traffic.sh 120
  scripts/generate-local-traffic.sh 15 http://127.0.0.1:8080
  ( . .local/data/perf-seed.env && scripts/generate-local-traffic.sh 120 --mix search --concurrency 8 --latency-file .local/logs/off.txt )
HELP
}

usage_error() {
  printf 'generate-local-traffic: %s\n' "$1" >&2
  printf 'generate-local-traffic: run scripts/generate-local-traffic.sh --help for usage\n' >&2
  printf 'RESULT: BLOCKED reason=usage ok=0 4xx=0 5xx=0 failed=0 duration=0s\n' >&2
  exit 2
}

positional=() mix="" concurrency="" latency_arg=""
while (( $# > 0 )); do
  case "$1" in
    -h|--help) help_text; exit 0 ;;
    --mix|--concurrency|--latency-file)
      (( $# >= 2 )) || usage_error "$1 needs a value"
      case "$1" in
        --mix) mix="$2" ;;
        --concurrency) concurrency="$2" ;;
        --latency-file) latency_arg="$2" ;;
      esac
      shift 2 ;;
    -*) usage_error "unknown option: $1" ;;
    *) positional+=("$1"); shift ;;
  esac
done

duration="${positional[0]:-$default_seconds}"
base="${positional[1]:-$default_base}"

(( ${#positional[@]} <= 2 )) || usage_error "expected at most two arguments, got ${#positional[@]}"
[[ -z "$mix" || "$mix" == search ]] || usage_error "--mix must be: search, got: $mix"
if [[ -z "$mix" && ( -n "$concurrency" || -n "$latency_arg" ) ]]; then
  usage_error "--concurrency and --latency-file need --mix search"
fi
concurrency="${concurrency:-$default_concurrency}"
traffic_concurrency_ok "$concurrency" || usage_error "--concurrency must be an integer from 1 to $max_concurrency, got: $concurrency"
concurrency=$((10#$concurrency))
latency_file=""
if [[ -n "$latency_arg" ]]; then
  latency_file="$(traffic_latency_path "$repo_root" "$latency_arg")" || usage_error "--latency-file must name a file that resolves below .local/logs/ (letters, digits, . _ - /; no ..), got: $latency_arg"
  latency_raw="$latency_arg"
  [[ "$latency_raw" == /* ]] || latency_raw="$repo_root/$latency_raw"
  [[ ! -L "$latency_raw" ]] || usage_error "--latency-file must not be a symbolic link, got: $latency_arg"
fi
[[ "$duration" =~ ^[0-9]{1,5}$ ]] && (( 10#$duration >= 1 && 10#$duration <= max_seconds )) || usage_error "seconds must be an integer from 1 to $max_seconds, got: $duration"
duration=$((10#$duration))
[[ "$base" =~ ^https?://[][A-Za-z0-9._:-]+(/[A-Za-z0-9._~/-]*)?$ ]] || usage_error "base-url must be http(s)://host[:port][/path] without credentials, query or spaces"
base="${base%/}"
if [[ "$mix" == search ]]; then
  traffic_base_ok "$base" || usage_error "--mix search sends a bearer token, so base-url must be loopback: http(s)://127.0.0.1, localhost or [::1] (optional port and path), got: $base"
fi

ok=0 c4xx=0 c5xx=0 failed=0 c429=0 gqlerr=0
work=""
started="$SECONDS"
last_code="none"

counts() {
  if [[ "$mix" == search ]]; then
    printf 'ok=%d 4xx=%d 429=%d 5xx=%d failed=%d gqlerr=%d' "$ok" "$c4xx" "$c429" "$c5xx" "$failed" "$gqlerr"
  else
    printf 'ok=%d 4xx=%d 5xx=%d failed=%d' "$ok" "$c4xx" "$c5xx" "$failed"
  fi
}
elapsed() { printf '%ds' $((SECONDS - started)); }

cleanup_work() {
  local pids
  pids="$(jobs -p 2>/dev/null || true)"
  [[ -z "$pids" ]] || kill $pids 2>/dev/null || true
  [[ -z "$work" ]] || rm -rf -- "$work"
}

on_interrupt() {
  trap '' INT TERM HUP
  local note="nothing to clean up"
  if [[ "$mix" == search ]]; then refresh_counts; note="workers stopped, temporary token files removed"; fi
  printf '\ngenerate-local-traffic: interrupted after %s of %ss; completed requests: %s (%s)\n' "$(elapsed)" "$duration" "$(counts)" "$note" >&2
  cleanup_work
  exit 130
}
trap on_interrupt INT TERM HUP
trap cleanup_work EXIT

next_beat=0
# Progress line at most every $heartbeat seconds; checked after every request so a slow API cannot silence it.
maybe_beat() {
  if (( SECONDS >= next_beat )); then
    if [[ "$mix" == search ]]; then
      refresh_counts
      printf '[%3ds/%ds] %s\n' $((SECONDS - started)) "$duration" "$(counts)"
    else
      printf '[%3ds/%ds] %s (last status %s)\n' $((SECONDS - started)) "$duration" "$(counts)" "$last_code"
    fi
    next_beat=$((SECONDS + heartbeat))
  fi
}

# request METHOD-ARGS...: one bounded curl; classifies the HTTP status into the counters.
request() {
  local remaining limit code
  remaining=$((started + duration - SECONDS))
  limit=$(( remaining < request_timeout ? remaining : request_timeout ))
  (( limit >= 1 )) || limit=1
  code="$(curl -g -sS -o /dev/null -w '%{http_code}' --connect-timeout 2 --max-time "$limit" "$@" 2>/dev/null)" || code=000
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

# ---- search measurement mix (opt-in) -------------------------------------------------------------------------------
readonly search_query='query OpenJobs($filter: JobFilter, $first: Int!) { jobs(filter: $filter, first: $first) { edges { node { id title skills location { country city remote } status } } pageInfo { hasNextPage endCursor } } }'
readonly search_variables='{"filter":{"skills":["perfseed"]},"first":20}'
readonly detail_query='query JobDetail($id: JobID!) { job(id: $id) { id title description requirements skills location { country city remote } status createdAt } }'

# Reads the bearer token(s) from the environment into 0600 header files; the values never reach a command line or output.
# Returns 1 when no token is set and 2 when a token has an unexpected format.
load_tokens() {
  local raw="${TRAFFIC_BEARER_TOKENS:-${TRAFFIC_BEARER_TOKEN:-}}" token i=0 header_file
  unset TRAFFIC_BEARER_TOKENS TRAFFIC_BEARER_TOKEN
  [[ -n "$raw" ]] || return 1
  local -a list
  IFS=, read -r -a list <<< "$raw"
  for token in "${list[@]}"; do
    [[ "$token" =~ ^[A-Za-z0-9._~+/=-]{10,4096}$ ]] || return 2
    header_file="$work/token_$i"
    printf 'Authorization: Bearer %s\n' "$token" > "$header_file"
    i=$((i + 1))
  done
  ntok=$i
}

# post_graphql TOKEN-INDEX BODY: one request; prints "<http-code> <seconds>" and leaves the response body in $work/probe.
# -g keeps curl from treating the brackets of [::1] as a glob.
post_graphql() {
  local out rc=0
  out="$(curl -g -sS -o "$work/probe" -w '%{http_code} %{time_total}' --connect-timeout 2 --max-time "$request_timeout" \
    -H "@$work/token_$1" -H 'Content-Type: application/json' --data-binary "$2" "$base/graphql" 2>/dev/null)" || rc=$?
  (( rc == 0 )) || out="000 0"
  printf '%s\n' "$out"
}

# probe_fields: prints "ERR code", "COST requested limit" and "ID id" lines found in $work/probe (python3 -I, stdlib only).
probe_fields() {
  python3 -I -c '
import json, sys
try:
    d = json.load(open(sys.argv[1]))
except Exception:
    print("ERR UNREADABLE"); sys.exit(0)
errs = d.get("errors") or []
if errs:
    print("ERR", str((errs[0].get("extensions") or {}).get("code", "UNKNOWN"))[:40])
c = (d.get("extensions") or {}).get("cost") or {}
print("COST", int(c.get("requested", 0)), int(c.get("limit", 0)))
data = d.get("data") or {}
for e in ((data.get("jobs") or {}).get("edges") or [])[:int(sys.argv[2])]:
    print("ID", e["node"]["id"])
if (data.get("job") or {}).get("id"):
    print("ID", data["job"]["id"])
' "$work/probe" "$max_search_ids" 2>/dev/null || printf 'ERR UNREADABLE\n'
}

# Counts of the measured requests only (w0.log, w1.log, ...); the warm-up lines live in boot.log.
refresh_counts() {
  local assignments
  assignments="$(cat "$work"/w[0-9]*.log 2>/dev/null | awk '{ c[$2]++ } END {
    printf "ok=%d c4xx=%d c429=%d c5xx=%d failed=%d gqlerr=%d", c["ok"], c["4xx"], c["429"], c["5xx"], c["failed"], c["gqlerr"] }')"
  eval "$assignments"
}

classify() { # http-code body-file -> ok|4xx|429|5xx|failed|gqlerr
  case "$1" in
    2??|3??) if grep -q '"errors"' "$2" 2>/dev/null; then echo gqlerr; else echo ok; fi ;;
    429) echo 429 ;;
    4??) echo 4xx ;;
    5??) echo 5xx ;;
    *) echo failed ;;
  esac
}

# search_worker INDEX: alternates search and detail requests until the end of the run, one log line per request. Starts after
# an offset of pause * INDEX / workers so the workers do not fire in lockstep. A request that starts before the end gets its full
# time limit and is allowed to finish, so the last request is never cut short and misclassified as failed.
search_worker() {
  local i="$1" n=0 kind data id out rc code t remaining
  local hdr="$work/token_$((i % ntok))" body="$work/body_$i" log="$work/w$i.log"
  sleep "$(awk -v p="$pause" -v i="$i" -v w="$concurrency" 'BEGIN { printf "%.3f", p * i / w }')"
  while (( SECONDS < end )); do
    if (( (n + i) % 2 == 0 )); then
      kind=search data="$search_body"
    else
      kind=detail id="${job_ids[$(((n / 2 + i) % ${#job_ids[@]}))]}"
      data="{\"query\":\"$detail_query\",\"variables\":{\"id\":\"$id\"}}"
    fi
    rc=0
    out="$(curl -g -sS -o "$body" -w '%{http_code} %{time_total}' --connect-timeout 2 --max-time "$request_timeout" \
      -H "@$hdr" -H 'Content-Type: application/json' --data-binary "$data" "$base/graphql" 2>/dev/null)" || rc=$?
    code="${out%% *}" t="${out#* }"
    (( rc == 0 )) && [[ -n "$code" ]] || { code=000; t="${t:-0}"; }
    printf '%s %s %s\n' "$kind" "$(classify "$code" "$body")" "$t" >> "$log"
    n=$((n + 1))
    remaining=$((end - SECONDS))
    if (( remaining <= 0 )); then break; fi
    if (( ${pause%.*} < remaining )); then sleep "$pause"; else sleep "$remaining"; fi
  done
}

run_search_mix() {
  export LC_ALL=C
  umask 077
  mkdir -p "$repo_root/.local/data"
  work="$(mktemp -d "$repo_root/.local/data/traffic.XXXXXX")"
  ntok=0
  local rc=0
  load_tokens || rc=$?
  if (( rc == 1 )); then
    printf 'generate-local-traffic: --mix search needs a bearer token in TRAFFIC_BEARER_TOKEN (or TRAFFIC_BEARER_TOKENS, comma-separated); scripts/seed-local-jobs.sh writes them\n' >&2
    finish BLOCKED 2 "reason=token-missing"
  elif (( rc == 2 )); then
    printf 'generate-local-traffic: a bearer token in the environment has an unexpected format (expected a JWT: letters, digits and . _ ~ + / = -; the value is not shown)\n' >&2
    finish BLOCKED 2 "reason=token-format"
  fi

  # Warm-up (not measured): the first search page provides the job ids and the query cost; one detail request provides its cost.
  search_body="{\"query\":\"$search_query\",\"variables\":$search_variables}"
  local out fields code cost_s limit_s cost_d limit_d line
  out="$(post_graphql 0 "$search_body")"
  code="${out%% *}"
  if [[ "$code" == 000 ]]; then
    printf 'generate-local-traffic: the warm-up search got no answer\n' >&2
    finish BLOCKED 2 "reason=api-unreachable"
  fi
  fields="$(probe_fields)"
  cp "$work/probe" "$work/probe_search"
  if grep -q 'PERSISTED_QUERY_NOT_FOUND' "$work/probe_search"; then
    printf 'generate-local-traffic: the persisted-query allowlist refused the search document; set GRAPHQL_PERSISTED_QUERIES_MODE=off for the measurement\n' >&2
    finish BLOCKED 2 "reason=persisted-query-allowlist"
  fi
  case "$fields" in
    *"ERR UNAUTHORIZED"*|*"ERR FORBIDDEN"*)
      printf 'generate-local-traffic: the API refused the token (expired or not a seeded candidate); run scripts/seed-local-jobs.sh again and re-source it\n' >&2
      finish BLOCKED 2 "reason=token-refused" ;;
    *"ERR RATE_LIMITED"*)
      printf 'generate-local-traffic: the query cost budget is exhausted for this token; wait one window (60 s) or use another token\n' >&2
      finish BLOCKED 2 "reason=cost-budget-exhausted" ;;
    *"ERR "*)
      printf 'generate-local-traffic: the warm-up search returned a GraphQL error (%s)\n' "$(grep -m1 '^ERR ' <<< "$fields")" >&2
      finish BLOCKED 2 "reason=warmup-error" ;;
  esac
  if [[ "$(classify "$code" "$work/probe_search")" != ok ]]; then
    printf 'generate-local-traffic: the warm-up search was not answered with success (HTTP %s); check the API and its logs\n' "$code" >&2
    finish BLOCKED 2 "reason=warmup-failed"
  fi
  job_ids=()
  while IFS= read -r line; do
    if [[ "$line" =~ ^ID\ ([0-9A-Fa-f-]{36})$ ]]; then job_ids+=("${BASH_REMATCH[1]}"); fi
  done <<< "$fields"
  if (( ${#job_ids[@]} == 0 )); then
    printf 'generate-local-traffic: the search returned no jobs with skill perfseed; seed the dataset first (scripts/seed-local-jobs.sh)\n' >&2
    finish BLOCKED 2 "reason=no-seeded-jobs"
  fi
  read -r _ cost_s limit_s <<< "$(grep -m1 '^COST ' <<< "$fields")"
  out="$(post_graphql 0 "{\"query\":\"$detail_query\",\"variables\":{\"id\":\"${job_ids[0]}\"}}")"
  code="${out%% *}"
  fields="$(probe_fields)"
  if [[ "$fields" == *"ERR "* || "$(classify "$code" "$work/probe")" != ok ]]; then
    printf 'generate-local-traffic: the warm-up job detail request failed (HTTP %s, %s)\n' "$code" "$(grep -m1 '^ERR ' <<< "$fields" || echo no-graphql-error)" >&2
    finish BLOCKED 2 "reason=warmup-detail-failed"
  fi
  read -r _ cost_d limit_d <<< "$(grep -m1 '^COST ' <<< "$fields")"
  if (( ${cost_s:-0} <= 0 || ${cost_d:-0} <= 0 || ${limit_s:-0} <= 0 || ${limit_d:-0} <= 0 )); then
    printf 'generate-local-traffic: the responses carry no extensions.cost (a shared overflow counter or an unresolved client address), so the pace cannot be computed\n' >&2
    finish BLOCKED 2 "reason=no-cost-information"
  fi
  pause="$(traffic_pause_seconds "$concurrency" "$ntok" $(( (cost_s + cost_d) / 2 )) "$limit_s")"
  local expected min_seconds
  expected="$(awk -v w="$concurrency" -v p="$pause" -v d="$duration" 'BEGIN { printf "%d", w / p * d }')"
  min_seconds="$(traffic_min_seconds "$concurrency" "$pause")"
  printf 'generate-local-traffic: search mix: %d worker(s), %d token(s), %d job id(s); query cost search=%s detail=%s of %s per 60 s per token; pause %ss per worker (staggered); about %d requests expected in %ss, %ss needed for %d\n' \
    "$concurrency" "$ntok" "${#job_ids[@]}" "$cost_s" "$cost_d" "$limit_s" "$pause" "$expected" "$duration" "$min_seconds" "$TRAFFIC_MIN_SAMPLES"
  if (( expected < TRAFFIC_MIN_SAMPLES )); then
    printf 'generate-local-traffic: warning: fewer than %d requests expected; run at least %ss, or use more tokens and workers, for reliable percentiles\n' "$TRAFFIC_MIN_SAMPLES" "$min_seconds" >&2
  fi

  # The clock of the measured run starts here, after the warm-up.
  local t0 t1 i
  started="$SECONDS"
  next_beat=$((SECONDS + heartbeat))
  end=$((SECONDS + duration))
  t0="$(date +%s.%N)"
  for (( i = 0; i < concurrency; i++ )); do search_worker "$i" & done
  while [[ -n "$(jobs -rp)" ]]; do
    sleep 1
    maybe_beat
  done
  t1="$(date +%s.%N)"
  refresh_counts

  local total=$((ok + c4xx + c429 + c5xx + failed + gqlerr)) stats n p50 p95 p99 rate detail latency_failed=0 resolved
  stats="$(cat "$work"/w[0-9]*.log | awk '$2 == "ok" { print $3 }' | traffic_percentiles)"
  read -r n p50 p95 p99 <<< "$stats"
  rate="$(awk -v n="$total" -v a="$t0" -v b="$t1" 'BEGIN { d = b - a; printf "%.2f", (d > 0 ? n / d : 0) }')"
  if [[ -n "$latency_file" ]]; then
    # Resolve again at write time so a link created during the run cannot redirect the write.
    if mkdir -p "$(dirname -- "$latency_file")" \
       && resolved="$(traffic_latency_path "$repo_root" "$latency_arg")" && [[ "$resolved" == "$latency_file" && ! -L "$latency_raw" ]] \
       && cat "$work"/w[0-9]*.log > "$latency_file"; then
      printf 'generate-local-traffic: wrote %d latency lines (kind class seconds) to %s\n' "$total" "${latency_file#"$repo_root"/}"
    else
      latency_failed=1
      printf 'generate-local-traffic: could not write the latency file %s (outside .local/logs/, a symbolic link, or not writable)\n' "$latency_arg" >&2
    fi
  fi
  detail="requests=$total throughput=${rate}req/s p50=${p50}ms p95=${p95}ms p99=${p99}ms samples=$n concurrency=$concurrency tokens=$ntok"
  (( n >= TRAFFIC_MIN_SAMPLES )) || detail="$detail low-sample=yes"
  if (( c429 > 0 )); then
    printf 'generate-local-traffic: %d request(s) hit the query cost budget (429); the measurement is not valid. Use more tokens (TRAFFIC_BEARER_TOKENS) or fewer workers\n' "$c429" >&2
    finish FAIL 1 "reason=cost-budget-exhausted $detail"
  elif (( c5xx > 0 || failed > 0 || c4xx > 0 || gqlerr > 0 )); then
    printf 'generate-local-traffic: %d of %d requests were not ok; check the API and its logs (docker compose ... logs api)\n' $((total - ok)) "$total" >&2
    finish FAIL 1 "reason=request-errors $detail"
  elif (( ok == 0 )); then
    finish FAIL 1 "reason=no-successful-request $detail"
  elif (( latency_failed == 1 )); then
    finish FAIL 1 "reason=latency-file-refused $detail"
  fi
  finish PASS 0 "$detail"
}

if [[ "$mix" == search ]]; then
  printf 'generate-local-traffic: measuring authenticated search and detail requests (queries only) against %s for %ss with %s worker(s) (defaults: %ss, %s, %s workers); progress every %ss; each request limited to %ss\n' \
    "$base" "$duration" "$concurrency" "$default_seconds" "$default_base" "$default_concurrency" "$heartbeat" "$request_timeout"
else
  printf 'generate-local-traffic: sending read-only requests to %s for %ss (defaults: %ss, %s); progress every %ss; each request limited to %ss\n' \
    "$base" "$duration" "$default_seconds" "$default_base" "$heartbeat" "$request_timeout"
fi

# Precondition: the API must answer on /health, otherwise nothing useful can be generated.
health_code="$(curl -g -sS -o /dev/null -w '%{http_code}' --connect-timeout 2 --max-time "$request_timeout" "$base/health" 2>/dev/null)" || health_code=000
if [[ "$health_code" == 000 ]]; then
  printf 'generate-local-traffic: no HTTP answer from %s/health within %ss\n' "$base" "$request_timeout" >&2
  printf 'generate-local-traffic: next: start the API (docker compose -f compose.yaml -f compose.observability.yaml --profile api up -d --build, or sbt run) and check the base-url and port\n' >&2
  finish BLOCKED 2 "reason=api-unreachable"
fi

[[ "$mix" != search ]] || run_search_mix

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
