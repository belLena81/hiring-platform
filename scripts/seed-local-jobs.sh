#!/usr/bin/env bash
# Seeds a local API with a repeatable synthetic dataset (one recruiter, N candidates, M open jobs) for latency measurements.
# Synthetic accounts and jobs only; credentials are written solely to .local/data/perf-seed.env (mode 0600) and never printed.
# Contract: see "Operator scripts" in docs/engineering-quality.md. Run with --help for usage and exit codes.
set -euo pipefail

readonly default_base="http://127.0.0.1:8080"
readonly default_jobs=50
readonly max_jobs=500
readonly default_candidates=1
readonly max_candidates=8 # login and signup are limited to 20 attempts per 60 s per client network
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
repo_root="$(cd -- "$script_dir/.." && pwd -P)"
# shellcheck source=scripts/traffic-lib.sh
source "$script_dir/traffic-lib.sh"

help_text() {
  cat <<HELP
Usage: scripts/seed-local-jobs.sh [base-url] [--jobs M] [--candidates N]
       scripts/seed-local-jobs.sh --help

Creates, through the public GraphQL API only, the synthetic recruiter "perf-recruiter", M published jobs (default $default_jobs,
1-$max_jobs; deterministic titles, the skill "perfseed" plus two fixed skills, five fixed cities) and N synthetic candidates
"perf-candidate-1".. (default $default_candidates, 1-$max_candidates). It then logs the candidates in and writes their bearer tokens to
.local/data/perf-seed.env (mode 0600), together with the random passwords generated for the accounts. Nothing secret is
printed; the next command to run is.

Idempotent: if the recruiter already owns at least M open jobs nothing is created; otherwise every job uses a fixed idempotency key,
so a rerun replays jobs that exist. Existing accounts are logged in with the password stored in the file; if the file is lost
while the accounts exist, the script stops with BLOCKED (reset the local database or remove the accounts).
Passwords are written to the file right after each signup, so an interrupted run cannot lose an account; the tokens are
written (after format checks) only when everything succeeded. Requests follow no redirect and use no proxy.
Needs the API running (BLOCKED, exit 2, if /health does not answer; also on a rate limit). Each request is limited to 5 s;
progress every 5 s; expected duration: about 0.05 s per job, so well under a minute for the default.
Tokens are short-lived: rerun the script to refresh them (login is cheap), then source the file again, in a subshell.

Parameters:
  base-url      API root of this machine (default $default_base): http or https, host exactly 127.0.0.1, localhost or [::1]
                (credentials are sent to it, so a remote host cannot be named); no credentials in the URL
  --jobs M      number of published jobs to have (default $default_jobs)
  --candidates N  number of synthetic candidates, one bearer token each (default $default_candidates)
  --one-fresh-job create and publish exactly one new job with random idempotency keys, ignoring --jobs and the existing
                jobs (used by scripts/run-alert-backlog-drill.sh; it always leaves one more job and one more event)

Exit codes: 0 PASS, 1 FAIL (the API refused an operation), 2 usage error or precondition not met (BLOCKED), 130 interrupted.
The last line is "RESULT: PASS|FAIL|BLOCKED ..." with counts, duration and base URL.

Examples:
  scripts/seed-local-jobs.sh
  scripts/seed-local-jobs.sh http://127.0.0.1:8080 --jobs 200 --candidates 4
  ( . .local/data/perf-seed.env && scripts/generate-local-traffic.sh 120 --mix search )
HELP
}

usage_error() {
  printf 'seed-local-jobs: %s\n' "$1" >&2
  printf 'seed-local-jobs: run scripts/seed-local-jobs.sh --help for usage\n' >&2
  printf 'RESULT: BLOCKED reason=usage duration=0s\n' >&2
  exit 2
}

positional=() jobs="$default_jobs" candidates="$default_candidates" fresh_job=0
while (( $# > 0 )); do
  case "$1" in
    -h|--help) help_text; exit 0 ;;
    --one-fresh-job) fresh_job=1; shift ;; # exactly one new published job, random idempotency keys (alert drills)
    --jobs|--candidates)
      (( $# >= 2 )) || usage_error "$1 needs a value"
      if [[ "$1" == --jobs ]]; then jobs="$2"; else candidates="$2"; fi
      shift 2 ;;
    -*) usage_error "unknown option: $1" ;;
    *) positional+=("$1"); shift ;;
  esac
done
(( ${#positional[@]} <= 1 )) || usage_error "expected at most one base-url argument, got ${#positional[@]}"
base="${positional[0]:-$default_base}"
[[ "$jobs" =~ ^[0-9]{1,4}$ ]] && (( 10#$jobs >= 1 && 10#$jobs <= max_jobs )) || usage_error "--jobs must be an integer from 1 to $max_jobs, got: $jobs"
[[ "$candidates" =~ ^[0-9]{1,2}$ ]] && (( 10#$candidates >= 1 && 10#$candidates <= max_candidates )) || usage_error "--candidates must be an integer from 1 to $max_candidates, got: $candidates"
traffic_base_ok "$base" || usage_error "base-url must be loopback, http(s)://127.0.0.1, localhost or [::1] with an optional port and path (no credentials, query or spaces), got: $base"
base="${base%/}"
jobs=$((10#$jobs)) candidates=$((10#$candidates))

printf 'seed-local-jobs: seeding %s with %d job(s) and %d candidate(s) (defaults: %d, %d, %s); credentials go to .local/data/perf-seed.env (0600); progress every 5s\n' \
  "$base" "$jobs" "$candidates" "$default_jobs" "$default_candidates" "$default_base"

child=""
on_interrupt() {
  trap '' INT TERM HUP
  if [[ -n "$child" ]]; then
    kill "$child" 2>/dev/null || true
    wait "$child" 2>/dev/null || true # let the helper clean up before this script exits
  fi
  printf '\nseed-local-jobs: interrupted; jobs already created stay (a rerun replays them); passwords of accounts signed up so far stay in .local/data/perf-seed.env, tokens are written only after a full success\n' >&2
  exit 130
}
trap on_interrupt INT TERM HUP

umask 077
code=0
python3 -I "$script_dir/seed-local-jobs.py" "$base" "$jobs" "$candidates" "$repo_root" "$fresh_job" &
child=$!
wait "$child" || code=$?
case "$code" in
  0|1|2|130) exit "$code" ;;
  *) printf 'seed-local-jobs: the seeding helper stopped unexpectedly (exit %s)\n' "$code" >&2
     printf 'RESULT: FAIL reason=unexpected-exit duration=%ds base=%s\n' "$SECONDS" "$base" >&2
     exit 1 ;;
esac
