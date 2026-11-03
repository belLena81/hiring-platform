#!/usr/bin/env bash
# Self-test of the option parsing of scripts/generate-local-traffic.sh and scripts/seed-local-jobs.sh and of the
# percentile, latency-path and pacing helpers in scripts/traffic-lib.sh. It uses no network, starts no service and writes nothing.
# Contract: see "Operator scripts" in docs/engineering-quality.md.
set -Eeuo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$repo_root"
checks=0
failures=0

if [[ "${1:-}" == -h || "${1:-}" == --help ]]; then
  cat <<'HELP'
Usage: scripts/tests/local-traffic-options.sh [--help]

Self-test of scripts/generate-local-traffic.sh (--mix, --concurrency, --latency-file parsing and refusal, default mode
unchanged on bad input), scripts/seed-local-jobs.sh (--jobs, --candidates and base-url parsing) and the helpers in
scripts/traffic-lib.sh (nearest-rank percentiles, loopback-only base URL, latency-file path guard including links, cost-budget
pacing for the busiest token) and of scripts/seed-local-jobs.py (hostile token or expiry refused before the credentials file, rate
limit never falls through to signup, password saved right after signup, redirects refused), the HTTP layer replaced. It needs no Docker, no API
and no network, and writes only to a temporary directory that it removes. Expected duration: under 10 s.

Exit codes: 0 PASS, 1 FAIL, 2 usage error. The last line is "RESULT: PASS|FAIL ...".
HELP
  exit 0
fi
[[ $# -eq 0 ]] || { printf 'local-traffic-options: unknown argument: %s (try --help)\n' "$1" >&2; printf 'RESULT: BLOCKED reason=usage\n' >&2; exit 2; }
printf 'local-traffic-options: checking option parsing and percentile helpers; expected under 5 s, no network\n'

# shellcheck source=scripts/traffic-lib.sh
source "$repo_root/scripts/traffic-lib.sh"
export LC_ALL=C # decimal point in generated test data

check() { # name expected actual
  checks=$((checks + 1))
  if [[ "$2" == "$3" ]]; then return 0; fi
  failures=$((failures + 1))
  printf 'FAIL %s: expected [%s] got [%s]\n' "$1" "$2" "$3" >&2
}

exit_code() { local code=0; "$@" >/dev/null 2>&1 || code=$?; printf '%s' "$code"; }
last_line() { "$@" 2>&1 >/dev/null | tail -n 1 || true; }

gen=scripts/generate-local-traffic.sh
seed=scripts/seed-local-jobs.sh

# Percentiles (nearest rank, milliseconds).
check "percentiles of 1..100 ms" "100 50.000 95.000 99.000" "$(seq 1 100 | awk '{ printf "%.6f\n", $1 / 1000 }' | traffic_percentiles)"
check "percentiles ignore input order" "5 3.000 5.000 5.000" "$(printf '0.005\n0.001\n0.003\n0.002\n0.004\n' | traffic_percentiles)"
check "percentiles of one value" "1 7.500 7.500 7.500" "$(printf '0.0075\n' | traffic_percentiles)"
check "percentiles of ten values use ceil(9.5)=10 for p95" "10 5.000 10.000 10.000" "$(seq 1 10 | awk '{ printf "%.3f\n", $1 / 1000 }' | traffic_percentiles)"
check "percentiles of no input" "0 0.000 0.000 0.000" "$(printf '' | traffic_percentiles)"

# Concurrency bound.
for value in 1 4 16 08; do check "concurrency $value accepted" 0 "$(exit_code traffic_concurrency_ok "$value")"; done
for value in 0 17 100 abc -1 ""; do check "concurrency [$value] refused" 1 "$(exit_code traffic_concurrency_ok "$value")"; done

# Latency file guard.
check "relative path under .local/logs" "$repo_root/.local/logs/off.txt" "$(traffic_latency_path "$repo_root" .local/logs/off.txt)"
check "absolute path under .local/logs" "$repo_root/.local/logs/a/b.txt" "$(traffic_latency_path "$repo_root" "$repo_root/.local/logs/a/b.txt")"
for path in /tmp/x.txt .local/data/x.txt .local/logs .local/logs/ .local/logs/../data/x .local/logs/a/../../data/x ../.local/logs/x 'x y' "$repo_root/.local/logs/../x" ""; do
  check "latency path [$path] refused" 1 "$(exit_code traffic_latency_path "$repo_root" "$path")"
done

# Pacing keeps the busiest token (ceil(workers / tokens) workers) at 80 percent of the budget.
check "pause for 4 workers, 1 token" "1.050" "$(traffic_pause_seconds 4 1 35 10000)"
check "pause with one worker per token" "0.300" "$(traffic_pause_seconds 4 8 40 10000)"
check "pause uses the busiest token: 10 workers on 4 tokens means 3 on one token" "0.900" "$(traffic_pause_seconds 10 4 40 10000)"
check "pause for a multiple of the tokens is unchanged" "0.600" "$(traffic_pause_seconds 8 4 40 10000)"
check "pause has a floor, no hot loop" "0.010" "$(traffic_pause_seconds 8 8 1 10000)"
check "pause capped at 10 s" "10.000" "$(traffic_pause_seconds 16 1 1000 10000)"
check "pause without cost information" "1.000" "$(traffic_pause_seconds 4 1 0 0)"
check "pause refuses a zero cost" "1.000" "$(traffic_pause_seconds 4 2 0 10000)"
check "minimum seconds for 1000 requests: 8 workers, 0.6 s pause" "75" "$(traffic_min_seconds 8 0.600)"

# Loopback-only base URL (credentials are sent to it).
for url in http://127.0.0.1 http://127.0.0.1:8080 https://localhost:8443/api http://[::1]:8080 http://localhost; do
  check "base [$url] accepted" 0 "$(exit_code traffic_base_ok "$url")"
done
for url in http://127.0.0.1.evil.com http://localhost.evil.com http://evil.com http://127.0.0.1@evil.com http://user:pw@127.0.0.1 \
           http://127.0.0.2 http://0.0.0.0:8080 http://[::2] http://127.0.0.1:80a ftp://127.0.0.1 http://127.0.0.1?x=1 "http://127.0.0.1 x" ""; do
  check "base [$url] refused" 1 "$(exit_code traffic_base_ok "$url")"
done

# Latency file guard against links: a symbolic link below .local/logs that points elsewhere is refused after resolution.
fake_repo="$(mktemp -d "${TMPDIR:-/tmp}/traffic-options.XXXXXX")"
trap 'rm -rf -- "$fake_repo"' EXIT
mkdir -p "$fake_repo/.local/logs" "$fake_repo/elsewhere"
ln -s "$fake_repo/elsewhere" "$fake_repo/.local/logs/link"
check "latency path through a link to another directory refused" 1 "$(exit_code traffic_latency_path "$fake_repo" .local/logs/link/x.txt)"
check "latency path in a real subdirectory accepted" 0 "$(exit_code traffic_latency_path "$fake_repo" .local/logs/real/x.txt)"

# generate-local-traffic.sh: usage errors exit 2 with a RESULT line before any network use.
check "generate --help" 0 "$(exit_code "$gen" --help)"
check "generate help mentions --mix search" 1 "$("$gen" --help | grep -c -- '--mix search' | awk '{ print ($1 > 0) }')"
for args in "abc" "0" "3601" "5 http://a b" "--bogus" "5 --concurrency 3" "5 --latency-file .local/logs/x" "5 --mix other" "5 --mix" \
            "5 --mix search --concurrency 0" "5 --mix search --concurrency 17" "5 --mix search --concurrency abc" \
            "5 --mix search --latency-file /tmp/x" "5 --mix search --latency-file .local/logs/../x" "5 http://127.0.0.1:8080 extra" \
            "5 http://127.0.0.1.evil.com --mix search" "5 http://evil.com --mix search" "5 http://localhost@evil.com --mix search"; do
  # shellcheck disable=SC2086
  check "generate refuses [$args]" 2 "$(exit_code "$gen" $args)"
  # shellcheck disable=SC2086
  check "generate [$args] ends with a RESULT line" "RESULT: BLOCKED reason=usage ok=0 4xx=0 5xx=0 failed=0 duration=0s" "$(last_line "$gen" $args)"
done

# seed-local-jobs.sh: usage errors exit 2 with a RESULT line before any network use.
check "seed --help" 0 "$(exit_code "$seed" --help)"
for args in "--jobs 0" "--jobs 501" "--jobs abc" "--jobs" "--candidates 0" "--candidates 9" "--candidates" "--bogus" \
            "http://a b" "ftp://127.0.0.1" "http://127.0.0.1:8080 extra" "http://127.0.0.1.evil.com" "http://evil.com" "http://localhost@evil.com"; do
  # shellcheck disable=SC2086
  check "seed refuses [$args]" 2 "$(exit_code "$seed" $args)"
  # shellcheck disable=SC2086
  check "seed [$args] ends with a RESULT line" "RESULT: BLOCKED reason=usage duration=0s" "$(last_line "$seed" $args)"
done

# Loopback URLs get past parsing: both scripts then stop at the unreachable API (port 1), never on usage.
check "generate accepts a loopback base in search mode" "BLOCKED" "$("$gen" 1 http://127.0.0.1:1 --mix search 2>&1 >/dev/null | tail -n 1 | awk '{ print $2 }')"
check "generate search mode stops on the unreachable API" "1" "$("$gen" 1 'http://[::1]:1' --mix search 2>&1 >/dev/null | tail -n 1 | grep -c 'reason=api-unreachable' || true)"
check "seed accepts a loopback base" "1" "$("$seed" http://localhost:1 2>&1 >/dev/null | tail -n 1 | grep -c 'reason=api-unreachable' || true)"

# seed-local-jobs.py: validation before anything reaches the credentials file, no fall-through after a rate limit,
# password saved right after signup, redirects refused. Pure Python, no network (the HTTP layer is replaced).
python_result="$(python3 -I - "$repo_root/scripts/seed-local-jobs.py" "$fake_repo" <<'PY'
import importlib.util, json, os, stat, sys, tempfile, urllib.error, urllib.request
sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location("seed", sys.argv[1])
seed = importlib.util.module_from_spec(spec)
spec.loader.exec_module(seed)
bad = []

def expect(name, condition):
    if not condition:
        bad.append(name)

GOOD_TOKEN, GOOD_EXPIRY = "header.payload.signature", "2026-11-03T12:00:00Z"
for hostile in ["short", "has space.in.token", "a;b;c;d;e;f;g;h", "token'; touch x; '", "tok\nen.with.newline", "tok,en.with.comma1", "x" * 4097, "", None, 5]:
    expect("token refused %r" % (hostile,), not seed.valid_token(hostile))
expect("token accepted", seed.valid_token(GOOD_TOKEN))
for hostile in ["2026-11-03 12:00:00Z", "2026-11-03T12:00:00+01:00", "2026-13-03T12:00:00Z", "2026-11-03T12:00:00Z'; rm -rf x", "2026-11-03T12:00:00Z\n", "tomorrow", "", None, 5]:
    expect("expiry refused %r" % (hostile,), not seed.valid_instant(hostile))
expect("expiry accepted", seed.valid_instant(GOOD_EXPIRY) and seed.valid_instant("2026-11-03T12:00:00.123456Z"))
for hostile in [dict(accessToken="bad token", expiresAt=GOOD_EXPIRY), dict(accessToken=GOOD_TOKEN, expiresAt="soon")]:
    try:
        seed.checked_session(hostile); bad.append("checked_session accepted %r" % hostile)
    except seed.Stop as stop:
        expect("checked_session fails closed with exit 1", stop.code == 1)
for hostile_tokens, hostile_expiries in [(["bad'token"], [GOOD_EXPIRY]), ([GOOD_TOKEN], ["x'; echo hi"])]:
    try:
        seed.render({}, hostile_tokens, hostile_expiries); bad.append("render accepted hostile values")
    except seed.Stop as stop:
        expect("render fails closed with exit 1", stop.code == 1)
text = seed.render({"PERF_RECRUITER_PASSWORD": "A" * 20}, [GOOD_TOKEN, GOOD_TOKEN + "2"], [GOOD_EXPIRY, GOOD_EXPIRY])
expect("render exports the tokens", "export TRAFFIC_BEARER_TOKENS='%s,%s'" % (GOOD_TOKEN, GOOD_TOKEN + "2") in text)

for url in ["http://127.0.0.1:8080", "http://localhost", "http://[::1]:1/x"]:
    expect("loopback accepted " + url, seed.loopback_base(url))
for url in ["http://127.0.0.1.evil.com", "http://evil.com", "http://localhost@evil.com", "http://127.0.0.1:80a", "ftp://localhost", ""]:
    expect("loopback refused " + url, not seed.loopback_base(url))

expect("redirects refused", seed.NoRedirect().redirect_request(None, None, 302, "Found", {}, "http://evil.example/") is None)
expect("no proxy handler, so proxy environment variables are ignored", not any(isinstance(h, urllib.request.ProxyHandler) for h in seed.OPENER.handlers))

class Response:
    def __init__(self, body): self.body = json.dumps(body).encode()
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def read(self, *a): return self.body
class Opener:
    def __init__(self, outcome): self.outcome = outcome
    def open(self, request, timeout=None):
        if isinstance(self.outcome, Exception): raise self.outcome
        return Response(self.outcome)
original_opener = seed.OPENER
try:
    seed.OPENER = Opener({"errors": [{"message": "x", "extensions": {"code": "RATE_LIMITED", "retryAfter": 30}}]})
    try:
        seed.post("http://127.0.0.1:1", "q", {}, rate_limit_reason="auth-rate-limited"); bad.append("top-level RATE_LIMITED did not stop")
    except seed.Stop as stop:
        expect("top-level RATE_LIMITED is BLOCKED auth-rate-limited", (stop.code, stop.reason) == (2, "auth-rate-limited"))
    seed.OPENER = Opener(urllib.error.HTTPError("http://127.0.0.1:1/graphql", 302, "Found", {}, None))
    try:
        seed.post("http://127.0.0.1:1", "q", {}); bad.append("redirect answer did not stop")
    except seed.Stop as stop:
        expect("redirect answer is a failure, not followed", stop.code == 1)
finally:
    seed.OPENER = original_opener

calls = []
def fake_post(responses):
    def post(base, query, variables, token=None, rate_limit_reason="rate-limited"):
        calls.append(query.split("(")[0].split()[1]); return responses.pop(0)
    return post
original_post = seed.post
directory = tempfile.mkdtemp(dir=sys.argv[2])
env_path = os.path.join(directory, "sub", "seed.env")
try:
    seed.post = fake_post([{"login": {"__typename": "DomainError", "code": "RATE_LIMITED", "retryAfterSeconds": 30}}])
    try:
        seed.auth("http://127.0.0.1:1", "n", "CANDIDATE", {}, "PERF_CANDIDATE_1_PASSWORD", {"PERF_CANDIDATE_1_PASSWORD": "B" * 20}, env_path)
        bad.append("rate limited login did not stop")
    except seed.Stop as stop:
        expect("rate limited login is BLOCKED auth-rate-limited", (stop.code, stop.reason) == (2, "auth-rate-limited"))
    expect("rate limited login never reaches signUp", calls == ["Login"])
    # A signup whose token is hostile: the password is still saved (the account exists), nothing else is written.
    passwords = {}
    seed.post = fake_post([{"signUp": {"__typename": "AuthSuccess", "accessToken": "bad token", "expiresAt": GOOD_EXPIRY}}])
    try:
        seed.auth("http://127.0.0.1:1", "n", "CANDIDATE", {}, "PERF_CANDIDATE_1_PASSWORD", passwords, env_path)
        bad.append("hostile signup token accepted")
    except seed.Stop as stop:
        expect("hostile signup token fails closed", stop.code == 1)
    saved = open(env_path).read()
    expect("password saved right after signup", "PERF_CANDIDATE_1_PASSWORD='%s'" % passwords["PERF_CANDIDATE_1_PASSWORD"] in saved)
    expect("no token written for a hostile signup", "TRAFFIC_BEARER_TOKENS" not in saved)
    expect("credentials file is 0600", stat.S_IMODE(os.stat(env_path).st_mode) == 0o600)
    expect("no temporary file left", os.listdir(os.path.dirname(env_path)) == ["seed.env"])
finally:
    seed.post = original_post
print("ok" if not bad else "FAILED: " + "; ".join(bad))
PY
)"
check "seed-local-jobs.py validation, rate limit, persistence and redirect behaviour" "ok" "$python_result"

if (( failures > 0 )); then
  printf 'RESULT: FAIL checks=%d failures=%d\n' "$checks" "$failures" >&2
  exit 1
fi
printf 'RESULT: PASS checks=%d failures=0\n' "$checks"