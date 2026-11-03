#!/usr/bin/env bash
# Pure helpers of scripts/generate-local-traffic.sh (the search measurement mix) and scripts/seed-local-jobs.sh. Sourced,
# never executed; no I/O besides stdout. Kept separate so scripts/tests/local-traffic-options.sh can test them without a network.

# Minimum number of successful requests per run for percentiles (p99 in particular) to mean anything.
readonly TRAFFIC_MIN_SAMPLES=1000

# traffic_percentiles: reads request durations in seconds (one number per line) on stdin and prints
# "count p50_ms p95_ms p99_ms" using the nearest-rank method (rank = ceil(p * n / 100) of the ascending values).
# With no input it prints "0 0.000 0.000 0.000".
traffic_percentiles() {
  LC_ALL=C sort -g | LC_ALL=C awk '
    NF { v[++n] = $1 }
    function rank(p,   r) { r = int(p * n / 100); if (r < p * n / 100) r++; return (r < 1 ? 1 : (r > n ? n : r)) }
    END {
      if (n == 0) { print "0 0.000 0.000 0.000"; exit }
      printf "%d %.3f %.3f %.3f\n", n, v[rank(50)] * 1000, v[rank(95)] * 1000, v[rank(99)] * 1000
    }'
}

# traffic_base_ok URL: succeeds only for an http(s) URL whose host is exactly 127.0.0.1, localhost or [::1], with an
# optional port and path (no credentials, query or spaces). Credentials are sent to this URL, so it must stay on this machine.
traffic_base_ok() {
  [[ "$1" =~ ^https?://(127\.0\.0\.1|localhost|\[::1\])(:[0-9]{1,5})?(/[A-Za-z0-9._~/-]*)?$ ]]
}

# traffic_latency_path REPO PATH: prints the resolved absolute path for --latency-file, or returns 1 when it is not strictly
# below REPO/.local/logs/ after resolving ".." and symbolic links (relative paths are relative to REPO; ".." is refused outright).
traffic_latency_path() {
  local repo="$1" path="$2" abs resolved logs
  [[ -n "$path" && "$path" != */ ]] || return 1
  [[ "/$path/" != *"/../"* ]] || return 1
  case "$path" in /*) abs="$path" ;; *) abs="$repo/$path" ;; esac
  logs="$(realpath -m -- "$repo/.local/logs")" || return 1
  resolved="$(realpath -m -- "$abs")" || return 1
  case "$resolved" in "$logs/"?*) ;; *) return 1 ;; esac
  [[ "${resolved#"$logs"/}" =~ ^[A-Za-z0-9._/-]+$ ]] || return 1
  printf '%s\n' "$resolved"
}

# traffic_concurrency_ok VALUE: succeeds for an integer from 1 to 16 (no leading-zero tricks, at most two digits).
traffic_concurrency_ok() {
  [[ "$1" =~ ^[0-9]{1,2}$ ]] && (( 10#$1 >= 1 && 10#$1 <= 16 ))
}

# traffic_pause_seconds WORKERS TOKENS AVG_COST LIMIT: seconds each worker waits between requests so that the busiest token
# stays at or below 80 percent of its per-actor cost budget (LIMIT per 60 s window). Workers use the tokens round-robin, so
# the busiest token serves ceil(WORKERS / TOKENS) workers: pause = ceil(W / K) * 60 * AVG_COST / (0.8 * LIMIT), between
# 0.010 s (no hot loop) and 10 s. Without cost information it prints 1.000.
traffic_pause_seconds() {
  LC_ALL=C awk -v w="$1" -v k="$2" -v c="$3" -v l="$4" 'BEGIN {
    if (l <= 0 || k <= 0 || c <= 0) { print "1.000"; exit }
    busiest = int((w + k - 1) / k)
    p = busiest * 60 * c / (0.8 * l); if (p > 10) p = 10; if (p < 0.01) p = 0.01
    printf "%.3f\n", p
  }'
}

# traffic_min_seconds WORKERS PAUSE: seconds a run needs to reach TRAFFIC_MIN_SAMPLES requests at the paced rate
# (WORKERS requests per PAUSE seconds, latency ignored, so a slight underestimate).
traffic_min_seconds() {
  LC_ALL=C awk -v w="$1" -v p="$2" -v n="$TRAFFIC_MIN_SAMPLES" 'BEGIN {
    if (w <= 0 || p <= 0) { print 0; exit }
    s = n * p / w; r = int(s); if (r < s) r++
    print r
  }'
}
