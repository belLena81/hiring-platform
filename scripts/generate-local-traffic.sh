#!/usr/bin/env bash
# Sends unauthenticated, read-only requests to a local API so the Grafana dashboard has data.
# Usage: scripts/generate-local-traffic.sh [seconds=60] [base-url=http://127.0.0.1:8080]
# Mix: health checks, a trivial GraphQL query, and an invalid query (4xx). No credentials or personal data are sent.
set -euo pipefail

duration="${1:-60}"
base="${2:-http://127.0.0.1:8080}"
end=$((SECONDS + duration))
sent=0

while ((SECONDS < end)); do
  curl -fsS -o /dev/null "$base/health" || true
  curl -sS -o /dev/null -H 'Content-Type: application/json' \
    -d '{"query":"{ __typename }"}' "$base/graphql" || true
  curl -sS -o /dev/null -H 'Content-Type: application/json' \
    -d '{"query":"{ notAField }"}' "$base/graphql" || true
  sent=$((sent + 3))
  sleep 0.2
done

echo "sent $sent requests to $base in ${duration}s"
