#!/usr/bin/env bash
set -euo pipefail
umask 077
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$repo_root"
case "${1:-}" in
  start)
    scripts/run-local-tests.sh start
    printf 'Run scripts/run-local-tests.sh interview for isolated Kafka integration scenarios. Each run owns locked temporary namespaces.\n'
    ;;
  retention) scripts/run-local-tests.sh retention ;;
  stop) scripts/run-local-tests.sh stop ;;
  *) printf 'Usage: %s start|retention|stop\n' "$0" >&2; exit 2 ;;
esac
