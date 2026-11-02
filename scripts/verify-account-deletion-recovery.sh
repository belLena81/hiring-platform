#!/usr/bin/env bash
# Runs the account-deletion recovery evidence (AccountDeletionRecoveryIntegrationSpec, HAL-04.4) through the test wrapper.
# Contract: see "Operator scripts" in docs/engineering-quality.md. Run with --help for usage and exit codes.
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"

case "${1:-}" in
  -h|--help)
    cat <<'HELP'
Usage: scripts/verify-account-deletion-recovery.sh
       scripts/verify-account-deletion-recovery.sh --help

Runs AccountDeletionRecoveryIntegrationSpec by calling `scripts/run-local-tests.sh recovery`: it starts or reuses this
workspace's isolated test services, compiles the API classpath, then runs the suite against synthetic data only.
No recorded duration exists for this mode alone (expect several minutes); each sbt invocation is limited to HIRING_TEST_TIMEOUT_SECONDS (default 14400). The wrapper prints a
status line (phase, elapsed time, last log line) every 10 s and logs to .local/logs/test-services/run.log.

Exit codes (from the wrapper): 0 PASS, 1 FAIL, 2 usage error or precondition not met (BLOCKED), 130 interrupted.
The last line is "RESULT: PASS|FAIL|BLOCKED mode=recovery ..." with test counts, duration and the log path.

Example:
  scripts/verify-account-deletion-recovery.sh
HELP
    exit 0 ;;
  "") ;;
  *)
    printf 'verify-account-deletion-recovery: unexpected argument: %s\n' "$1" >&2
    printf 'verify-account-deletion-recovery: run scripts/verify-account-deletion-recovery.sh --help for usage\n' >&2
    printf 'RESULT: BLOCKED mode=recovery reason=usage duration=0s\n' >&2
    exit 2 ;;
esac

printf 'verify-account-deletion-recovery: delegating to scripts/run-local-tests.sh recovery (no recorded duration, expect several minutes; the wrapper reports progress every 10 s)\n'
# exec keeps signals and the exit code (0, 1, 2, 130) exactly as the wrapper reports them.
exec "$repo_root/scripts/run-local-tests.sh" recovery
