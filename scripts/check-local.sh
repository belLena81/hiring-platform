#!/usr/bin/env bash
# Full local verification: skills, container scripts, Java, formatting, then unit + integration suites of both builds.
# Contract: see "Operator scripts" in docs/engineering-quality.md. Run with --help for usage and exit codes.
set -euo pipefail

project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"
# shellcheck source=scripts/operator-lib.sh
source "$project_root/scripts/operator-lib.sh"

readonly total_steps=8
format_limit="${CHECK_LOCAL_FORMAT_TIMEOUT_SECONDS:-900}"
readonly log_dir_display=".local/logs"
current_step="starting"
step_number=0
step_started=$SECONDS
step_times=()
result_done=0

help_text() {
  cat <<'HELP'
Usage: scripts/check-local.sh
       scripts/check-local.sh --help

Runs, in order, and stops at the first failure:
  1/8 project skill validation        (python3 scripts/check-skills.py)
  2/8 documentation currency and spec verdicts (python3 scripts/check-docs-current.py: uncommitted and unpushed decision changes carry docs;
                                      python3 scripts/check-spec-verdicts.py: every done spec records Code, Security and QA verdicts)
  3/8 container script checks         (scripts/check-container-runtime.sh)
  4/8 Java 17+ check
  5/8 root build formatting           (sbt scalafmtCheckAll scalafmtSbtCheck, limit 15 min)
  6/8 analytics build formatting      (same, limit 15 min)
  7/8 root unit + integration suites under JaCoCo, 85% merged coverage gate (scripts/run-local-tests.sh test)
  8/8 analytics unit + integration suites under JaCoCo (scripts/run-local-tests.sh analytics)
Needs Docker for steps 7-8. Recorded total for a whole run: 1h48m-2h18m. Each step's duration is printed when it ends, and a summary of
all step durations (including the step that failed or was blocked) is printed at the end of a PASS, FAIL or BLOCKED run, not after an interrupt. A status line is printed at least every 10 s; the test steps log to
.local/logs/test-services/run.log and the formatting steps to .local/logs/check-local-*.log.

Environment:
  JAVA_HOME                          JDK used for the checks and passed to sbt (-java-home)
  HIRING_TEST_JAVA_HOME              JDK for the test wrapper (defaults to JAVA_HOME)
  HIRING_TEST_TIMEOUT_SECONDS        limit per sbt invocation of the test wrapper, 60-86400 (default 14400)
  CHECK_LOCAL_FORMAT_TIMEOUT_SECONDS limit for each formatting step, 60-7200 (default 900)
  (the wrapper also reads HIRING_TEST_LOCK_WAIT_SECONDS and COMPOSE_TIMEOUT_SECONDS; see scripts/run-local-tests.sh --help)

Exit codes: 0 PASS, 1 FAIL, 2 usage error or precondition not met (BLOCKED), 130 interrupted.
The last line is "RESULT: PASS|FAIL|BLOCKED ..." with the step reached and duration.

Examples:
  bash scripts/check-local.sh
  JAVA_HOME=/path/to/jdk17 bash scripts/check-local.sh
HELP
}

finish() { # RESULT code detail
  local line="RESULT: $1 steps_completed=$step_number/$total_steps $3 duration=$(op_elapsed)"
  result_done=1
  if ((${#step_times[@]} > 0)); then
    printf 'check-local: step durations:\n' >&2
    printf '  %s\n' "${step_times[@]}" >&2
  fi
  if [[ "$1" == PASS ]]; then printf '%s\n' "$line"; else printf '%s\n' "$line" >&2; fi
  exit "$2"
}

on_interrupt() {
  trap '' INT TERM HUP
  op_ticker_stop
  printf '\n' >&2
  op_say "interrupted during step $current_step; steps completed: $((step_number > 0 ? step_number - 1 : 0))/$total_steps; stopping what this script started" >&2
  op_stop_child 30
  result_done=1
  exit 130
}
on_exit() {
  local code=$?
  op_ticker_stop
  if (( code != 0 && result_done == 0 )); then
    printf 'check-local: unexpected failure during step %s (exit %s)\n' "$current_step" "$code" >&2
    printf 'RESULT: FAIL steps_completed=%s/%s step="%s" duration=%s\n' "$step_number" "$total_steps" "$current_step" "$(op_elapsed)" >&2
  fi
}
trap on_interrupt INT TERM HUP
trap on_exit EXIT

usage_error() {
  printf 'check-local: %s\ncheck-local: run scripts/check-local.sh --help for usage\n' "$1" >&2
  printf 'RESULT: BLOCKED reason=usage steps_completed=0/%s duration=0s\n' "$total_steps" >&2
  result_done=1
  exit 2
}
if [[ "${1:-}" == -h || "${1:-}" == --help ]]; then help_text; result_done=1; exit 0; fi
if (( $# > 0 )); then usage_error "unexpected argument: $1"; fi
[[ "$format_limit" =~ ^[0-9]{1,5}$ ]] && (( 10#$format_limit >= 60 && 10#$format_limit <= 7200 )) ||
  usage_error "CHECK_LOCAL_FORMAT_TIMEOUT_SECONDS must be an integer from 60 to 7200, got: $format_limit"
format_limit=$((10#$format_limit))
readonly format_limit

printf 'check-local: runs %s checks in order (skills, documentation currency, container scripts, Java, formatting x2, tests x2); recorded total for a whole run 1h48m-2h18m; each long step bounded (formatting %s, sbt invocations HIRING_TEST_TIMEOUT_SECONDS=%s)\n' \
  "$total_steps" "$(op_duration "$format_limit")" "${HIRING_TEST_TIMEOUT_SECONDS:-14400}"

begin_step() { # description
  step_number=$((step_number + 1))
  current_step="$step_number/$total_steps $1"
  step_started=$SECONDS
  op_say "step $current_step"
}

# step_failed EXIT_CODE ADVICE [LOG]: reports what failed and what to do, then ends the run.
step_failed() {
  local code="$1" advice="$2" log="${3:-}"
  step_times+=("$current_step: $(op_duration $((SECONDS - step_started))) (failed)")
  step_number=$((step_number - 1))
  if [[ -n "$log" ]]; then op_show_failure_tail "$log" 30; fi
  printf 'check-local: step "%s" failed (exit %s)\n' "$current_step" "$code" >&2
  printf 'check-local: next: %s\n' "$advice" >&2
  case "$code" in
    130) result_done=1; exit 130 ;;
    2) finish BLOCKED 2 "step=\"$current_step\"" ;;
    *) finish FAIL 1 "step=\"$current_step\"" ;;
  esac
}

# Each step's own duration is printed and kept for the summary, so the slow phase is visible without log archaeology.
completed() {
  local took
  took="$(op_duration $((SECONDS - step_started)))"
  step_times+=("$current_step: $took")
  op_say "step ok: $current_step (took $took)"
}

# run_foreground DESCRIPTION ADVICE COMMAND...: output goes to the terminal; each of these steps is short and bounded.
run_foreground() {
  local description="$1" advice="$2" rc=0
  shift 2
  begin_step "$description"
  # Background + wait so a TERM/INT/HUP trap runs at once and can forward the signal; own session so only we signal it.
  setsid "$@" </dev/null &
  OP_CHILD_PID=$!
  wait "$OP_CHILD_PID" || rc=$?
  OP_CHILD_PID=""
  if (( rc != 0 )); then step_failed "$rc" "$advice"; fi
  completed
}

# run_sbt_step DESCRIPTION LOGNAME COMMAND...: output to a log, heartbeat every 10 s, bounded.
run_sbt_step() {
  local description="$1" logname="$2" rc=0 log
  shift 2
  log="$project_root/.local/logs/$logname"
  begin_step "$description"
  op_run_logged "$description" "$format_limit" "$log" yes "$@" || rc=$?
  if (( rc == 124 )); then step_failed 1 "formatting did not finish within $(op_duration "$format_limit"); raise CHECK_LOCAL_FORMAT_TIMEOUT_SECONDS or read $log_dir_display/$logname" "$log"; fi
  if (( rc != 0 )); then step_failed "$rc" "run sbt scalafmtAll scalafmtSbt (in analytics/ for the analytics build) to fix formatting, or read $log_dir_display/$logname" "$log"; fi
  completed
}

git check-ignore -q "$project_root/.local/logs/check-local-root-format.log" || {
  printf 'check-local: .local/logs is not git-ignored; add it to .gitignore first\n' >&2
  finish BLOCKED 2 "reason=logs-not-ignored"
}
mkdir -p "$project_root/.local/logs"

run_foreground "project skill validation" "fix the reported skill or agent definition, then run again" python3 scripts/check-skills.py
run_foreground "documentation currency and spec verdicts" "update README.md, ARCHITECTURE.md or docs/ (docs/wiki/ included) for the listed decision changes, or commit with a Docs-Impact trailer first; record the missing review verdict lines of a done spec" bash -c 'python3 -I scripts/check-docs-current.py && python3 -I scripts/check-spec-verdicts.py'
run_foreground "container script checks" "read the failure printed above, fix scripts/check-container-runtime.sh or docker/*.sh" scripts/check-container-runtime.sh

begin_step "Java 17+ check"
java_command="${JAVA_HOME:+$JAVA_HOME/bin/}java"
if ! command -v "$java_command" >/dev/null 2>&1; then
  step_failed 2 "install a JDK 17 or newer and set JAVA_HOME to it"
fi
java_version="$("$java_command" -XshowSettings:properties -version 2>&1 | awk '/java.specification.version =/ { print $3 }')"
java_major="${java_version#1.}"
if [[ ! "$java_major" =~ ^[0-9]+$ ]] || (( java_major < 17 )); then
  printf '%s\n' 'Java 17 or newer is required. Set JAVA_HOME to your JDK installation before running local checks.' >&2
  step_failed 2 "set JAVA_HOME to a JDK 17 or newer installation, then run again"
fi
completed

# The sbt launcher uses the PATH java unless told otherwise; both builds target Java 17.
sbt_args=()
if [[ -n "${JAVA_HOME:-}" ]]; then
  sbt_args=(-java-home "$JAVA_HOME")
fi
command -v sbt >/dev/null 2>&1 || { current_step="sbt lookup"; step_failed 2 "install sbt and put it on PATH"; }

# Both builds: formatting gates first, then unit and integration suites under JaCoCo.
# The integration suites need Docker; the runner owns this workspace's test services.
# A full run fails when merged unit + integration line coverage is below 85%.
run_sbt_step "root build formatting" check-local-root-format.log sbt "${sbt_args[@]}" scalafmtCheckAll scalafmtSbtCheck
run_sbt_step "analytics build formatting" check-local-analytics-format.log bash -c 'cd analytics && exec sbt "$@"' check-local "${sbt_args[@]}" scalafmtCheckAll scalafmtSbtCheck
export HIRING_TEST_JAVA_HOME="${HIRING_TEST_JAVA_HOME:-${JAVA_HOME:-}}"
run_foreground "root unit + integration suites with coverage gate" "read the RESULT and log excerpt above; log: .local/logs/test-services/run.log" scripts/run-local-tests.sh test
run_foreground "analytics unit + integration suites with coverage gate" "read the RESULT and log excerpt above; log: .local/logs/test-services/run.log" scripts/run-local-tests.sh analytics

printf '%s\n' 'Formatting, unit, integration and merged coverage checks passed for the application and analytics builds. Run any additional migration, compatibility, or performance checks required by this task; see docs/engineering-quality.md.'
finish PASS 0 "checks=formatting,unit,integration,coverage"
