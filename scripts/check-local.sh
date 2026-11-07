#!/usr/bin/env bash
# Full local verification: skills, docs, container scripts, Java, then unit + integration suites of both builds.
# Formatting is not checked here: the pre-commit hook formats every commit. A suite whose inputs are unchanged since its last PASS is skipped.
# Contract: see "Operator scripts" in docs/engineering-quality.md. Run with --help for usage and exit codes.
set -euo pipefail

project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"
# shellcheck source=scripts/operator-lib.sh
source "$project_root/scripts/operator-lib.sh"

readonly total_steps=6
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
  1/6 project skill validation        (python3 -I scripts/check-skills.py)
  2/6 documentation currency and spec verdicts (python3 -I scripts/check-docs-current.py: unpushed decision commits carry docs, uncommitted
                                      changes only warn; python3 -I scripts/check-spec-verdicts.py: every done spec records its verdicts)
  3/6 container script checks         (scripts/check-container-runtime.sh)
  4/6 Java 17+ check
  5/6 root unit + integration suites under JaCoCo, 85% merged coverage gate (scripts/run-local-tests.sh test)
  6/6 analytics unit + integration suites under JaCoCo (scripts/run-local-tests.sh analytics)
Formatting is not checked: .githooks/pre-commit formats every commit, and agents may not skip it.
Steps 5 and 6 are skipped when the content of their inputs (sources, tests, build, docker, compose, test wrapper; never docs, specs,
.claude or other scripts) equals the content that last passed, whatever was committed in between. The fingerprints are kept in
.local/logs/check-local-passed-root and -analytics, written only on a PASS. CHECK_LOCAL_FORCE=1 runs them regardless.
Needs Docker for steps 5-6 when they run. Recorded total for a whole run: 1h48m-2h18m. Each step's duration is printed when it ends, and a summary of
all step durations (including the step that failed or was blocked) is printed at the end of a PASS, FAIL or BLOCKED run, not after an interrupt. A status line is printed at least every 10 s; the test steps log to
.local/logs/test-services/run.log and the formatting steps to .local/logs/check-local-*.log.

Environment:
  JAVA_HOME                          JDK used for the checks and passed to sbt (-java-home)
  HIRING_TEST_JAVA_HOME              JDK for the test wrapper (defaults to JAVA_HOME)
  HIRING_TEST_TIMEOUT_SECONDS        limit per sbt invocation of the test wrapper, 60-86400 (default 14400)
  CHECK_LOCAL_FORCE                  1 runs both suites even when their inputs are unchanged since the last PASS
  (the wrapper also reads HIRING_TEST_LOCK_WAIT_SECONDS and COMPOSE_TIMEOUT_SECONDS; see scripts/run-local-tests.sh --help)

Exit codes: 0 PASS, 1 FAIL, 2 usage error or precondition not met (BLOCKED), 130 interrupted.
The last line is "RESULT: PASS|FAIL|BLOCKED ..." with the step reached and duration.

Examples:
  scripts/check-local.sh
  JAVA_HOME=/path/to/jdk17 scripts/check-local.sh
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
printf 'check-local: runs %s checks in order (skills, documentation currency, container scripts, Java, tests x2; a suite whose inputs are unchanged since its last PASS is skipped); a full run is recorded at 1h48m-2h18m; sbt invocations bounded by HIRING_TEST_TIMEOUT_SECONDS=%s\n' \
  "$total_steps" "${HIRING_TEST_TIMEOUT_SECONDS:-14400}"

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

git check-ignore -q "$project_root/.local/logs/check-local-passed-root" || {
  printf 'check-local: .local/logs is not git-ignored; add it to .gitignore first\n' >&2
  finish BLOCKED 2 "reason=logs-not-ignored"
}
mkdir -p "$project_root/.local/logs"

run_foreground "project skill validation" "fix the reported skill or agent definition, then run again" python3 -I scripts/check-skills.py
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

command -v sbt >/dev/null 2>&1 || { current_step="sbt lookup"; step_failed 2 "install sbt and put it on PATH"; }

# The fingerprint covers file content, not commits, so committing (and the pre-commit formatter) between a PASS and the next run
# changes nothing unless a covered file's content changed. Docs, specs, .claude and the other scripts are never covered.
fingerprint() { # PATHSPEC...
  git ls-files -z --cached --others --exclude-standard -- "$@" ':(exclude)*.md' | sort -zu | xargs -0 -r sha256sum 2>/dev/null | sha256sum | cut -d' ' -f1
}
common_inputs=(test-support docker compose.yaml compose.test.yaml Dockerfile .sbtopts .jvmopts scripts/run-local-tests.sh scripts/operator-lib.sh scripts/acceptance-lib.sh)

# run_suite_step NAME DESCRIPTION MODE PATHSPEC...: runs the wrapper unless the inputs equal the last PASS, then records the PASS.
# The integration suites need Docker; the runner owns this workspace's test services. A run fails when merged coverage is below 85%.
run_suite_step() {
  local name="$1" description="$2" mode="$3" stamp current
  shift 3
  stamp="$project_root/.local/logs/check-local-passed-$name"
  current="$(fingerprint "$@")"
  if [[ "${CHECK_LOCAL_FORCE:-0}" != 1 && -f "$stamp" && "$(cat "$stamp")" == "$current" ]]; then
    begin_step "$description"
    op_say "skipped: inputs unchanged since the last PASS (CHECK_LOCAL_FORCE=1 runs it anyway)"
    completed
    return 0
  fi
  rm -f "$stamp"
  run_foreground "$description" "read the RESULT and log excerpt above; log: .local/logs/test-services/run.log" scripts/run-local-tests.sh "$mode"
  printf '%s\n' "$current" >"$stamp"
}

export HIRING_TEST_JAVA_HOME="${HIRING_TEST_JAVA_HOME:-${JAVA_HOME:-}}"
run_suite_step root "root unit + integration suites with coverage gate" test src build.sbt project "${common_inputs[@]}"
run_suite_step analytics "analytics unit + integration suites with coverage gate" analytics "${common_inputs[@]}" analytics

printf '%s\n' 'Unit, integration and merged coverage checks passed (or their inputs were unchanged since the last PASS) for the application and analytics builds. Run any additional migration, compatibility, or performance checks required by this task; see docs/engineering-quality.md.'
finish PASS 0 "checks=unit,integration,coverage"
