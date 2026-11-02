#!/usr/bin/env bash
# One scripted acceptance run of the hiring end-to-end story (UC01-UC13) on the local stack, with an evidence file.
# Contract: see "Operator scripts" in docs/engineering-quality.md. Spec: docs/specs/hiring-end-to-end-acceptance.md.
# Run with --help for usage and exit codes. This script only calls scripts/run-local-tests.sh (the owner of the
# isolated test services); it never starts, stops or touches any other compose stack, and it never commits.
set -Eeuo pipefail
umask 077
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$repo_root"
# shellcheck source=scripts/operator-lib.sh
source "$repo_root/scripts/operator-lib.sh"
run_start_marker="" # set per run; the verdict functions live in scripts/acceptance-lib.sh (self-test: scripts/tests/)
chain_dir="$repo_root/.local/data/hiring-story-analytics"
# shellcheck source=scripts/acceptance-lib.sh
source "$repo_root/scripts/acceptance-lib.sh"

exec 3>&1 4>&2 # terminal descriptors, restored by the traps
wrapper="scripts/run-local-tests.sh"
wrapper_path="$repo_root/$wrapper"
step_limit="${ACCEPTANCE_STEP_TIMEOUT_SECONDS:-3600}"
repeat=1
result_done=0
current_phase="preparing"
err_line=""
err_cmd=""
run_stamp="$(date -u +%Y%m%dT%H%M%SZ)"
evidence_dir_rel=".local/logs/acceptance/$run_stamp"
evidence_dir="$repo_root/$evidence_dir_rel"
evidence_file="$evidence_dir/evidence.md"
chain_dir="$repo_root/.local/data/hiring-story-analytics"
chain_marker="$chain_dir/chain-started"
marker_owned=0
overall_blocked_reason=""

declare -A UC_TITLE=(
  [01]="Search jobs by structured filters" [02]="Semantic / hybrid job search" [03]="View job details"
  [04]="Submit application" [05]="View own applications" [06]="Recommend jobs to a candidate" [07]="Manage job"
  [08]="Recruiter lists applications for a job" [09]="Change application status" [10]="Semantic candidate search"
  [11]="Publish and process domain events" [12]="Hiring funnel analytics" [13]="Time-to-hire analytics"
)
# Per run (key "run:id"): step status, exit code, log, counts, seconds; UC parts "status|step|detail" joined by newlines.
sbt_log_source="$repo_root/.local/logs/test-services/run.log"
declare -A STEP_STATUS STEP_SBT STEP_RC STEP_LOG STEP_COUNTS STEP_SECONDS STEP_NOTE RUN_SIGNATURE
declare -A RUN_GIT_STATUS=()
steps_run=()

help_text() {
  cat <<'HELP'
Usage: scripts/run-acceptance.sh [--repeat N] [--help]

Runs the hiring end-to-end acceptance story once (or N times) on this workspace's isolated test stack and writes an
evidence file that maps every use case UC01-UC13 and HEA-14..16 to PASS, FAIL, BLOCKED or SKIPPED with the log of the
step that produced it. Steps, in order (each is one bounded call of scripts/run-local-tests.sh, labelled as in the run):
  1/5 Tier A story and role matrix
               test '*HiringStoryAcceptanceIntegrationSpec' '*HiringRoleMatrixAcceptanceIntegrationSpec'
               UC01, 03, 04, 05, 07, 08, 09, UC11 publication, HEA-14 role matrix; exports the story's events
  The analytics chain of design D7 for both datasets (12-candidate story and hired cohort) is steps 2 to 4. The chain
  marker .local/data/hiring-story-analytics/chain-started is created before step 1 and removed when the script ends,
  whatever the outcome; the script also checks that each report is newer than the run and was built from the current
  events file (SHA-256 binding). They give UC11 consumption, UC12 and UC13:
  2/5 chain: hired cohort export
               test '*HiringCohortStoryIntegrationSpec'
  3/5 chain: Kafka to Spark/Delta to report
               analytics '*HiringStoryAnalyticsReportIntegrationSpec' '*HiringCohortAnalyticsReportIntegrationSpec'
  4/5 chain: Admin report through the API
               test '*HiringStoryAnalyticsAdminReportIntegrationSpec' '*HiringCohortAnalyticsAdminReportIntegrationSpec'
  5/5 Tier B search on Atlas Local
               test '*HiringSearchAcceptanceIntegrationSpec': UC02, UC06, UC10. Runs only when
               ATLAS_TEST_URI, ATLAS_TEST_DISPOSABLE=true and ATLAS_TEST_ALLOWED_HOSTS are set and Atlas Local answers
               on loopback; otherwise UC02/06/10 are BLOCKED and the exact start command is printed.

Options:
  --repeat N   run the whole sequence N times (1-3, default 1) and compare the counts between runs (HEA-15)
  --help       print this text

Environment:
  ACCEPTANCE_STEP_TIMEOUT_SECONDS  limit for each wrapper call, 600-86400 (default 3600); the wrapper's own limit is set
                                   120 s lower so it stops first and cleans up
  ATLAS_TEST_URI, ATLAS_TEST_DISPOSABLE, ATLAS_TEST_ALLOWED_HOSTS  Tier B guard (see docs/search-evaluation.md)
  HIRING_TEST_JAVA_HOME            JDK for the wrapper (Java 17+)

Expected duration per run: about 30-45 min without Tier B (Tier A and the Admin step a few minutes each, the analytics
step about 2 x 310 s unloaded and up to 2 x 620 s under load), Tier B adds roughly 10-20 min (not measured). Progress is
printed at least every 10 s; step logs are under .local/logs/acceptance/<UTC timestamp>/run-N/ and the wrapper's own log
is .local/logs/test-services/run.log. Total bound: runs x 5 wrapper calls x the step limit.

Exit codes: 0 PASS (every UC01-13 PASS), 1 FAIL, 2 BLOCKED (a use case could not run, for example Atlas Local not
started) or usage error, 130 interrupted (Ctrl+C, SIGTERM or SIGHUP). The last line is
"RESULT: PASS|FAIL|BLOCKED ..." with counts, duration and the evidence path.

Start Atlas Local for Tier B (a human runs these; agents cannot):
  docker compose -f compose.atlas-local.yaml up -d --wait
  export ATLAS_TEST_URI='mongodb://127.0.0.1:27018/?directConnection=true' ATLAS_TEST_DISPOSABLE=true \
         ATLAS_TEST_ALLOWED_HOSTS=127.0.0.1:27018
  scripts/run-acceptance.sh --repeat 2
  docker compose -f compose.atlas-local.yaml down --volumes

Examples:
  scripts/run-acceptance.sh
  scripts/run-acceptance.sh --repeat 2
HELP
}

finish() { # RESULT code detail
  local line="RESULT: $1 $3 duration=$(op_elapsed) evidence=$evidence_dir_rel/evidence.md"
  result_done=1
  if [[ "$1" == PASS ]]; then printf '%s\n' "$line"; else printf '%s\n' "$line" >&2; fi
  exit "$2"
}

usage_error() {
  printf 'run-acceptance: %s\n' "$1" >&2
  printf 'run-acceptance: run scripts/run-acceptance.sh --help for usage\n' >&2
  result_done=1
  printf 'RESULT: BLOCKED reason=usage duration=%s\n' "$(op_elapsed)" >&2
  exit 2
}

remove_marker() {
  if (( marker_owned )); then
    rm -f -- "$chain_marker" 2>/dev/null || true
    marker_owned=0
  fi
}

on_error() { err_line="${BASH_LINENO[0]:-?}"; err_cmd="$BASH_COMMAND"; }
on_exit() {
  local code=$?
  exec 1>&3 2>&4
  op_ticker_stop
  remove_marker
  if (( code != 0 && result_done == 0 )); then
    printf 'run-acceptance: unexpected failure in phase "%s" at line %s (%s), exit %s\n' "$current_phase" "$err_line" "$err_cmd" "$code" >&2
    printf 'run-acceptance: next: read the message above and the step logs under %s\n' "$evidence_dir_rel" >&2
    printf 'RESULT: FAIL phase=%s duration=%s evidence=%s/evidence.md\n' "$current_phase" "$(op_elapsed)" "$evidence_dir_rel" >&2
  fi
}
on_interrupt() {
  exec 1>&3 2>&4
  trap '' INT TERM HUP
  op_ticker_stop
  printf '\n' >&2
  op_say "interrupted during \"$current_phase\"; stopping only the wrapper call this script started (up to 20 s)" >&2
  op_stop_child 20
  remove_marker
  if [[ -d "$evidence_dir" ]]; then
    write_evidence INTERRUPTED >/dev/null 2>&1 || true
    op_say "interrupted: completed steps are listed in $evidence_dir_rel/evidence.md; test services stay running for reuse" >&2
  else
    op_say "interrupted before any step ran" >&2
  fi
  result_done=1
  exit 130
}
trap on_error ERR
trap on_exit EXIT
trap on_interrupt INT TERM HUP

# ---- arguments --------------------------------------------------------------------------------------------------
while (( $# > 0 )); do
  case "$1" in
    -h|--help) help_text; result_done=1; exit 0 ;;
    --repeat)
      (( $# >= 2 )) || usage_error "--repeat needs a value from 1 to 3"
      [[ "$2" =~ ^[1-3]$ ]] || usage_error "--repeat must be 1, 2 or 3, got: $2"
      repeat="$2"; shift 2 ;;
    *) usage_error "unknown argument: $1" ;;
  esac
done
[[ "$step_limit" =~ ^[0-9]{1,6}$ ]] && (( 10#$step_limit >= 600 && 10#$step_limit <= 86400 )) \
  || usage_error "ACCEPTANCE_STEP_TIMEOUT_SECONDS must be an integer from 600 to 86400, got: $step_limit"
step_limit=$((10#$step_limit))
inner_limit=$((step_limit - 120))

blocked() { # message, next action
  printf 'run-acceptance: %s\n' "$1" >&2
  printf 'run-acceptance: next: %s\n' "${2:-fix the precondition above and run again}" >&2
  result_done=1
  printf 'RESULT: BLOCKED reason=precondition duration=%s\n' "$(op_elapsed)" >&2
  exit 2
}

for tool in python3 flock setsid timeout git sha256sum sed awk grep; do
  command -v "$tool" >/dev/null 2>&1 || blocked "required tool not found: $tool" "install $tool and put it on PATH"
done
[[ -x "$wrapper_path" ]] || blocked "the test wrapper ${wrapper_path#"$repo_root"/} is missing or not executable" "restore it from the repository"
git check-ignore -q ".local/logs/acceptance/x" || blocked "the evidence directory is not git-ignored" "add .local/ to .gitignore, then run again"
git check-ignore -q ".local/data/hiring-story-analytics/chain-started" || blocked "the chain hand-off directory is not git-ignored" "add .local/ to .gitignore, then run again"

mkdir -p -m 700 "$repo_root/.local/logs/acceptance"
exec 9>"$repo_root/.local/logs/acceptance/.lock"
flock -n 9 || blocked "another scripts/run-acceptance.sh run holds the lock" "wait for it to finish"
mkdir -p -m 700 "$evidence_dir"

# ---- helpers ----------------------------------------------------------------------------------------------------

# Atlas Local availability for Tier B. Prints "ok" or a reason; never prints the URI (it may carry credentials).
atlas_state() {
  local uri="${ATLAS_TEST_URI:-}" authority host port
  if [[ -z "$uri" || "${ATLAS_TEST_DISPOSABLE:-}" != true || -z "${ATLAS_TEST_ALLOWED_HOSTS:-}" ]]; then
    printf 'ATLAS_TEST_URI, ATLAS_TEST_DISPOSABLE=true and ATLAS_TEST_ALLOWED_HOSTS are not all set'; return 0
  fi
  authority="${uri#*://}"; authority="${authority%%[/?]*}"; authority="${authority##*@}"; authority="${authority%%,*}"
  host="${authority%%:*}"; port="${authority##*:}"
  [[ "$authority" == *:* ]] || port=27017
  case "$host" in
    127.0.0.1|localhost) ;;
    *) printf 'ATLAS_TEST_URI is not a loopback host (Tier B runs on Atlas Local only)'; return 0 ;;
  esac
  [[ "$port" =~ ^[0-9]{1,5}$ ]] || { printf 'ATLAS_TEST_URI has no usable port'; return 0; }
  if timeout 5 bash -c "exec 5<>/dev/tcp/127.0.0.1/$port" 2>/dev/null; then printf 'ok'
  else printf 'nothing answers on 127.0.0.1:%s' "$port"; fi
}

atlas_start_hint() {
  cat >&2 <<'HINT'
run-acceptance: to run Tier B (UC02, UC06, UC10) start Atlas Local and authorize the disposable deployment:
  docker compose -f compose.atlas-local.yaml up -d --wait
  export ATLAS_TEST_URI='mongodb://127.0.0.1:27018/?directConnection=true' ATLAS_TEST_DISPOSABLE=true ATLAS_TEST_ALLOWED_HOSTS=127.0.0.1:27018
  scripts/run-acceptance.sh
  (afterwards: docker compose -f compose.atlas-local.yaml down --volumes)
HINT
}


# The sbt log of a step when the wrapper produced one, else the wrapper's own log.
sbt_log_of() { printf '%s' "${STEP_SBT[$1]:-$repo_root/${STEP_LOG[$1]}}"; }

# run_step RUN ID LABEL INNER... : one bounded wrapper call. Sets STEP_* for "RUN:ID" and returns 0 whatever the outcome,
# except an interrupt, which the trap handles.
run_step() {
  local run="$1" id="$2" label="$3" key="$1:$2" log rc=0 started="$SECONDS" counts
  shift 3
  log="$evidence_dir/run-$run/$(printf '%02d' $(( ${#steps_run[@]} + 1 )))-$id.log"
  steps_run+=("$key")
  STEP_LOG[$key]="${log#"$repo_root"/}"
  current_phase="run $run: $label"
  op_say "run $run: $label (limit $(op_duration "$step_limit"); log: ${STEP_LOG[$key]})"
  : >"$evidence_dir/.step-start"
  # fd 9 (the run lock) is closed for the child so a stray child cannot keep this script's lock.
  HIRING_TEST_TIMEOUT_SECONDS="$inner_limit" op_run_logged "run $run: $label" "$step_limit" "$log" yes "$@" 9>&- || rc=$?
  STEP_RC[$key]="$rc"
  # The wrapper prints only a heartbeat; the per-suite results are in its sbt log, which the next call overwrites.
  if [[ "$sbt_log_source" -nt "$evidence_dir/.step-start" ]]; then
    cp -- "$sbt_log_source" "${log%.log}.sbt.log"
    STEP_SBT[$key]="${log%.log}.sbt.log"
  fi
  STEP_SECONDS[$key]=$((SECONDS - started))
  read -r total passed failed skipped <<<"$(op_count_tests "${STEP_SBT[$key]:-$log}")"
  counts="total=$total passed=$passed failed=$failed skipped=$skipped"
  STEP_COUNTS[$key]="$counts"
  case "$rc" in
    0) STEP_STATUS[$key]=PASS ;;
    2) STEP_STATUS[$key]=BLOCKED; STEP_NOTE[$key]="the wrapper reported a precondition failure (exit 2)" ;;
    124) STEP_STATUS[$key]=FAIL; STEP_NOTE[$key]="exceeded the step limit of $(op_duration "$step_limit")" ;;
    *) STEP_STATUS[$key]=FAIL; STEP_NOTE[$key]="the wrapper exited $rc" ;;
  esac
  op_say "run $run: $label finished: ${STEP_STATUS[$key]} ($counts, $(op_duration "${STEP_SECONDS[$key]}"))"
}

# The wrapper never sees the Atlas guard variables except in the Tier B step (which inherits them, so the URI is never
# placed on a command line).
wrapper_plain=(env -u ATLAS_TEST_URI -u ATLAS_TEST_DISPOSABLE -u ATLAS_TEST_ALLOWED_HOSTS "$wrapper_path")
wrapper_atlas=("$wrapper_path")



# Event counts per type of an exported events file, one "TYPE=n" per line (HEA-15 compares them between runs).
event_counts() { # EVENTS_FILE
  [[ -f "$chain_dir/$1" ]] || { printf 'missing'; return 0; }
  python3 -I - "$chain_dir/$1" <<'PY'
import json, sys, collections
counts = collections.Counter()
with open(sys.argv[1], encoding="utf-8") as handle:
    for line in handle:
        line = line.strip()
        if line:
            counts[json.loads(json.loads(line)["value"]).get("eventType", "?")] += 1
print(";".join(f"{k}={counts[k]}" for k in sorted(counts)) + f";total={sum(counts.values())}")
PY
}

git_state() { { git rev-parse HEAD; git status --porcelain=v1 --untracked-files=all; } 2>/dev/null | sha256sum | awk '{print $1}'; }

# ---- one run ----------------------------------------------------------------------------------------------------
tier_a_globs=("*HiringStoryAcceptanceIntegrationSpec" "*HiringRoleMatrixAcceptanceIntegrationSpec")
chain_story=("*HiringStoryAnalyticsReportIntegrationSpec" "*HiringCohortAnalyticsReportIntegrationSpec")
chain_admin=("*HiringStoryAnalyticsAdminReportIntegrationSpec" "*HiringCohortAnalyticsAdminReportIntegrationSpec")
tier_b_globs=("*HiringSearchAcceptanceIntegrationSpec")

execute_run() {
  local run="$1" key log chain_ok=1 state binding note
  mkdir -p -m 700 "$evidence_dir/run-$run"
  # Hand-off files must be newer than this marker to count (verify_binding); the exports and reports are rewritten per run.
  run_start_marker="$evidence_dir/run-$run/.run-start"
  : >"$run_start_marker"

  # Step 1: Tier A (the story suite also exports the story's events because the chain marker exists).
  run_step "$run" tierA "step 1/5 Tier A story and role matrix" "${wrapper_plain[@]}" test "${tier_a_globs[@]}"
  key="$run:tierA"; log="$(sbt_log_of "$key")"
  tier_a_verdict "$run" "${STEP_STATUS[$key]}" "${STEP_NOTE[$key]:-}" "$log"
  if [[ "${STEP_STATUS[$key]}" == BLOCKED ]]; then
    for step in cohortExport analytics adminRead tierB; do STEP_STATUS["$run:$step"]=BLOCKED; STEP_NOTE["$run:$step"]="not run: Tier A was blocked"; done
    return 0
  fi
  # The chain needs a clean story export: its suite must have passed without skips.
  if [[ "$(uc_status "$run" 11)" != PASS ]]; then chain_ok=0; fi

  # Step 2: the analytics chain (design D7), for both datasets.
  if (( chain_ok )); then
    run_step "$run" cohortExport "step 2/5 chain: hired cohort export" "${wrapper_plain[@]}" test "*HiringCohortStoryIntegrationSpec"
    key="$run:cohortExport"; log="$(sbt_log_of "$key")"
    if [[ "${STEP_STATUS[$key]}" == BLOCKED ]]; then blanket "$run" BLOCKED cohortExport "${STEP_NOTE[$key]}" 11 12 13; chain_ok=0
    else
      collect "$run" cohortExport "$log" HiringCohortStoryIntegrationSpec 11 12
      [[ "${STEP_STATUS[$key]}" == PASS ]] || {
        step_has_fail "$run" cohortExport 11 12 || blanket "$run" FAIL cohortExport "${STEP_NOTE[$key]:-failed}" 11 12
        chain_ok=0
      }
    fi
  else
    blanket "$run" SKIPPED chain "story export did not pass, so the analytics chain was not run" 11 12 13
  fi
  if (( chain_ok )); then
    run_step "$run" analytics "step 3/5 chain: Kafka to Spark/Delta to report" "${wrapper_plain[@]}" analytics "${chain_story[@]}"
    key="$run:analytics"; log="$(sbt_log_of "$key")"
    if [[ "${STEP_STATUS[$key]}" == BLOCKED ]]; then blanket "$run" BLOCKED analytics "${STEP_NOTE[$key]}" 11 12 13; chain_ok=0
    else
      collect "$run" analytics "$log" HiringStoryAnalyticsReportIntegrationSpec 11 12 13
      collect "$run" analytics "$log" HiringCohortAnalyticsReportIntegrationSpec 11 12 13
      [[ "${STEP_STATUS[$key]}" == PASS ]] || {
        step_has_fail "$run" analytics 11 12 13 || blanket "$run" FAIL analytics "${STEP_NOTE[$key]:-failed}" 11 12 13
        chain_ok=0
      }
    fi
  fi
  if (( chain_ok )); then
    for binding in "story story-events.jsonl report-documents.jsonl" "cohort cohort-events.jsonl cohort-report-documents.jsonl"; do
      read -r dataset events report <<<"$binding"
      note="$(verify_binding "$events" "$report")"
      if [[ "$note" == PASS ]]; then op_say "run $run: $dataset report is bound to the current events file (SHA-256 match)"
      else op_say "run $run: $dataset hand-off check failed: ${note#FAIL }"; blanket "$run" FAIL analytics "${note#FAIL }" 11 12 13; chain_ok=0; fi
    done
    STEP_NOTE["$run:analytics"]="events: story $(event_counts story-events.jsonl); cohort $(event_counts cohort-events.jsonl)"
  fi
  if (( chain_ok )); then
    run_step "$run" adminRead "step 4/5 chain: Admin report through the API" "${wrapper_plain[@]}" test "${chain_admin[@]}"
    key="$run:adminRead"; log="$(sbt_log_of "$key")"
    if [[ "${STEP_STATUS[$key]}" == BLOCKED ]]; then blanket "$run" BLOCKED adminRead "${STEP_NOTE[$key]}" 12 13
    else
      collect "$run" adminRead "$log" HiringStoryAnalyticsAdminReportIntegrationSpec 12 13
      collect "$run" adminRead "$log" HiringCohortAnalyticsAdminReportIntegrationSpec 12 13
      [[ "${STEP_STATUS[$key]}" == PASS ]] || step_has_fail "$run" adminRead 12 13 || blanket "$run" FAIL adminRead "${STEP_NOTE[$key]:-failed}" 12 13
    fi
  elif [[ -z "${STEP_STATUS["$run:adminRead"]:-}" ]]; then
    STEP_STATUS["$run:adminRead"]=SKIPPED; STEP_NOTE["$run:adminRead"]="not run: an earlier chain step did not pass"
    blanket "$run" SKIPPED adminRead "the Admin report read was not run because an earlier chain step did not pass" 12 13
  fi

  # Step 3: Tier B on Atlas Local.
  state="$(atlas_state)"
  if [[ "$state" == ok ]]; then
    run_step "$run" tierB "step 5/5 Tier B search on Atlas Local" "${wrapper_atlas[@]}" test "${tier_b_globs[@]}"
    key="$run:tierB"; log="$(sbt_log_of "$key")"
    if [[ "${STEP_STATUS[$key]}" == BLOCKED ]]; then blanket "$run" BLOCKED tierB "${STEP_NOTE[$key]}" 02 06 10
    else
      collect "$run" tierB "$log" HiringSearchAcceptanceIntegrationSpec 02 06 10
      [[ "${STEP_STATUS[$key]}" == PASS ]] || step_has_fail "$run" tierB 02 06 10 || blanket "$run" FAIL tierB "${STEP_NOTE[$key]:-failed}" 02 06 10
    fi
  else
    STEP_STATUS["$run:tierB"]=BLOCKED; STEP_NOTE["$run:tierB"]="Atlas Local unavailable: $state"
    STEP_LOG["$run:tierB"]="-"
    blanket "$run" BLOCKED tierB "Atlas Local unavailable: $state" 02 06 10
    op_say "run $run: Tier B BLOCKED ($state)"
    [[ -n "$overall_blocked_reason" ]] || atlas_start_hint
    overall_blocked_reason="atlas"
  fi

  # Signature for HEA-15: test counts of each step and the event counts per type.
  RUN_SIGNATURE[$run]="$(
    for step in tierA cohortExport analytics adminRead tierB; do
      [[ -n "${STEP_COUNTS["$run:$step"]:-}" ]] && printf '%s: %s\n' "$step" "${STEP_COUNTS["$run:$step"]}"
    done
    printf 'events story: %s\nevents cohort: %s\n' "$(event_counts story-events.jsonl)" "$(event_counts cohort-events.jsonl)"
  )"
}


uc_log() { # RUN UC -> the distinct step logs behind a use case
  local step seen="" p
  while IFS='|' read -r _ step _; do
    [[ -n "$step" ]] || continue
    [[ " $seen " == *" $step "* ]] && continue
    seen+=" $step"
    p="${STEP_SBT["$1:$step"]:-}"; p="${p#"$repo_root"/}"
    printf '%s ' "${p:-${STEP_LOG["$1:$step"]:--}}"
  done <<<"${UC_PARTS["$1:$2"]:-}"
}

# ---- evidence ---------------------------------------------------------------------------------------------------
hea15a_status="NOT CHECKED"
hea15b_status="NOT CLAIMED (single run; use --repeat 2)"
hea15b_detail=""

write_evidence() { # OUTCOME
  local outcome="$1" run uc status step sbt_rel
  {
    printf '# Hiring end-to-end acceptance evidence\n\n'
    printf -- '- Outcome: **%s**\n- Started (UTC): %s\n- Runs requested: %s, completed: %s\n- Repository HEAD: %s\n' \
      "$outcome" "$run_stamp" "$repeat" "${completed_runs:-0}" "$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
    printf -- '- Atlas Local for Tier B: %s\n- Spec: docs/specs/hiring-end-to-end-acceptance.md\n' "$(atlas_state)"
    printf -- '- Logs: %s (git-ignored; wrapper log: .local/logs/test-services/run.log is overwritten by every call)\n\n' "$evidence_dir_rel"
    for run in $(seq 1 "$repeat"); do
      [[ -d "$evidence_dir/run-$run" ]] || continue
      printf '## Run %s\n\n### Steps\n\n| Step | Status | Counts | Duration | Log |\n|---|---|---|---|---|\n' "$run"
      for step in tierA cohortExport analytics adminRead tierB; do
        [[ -n "${STEP_STATUS["$run:$step"]:-}" ]] || continue
        printf '| %s | %s%s | %s | %s | %s |\n' "$step" "${STEP_STATUS["$run:$step"]}" \
          "$([[ -n "${STEP_NOTE["$run:$step"]:-}" ]] && printf ' (%s)' "${STEP_NOTE["$run:$step"]}")" \
          "${STEP_COUNTS["$run:$step"]:--}" "$(op_duration "${STEP_SECONDS["$run:$step"]:-0}")" \
          "${STEP_LOG["$run:$step"]:--} ${STEP_SBT["$run:$step"]:+and ${STEP_SBT["$run:$step"]#"$repo_root"/} (sbt)}"
      done
      printf '\n### Use cases\n\n| Use case | Status | Evidence log | Detail |\n|---|---|---|---|\n'
      for uc in "${UCS[@]}"; do
        status="$(uc_status "$run" "$uc")"
        printf '| UC%s %s | %s | %s | %s |\n' "$uc" "${UC_TITLE[$uc]}" "$status" "$(uc_log "$run" "$uc")" \
          "$(awk -F'|' 'NF>2{printf "%s; ", $3}' <<<"${UC_PARTS["$run:$uc"]:-}")"
      done
      printf '\n| Criterion | Status | Evidence log |\n|---|---|---|\n'
      sbt_rel="${STEP_SBT["$run:tierA"]:-${STEP_LOG["$run:tierA"]:--}}"
      printf '| HEA-14 role matrix | %s | %s |\n\n' "${HEA14_STATUS[$run]:-SKIPPED}" "${sbt_rel#"$repo_root"/}"
    done
    printf '## Reproducibility and hygiene\n\n'
    printf -- '- HEA-15a no data outside .local/ and no commit (git HEAD and status unchanged across the whole script): %s. Evidence: git state compared before and after; the run logs are under %s\n' "$hea15a_status" "$evidence_dir_rel"
    printf -- '- HEA-15b same counts across runs: %s %s. Evidence: the counts per run below, from the step logs under %s\n' "$hea15b_status" "$hea15b_detail" "$evidence_dir_rel"
    for run in $(seq 1 "$repeat"); do
      [[ -n "${RUN_SIGNATURE[$run]:-}" ]] && printf '\nRun %s counts:\n\n```\n%s\n```\n' "$run" "${RUN_SIGNATURE[$run]}"
    done
    printf '\n## HEA-16 evidence completeness\n\n- This file (%s/evidence.md) maps UC01 to UC13 to a status and a step log for each completed run. A failed, blocked or skipped use case is listed above with its status, never dropped.\n' "$evidence_dir_rel"
  } >"$evidence_file"
}

# ---- main -------------------------------------------------------------------------------------------------------
completed_runs=0
printf 'run-acceptance: hiring end-to-end acceptance (UC01-UC13), %s run(s). Steps per run: Tier A story and role matrix; analytics chain (cohort export, Spark/Delta report, Admin read); Tier B on Atlas Local when authorized. Parameters: repeat=%s, step limit %s, wrapper limit %s. Expected: about 30-45 min per run without Tier B, Tier B roughly 10-20 min more (not measured). Evidence: %s/evidence.md\n' \
  "$repeat" "$repeat" "$(op_duration "$step_limit")" "$(op_duration "$inner_limit")" "$evidence_dir_rel"

# The chain marker (design D7): created before step 1 with owner-only permissions, removed when the script ends.
current_phase="chain marker"
mkdir -p -m 700 "$chain_dir"
[[ ! -L "$chain_dir" ]] || blocked "$chain_dir is a symlink" "replace it with a real directory under .local/data"
if [[ -e "$chain_marker" || -L "$chain_marker" ]]; then
  [[ -f "$chain_marker" && ! -L "$chain_marker" && -O "$chain_marker" ]] \
    || blocked "the chain marker is not a regular file owned by you" "inspect and remove .local/data/hiring-story-analytics/chain-started"
  op_say "a chain marker from an earlier run exists; this run takes it over and removes it when it ends (no other acceptance run holds the lock)"
else
  : >"$chain_marker"
fi
chmod 600 "$chain_marker"
marker_owned=1

git_before="$(git_state)"
for run in $(seq 1 "$repeat"); do
  execute_run "$run"
  completed_runs=$run
  write_evidence "in progress"
done

# Marker removal comes first so a later full run is never left in chain mode.
remove_marker
git_after="$(git_state)"
if [[ "$git_before" == "$git_after" ]]; then hea15a_status=PASS; else hea15a_status="FAIL (git HEAD or working tree changed during the run)"; fi
if (( repeat >= 2 )); then
  if [[ "${RUN_SIGNATURE[1]:-}" == "${RUN_SIGNATURE[2]:-x}" && ( "$repeat" -lt 3 || "${RUN_SIGNATURE[1]:-}" == "${RUN_SIGNATURE[3]:-x}" ) ]]; then hea15b_status=PASS
  else hea15b_status=FAIL; hea15b_detail="(run counts differ; compare the signatures below)"; fi
fi

# ---- verdict ----------------------------------------------------------------------------------------------------
fail_count=0; blocked_count=0; skipped_count=0; pass_count=0
current_phase="summary"
printf '\n'
for run in $(seq 1 "$repeat"); do
  printf 'Run %s\n' "$run"
  for uc in "${UCS[@]}"; do
    printf '  UC%s %-8s %-40s %s\n' "$uc" "$(uc_status "$run" "$uc")" "${UC_TITLE[$uc]}" "$(uc_log "$run" "$uc")"
  done
  printf '  HEA-14 %-8s role matrix\n' "${HEA14_STATUS[$run]:-SKIPPED}"
  tally_run "$run"
done
printf '  HEA-15a %s\n  HEA-15b %s %s\n  HEA-16 evidence file: %s/evidence.md\n' "$hea15a_status" "$hea15b_status" "$hea15b_detail" "$evidence_dir_rel"
[[ "$hea15a_status" == PASS ]] || fail_count=$((fail_count + 1))
[[ "$hea15b_status" != FAIL ]] || fail_count=$((fail_count + 1))

decide_verdict
write_evidence "$outcome"
# HEA-16 self-check: the evidence file exists and names every use case.
for uc in "${UCS[@]}"; do
  grep -q "^| UC$uc " "$evidence_file" || { outcome=FAIL; code=1; printf 'run-acceptance: HEA-16 evidence is missing UC%s\n' "$uc" >&2; }
done
detail="runs=$repeat uc_pass=$pass_count uc_fail=$fail_count uc_blocked=$blocked_count uc_skipped=$skipped_count hea15a=${hea15a_status%% *} hea15b=${hea15b_status%% *}"
if [[ "$outcome" != PASS ]]; then
  printf 'run-acceptance: next: read %s/evidence.md and the failing or blocked step log named there\n' "$evidence_dir_rel" >&2
fi
finish "$outcome" "$code" "$detail"
