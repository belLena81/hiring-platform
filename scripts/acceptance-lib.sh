#!/usr/bin/env bash
# Pure verdict logic of scripts/run-acceptance.sh: sbt log parsing, per-use-case statuses and the overall verdict.
# Sourced by scripts/run-acceptance.sh and by scripts/tests/run-acceptance-parsing.sh, never executed. It runs no
# command of its own and touches no file except the logs and hand-off files it is given.
# The caller defines chain_dir and run_start_marker (used by verify_binding).

readonly UCS=(01 02 03 04 05 06 07 08 09 10 11 12 13)
# UC_PARTS["run:uc"] holds one "STATUS|step|detail" line per contributing suite; HEA14_STATUS["run"] the role matrix.
declare -A UC_PARTS=() HEA14_STATUS=()

clean() { [[ -r "$1" ]] || return 0; sed -e 's/\x1b\[[0-9;?]*[A-Za-z]//g' -e 's/\r//g' "$1"; }

# Combines statuses: FAIL beats BLOCKED beats SKIPPED beats PASS.
combine() {
  local best=PASS s
  for s in "$@"; do
    case "$s" in
      FAIL) best=FAIL; break ;;
      BLOCKED) [[ "$best" == FAIL ]] || best=BLOCKED ;;
      SKIPPED) [[ "$best" == FAIL || "$best" == BLOCKED ]] || best=SKIPPED ;;
    esac
  done
  printf '%s' "$best"
}

# add_part RUN UC STATUS STEP DETAIL
add_part() { UC_PARTS["$1:$2"]+="$3|$4|$5"$'\n'; }

# Every UC of an unrunnable or failed step gets a part with the given status.
blanket() { # RUN STATUS STEP DETAIL UC...
  local run="$1" status="$2" step="$3" detail="$4" uc
  shift 4
  for uc in "$@"; do add_part "$run" "$uc" "$status" "$step" "$detail"; done
}

# collect RUN STEP LOG CLASS UC...: reads "Test run <class> finished: F failed, I ignored, T total" and the failed test
# names from an sbt log and records one status part per use case the suite covers. Failing tests carry their HEA tag.
collect() {
  local run="$1" step="$2" log="$3" class="$4" uc line failed ignored total tags untagged=0 failures f tag
  shift 4
  line="$(clean "$log" | grep -E "Test run .*\.${class} finished: " | tail -n 1 || true)"
  if [[ -z "$line" ]]; then
    for uc in "$@"; do add_part "$run" "$uc" FAIL "$step" "suite $class did not report a result"; done
    return 0
  fi
  failed="$(sed -E 's/.*finished: ([0-9]+) failed, ([0-9]+) ignored, ([0-9]+) total.*/\1/' <<<"$line")"
  ignored="$(sed -E 's/.*finished: ([0-9]+) failed, ([0-9]+) ignored, ([0-9]+) total.*/\2/' <<<"$line")"
  total="$(sed -E 's/.*finished: ([0-9]+) failed, ([0-9]+) ignored, ([0-9]+) total.*/\3/' <<<"$line")"
  if [[ ! "$total" =~ ^[0-9]+$ || "$total" -eq 0 ]]; then
    for uc in "$@"; do add_part "$run" "$uc" FAIL "$step" "suite $class ran no tests"; done
    return 0
  fi
  # A test skipped by an unmet assumption is printed as "==> s" and is not part of the "ignored" count.
  ignored=$((ignored + $(clean "$log" | grep -F '==> s ' | grep -cF ".${class}." || true)))
  failures="$(clean "$log" | grep -F '==> X' | grep -F ".${class}." || true)"
  tags="$(grep -o 'HEA-[0-9][0-9]' <<<"$failures" | sort -u | tr '\n' ' ' || true)"
  if (( failed > 0 )); then
    # A failure whose tag is not one of this suite's use cases cannot be attributed, so it fails the whole suite.
    for tag in $tags; do
      [[ " $* " == *" ${tag#HEA-} "* ]] || untagged=1
    done
    # A failure without a use-case tag, or in a setup test (HEA-00, HEA-02.0 or HEA-12.0), invalidates the whole suite.
    while IFS= read -r f; do
      [[ -z "$f" ]] && continue
      if ! grep -q 'HEA-[0-9][0-9]' <<<"$f" || grep -qE 'HEA-00|HEA-02\.0|HEA-12\.0' <<<"$f"; then untagged=1; fi
    done <<<"$failures"
    (( $(grep -c . <<<"$failures" || true) >= failed )) || untagged=1
  fi
  for uc in "$@"; do
    if (( failed > 0 )) && { (( untagged )) || [[ " $tags " == *" HEA-$uc "* ]]; }; then
      add_part "$run" "$uc" FAIL "$step" "$class: $failed of $total tests failed (${tags:-no tag})"
    elif (( ignored > 0 )); then
      add_part "$run" "$uc" SKIPPED "$step" "$class: $ignored of $total tests skipped (assumption not met)"
    else
      add_part "$run" "$uc" PASS "$step" "$class: $total tests, $failed failed"
    fi
  done
}

# step_has_fail RUN STEP UC...: true when a suite of that step already recorded a failure for one of the use cases.
step_has_fail() {
  local run="$1" step="$2" uc
  shift 2
  for uc in "$@"; do
    [[ "${UC_PARTS["$run:$uc"]:-}" == *"FAIL|$step|"* ]] && return 0
  done
  return 1
}

# suite_status LOG CLASS -> PASS|FAIL|SKIPPED for a whole suite (used for HEA-14).
suite_status() {
  local line failed ignored total
  line="$(clean "$1" | grep -E "Test run .*\.${2} finished: " | tail -n 1 || true)"
  [[ -n "$line" ]] || { printf 'FAIL'; return 0; }
  failed="$(sed -E 's/.*finished: ([0-9]+) failed, ([0-9]+) ignored, ([0-9]+) total.*/\1/' <<<"$line")"
  ignored="$(sed -E 's/.*finished: ([0-9]+) failed, ([0-9]+) ignored, ([0-9]+) total.*/\2/' <<<"$line")"
  total="$(sed -E 's/.*finished: ([0-9]+) failed, ([0-9]+) ignored, ([0-9]+) total.*/\3/' <<<"$line")"
  ignored=$((ignored + $(clean "$1" | grep -F '==> s ' | grep -cF ".${2}." || true)))
  if [[ ! "$total" =~ ^[0-9]+$ || "$total" -eq 0 || "$failed" -gt 0 ]]; then printf 'FAIL'
  elif (( ignored > 0 )); then printf 'SKIPPED'
  else printf 'PASS'; fi
}

# tier_a_verdict RUN STEP_STATUS NOTE SBT_LOG: the story suite's use cases and the role matrix of step 1. A BLOCKED call
# blocks every use case (Tier B ones included), because nothing ran. A failed call is explained by a failed suite
# (story or role matrix); when every suite passed, the call itself failed (for example a timeout) and the story's use
# cases fail.
tier_a_verdict() {
  local run="$1" status="$2" note="$3" log="$4"
  if [[ "$status" == BLOCKED ]]; then
    blanket "$run" BLOCKED tierA "$note" "${UCS[@]}"
    HEA14_STATUS[$run]=BLOCKED
    return 0
  fi
  collect "$run" tierA "$log" HiringStoryAcceptanceIntegrationSpec 01 03 04 05 07 08 09 11
  HEA14_STATUS[$run]="$(suite_status "$log" HiringRoleMatrixAcceptanceIntegrationSpec)"
  if [[ "$status" == FAIL && "${HEA14_STATUS[$run]}" != FAIL ]] && ! step_has_fail "$run" tierA 01 03 04 05 07 08 09 11; then
    blanket "$run" FAIL tierA "${note:-the wrapper call failed}" 01 03 04 05 07 08 09 11
  fi
}

uc_status() { # RUN UC
  local parts="${UC_PARTS["$1:$2"]:-}"
  if [[ -z "$parts" ]]; then printf 'SKIPPED'; return 0; fi
  # shellcheck disable=SC2046
  combine $(awk -F'|' 'NF>1{print $1}' <<<"$parts")
}

# tally_run RUN: adds the run's 13 use cases and its role matrix to pass_count, fail_count, blocked_count, skipped_count.
# A role matrix that is not PASS counts like a use case that is not PASS, so a skipped matrix can never give exit 0.
tally_run() {
  local run="$1" uc status
  for uc in "${UCS[@]}"; do
    status="$(uc_status "$run" "$uc")"
    case "$status" in PASS) pass_count=$((pass_count + 1)) ;; FAIL) fail_count=$((fail_count + 1)) ;; BLOCKED) blocked_count=$((blocked_count + 1)) ;; *) skipped_count=$((skipped_count + 1)) ;; esac
  done
  case "${HEA14_STATUS[$run]:-SKIPPED}" in
    PASS) ;; FAIL) fail_count=$((fail_count + 1)) ;; BLOCKED) blocked_count=$((blocked_count + 1)) ;; *) skipped_count=$((skipped_count + 1)) ;;
  esac
}

# decide_verdict: from the counts, sets outcome and code (0 PASS, 1 FAIL, 2 BLOCKED).
decide_verdict() {
  if (( fail_count > 0 )); then outcome=FAIL; code=1
  elif (( blocked_count > 0 || skipped_count > 0 )); then outcome=BLOCKED; code=2
  else outcome=PASS; code=0; fi
}

sha256_of() { sha256sum -- "$1" | awk '{print $1}'; }

# The report file starts with "events-sha256<TAB><digest of the events file it was built from>" (design D7). Both files
# must also be newer than the start of this run (run_start_marker), so a leftover pair cannot satisfy a new run.
verify_binding() { # EVENTS REPORT -> prints PASS or "FAIL reason"
  local events="$chain_dir/$1" report="$chain_dir/$2" first expected
  [[ -f "$events" && ! -L "$events" ]] || { printf 'FAIL events file %s missing' "$1"; return 0; }
  [[ -f "$report" && ! -L "$report" ]] || { printf 'FAIL report file %s missing' "$2"; return 0; }
  [[ "$events" -nt "$run_start_marker" ]] || { printf 'FAIL events file %s is older than this run' "$1"; return 0; }
  [[ "$report" -nt "$run_start_marker" ]] || { printf 'FAIL report file %s is older than this run' "$2"; return 0; }
  first="$(head -n 1 "$report")"
  expected="events-sha256"$'\t'"$(sha256_of "$events")"
  if [[ "$first" == "$expected" ]]; then printf 'PASS'; else printf 'FAIL report %s was not built from the current %s' "$2" "$1"; fi
}
