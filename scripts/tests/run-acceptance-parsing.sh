#!/usr/bin/env bash
# Self-test of the verdict logic behind scripts/run-acceptance.sh (scripts/acceptance-lib.sh): feeds fixture sbt logs and
# hand-off files and checks per-use-case statuses, the overall verdict and the exit code. It starts no test service,
# calls no wrapper and writes only to a temporary directory under .local/data/ that it removes.
# Contract: see "Operator scripts" in docs/engineering-quality.md.
set -Eeuo pipefail
umask 077
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$repo_root"
result_done=0
checks=0
failures=0
work=""

if [[ "${1:-}" == -h || "${1:-}" == --help ]]; then
  cat <<'HELP'
Usage: scripts/tests/run-acceptance-parsing.sh [--help]

Self-test of the verdict logic of scripts/run-acceptance.sh (scripts/acceptance-lib.sh) on fixture sbt logs: pass,
failures with and without a HEA tag, setup failures (HEA-00, HEA-02.0, HEA-12.0), skipped tests, an unmapped tag, a
timed-out call, a blocked call, a skipped role matrix, and the SHA-256 and freshness check of the hand-off files.
Expected duration: under 10 s. It needs no Docker and no test service and writes only to a temporary directory under
.local/data/ that it removes.

Exit codes: 0 PASS, 1 FAIL, 2 usage error, 130 interrupted. The last line is "RESULT: PASS|FAIL ...".
HELP
  exit 0
fi
[[ $# -eq 0 ]] || { printf 'run-acceptance-parsing: unknown argument: %s (try --help)\n' "$1" >&2; printf 'RESULT: BLOCKED reason=usage\n' >&2; exit 2; }
printf 'run-acceptance-parsing: checking the acceptance verdict logic on fixture logs; expected under 10 s, no Docker needed\n'

# shellcheck source=scripts/operator-lib.sh
source "$repo_root/scripts/operator-lib.sh"
chain_dir=""
run_start_marker=""
# shellcheck source=scripts/acceptance-lib.sh
source "$repo_root/scripts/acceptance-lib.sh"

cleanup() { [[ -z "$work" ]] || rm -rf -- "$work"; }
on_exit() {
  local code=$?
  cleanup
  if (( code != 0 && result_done == 0 )); then printf 'RESULT: FAIL reason=unexpected exit=%s\n' "$code" >&2; fi
}
on_interrupt() { trap '' INT TERM HUP; cleanup; result_done=1; printf 'run-acceptance-parsing: interrupted\n' >&2; exit 130; }
trap on_exit EXIT
trap on_interrupt INT TERM HUP
mkdir -p "$repo_root/.local/data"
work="$(mktemp -d "$repo_root/.local/data/acceptance-parsing-test.XXXXXX")"

expect() { # NAME ACTUAL EXPECTED
  checks=$((checks + 1))
  if [[ "$2" == "$3" ]]; then printf '  ok   %s\n' "$1"
  else failures=$((failures + 1)); printf '  FAIL %s: expected "%s", got "%s"\n' "$1" "$3" "$2"; fi
}

reset() { UC_PARTS=(); HEA14_STATUS=(); pass_count=0; fail_count=0; blocked_count=0; skipped_count=0; }

# suite LOG CLASS FAILED SKIPPED TOTAL [FAILING TEST NAME...]: one sbt "Test run" block.
suite() {
  local log="$1" class="$2" failed="$3" skipped="$4" total="$5" name i
  shift 5
  printf 'Test run com.example.%s started\n' "$class" >>"$log"
  for name in "$@"; do printf '==> X com.example.%s.%s 1s java.lang.AssertionError\n' "$class" "$name" >>"$log"; done
  for (( i = 0; i < skipped; i++ )); do printf '==> s com.example.%s.HEA-99.%s skipped 0s\n' "$class" "$i" >>"$log"; done
  printf 'Test run com.example.%s finished: %s failed, 0 ignored, %s total 1s\n' "$class" "$failed" "$total" >>"$log"
}

story=HiringStoryAcceptanceIntegrationSpec
matrix=HiringRoleMatrixAcceptanceIntegrationSpec
statuses() { local uc out=""; for uc in "$@"; do out+="$uc=$(uc_status t "$uc") "; done; printf '%s' "${out% }"; }
log() { : >"$work/$1.log"; printf '%s' "$work/$1.log"; }
story_ucs=(01 03 04 05 07 08 09 11)
all_pass="01=PASS 03=PASS 04=PASS 05=PASS 07=PASS 08=PASS 09=PASS 11=PASS"
all_fail="01=FAIL 03=FAIL 04=FAIL 05=FAIL 07=FAIL 08=FAIL 09=FAIL 11=FAIL"

printf 'Tier A suites\n'
reset; l="$(log a1)"; suite "$l" "$story" 0 0 22; suite "$l" "$matrix" 0 0 6
tier_a_verdict t PASS "" "$l"
expect "all suites pass: story use cases" "$(statuses "${story_ucs[@]}")" "$all_pass"
expect "all suites pass: role matrix" "${HEA14_STATUS[t]}" PASS

reset; l="$(log a2)"; suite "$l" "$story" 1 0 22 "HEA-04.2 duplicate submission"; suite "$l" "$matrix" 0 0 6
tier_a_verdict t FAIL "" "$l"
expect "tagged failure fails only its use case" "$(statuses 01 04 09)" "01=PASS 04=FAIL 09=PASS"

reset; l="$(log a3)"; suite "$l" "$story" 1 0 22 "something without a tag"; suite "$l" "$matrix" 0 0 6
tier_a_verdict t FAIL "" "$l"
expect "untagged failure fails every use case of the suite" "$(statuses "${story_ucs[@]}")" "$all_fail"

reset; l="$(log a4)"; suite "$l" "$story" 1 0 22 "HEA-00 accounts"; suite "$l" "$matrix" 0 0 6
tier_a_verdict t FAIL "" "$l"
expect "HEA-00 setup failure fails every use case of the suite" "$(statuses "${story_ucs[@]}")" "$all_fail"

reset; l="$(log a5)"; suite "$l" "$story" 1 0 22 "HEA-99.1 unknown"; suite "$l" "$matrix" 0 0 6
tier_a_verdict t FAIL "" "$l"
expect "unmapped tag fails every use case of the suite" "$(statuses "${story_ucs[@]}")" "$all_fail"

reset; l="$(log a6)"; suite "$l" "$story" 0 22 22; suite "$l" "$matrix" 0 0 6
tier_a_verdict t PASS "" "$l"
expect "skipped tests are SKIPPED, not PASS" "$(statuses 01 11)" "01=SKIPPED 11=SKIPPED"

reset; l="$(log a7)"; suite "$l" "$story" 0 0 22; suite "$l" "$matrix" 0 0 6
tier_a_verdict t FAIL "exceeded the step limit" "$l"
expect "failed call with passing suites (timeout) fails the story use cases" "$(statuses "${story_ucs[@]}")" "$all_fail"

reset; l="$(log a8)"; suite "$l" "$story" 0 0 22; suite "$l" "$matrix" 1 0 6 "HEA-14.4 garbage token"
tier_a_verdict t FAIL "" "$l"
expect "role matrix failure leaves the story use cases PASS" "$(statuses "${story_ucs[@]}")" "$all_pass"
expect "role matrix failure is HEA-14 FAIL" "${HEA14_STATUS[t]}" FAIL

reset; l="$(log a9)"; suite "$l" "$story" 1 0 22 "HEA-11.1 events"; suite "$l" "$matrix" 1 0 6 "HEA-14.4 garbage token"
tier_a_verdict t FAIL "" "$l"
expect "story and matrix both failing: only UC11 and HEA-14 fail" "$(statuses 01 11) ${HEA14_STATUS[t]}" "01=PASS 11=FAIL FAIL"

reset; l="$(log a10)"; : >"$l"
tier_a_verdict t FAIL "the wrapper exited 1" "$l"
expect "no suite result in the log fails every use case" "$(statuses "${story_ucs[@]}")" "$all_fail"
expect "no role matrix result is FAIL" "${HEA14_STATUS[t]}" FAIL

reset
tier_a_verdict t BLOCKED "precondition" "$work/none.log"
blanket_expect="$(statuses 01 02 06 10 13) ${HEA14_STATUS[t]}"
expect "blocked Tier A blocks every use case, Tier B ones included" "$blanket_expect" "01=BLOCKED 02=BLOCKED 06=BLOCKED 10=BLOCKED 13=BLOCKED BLOCKED"

printf 'Chain and Tier B suites\n'
reset; l="$(log c1)"; suite "$l" HiringCohortStoryIntegrationSpec 1 0 2 "HEA-12.0 cohort lifecycle"
collect t cohortExport "$l" HiringCohortStoryIntegrationSpec 11 12
expect "HEA-12.0 setup failure fails UC11 and UC12" "$(statuses 11 12)" "11=FAIL 12=FAIL"

reset; l="$(log c2)"; suite "$l" HiringStoryAnalyticsAdminReportIntegrationSpec 1 0 2 "HEA-12.2 admin only"
collect t adminRead "$l" HiringStoryAnalyticsAdminReportIntegrationSpec 12 13
expect "tagged chain failure fails only its use case" "$(statuses 12 13)" "12=FAIL 13=PASS"

reset; l="$(log b1)"; suite "$l" HiringSearchAcceptanceIntegrationSpec 1 0 7 "HEA-02.0 accounts and embeddings"
collect t tierB "$l" HiringSearchAcceptanceIntegrationSpec 02 06 10
expect "HEA-02.0 setup failure fails UC02, UC06 and UC10" "$(statuses 02 06 10)" "02=FAIL 06=FAIL 10=FAIL"

reset; l="$(log b2)"; suite "$l" HiringSearchAcceptanceIntegrationSpec 1 0 7 "HEA-06.2 stale refusal"
collect t tierB "$l" HiringSearchAcceptanceIntegrationSpec 02 06 10
expect "HEA-06.2 fails only UC06" "$(statuses 02 06 10)" "02=PASS 06=FAIL 10=PASS"

reset; l="$(log b3)"; suite "$l" HiringSearchAcceptanceIntegrationSpec 0 7 7
collect t tierB "$l" HiringSearchAcceptanceIntegrationSpec 02 06 10
expect "Tier B skipped without the Atlas guard is SKIPPED" "$(statuses 02 06 10)" "02=SKIPPED 06=SKIPPED 10=SKIPPED"

printf 'Overall verdict and exit code\n'
all_ucs_pass() { local uc; for uc in "${UCS[@]}"; do add_part t "$uc" PASS fixture "fixture"; done; }
verdict() { pass_count=0; fail_count=0; blocked_count=0; skipped_count=0; tally_run t; decide_verdict; printf '%s/%s' "$outcome" "$code"; }
reset; all_ucs_pass; HEA14_STATUS[t]=PASS
expect "all 13 use cases and the role matrix PASS" "$(verdict)" "PASS/0"
reset; all_ucs_pass; HEA14_STATUS[t]=SKIPPED
expect "a skipped role matrix can never exit 0" "$(verdict)" "BLOCKED/2"
reset; all_ucs_pass; HEA14_STATUS[t]=BLOCKED
expect "a blocked role matrix is BLOCKED" "$(verdict)" "BLOCKED/2"
reset; all_ucs_pass
expect "a missing role matrix result is not a pass" "$(verdict)" "BLOCKED/2"
reset; all_ucs_pass; HEA14_STATUS[t]=FAIL
expect "a failed role matrix is FAIL" "$(verdict)" "FAIL/1"
reset; all_ucs_pass; UC_PARTS["t:02"]="BLOCKED|tierB|Atlas Local unavailable"$'\n'; HEA14_STATUS[t]=PASS
expect "UC02 BLOCKED while the rest PASS is BLOCKED" "$(verdict)" "BLOCKED/2"
reset; all_ucs_pass; UC_PARTS["t:12"]="FAIL|analytics|x"$'\n'; UC_PARTS["t:02"]="BLOCKED|tierB|x"$'\n'; HEA14_STATUS[t]=PASS
expect "FAIL beats BLOCKED" "$(verdict)" "FAIL/1"
reset; all_ucs_pass; UC_PARTS["t:13"]="SKIPPED|adminRead|x"$'\n'; HEA14_STATUS[t]=PASS
expect "a skipped use case is never a pass" "$(verdict)" "BLOCKED/2"

printf 'Hand-off binding\n'
chain_dir="$work/chain"; mkdir -p "$chain_dir"
run_start_marker="$work/run-start"; touch -d '2000-01-01 00:00:00' "$run_start_marker"
printf 'e1\ne2\n' >"$chain_dir/ev.jsonl"
digest="$(sha256_of "$chain_dir/ev.jsonl")"
printf 'events-sha256\t%s\nrow\n' "$digest" >"$chain_dir/rep.jsonl"
expect "report bound to the current events file" "$(verify_binding ev.jsonl rep.jsonl)" PASS
printf 'events-sha256\t%s\nrow\n' "0000" >"$chain_dir/rep.jsonl"
expect "a report built from other events fails" "$(verify_binding ev.jsonl rep.jsonl | cut -c1-4)" FAIL
printf 'events-sha256\t%s\nrow\n' "$digest" >"$chain_dir/rep.jsonl"
touch -d '1999-01-01 00:00:00' "$chain_dir/rep.jsonl"
expect "a report older than the run fails" "$(verify_binding ev.jsonl rep.jsonl | cut -c1-4)" FAIL
touch -d '1999-01-01 00:00:00' "$chain_dir/ev.jsonl"; touch "$chain_dir/rep.jsonl"
expect "events older than the run fail" "$(verify_binding ev.jsonl rep.jsonl | cut -c1-4)" FAIL
expect "a missing report fails" "$(verify_binding ev.jsonl none.jsonl | cut -c1-4)" FAIL

result_done=1
if (( failures == 0 )); then
  printf 'RESULT: PASS checks=%s failures=0\n' "$checks"
  exit 0
fi
printf 'RESULT: FAIL checks=%s failures=%s\n' "$checks" "$failures" >&2
exit 1
