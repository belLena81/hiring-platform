#!/usr/bin/env bash
# Fast "may this session finish?" gate for the Claude Code Stop and SubagentStop hooks.
# Contract: see "Operator scripts" in docs/engineering-quality.md. Run with --help for usage and exit codes.
set -uo pipefail

project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

help_text() {
  cat <<'HELP'
Usage: scripts/check-done.sh            (Stop hook: skills, spec verdicts, documentation currency, compile freshness)
       scripts/check-done.sh --subagent (SubagentStop hook: skills and spec verdicts only)
       scripts/check-done.sh --help

Runs the checks that take seconds and need no Docker, so "done" cannot skip them:
  python3 -I scripts/check-skills.py
  python3 -I scripts/check-spec-verdicts.py           (--lines-only on a repeated Stop)
  python3 -I scripts/check-docs-current.py            (first Stop only)
  compile freshness of each build                     (first Stop only)
Skills and verdict lines block every Stop, because they cannot legitimately fail in the middle of a task; after eight blocks in
a row Claude Code overrides the next one and ends the turn, so this cannot loop forever. The review log comparison, documentation
and compile freshness block once, because each can fail for a reason the agent cannot or should not fix at once, such as an
unrelated commit or a turn ending unfinished. A subagent stop blocks once, because a read-only reviewer cannot fix a failing gate.

Compile freshness compares file times only: a changed Scala file newer than the build's newest compiler analysis asks the agent
to run sbt Test/compile. This script never compiles, because hooks run outside the Bash sandbox and compiling executes
build definitions and macros. A reminder, not proof: an analysis from a partly successful compile can look fresh.

It never runs scripts/check-local.sh (about two hours, needs Docker). On success it tells the user, through a systemMessage,
that the local check and independent QA evidence are still required when code changed.

Hook input: the hook JSON on stdin; "stop_hook_active" true marks a repeated stop.

Exit codes: 0 PASS (or nothing left to report), 2 FAIL or usage error (the failing output is on stderr and goes back to the
agent). The last line on stderr is "RESULT: PASS|FAIL checks=<n> failed=<n>"; stdout carries only the Stop-mode JSON systemMessage.
HELP
}

mode="stop"
case "${1:-}" in
  "") ;;
  --subagent) mode="subagent" ;;
  -h | --help) help_text; exit 0 ;;
  *) printf 'check-done: unexpected argument: %s (run scripts/check-done.sh --help)\n' "$1" >&2; exit 2 ;;
esac

repeated=false
if [ ! -t 0 ]; then
  input="$(cat)"
  if printf '%s' "$input" | python3 -I -c 'import json,sys; sys.exit(0 if json.load(sys.stdin).get("stop_hook_active") is True else 1)' 2>/dev/null; then
    repeated=true
    [ "$mode" = "subagent" ] && exit 0
  fi
fi

first_stop=false
[ "$mode" = "stop" ] && [ "$repeated" = false ] && first_stop=true

checks=("python3 -I scripts/check-skills.py")
if [ "$first_stop" = true ] || [ "$mode" = "subagent" ]; then
  checks+=("python3 -I scripts/check-spec-verdicts.py")
else
  checks+=("python3 -I scripts/check-spec-verdicts.py --lines-only")
fi
[ "$first_stop" = true ] && checks+=("python3 -I scripts/check-docs-current.py")

failed=0
for check in "${checks[@]}"; do
  if ! output="$($check 2>&1)"; then
    failed=$((failed + 1))
    printf '%s\n--- %s failed ---\n%s\n' "check-done: cannot finish yet" "$check" "$output" >&2
  fi
done

# The newest modification time, in whole seconds, among the files named on stdin (NUL-separated); 0 when none can be read.
# A deleted file is skipped by stat, so it never counts as a change.
newest_time() {
  local newest
  newest="$(xargs -0 -r stat -c '%Y' 2>/dev/null | sort -n | tail -1)"
  printf '%s\n' "${newest:-0}"
}

# Reports a build whose changed Scala files are newer than its newest compiler analysis. Build definitions are left out:
# sbt rewrites the analysis only when something recompiled, so an sbt edit could never clear this reminder.
compile_freshness() {
  local name="$1" directory="$2" sources analysis
  shift 2
  # -z throughout: paths are unquoted, so a non-ASCII name is still seen.
  sources="$({ git diff --name-only -z HEAD -- "$@"; git ls-files --others --exclude-standard -z -- "$@"; } 2>/dev/null \
    | grep -z '\.scala$' | newest_time)"
  [ "$sources" -gt 0 ] || return 0
  analysis="$(find "$directory/target" -type f -name 'inc_compile*' -print0 2>/dev/null | newest_time)"
  checks+=("compile freshness $name")
  if [ "$sources" -gt "$analysis" ]; then
    failed=$((failed + 1))
    printf 'check-done: cannot finish yet\n--- %s has Scala changes newer than its last compile ---\nRun sbt Test/compile in %s and fix any errors, then finish again.\n' \
      "$name" "$directory" >&2
  fi
}

if [ "$first_stop" = true ]; then
  # Integration sources are left out: sbt Test/compile does not compile them, so it could never clear this reminder.
  compile_freshness "application build" . src ':(exclude)src/it' test-support
  # analytics/build.sbt also compiles test-support, under its own Scala version.
  compile_freshness "analytics build" analytics analytics/src ':(exclude)analytics/src/it' test-support
fi

if [ "$failed" -gt 0 ]; then
  printf 'check-done: fix the failures above, then finish again\nRESULT: FAIL checks=%s failed=%s\n' "${#checks[@]}" "$failed" >&2
  exit 2
fi

# Hook mode keeps stdout a single JSON document (the harness parses it); the RESULT line goes to stderr.
printf 'RESULT: PASS checks=%s failed=0\n' "${#checks[@]}" >&2
if [ "$mode" = "stop" ]; then
  printf '{"systemMessage":"check-done PASS (skills, spec verdicts, documentation currency, compile freshness). If code changed, scripts/check-local.sh and independent QA evidence are still required before closing."}\n'
fi
exit 0