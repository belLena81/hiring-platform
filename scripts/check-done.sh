#!/usr/bin/env bash
# Fast "may this session finish?" gate for the Claude Code Stop and SubagentStop hooks.
# Contract: see "Operator scripts" in docs/engineering-quality.md. Run with --help for usage and exit codes.
set -uo pipefail

project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

help_text() {
  cat <<'HELP'
Usage: scripts/check-done.sh            (Stop hook: skills, documentation currency, spec verdicts)
       scripts/check-done.sh --subagent (SubagentStop hook: skills and spec verdicts only)
       scripts/check-done.sh --help

Runs the checks that take seconds and need no Docker, so "done" cannot skip them:
  python3 -I scripts/check-skills.py
  python3 -I scripts/check-docs-current.py          (Stop only: a subagent works inside an unfinished task)
  python3 -I scripts/check-spec-verdicts.py
It never runs scripts/check-local.sh (about two hours, needs Docker). On success it asks, through a systemMessage, for the
local check and independent QA evidence when code changed.

Hook input: the hook JSON on stdin. When "stop_hook_active" is true the script exits 0 at once, so a failing gate cannot
trap the session in a loop. The gate therefore reports a failure once: the second stop is not re-checked, so it is a
strong reminder, not a lock.

Exit codes: 0 PASS (or already reported once), 2 FAIL (the failing output is on stderr and goes back to the agent).
The last line on stderr is "RESULT: PASS|FAIL checks=<n> failed=<n>"; stdout carries only the Stop-mode JSON systemMessage.
HELP
}

mode="stop"
case "${1:-}" in
  "") ;;
  --subagent) mode="subagent" ;;
  -h | --help) help_text; exit 0 ;;
  *) printf 'check-done: unexpected argument: %s (run scripts/check-done.sh --help)\n' "$1" >&2; exit 2 ;;
esac

if [ ! -t 0 ]; then
  input="$(cat)"
  if printf '%s' "$input" | python3 -I -c 'import json,sys; sys.exit(0 if json.load(sys.stdin).get("stop_hook_active") is True else 1)' 2>/dev/null; then
    exit 0
  fi
fi

checks=("python3 -I scripts/check-skills.py")
[ "$mode" = "stop" ] && checks+=("python3 -I scripts/check-docs-current.py")
checks+=("python3 -I scripts/check-spec-verdicts.py")

failed=0
for check in "${checks[@]}"; do
  if ! output="$($check 2>&1)"; then
    failed=$((failed + 1))
    printf '%s\n--- %s failed ---\n%s\n' "check-done: cannot finish yet" "$check" "$output" >&2
  fi
done

if [ "$failed" -gt 0 ]; then
  printf 'check-done: fix the failures above, then finish again\nRESULT: FAIL checks=%s failed=%s\n' "${#checks[@]}" "$failed" >&2
  exit 2
fi

# Hook mode keeps stdout a single JSON document (the harness parses it); the RESULT line goes to stderr.
printf 'RESULT: PASS checks=%s failed=0\n' "${#checks[@]}" >&2
if [ "$mode" = "stop" ]; then
  printf '{"systemMessage":"check-done PASS (skills, documentation currency, spec verdicts). If code changed, scripts/check-local.sh and independent QA evidence are still required before closing."}\n'
fi
exit 0
