"""Checks that every finished specification records its three independent review verdicts.

Contract: see "Operator scripts" in docs/engineering-quality.md.
"""

import json
from pathlib import Path
import re
import subprocess
import sys
import time

HELP = """Usage: python3 -I scripts/check-spec-verdicts.py
       python3 -I scripts/check-spec-verdicts.py --lines-only
       python3 -I scripts/check-spec-verdicts.py --help

Checks offline (no network, about 1 second) every docs/specs/*.md whose first "- Status:" line begins with "done". Such a spec
must carry the three verdict lines of docs/templates/feature-spec.md, each ending in a verdict:
  - Code Reviewer verdict[ (scope)]: PASS ... | not applicable - <reason of at least 10 characters>
  - Security Engineer verdict[ (scope)]: PASS ... | not applicable - <reason of at least 10 characters>
  - Final independent QA verdict[ (scope)]: PASS ... | not applicable - <reason of at least 10 characters>
"pending", FAIL, BLOCKED, CONDITIONAL and a missing line all fail. A reviewer's "not applicable" reason is the tiering
reason from docs/agent-development.md (Review tiers); reviewers challenge a weak reason, this script only requires one.

An uncommitted done spec is also compared with the local review log (.local/logs/review-runs.jsonl, written by the SubagentStop
and SubagentHandback hooks): each PASS needs a logged run of that reviewer since the last commit, and the latest such run must be PASS. Committed
specs are not compared, because older reviews predate the log and the log is local to this clone. --lines-only skips this
comparison: the Stop hook uses it on repeated stops, because an unrelated commit can make a valid review look stale.

Exit codes: 0 PASS, 1 FAIL (problems are listed), 2 usage error or required folder missing (BLOCKED), 130 interrupted.
The last line is "RESULT: PASS|FAIL|BLOCKED specs=<n> done=<n> problems=<n> duration=<s>".

Example:
  python3 -I scripts/check-spec-verdicts.py
"""

STARTED = time.monotonic()
STATUS_LINE = re.compile(r"^- Status:.*$", re.MULTILINE)
STATUS_DONE = re.compile(r"^- Status:\s*\**\s*done\b", re.IGNORECASE)
CONDITIONAL = re.compile(r"\bconditional\b", re.IGNORECASE)


def is_done(text):
    """Pure: a spec is done when its first '- Status:' line (the Identity section) begins with 'done'."""
    first = STATUS_LINE.search(text)
    return first is not None and STATUS_DONE.match(first.group(0)) is not None
# "<role> verdict", then an optional label and an optional parenthesised scope (which may itself contain colons), then
# the verdict after the first colon that follows the scope.
VERDICT_LINES = (
    ("Code Reviewer", re.compile(r"^- Code Reviewer verdict\b[^:(\n]*(?:\([^)\n]*\)[^:(\n]*)?:\s*(.*)$", re.MULTILINE)),
    ("Security Engineer", re.compile(r"^- Security Engineer verdict\b[^:(\n]*(?:\([^)\n]*\)[^:(\n]*)?:\s*(.*)$", re.MULTILINE)),
    ("Final independent QA", re.compile(r"^- Final independent QA verdict\b[^:(\n]*(?:\([^)\n]*\)[^:(\n]*)?:\s*(.*)$", re.MULTILINE)),
)
# PASS must stand alone as the verdict word: end of text or a separator such as ";", ",", ":", "(" or a space, so
# "PASS-with-FAIL" and "PASSED" are rejected.
PASS = re.compile(r"^\**PASS\**(?:$|[\s;:,.(])", re.IGNORECASE)
# The reason must be real text: an unedited "<tier reason>" placeholder from the report template does not count.
NOT_APPLICABLE = re.compile(r"^not applicable\s+-\s+(?!.*<[^>]*>)(.{10,})$", re.IGNORECASE)


def problems_in(text):
    """Pure: the verdict problems of one done specification, as readable strings."""
    problems = []
    for role, pattern in VERDICT_LINES:
        values = [value.strip() for value in pattern.findall(text)]
        if not values:
            problems.append(f"missing '{role} verdict' line")
        elif not all(
            (PASS.match(value) and not CONDITIONAL.search(value)) or NOT_APPLICABLE.match(value) for value in values
        ):
            problems.append(f"'{role} verdict' must be PASS or 'not applicable - <reason>': {values[0][:60]!r}")
    return problems


LOGGED_ROLES = {"Code Reviewer": "code-reviewer", "Security Engineer": "security-engineer", "Final independent QA": "qa-engineer"}


def review_log_problems(text, entries, since_epoch):
    """Pure: PASS lines of a done spec with no logged reviewer run since since_epoch, or whose latest logged run did not pass."""
    problems = []
    for role, pattern in VERDICT_LINES:
        if not any(PASS.match(value.strip()) for value in pattern.findall(text)):
            continue
        runs = [e for e in entries if e.get("role") == LOGGED_ROLES[role] and e.get("epoch", 0) >= since_epoch]
        if not runs:
            problems.append(f"'{role} verdict' says PASS but no {LOGGED_ROLES[role]} run is logged since the last commit")
        elif runs[-1].get("verdict") != "PASS":
            problems.append(f"'{role} verdict' says PASS but the latest logged {LOGGED_ROLES[role]} run says {runs[-1].get('verdict')}")
    return problems


def read_review_log(path):
    """The readable entries of the local review log; a missing file or a damaged line counts as no entry."""
    try:
        lines = path.read_text().splitlines()
    except OSError:
        return []
    entries = []
    for line in lines:
        try:
            entry = json.loads(line)
        except ValueError:
            continue
        if isinstance(entry, dict):
            entries.append(entry)
    return entries


def uncommitted_specs(project_root):
    """The spec paths git reports as changed or new, with the last commit's time; (None, 0) when git cannot answer."""
    try:
        # -z: paths are unquoted, so a non-ASCII spec name still matches.
        status = subprocess.run(["git", "status", "--porcelain", "-z", "--no-renames", "--untracked-files=all", "--", "docs/specs"],
                                cwd=project_root, capture_output=True, text=True, timeout=30, check=True).stdout
        head = subprocess.run(["git", "log", "-1", "--format=%ct"], cwd=project_root, capture_output=True, text=True,
                              timeout=30, check=True).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return None, 0
    return {entry[3:] for entry in status.split("\0") if entry}, int(head or 0)


def main(compare_log=True):
    project_root = Path(__file__).resolve().parent.parent
    spec_root = project_root / "docs" / "specs"
    if not spec_root.is_dir():
        print("check-spec-verdicts: docs/specs is missing; run from the project checkout", file=sys.stderr)
        print(
            f"RESULT: BLOCKED reason=missing-folder specs=0 done=0 problems=0 duration={time.monotonic() - STARTED:.1f}s",
            file=sys.stderr,
        )
        return 2
    specs = sorted(spec_root.glob("*.md"))
    errors = []
    done = 0
    uncommitted, head_epoch = uncommitted_specs(project_root) if compare_log else (set(), 0)
    if uncommitted is None:
        print("check-spec-verdicts: git is unavailable; verdict lines are not compared with the review log")
    entries = read_review_log(project_root / ".local" / "logs" / "review-runs.jsonl")
    for spec in specs:
        text = spec.read_text()
        if not is_done(text):
            continue
        done += 1
        relative = spec.relative_to(project_root)
        errors.extend(f"{relative}: {problem}" for problem in problems_in(text))
        if uncommitted and relative.as_posix() in uncommitted:
            errors.extend(f"{relative}: {problem}" for problem in review_log_problems(text, entries, head_epoch))
    counts = f"specs={len(specs)} done={done}"
    duration = f"duration={time.monotonic() - STARTED:.1f}s"
    if errors:
        print("\n".join(errors), file=sys.stderr)
        print(
            "check-spec-verdicts: next: obtain the independent review and record its verdict, or 'not applicable - <reason>' "
            "when the review tier allows it, on the lines listed above",
            file=sys.stderr,
        )
        print(f"RESULT: FAIL {counts} problems={len(errors)} {duration}", file=sys.stderr)
        return 1
    print(f"Validated verdict lines of {done} done specifications in docs/specs")
    print(f"RESULT: PASS {counts} problems=0 {duration}")
    return 0


def run():
    arguments = sys.argv[1:]
    if arguments in (["-h"], ["--help"]):
        print(HELP, end="")
        return 0
    compare_log = arguments != ["--lines-only"]
    if arguments and compare_log:
        print(f"check-spec-verdicts: unexpected argument: {arguments[0]}", file=sys.stderr)
        print("check-spec-verdicts: run python3 -I scripts/check-spec-verdicts.py --help for usage", file=sys.stderr)
        print("RESULT: BLOCKED reason=usage specs=0 done=0 problems=0 duration=0.0s", file=sys.stderr)
        return 2
    print("check-spec-verdicts: checking review verdict lines of done specs in docs/specs; no parameters; offline, about 1 s")
    try:
        return main(compare_log)
    except OSError as error:
        print(f"check-spec-verdicts: cannot read a required file: {error.filename or error}", file=sys.stderr)
        print(
            f"RESULT: BLOCKED reason=missing-file specs=0 done=0 problems=0 duration={time.monotonic() - STARTED:.1f}s",
            file=sys.stderr,
        )
        return 2
    except KeyboardInterrupt:
        print("\ncheck-spec-verdicts: interrupted; nothing was modified (read-only check)", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(run())
