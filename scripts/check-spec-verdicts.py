"""Checks that every finished specification records its three independent review verdicts.

Contract: see "Operator scripts" in docs/engineering-quality.md.
"""

from pathlib import Path
import re
import sys
import time

HELP = """Usage: python3 -I scripts/check-spec-verdicts.py
       python3 -I scripts/check-spec-verdicts.py --help

Checks offline (no network, about 1 second) every docs/specs/*.md whose first "- Status:" line begins with "done". Such a spec
must carry the three verdict lines of docs/templates/feature-spec.md, each ending in a verdict:
  - Code Reviewer verdict[ (scope)]: PASS ... | not applicable - <reason of at least 10 characters>
  - Security Engineer verdict[ (scope)]: PASS ... | not applicable - <reason of at least 10 characters>
  - Final independent QA verdict[ (scope)]: PASS ... | not applicable - <reason of at least 10 characters>
"pending", FAIL, BLOCKED, CONDITIONAL and a missing line all fail. A reviewer's "not applicable" reason is the tiering
reason from docs/agent-development.md (Review tiers); reviewers challenge a weak reason, this script only requires one.

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
VERDICT_LINES = (
    ("Code Reviewer", re.compile(r"^- Code Reviewer verdict\b[^:\n]*:\s*(.*)$", re.MULTILINE)),
    ("Security Engineer", re.compile(r"^- Security Engineer verdict\b[^:\n]*:\s*(.*)$", re.MULTILINE)),
    ("Final independent QA", re.compile(r"^- Final independent QA verdict\b[^:\n]*:\s*(.*)$", re.MULTILINE)),
)
# PASS must stand alone as the verdict word: end of text or a separator such as ";", ",", ":", "(" or a space, so
# "PASS-with-FAIL" and "PASSED" are rejected.
PASS = re.compile(r"^\**PASS\**(?:$|[\s;:,.(])", re.IGNORECASE)
NOT_APPLICABLE = re.compile(r"^not applicable\s+-\s+(.{10,})$", re.IGNORECASE)


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


def main():
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
    for spec in specs:
        text = spec.read_text()
        if not is_done(text):
            continue
        done += 1
        errors.extend(f"{spec.relative_to(project_root)}: {problem}" for problem in problems_in(text))
    counts = f"specs={len(specs)} done={done}"
    duration = f"duration={time.monotonic() - STARTED:.1f}s"
    if errors:
        print("\n".join(errors), file=sys.stderr)
        print(
            "check-spec-verdicts: next: record the independent verdict (or 'not applicable - <reason>') on the lines "
            "listed above, or reopen the spec by changing its Status",
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
    if arguments:
        print(f"check-spec-verdicts: unexpected argument: {arguments[0]}", file=sys.stderr)
        print("check-spec-verdicts: run python3 -I scripts/check-spec-verdicts.py --help for usage", file=sys.stderr)
        print("RESULT: BLOCKED reason=usage specs=0 done=0 problems=0 duration=0.0s", file=sys.stderr)
        return 2
    print("check-spec-verdicts: checking review verdict lines of done specs in docs/specs; no parameters; offline, about 1 s")
    try:
        return main()
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
