"""Hook: records that an independent reviewer delivered a verdict, so a verdict line in a spec can be compared with a real run.

Runs on SubagentStop (final message) and on PostToolUse of SubagentHandback (the report, when delivered through that tool). Reads
the hook JSON on stdin and appends one line to .local/logs/review-runs.jsonl: role, verdict, UTC time, epoch seconds and commit. It
stores nothing from the review itself. The verdict is a line that is exactly "VERDICT: PASS", "VERDICT: FAIL" or "VERDICT: BLOCKED",
so the report template in docs/agent-development.md never counts. Only the reviewer's own report text is read: never tool results or
the transcript, which can quote that template or a spec. This hook never blocks: a logging failure only means the comparison in
check-spec-verdicts.py reports no run.
"""

import datetime
import json
import os
import re
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # -I drops the script directory; review_fingerprint lives next to us
import review_fingerprint  # noqa: E402

PROJECT = Path(__file__).resolve().parent.parent
LOG = PROJECT / ".local" / "logs" / "review-runs.jsonl"
REVIEWERS = {"code-reviewer", "security-engineer", "qa-engineer"}
VERDICT = re.compile(r"^[ \t]*VERDICT:[ \t]*(PASS|FAIL|BLOCKED)[ \t]*$", re.MULTILINE)


def verdict_in(text):
    """Pure: the verdict lines in this text, the worst when they disagree, so a quoted PASS never overrides a FAIL; or None."""
    found = VERDICT.findall(text if isinstance(text, str) else "")
    return next((verdict for verdict in ("FAIL", "BLOCKED", "PASS") if verdict in found), None)


def report_text(event):
    """Pure: the reviewer's own report in this hook event."""
    if event.get("hook_event_name") == "PostToolUse":
        return (event.get("tool_input") or {}).get("message")
    return event.get("last_assistant_message")


def entry_for(event, now, commit, tree=None):
    """Pure: the log entry for one hook event, or None when it is not a reviewer report with a verdict.
    tree is the content fingerprint of the reviewed tree (scripts/review_fingerprint.py); the verdict check compares it."""
    if event.get("agent_type") not in REVIEWERS:
        return None
    verdict = verdict_in(report_text(event))
    if verdict is None:
        return None
    return {"role": event["agent_type"], "verdict": verdict, "at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "epoch": int(now.timestamp()), "commit": commit, **({"tree": tree} if tree else {})}


def main():
    event = json.load(sys.stdin)
    commit = subprocess.run(["git", "rev-parse", "HEAD"], cwd=PROJECT, capture_output=True, text=True, timeout=10).stdout.strip()
    entry = entry_for(event, datetime.datetime.now(datetime.timezone.utc), commit, review_fingerprint.fingerprint(PROJECT))
    if entry:
        LOG.parent.mkdir(parents=True, exist_ok=True)
        # O_NOFOLLOW: this hook runs outside the sandbox, so it must not follow a link planted at the log path.
        descriptor = os.open(LOG, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "a") as log:
            log.write(json.dumps(entry) + "\n")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:  # never block a subagent because logging failed
        print(f"record-review-run: not recorded: {error.__class__.__name__}", file=sys.stderr)
    sys.exit(0)
