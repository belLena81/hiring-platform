"""Documentation currency gate: a change to behavior, contracts, configuration or the workflow carries its docs.

Contract: see "Operator scripts" in docs/engineering-quality.md. The --hook mode is a PreToolUse hook (Claude Code hook
protocol) and, like guard-harness.py, prints a decision object instead of a RESULT line.
"""

import json
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # -I drops the script directory; harness_paths lives next to us
import harness_paths  # noqa: E402

HELP = """Usage: python3 -I scripts/check-docs-current.py                  check uncommitted changes and unpushed gated commits
       python3 -I scripts/check-docs-current.py --staged         check only what the next commit would contain
       python3 -I scripts/check-docs-current.py --range A..B     check every commit in a range, one by one
       python3 -I scripts/check-docs-current.py --hook           PreToolUse hook for `git commit` (reads JSON on stdin)
       python3 -I scripts/check-docs-current.py --commit-msg F   git commit-msg hook (.githooks/commit-msg): index + message F
       python3 -I scripts/check-docs-current.py --help

A change that touches decision-bearing paths (production source, configuration, provisioning, build, operator scripts
or the agent workflow) must also change canonical documentation: README.md, ARCHITECTURE.md, or docs/ outside
docs/specs/ and docs/templates/ (docs/wiki/ counts). A commit that genuinely needs no canonical documentation change
says so with a trailer in its message, with a reason of at least ten characters:

  Docs-Impact: none - <reason>          (no decision or documented behavior changed)
  Docs-Impact: spec-only - <reason>     (the decision is recorded only in its docs/specs/ file, deliberately)

The default run also checks each commit not yet on the upstream (or main) made after this script was first committed,
so a commit that bypassed the hooks is caught before it is pushed. Working-tree changes have no message, so a change
that legitimately needs no docs is reported until it is committed with its trailer.
The Claude Code hook reads -m text, heredoc messages and -F files; for a message it cannot see (editor, -C, --amend
--no-edit) it defers to the commit-msg hook when core.hooksPath is .githooks. It recognizes direct `git commit` calls,
including `git --no-pager commit`, a path to git and a quoted `sh -c "git commit ..."`; aliases and scripts are only
caught by the commit-msg hook.
Merge commits are not checked: their changes were checked when they were committed on their own branch.
Offline, git only, each git call bounded to 30 s; usually well under 1 s, so no progress lines.

Exit codes: 0 PASS, 1 FAIL (offending changes are listed), 2 usage error or git unavailable (BLOCKED), 130 interrupted.
The last line is "RESULT: PASS|FAIL|BLOCKED changes=<n> problems=<n> duration=<s>".

Examples:
  python3 -I scripts/check-docs-current.py
  python3 -I scripts/check-docs-current.py --range main..HEAD
"""

PROJECT = Path(__file__).resolve().parent.parent
GIT_TIMEOUT_SECONDS = 30
MIN_REASON_CHARS = 10

# The workflow files come from scripts/harness_paths.py (shared with guard-harness.py); production source and provisioning
# are this gate's own additions.
DECISION_PREFIXES = harness_paths.WORKFLOW_PREFIXES + ("src/main/", "analytics/src/main/", "docker/")
DECISION_FILES = set(harness_paths.WORKFLOW_FILES)
DECISION_PATTERNS = (harness_paths.COMPOSE_FILE,)
NOT_DECISION_PREFIXES = ("scripts/tests/", "scripts/__pycache__/", ".claude/settings.local.json")
CANONICAL_DOC_FILES = {"README.md", "ARCHITECTURE.md"}
NON_CANONICAL_DOC_PREFIXES = ("docs/specs/", "docs/templates/")
TRAILER = re.compile(r"^Docs-Impact:\s*(none|spec-only)\s*[-:–—]\s*(.+?)\s*$", re.IGNORECASE | re.MULTILINE)
STARTED = time.monotonic()


class GitUnavailable(Exception):
    pass


def git(*args):
    try:
        done = subprocess.run(["git", *args], cwd=PROJECT, capture_output=True, text=True,
                              timeout=GIT_TIMEOUT_SECONDS, check=False)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise GitUnavailable(f"git {args[0]} did not complete: {error.__class__.__name__}") from error
    if done.returncode != 0:
        raise GitUnavailable(f"git {args[0]} failed: {done.stderr.strip().splitlines()[-1:] or ['no output']}")
    return done.stdout


def is_decision(path):
    if path.startswith(NOT_DECISION_PREFIXES):
        return False
    return (path.startswith(DECISION_PREFIXES) or path in DECISION_FILES
            or any(p.match(path) for p in DECISION_PATTERNS))


def is_canonical_doc(path):
    if path in CANONICAL_DOC_FILES:
        return True
    return path.startswith("docs/") and path.endswith(".md") and not path.startswith(NON_CANONICAL_DOC_PREFIXES)


def trailer_reason(message):
    """The declared impact and reason, or None when the message carries no valid trailer."""
    match = TRAILER.search(message or "")
    if match is None or len(match.group(2)) < MIN_REASON_CHARS:
        return None
    return match.group(1).lower(), match.group(2)


def verdict(paths, message=None):
    """None when the change is documented (or declared as needing no docs), otherwise the problem text."""
    decisions = sorted(p for p in paths if is_decision(p))
    if not decisions or any(is_canonical_doc(p) for p in paths):
        return None
    if message is not None and trailer_reason(message) is not None:
        return None
    shown = ", ".join(decisions[:5]) + (f" and {len(decisions) - 5} more" if len(decisions) > 5 else "")
    return (f"decision-bearing paths changed without canonical documentation: {shown}. Update README.md, "
            "ARCHITECTURE.md or docs/ (docs/wiki/ included) to state the decision and why it matters, or add "
            "'Docs-Impact: none - <reason>' or 'Docs-Impact: spec-only - <reason>' to the commit message")


def names(*diff_args):
    return {line for line in git("diff", "--name-only", *diff_args).splitlines() if line}


def commit_paths(sha):
    return {p for p in git("diff-tree", "--root", "--no-commit-id", "--name-only", "-r", sha).splitlines() if p}


def commit_change(sha):
    message = git("log", "-1", "--format=%B", sha)
    return f"commit {sha[:7]} {message.splitlines()[0] if message else ''}", commit_paths(sha), message


def unpushed_gated_commits():
    """Commits not yet on the upstream (or main) that were made after this gate was introduced.

    Earlier commits predate the rule and are not judged by it. A commit that bypassed the hooks (--no-verify, an
    unset core.hooksPath, a git error) is still found here before it is pushed.
    """
    base = None
    for candidate in ("@{u}", "main"):
        try:
            git("rev-parse", "--verify", "-q", candidate)
            base = candidate
            break
        except GitUnavailable:
            continue
    introduced = git("log", "--diff-filter=A", "--format=%H", "--", "scripts/check-docs-current.py").split()
    if base is None or not introduced:
        return []
    gate_commit = introduced[-1]
    gated = []
    for sha in git("rev-list", "--no-merges", "--reverse", f"{base}..HEAD").split():
        if subprocess.run(["git", "merge-base", "--is-ancestor", gate_commit, sha], cwd=PROJECT, capture_output=True,
                          timeout=GIT_TIMEOUT_SECONDS, check=False).returncode == 0:
            gated.append(commit_change(sha))
    return gated


def untracked():
    return {line for line in git("ls-files", "--others", "--exclude-standard").splitlines() if line}


def commit_includes_unstaged(commit_line):
    """Whether `git commit` also takes unstaged changes (-a, --include/--only, or pathspecs); unclear means yes."""
    try:
        words = shlex.split(commit_line, comments=False, posix=True)
    except ValueError:
        return True
    if "commit" not in words:
        return True
    after = words[words.index("commit") + 1:]
    skip_value = {"-m", "--message", "-F", "--file", "-C", "-c", "--author", "--date", "--fixup", "--squash",
                  "--cleanup", "-t", "--template", "--trailer"}
    expecting_value = False
    for word in after:
        if word in ("&&", "||", ";", "|") or word.startswith("<<"):
            break
        if expecting_value:
            expecting_value = False
            continue
        if word in skip_value:
            expecting_value = True
            continue
        if word in ("-a", "--all", "--include", "-i", "--only", "-o") or re.match(r"^-[a-zA-Z]*a[a-zA-Z]*$", word):
            return True
        if word == "--":
            return True
        if not word.startswith("-"):
            return True
    return False


_GIT = r"(^|[;&|(\n'\"`]\s*)(\S*/)?git\s+((-[cC]\s+\S+|--[a-z-]+(=\S+)?|-p|-P)\s+)*"
GIT_COMMIT = re.compile(_GIT + r"commit\b")
GIT_ADD = re.compile(_GIT + r"(add|stage|rm|mv)\b")
INLINE_MESSAGE = re.compile(r"(^|\s)(-[a-zA-Z]*m|--message)(=|\s|$)|<<")


def message_file(commit_line):
    """The file named by -F/--file, when the commit reads its message from one (`-` is stdin, not a file)."""
    try:
        words = shlex.split(commit_line, comments=False, posix=True)
    except ValueError:
        return None
    for index, word in enumerate(words):
        if word in ("-F", "--file") and index + 1 < len(words) and words[index + 1] != "-":
            return words[index + 1]
        if word.startswith("--file=") and word != "--file=-":
            return word.split("=", 1)[1]
    return None


SKIPS_HOOKS = re.compile(r"(^|\s)(--no-verify|-[a-zA-Z]*n[a-zA-Z]*)(\s|$)")
MAX_MESSAGE_FILE_BYTES = 64 * 1024


def bounded_message_file(named_file):
    """A -F message file's text when it is a regular file inside the project, read up to 64 KiB; otherwise empty."""
    try:
        path = (PROJECT / named_file).resolve()
        if not path.is_relative_to(PROJECT.resolve()) or not path.is_file():
            return ""
        with path.open("rb") as handle:
            return handle.read(MAX_MESSAGE_FILE_BYTES).decode("utf-8", errors="replace")
    except OSError:
        return ""


def commit_msg_hook_enabled():
    try:
        return git("config", "--get", "core.hooksPath").strip() == ".githooks"
    except GitUnavailable:
        return False


def hook():
    try:
        payload = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        return 0
    if payload.get("tool_name") != "Bash":
        return 0
    command = (payload.get("tool_input") or {}).get("command", "")
    found = GIT_COMMIT.search(command)
    if found is None:
        return 0
    commit_line = command[found.start():].lstrip(";&|(\n'\"` \t").split("\n", 1)[0].rstrip("'\"`)")
    try:
        if merging():
            return 0
        paths = names("--cached")
        if commit_includes_unstaged(commit_line):
            paths |= names()
        if GIT_ADD.search(command[:found.start()]):
            # `git add … && git commit` in one command: the hook runs before the add, so count what it may stage.
            paths |= names() | untracked()
    except (GitUnavailable, OSError, subprocess.TimeoutExpired):
        return 0  # git itself will report; this gate never blocks on its own failure to read
    # The command carries -m text and heredoc messages; a -F file is read; any other source (editor, -C, --amend
    # --no-edit) is invisible here, so the decision is left to the commit-msg hook when it is enabled.
    message = command
    named_file = message_file(commit_line)
    if named_file is not None:
        message += "\n" + bounded_message_file(named_file)
    elif not INLINE_MESSAGE.search(command[found.start():]) and not SKIPS_HOOKS.search(commit_line) \
            and commit_msg_hook_enabled():
        return 0
    problem = verdict(paths, message)
    if problem is not None:
        print(json.dumps({"hookSpecificOutput": {"hookEventName": "PreToolUse", "permissionDecision": "deny",
                                                 "permissionDecisionReason": f"Documentation currency gate: {problem}"}}))
    return 0


def merging():
    """A merge in progress: its commit only records changes already checked on their own branch."""
    return subprocess.run(["git", "rev-parse", "-q", "--verify", "MERGE_HEAD"], cwd=PROJECT, capture_output=True,
                          timeout=GIT_TIMEOUT_SECONDS, check=False).returncode == 0


def commit_msg(message_file):
    """git commit-msg hook: the index git is committing (GIT_INDEX_FILE is honoured, so `commit -a` is covered)."""
    try:
        if merging():
            return 0
        message = "\n".join(line for line in message_file.read_text(errors="replace").splitlines()
                            if not line.startswith("#"))
        changes = [("this commit", names("--cached"), message)]
    except (OSError, subprocess.TimeoutExpired, GitUnavailable) as error:
        print(f"check-docs-current: cannot read the commit being made ({error.__class__.__name__}); "
              "the commit is allowed, run python3 -I scripts/check-docs-current.py --range HEAD~1..HEAD after it",
              file=sys.stderr)
        return 0
    return report(changes, check(changes))


def check(changes):
    problems = []
    for label, paths, message in changes:
        problem = verdict(paths, message)
        if problem is not None:
            problems.append(f"{label}: {problem}")
    return problems


def report(changes, problems):
    counts = f"changes={len(changes)} problems={len(problems)} duration={time.monotonic() - STARTED:.1f}s"
    if problems:
        print("\n".join(problems), file=sys.stderr)
        print("check-docs-current: next: update the canonical documentation named above in the same change, or "
              "declare the impact with a Docs-Impact trailer in the commit message", file=sys.stderr)
        print(f"RESULT: FAIL {counts}", file=sys.stderr)
        return 1
    print(f"RESULT: PASS {counts}")
    return 0


def blocked(reason):
    print(f"check-docs-current: {reason}", file=sys.stderr)
    print("check-docs-current: run python3 -I scripts/check-docs-current.py --help for usage", file=sys.stderr)
    print(f"RESULT: BLOCKED reason=usage changes=0 problems=0 duration={time.monotonic() - STARTED:.1f}s",
          file=sys.stderr)
    return 2


def run():
    arguments = sys.argv[1:]
    if arguments in (["-h"], ["--help"]):
        print(HELP, end="")
        return 0
    if arguments == ["--hook"]:
        return hook()
    if len(arguments) == 2 and arguments[0] == "--commit-msg":
        return commit_msg(Path(arguments[1]))
    try:
        if arguments == []:
            print("check-docs-current: checking uncommitted changes (staged, unstaged, untracked) and unpushed "
                  "commits made since this gate was introduced, for documentation; offline, under 1 s")
            changes = [("working tree", names("HEAD") | untracked(), None)] + unpushed_gated_commits()
        elif arguments == ["--staged"]:
            print("check-docs-current: checking staged changes for documentation; offline, under 1 s")
            changes = [("staged changes", names("--cached"), None)]
        elif len(arguments) == 2 and arguments[0] == "--range" and ".." in arguments[1] \
                and not arguments[1].startswith("-"):
            print(f"check-docs-current: checking each commit in {arguments[1]} for documentation; offline")
            changes = []
            for sha in git("rev-list", "--no-merges", "--reverse", arguments[1]).split():
                changes.append(commit_change(sha))
        else:
            return blocked(f"unexpected arguments: {' '.join(arguments)}")
    except (GitUnavailable, OSError, subprocess.TimeoutExpired) as error:
        print(f"check-docs-current: {error}", file=sys.stderr)
        print("check-docs-current: next: run from the project checkout with git installed", file=sys.stderr)
        print(f"RESULT: BLOCKED reason=git changes=0 problems=0 duration={time.monotonic() - STARTED:.1f}s",
              file=sys.stderr)
        return 2
    return report(changes, check(changes))


if __name__ == "__main__":
    try:
        sys.exit(run())
    except KeyboardInterrupt:
        print("check-docs-current: interrupted; nothing to clean up", file=sys.stderr)
        print(f"RESULT: BLOCKED reason=interrupted changes=0 problems=0 duration={time.monotonic() - STARTED:.1f}s",
              file=sys.stderr)
        sys.exit(130)
