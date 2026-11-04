"""Validates the project-local role skills and subagent definitions.

Contract: see "Operator scripts" in docs/engineering-quality.md.
"""

import contextlib
import io
import json
import os
from pathlib import Path
import re
import sys
import time

HELP = """Usage: python3 -I scripts/check-skills.py
       python3 -I scripts/check-skills.py --hook   (PostToolUse hook: reads the hook JSON on stdin)
       python3 -I scripts/check-skills.py --help

With --hook the check runs only when the edited file is AGENTS.md or under .claude/, prints nothing on success, and exits 2
on a failure so Claude Code returns the problems to the agent.

Checks offline (no network, about 1 second, so no progress lines) that every role named in AGENTS.md has a project-local
.claude/skills/<role>/SKILL.md with valid frontmatter, that no unexpected skill folder exists, and that each delegated
role has a subagent definition in .claude/agents with an explicit tool list (read-only reviewers get no write tools).

Exit codes: 0 PASS, 1 FAIL (problems are listed), 2 usage error or required file missing (BLOCKED), 130 interrupted.
The last line is "RESULT: PASS|FAIL|BLOCKED skills=<n> agents=<n> problems=<n> duration=<s>".

Example:
  python3 -I scripts/check-skills.py
"""

READ_ONLY_ROLES = {"code-reviewer", "security-engineer", "qa-engineer"}
WRITE_TOOLS = {"Edit", "Write", "NotebookEdit"}
COORDINATOR_ROLES = {"product-manager"}


def frontmatter(path):
    content = path.read_text()
    match = re.match(r"\A---\n(.*?)\n---(?:\n|$)", content, re.DOTALL)
    if match is None:
        return None
    return dict(re.findall(r"^([a-zA-Z_-]+):\s*([^\n]+)$", match.group(1), re.MULTILINE))


def main():
    project_root = Path(__file__).resolve().parent.parent
    skill_root = project_root / ".claude" / "skills"
    agent_root = project_root / ".claude" / "agents"
    rules = (project_root / "AGENTS.md").read_text()
    expected_roles = set(re.findall(r"`\.claude/skills/([a-z0-9-]+)/SKILL\.md`", rules))
    errors = []

    if not expected_roles:
        errors.append("No project roles found in AGENTS.md")

    for role in sorted(expected_roles):
        skill_file = skill_root / role / "SKILL.md"
        if not skill_file.is_file():
            errors.append(f"Missing project skill: {role}")
            continue
        if skill_file.is_symlink() or skill_file.parent.is_symlink():
            errors.append(f"Canonical skill must be stored in this project: {role}")
        metadata = frontmatter(skill_file)
        if metadata is None:
            errors.append(f"Missing skill frontmatter: {role}")
        elif metadata.get("name") != role or not metadata.get("description", "").strip():
            errors.append(f"Invalid skill name or description: {role}")
        if not skill_file.resolve().is_relative_to(project_root):
            errors.append(f"Skill resolves outside the project: {role}")

    actual_skill_roles = {e.name for e in skill_root.iterdir() if e.is_dir()} if skill_root.is_dir() else set()
    if actual_skill_roles != expected_roles:
        errors.append("Unexpected or missing role folders in .claude/skills")

    delegated_roles = expected_roles - COORDINATOR_ROLES
    for role in sorted(delegated_roles):
        agent_file = agent_root / f"{role}.md"
        if not agent_file.is_file():
            errors.append(f"Missing subagent definition: {role}")
            continue
        metadata = frontmatter(agent_file)
        if metadata is None or metadata.get("name") != role or not metadata.get("description", "").strip():
            errors.append(f"Invalid subagent frontmatter: {role}")
            continue
        if "tools" not in metadata or "model" not in metadata:
            # An omitted tools line inherits every tool, including Edit and Write.
            errors.append(f"Subagent must declare explicit tools and model: {role}")
            continue
        tools = {t.strip() for t in metadata["tools"].split(",") if t.strip()}
        if role in READ_ONLY_ROLES and tools & WRITE_TOOLS:
            errors.append(f"Read-only reviewer has write tools: {role}")
        if role in READ_ONLY_ROLES and not tools:
            errors.append(f"Read-only reviewer must declare an explicit tool list: {role}")

    counts = f"skills={len(expected_roles)} agents={len(delegated_roles)}"
    if errors:
        print("\n".join(errors), file=sys.stderr)
        print(
            "check-skills: next: fix the problems listed above (each names the role), then run again",
            file=sys.stderr,
        )
        print(
            f"RESULT: FAIL {counts} problems={len(errors)} duration={time.monotonic() - STARTED:.1f}s",
            file=sys.stderr,
        )
        return 1
    print(
        f"Validated {len(expected_roles)} project skills and {len(delegated_roles)} subagent definitions "
        "in .claude/skills and .claude/agents"
    )
    print(f"RESULT: PASS {counts} problems=0 duration={time.monotonic() - STARTED:.1f}s")
    return 0


STARTED = time.monotonic()


def touches_role_definitions(hook_input):
    """Pure: does this tool call edit AGENTS.md or something under .claude/? Unknown input counts as yes (fail closed).

    A relative path is taken relative to the project root. Two readings are tried and either one triggers the check: the
    path as written (symlinks not followed, so a link inside .claude/ that points out of the project cannot hide an edit)
    and the fully resolved path (so a link outside the project that points into .claude/, or a checkout reached through a
    symlinked directory, is still recognised). The Stop hook runs the full check as the backstop."""
    try:
        tool_input = json.loads(hook_input).get("tool_input") or {}
        edited = tool_input.get("file_path") or tool_input.get("notebook_path")
    except (ValueError, AttributeError):
        return True
    if not isinstance(edited, str) or not edited:
        return True
    project_root = Path(__file__).resolve().parent.parent
    for root in {Path(os.path.abspath(Path(__file__).parent.parent)), project_root}:
        for candidate in (os.path.abspath(os.path.join(root, edited)), os.path.realpath(os.path.join(root, edited))):
            try:
                relative = Path(candidate).relative_to(root).as_posix()
            except ValueError:
                continue  # outside this reading of the project
            except (OSError, RuntimeError):
                return True
            if relative == "AGENTS.md" or relative.startswith(".claude/"):
                return True
    return False


def hook():
    """PostToolUse mode: silent unless AGENTS.md or .claude/ was edited; a failure exits 2 so Claude sees it."""
    if not touches_role_definitions(sys.stdin.read()):
        return 0
    captured = io.StringIO()
    with contextlib.redirect_stdout(captured):
        code = main()
    return 0 if code == 0 else 2


def run():
    arguments = sys.argv[1:]
    if arguments in (["-h"], ["--help"]):
        print(HELP, end="")
        return 0
    if arguments == ["--hook"]:
        try:
            return hook()
        except Exception as error:  # fail closed: any failure in hook mode must reach the agent as exit 2
            print(f"check-skills: hook mode failed ({error.__class__.__name__}): {error}", file=sys.stderr)
            return 2
    if arguments:
        print(f"check-skills: unexpected argument: {arguments[0]}", file=sys.stderr)
        print("check-skills: run python3 -I scripts/check-skills.py --help for usage", file=sys.stderr)
        print("RESULT: BLOCKED reason=usage skills=0 agents=0 problems=0 duration=0.0s", file=sys.stderr)
        return 2
    print(
        "check-skills: validating project skills (.claude/skills) and subagent definitions (.claude/agents) "
        "against AGENTS.md; no parameters; offline, about 1 s"
    )
    try:
        return main()
    except OSError as error:
        print(f"check-skills: cannot read a required file: {error.filename or error}", file=sys.stderr)
        print("check-skills: next: run from the project checkout; AGENTS.md and .claude/ must exist", file=sys.stderr)
        print(
            f"RESULT: BLOCKED reason=missing-file skills=0 agents=0 problems=0 duration={time.monotonic() - STARTED:.1f}s",
            file=sys.stderr,
        )
        return 2
    except KeyboardInterrupt:
        print("\ncheck-skills: interrupted; nothing was modified (read-only check)", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(run())
