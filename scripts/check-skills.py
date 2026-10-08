from pathlib import Path
import re
import sys

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

    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(
        f"Validated {len(expected_roles)} project skills and {len(delegated_roles)} subagent definitions "
        "in .claude/skills and .claude/agents"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
