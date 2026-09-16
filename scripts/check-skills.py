from pathlib import Path
import re
import sys


def main():
    project_root = Path(__file__).resolve().parent.parent
    skill_root = project_root / ".agents" / "skills"
    rules = (project_root / "AGENTS.md").read_text()
    roles = re.findall(r"`\.agents/skills/([a-z0-9-]+)/SKILL\.md`", rules)
    expected_roles = set(roles)
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
        content = skill_file.read_text()
        frontmatter = re.match(r"\A---\n(.*?)\n---(?:\n|$)", content, re.DOTALL)
        if frontmatter is None:
            errors.append(f"Missing skill frontmatter: {role}")
            continue
        metadata = dict(re.findall(r"^([a-z_]+):\s*([^\n]+)$", frontmatter[1], re.MULTILINE))
        if metadata.get("name") != role or not metadata.get("description", "").strip():
            errors.append(f"Invalid name or description: {role}")
        if not skill_file.resolve().is_relative_to(project_root):
            errors.append(f"Skill resolves outside the project: {role}")

    actual_roles = {entry.name for entry in skill_root.iterdir()} if skill_root.is_dir() else set()
    if actual_roles != expected_roles:
        errors.append("Unexpected or missing role folders in .agents/skills")

    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"Validated {len(expected_roles)} project-local skills in .agents/skills")
    return 0


if __name__ == "__main__":
    sys.exit(main())
