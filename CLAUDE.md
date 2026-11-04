# Claude Code instructions

@AGENTS.md

## Claude Code usage

- Project rules live in `AGENTS.md` (imported above); this file adds only Claude Code mechanics. Do not duplicate rules here.
- Role skills: `.claude/skills/<role>/SKILL.md`. Invoke one with the Skill tool or `/<role>`. Product Manager is the main-session coordinator; load it first for substantial work.
- Delegated roles: `.claude/agents/<role>.md`. Reviewers (`code-reviewer`, `security-engineer`, `qa-engineer`) have no edit tools, and a frontmatter hook limits their Bash to collecting evidence; their skills fork, so they never run in the author's context. They must never be given an author's write scope.
- Path-scoped rules: `.claude/rules/*.md` load when work touches matching paths.
- Shared settings and permissions: `.claude/settings.json`. Personal overrides go in `.claude/settings.local.json` (gitignored).
- Verification entry points: `scripts/check-local.sh`; skill layout check: `python3 -I scripts/check-skills.py`; documentation currency: `python3 -I scripts/check-docs-current.py` (also a commit hook in `.claude/settings.json`).
- No commit, push, or deploy happens without an explicit request; those git commands require confirmation.
