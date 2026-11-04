# Agent workflow enforcement

## Identity and scope

- Task / use case / roadmap phase: workflow hardening (no roadmap phase); follows the 2026-11-04 review of the agentic setup.
- Status: done for AWE-01, 02, 03, 05 and 07 (2026-11-04); AWE-04.1 and AWE-06.1 are deferred, see the Checkpoint.
- Coordinator / implementation owners: Product Manager (main session) authored the scripts, settings, agent frontmatter and documents named below.
- User outcome: a task cannot close with a review skipped, the review roles run on a stronger model, and the review effort is spent where the risk is.
- Authorized scope and non-goals: items 1 to 7 of the 2026-11-04 list. Non-goals: running `scripts/check-local.sh` from a hook; changing application code; committing.
- Dependencies and blocked work: item 4 (allowlist) needs a `/fewer-permission-prompts` run after this change is committed.

## Source context and decisions

- Verified implementation: `scripts/check-skills.py` (layout, explicit tools/model per subagent), `scripts/check-docs-current.py` (documentation gate), `.claude/settings.json` hooks, `docs/templates/feature-spec.md` verdict lines, 36 specs under `docs/specs/`.
- Decisions (2026-11-04, owner): (D1) all four review-side roles, including the architect, move to Opus at high effort; (D2) the Stop hook runs only the fast checks plus a reminder, because `check-local.sh` takes 1 h 48 m to 2 h 18 m and needs Docker; (D3) the verdict gate is strict for every `done` spec, so legacy specs were given a `## Review verdicts` section built only from verdicts they already record.
- Accepted risks (first review round, 2026-11-04): the Stop gate is one-shot (`stop_hook_active` exits 0 to prevent loops), so it is a strong reminder, not a lock; the verdict gate is self-attested and cannot judge a reason or apply a tier; `SubagentStop` also fires for read-only reviewers that cannot fix the failure, bounded by `stop_hook_active`.
- Recorded gaps found while normalising legacy specs: `hiring-saga-orchestration.md` has a Code Reviewer verdict only for slices 2 and 3, and no recorded Security scope; `analytics-erasure-completion-window.md` has no re-review of remediation R1 to R6. The verdict lines state those scopes. These are open review debt, not new PASSes.

## Behavior and contracts

- `scripts/check-spec-verdicts.py`: every `docs/specs/*.md` with a `- Status:` line beginning `done` needs `Code Reviewer verdict`, `Security Engineer verdict` and `Final independent QA verdict` lines ending in `PASS` or `not applicable - <reason of 10+ characters>`. Exit 0/1/2, final `RESULT:` line.
- `scripts/check-done.sh`: Stop runs skills + docs currency + verdicts; `--subagent` runs skills + verdicts; exit 2 returns failures to the agent; `stop_hook_active` exits 0.
- Review tiers in `AGENTS.md`; model table in `docs/agent-development.md`.

## Acceptance and evidence

| ID | Criterion | Evidence |
|---|---|---|
| AWE-01.1 | A `done` spec with a `pending`, FAIL, BLOCKED or missing verdict line fails; PASS and `not applicable - <reason>` pass | `scripts/tests/test_check_spec_verdicts.py` (7 tests) |
| AWE-01.2 | All 36 current specs pass the gate; the gate reads the first `- Status:` line, rejects `PASS-with-FAIL`, `PASSED` and conditional PASS | `python3 -I scripts/check-spec-verdicts.py`: `specs=36 done=7 problems=0`; 7 unit tests |
| AWE-01.3 | `check-local.sh` step 2 runs the gate | `scripts/check-local.sh --help` text and step command; full run not executed (needs Docker, about 2 h) |
| AWE-02.1 | The four review-side agents declare `model: opus` and `effort: high`; authors and writer unchanged | `.claude/agents/*.md`; `python3 -I scripts/check-skills.py` |
| AWE-03.1 | Stop and SubagentStop hooks call `check-done.sh`; a failing check exits 2; `stop_hook_active` exits 0 | manual runs recorded in the Checkpoint |
| AWE-03.2 | The hook never starts `check-local.sh` | `scripts/check-done.sh` source |
| AWE-05.1 | Session model is set only in `.claude/settings.json` | The owner approved removing `model` from `.claude/settings.local.json` on 2026-11-04 (all agents pin their own model); done |
| AWE-06.1 | The four modified skills pass layout checks and an independent scenario walkthrough | pending reviewer |
| AWE-07.1 | Review tiers are in `AGENTS.md`, the product-manager skill and the template; no tier lowers a Security rule | document review |
| AWE-04.1 | Allowlist additions are read-only and reviewed | pending `/fewer-permission-prompts` |

## Documentation impact

Updated: `AGENTS.md` (review tiers, step 7), `docs/agent-development.md` (model table, verdict gate and Stop hooks), `docs/engineering-quality.md` (operator script list), `docs/templates/feature-spec.md` (verdict format), `.claude/skills/product-manager/SKILL.md`. Seven done specs gained a `## Review verdicts` section.

## Checkpoint and review

- Completed criteria and changed files: AWE-01.1, 01.2, 01.3 (help and command only), 02.1, 03.1 and 07.1 are implemented; see Documentation impact for the files.
- Latest commands/results: unit tests 7/7; verdict check PASS; `check-done.sh` exits 0 on `stop_hook_active`.
- Blockers and next action: none for this change. Deferred, not done: AWE-04.1 (run `/fewer-permission-prompts` after this commit and have the result reviewed) and AWE-06.1 (the walkthrough belongs to the separate analytics Spark-session change, which is not in this commit). The Stop and SubagentStop hooks were exercised by script only, not in a live Claude Code session.
- Code Reviewer verdict and scope, when required (2026-11-04, harness files and the seven verdict blocks; the Spark-session skills and hunk excluded): PASS on the second round, after a FAIL on two overstated verdict blocks and four medium findings, all fixed. Open lows: `PASS or FAIL` wording would pass the regex; `is_done` placement.
- Security Engineer verdict and scope, when required (2026-11-04, settings, hooks, scripts, agent frontmatter, tier table; static, hooks not run in a live session): PASS on the second round, after a FAIL on the tier table (F1) and the self-attested gate (F2); F3 is an accepted risk.
- Final independent QA verdict (2026-11-04, final tree, `check-local.sh` not run): PASS for AWE-01.1, 01.2, 01.3 (help and command only), 02.1, 03.1, 03.2, 05.1 and 07.1; AWE-04.1 and AWE-06.1 were not scored.
