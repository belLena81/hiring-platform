# Agent workflow enforcement

## Identity and scope

- Task / use case / roadmap phase: workflow hardening (no roadmap phase); follows the 2026-11-04 review of the agentic setup.
- Status: done for AWE-01, 02, 03, 05, 07 and 08 (2026-11-04; round 1 committed as `1b83d29`, round 2 reviewed PASS by the three roles in the Checkpoint); AWE-04.1 and AWE-06.1 are deferred, see the Checkpoint.
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
| AWE-01.2 | All 36 specs pass the gate (the gate also counts the index file, hence `specs=37`); the gate reads the first `- Status:` line, rejects `PASS-with-FAIL`, `PASSED` and conditional PASS | `python3 -I scripts/check-spec-verdicts.py`: `specs=37 done=12 problems=0`; 10 unit tests in `test_check_spec_verdicts.py` (one added at close: a scope with colons inside the parentheses is read to its verdict, which the three reviewers did not see) |
| AWE-01.3 | `check-local.sh` step 2 runs the gate | `scripts/check-local.sh --help` text and step command; full run not executed (needs Docker, about 2 h) |
| AWE-02.1 | The four review-side agents declare `model: opus` and `effort: high`; authors and writer unchanged | `.claude/agents/*.md`; `python3 -I scripts/check-skills.py` |
| AWE-03.1 | Stop and SubagentStop hooks call `check-done.sh`; a failing check exits 2; `stop_hook_active` exits 0 | manual runs recorded in the Checkpoint |
| AWE-03.2 | The hook never starts `check-local.sh` | `scripts/check-done.sh` source |
| AWE-05.1 | Session model is set only in `.claude/settings.json` | The owner approved removing `model` from `.claude/settings.local.json` on 2026-11-04 (all agents pin their own model); done |
| AWE-06.1 | The four modified skills pass layout checks and an independent scenario walkthrough | pending reviewer |
| AWE-07.1 | Review tiers are in `AGENTS.md`, the product-manager skill and the template; no tier lowers a Security rule | document review |
| AWE-04.1 | Allowlist additions are read-only and reviewed | pending `/fewer-permission-prompts` |

## Documentation impact

Round 2 also updated: `docs/mongodb-design.md` (low-selectivity leading key, no in-memory sort, no counter reuse), `docs/engineering-quality.md` (operator contract exemption for `check-skills.py --hook`), `docs/specs/README.md` (new index), `.claude/skills/{data-engineer,security-engineer,software-architect}/SKILL.md`, and the three reviewer agent files.

Round 1 updated: `AGENTS.md` (review tiers, step 7), `docs/agent-development.md` (model table, verdict gate and Stop hooks), `docs/engineering-quality.md` (operator script list), `docs/templates/feature-spec.md` (verdict format), `.claude/skills/product-manager/SKILL.md`. Seven done specs gained a `## Review verdicts` section.

## Round 2: robustness and efficiency (2026-11-04, owner request "do all one by one")

Source: two read-only audits (test setup; harness scripts and instructions). Reviewed in three rounds; see the review history after the table.

| ID | Change | Evidence |
|---|---|---|
| AWE-08.1 | `check-skills.py --hook` runs only for edits of `AGENTS.md` or `.claude/`, is silent on success and exits 2 on failure, so a broken skill reaches the agent | `scripts/tests/test_check_skills_hook.py`; scratch copy with a broken agent file returned exit 2 with the message |
| AWE-08.2 | `check-local.sh` prints each step's duration and a summary (the failing step included), so the slow phase can be located | `bash -n`; help text now describes it; simulated under `set -u` by QA; not run end to end (needs Docker, ~2 h) |
| AWE-08.3 | `scripts/harness_paths.py` is the one list of workflow files, imported by `guard-harness.py` and `check-docs-current.py`; the guard now protects any `compose*.yaml` and `Dockerfile` | `scripts/tests/test_harness_paths.py`; guard run on `compose.observability.yaml` returned "ask" |
| AWE-08.4 | Instruction trim: `data-engineer` skill checklist links to the canonical standard instead of restating it (979 to about 675 words after the review fixes), the naming rule kept in `AGENTS.md` with a one-line pointer in the data-engineer, architect and security skills, two `AGENTS.md` bullets merged, tier paragraph shortened | `wc -w`; `check-skills.py` |
| AWE-08.5 | `docs/specs/README.md` indexes the specs by status; handoff says to read only the named spec | file review |
| AWE-08.6 | One reviewer report format in `docs/agent-development.md`, referenced by the three reviewer agents | file review |
| AWE-08.7 | Owner decision 2026-11-04: `.mcp.json`, `.scalafmt.conf`, `.sbtopts`, `.jvmopts`, `analytics/Dockerfile`, `.dockerignore` (and the `analytics/` copies) join the shared list, so the guard asks and the docs gate requires documentation | `scripts/tests/test_harness_paths.py` iterates the whole shared list (49 script tests pass) |
| AWE-08.8 | Owner decision 2026-11-04: the eight specs with a prose status now start with `done` or `in progress`; `done` only where all three verdicts are recorded (three got a `## Review verdicts` block built from their own records; `hiring-end-to-end-acceptance` already had its lines), `in progress` where a gate is open | `python3 -I scripts/check-spec-verdicts.py`: `specs=37 done=12 problems=0`; `docs/specs/README.md` |
| AWE-08.9 | Review round 1 of round 2 (Code, Security, QA all FAIL) was fixed: the self-attestation caveat and reviewer cross-check restored in `AGENTS.md`; the removed `data-engineer` rules moved into `docs/mongodb-design.md`; `check-skills.py --hook` exemption documented, relative paths and links handled, any failure exits 2; unsourced duration claim removed; unedited `<placeholder>` reasons rejected; failing step timed | tests above; file review |

Not done, with reasons: dropping `-Dcats.effect.trackFiberContext=true` (the telemetry specs depend on it); merging the two sbt calls in `recovery` mode and the two scalafmt runs (they are different builds); sharing the Mongo client per integration suite, splitting serial integration groups, grouping analytics unit suites and shared index setup (no baseline yet; AGENTS.md requires a measured bottleneck first, and these need the Docker stack to verify). Next action: run `scripts/check-local.sh` once and read the step durations.

Owner decisions of 2026-11-04 are recorded as AWE-08.7 and AWE-08.8; no owner decision is open for this round.

Accepted from the round 2 reviews, not changed: the docs-gate PreToolUse hook fails open if `harness_paths.py` is missing (the git `commit-msg` hook is the backstop; commit `harness_paths.py` with both importers); a gitignored `__pycache__` copy of `harness_paths` is loaded by the guard (tampering needs code execution the guard cannot see, a documented limit); `docker/` is decision-bearing for documentation but not guard-protected; the PostToolUse matcher omits `MultiEdit|NotebookEdit` (the Stop hook is the backstop); the duration summary is not printed after an interrupt.

Round 2 review history: the first review of round 2 failed in all three roles (the shared blocker was the restored self-attestation caveat, plus spec text and a data-engineer rule move); the second failed on spec text only; the third passed in all three roles after the fixes above.

## Checkpoint and review

- Completed criteria and changed files: AWE-01.1, 01.2, 01.3 (help and command only), 02.1, 03.1, 03.2, 05.1, 07.1 (round 1, commit `1b83d29`) and AWE-08.1 to 08.9 (round 2); see Documentation impact for the files.
- Latest commands/results: all 49 script tests pass; `check-skills.py`, `check-spec-verdicts.py` (`specs=37 done=12`), `check-docs-current.py` and `check-done.sh` PASS; `bash -n scripts/check-local.sh` passes.
- Blockers and next action: none for this change. Deferred, not done: AWE-04.1 (the `/fewer-permission-prompts` run found one low-impact rule, the exact `unittest discover` command, added to `.claude/settings.json`; the guard still asks for `python3` commands, so it is inert, and the Security Engineer reviewed it) and AWE-06.1 (the walkthrough belongs to the separate analytics Spark-session change, which is not in this commit). The Stop and SubagentStop hooks were exercised by script only, not in a live Claude Code session; `scripts/check-local.sh` was not run end to end.
- Code Reviewer verdict (2026-11-04, round 2 AWE-08.1 to 08.9: harness scripts and tests, settings, reviewer agents, data-engineer, security-engineer and software-architect skills, AGENTS.md, agent-development.md, engineering-quality.md operator contract, mongodb-design.md modeling rules, specs index and the three new verdict blocks; analytics/, the four Spark-session skills and the Spark hunk excluded; check-local.sh not run): PASS. Open lows: the full report template line pasted unedited would pass the gate; an sbt-lookup failure is timed from the start of the Java step.
- Security Engineer verdict (2026-11-04, round 2 harness: harness_paths.py, guard-harness.py, check-docs-current.py, check-skills.py --hook including the symlink-aware touches_role_definitions, check-spec-verdicts.py, check-local.sh timing, settings.json, reviewer agents, AGENTS.md tier text; static plus script runs, hooks not run in a live session): PASS. Accepted risks are listed above.
- Final independent QA verdict (2026-11-04, round 2 AWE-08.1 to 08.9 on the final tree plus round-1 regression; check-local.sh not run, hooks exercised by script only): PASS; AWE-04.1 and AWE-06.1 were not scored.
