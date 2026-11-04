# Agent-driven development

## Current decisions and implementation

[README](../README.md) describes the Scala 3, Cats Effect, FS2, Sangria, http4s, and MongoDB platform. [Architecture](../ARCHITECTURE.md), [use cases](use-cases.md), and the [API reference](api.md) describe the active system; [schema evolution](schema-evolution.md) and the specifications under `specs/` hold its evidence.

The current build uses Scala 3.9 LTS, Java 17+, Cats Effect, Sangria/http4s, and MUnit. Unit tests are Docker-independent; the explicit `IntegrationTest` configuration contains live HTTP and disposable MongoDB checks. Inspect the live build before each task.

Application configuration follows the library defaults: `ConfigSource.default` delegates source loading and precedence to Typesafe Config. Put packaged defaults in `application.conf`; use the standard `config.file` or `config.resource` selectors for local/test overrides. Decode into typed PureConfig models, validate into typed settings/errors before runtime use, and keep the source/validation boundary explicit. Do not add project-owned filename constants or custom source-merging layers unless a new requirement establishes a different source boundary.

Analytics batch and worker runtime configuration uses the same `application.conf`/PureConfig approach as the main app: packaged defaults live in HOCON, and environment variables may supply HOCON substitutions. Production Scala runtime code must not read environment variables directly. Configuration values stay typed; independent validation failures accumulate with Cats `ValidatedNec` and convert to typed `Either`/effect errors at the owning boundary. Iron refinements protect validated settings and domain values. Keep transforms and domain validation pure, use `IO` and `Resource` for effects and resource ownership, and isolate exceptions required by Spark at the Spark adapter boundary, translating them into typed analytics errors. Existing test-only environment switches remain valid and are outside this production configuration rule.

For implementation and review standards covering pure/effect boundaries, ADTs and error channels, dependency use, query costs, and RBAC, follow [local engineering quality](engineering-quality.md). Use the configured library idioms and existing dependency versions before proposing custom infrastructure or another dependency. Query improvements must account for database work and application-side work, and authorization must be preserved through service and repository access.

Startup preserves hiring-owned MongoDB data by default. Set `mongo.reset-on-start = true` or
`MONGODB_RESET_ON_START=true` only for an explicitly authorized local reset.

## Entry point and roles

Start a task with:

```text
Use the product-manager skill to deliver the next scoped roadmap slice. Inspect the current
code and decisions, define acceptance criteria, delegate relevant specialists,
and finish with independent code/security reviews as applicable and final QA.
```

Or ask a specialist directly:

```text
Use the data-engineer skill to review the UC08 access pattern and index tradeoffs.
Use the big-data-engineer skill to plan UC11 replay and idempotency with local fixtures.
Use the qa-engineer subagent to validate the final changes against the task criteria.
```

Canonical Claude Code layout: role skills are real files at `.claude/skills/<role>/SKILL.md` (invoke with the Skill tool or `/<role>`). Delegated roles are subagent definitions at `.claude/agents/<role>.md`; reviewers and QA are read-only by tool list. Root rules live in `AGENTS.md`, imported by `CLAUDE.md`. Shared settings and the skill-layout hook are in `.claude/settings.json`; personal overrides belong in the ignored `.claude/settings.local.json`. Run `python3 -I scripts/check-skills.py` after changing skills or agents; the PostToolUse hook runs it as `check-skills.py --hook`, which does nothing unless the edited file is `AGENTS.md` or under `.claude/`, prints nothing on success, and exits 2 on a failure so Claude Code returns the problems to the agent (a plain exit 1 would only show in the transcript). A second PreToolUse hook runs `scripts/check-docs-current.py --hook` on every `git commit` and denies a commit that changes decision-bearing paths without canonical documentation (see Validation).

Model policy: the main session uses Sonnet at medium effort (`model`/`effortLevel` in `.claude/settings.json`, the only place the session model is set; a personal `settings.local.json` must not override it), including after plan mode; do not use `opusplan`. Each subagent sets its own model and effort in its frontmatter, and that frontmatter is the policy:

| Agents | Model, effort | Reason |
|---|---|---|
| `scala-developer`, `data-engineer`, `big-data-engineer` | Sonnet, medium | Author work is bounded by a spec and checked by the reviewers |
| `code-reviewer`, `security-engineer`, `qa-engineer`, `software-architect` | Opus, high | A missed defect in review, or a wrong structural decision, costs more than the extra tokens; these roles run once per task, not per edit |
| `technical-writer` | Haiku, medium | Documentation from verified facts |

Do not pass a per-call `model` override to the Agent tool; change the frontmatter instead so the policy stays in one place. `scripts/check-skills.py` requires every subagent to declare tools and a model.

Use these project skills and agents only; do not fall back to a global installation or substitute a same-named global role. Existing `.codex/config.toml` model/permission preferences remain local and separate.

Product Manager coordinates; Architect decides technical boundaries; Scala Developer implements application code; Data Engineer owns operational data; Big Data Engineer owns analytical pipelines; Code Reviewer and Security Engineer review their scopes; QA independently validates the final result. Direct specialist tasks still follow the root review gates.

## Task handoff

For substantial work, use the [spec-driven workflow](spec-driven-development.md), [feature spec template](templates/feature-spec.md), and [illustrative UC04 draft](examples/submit-application-spec.md). Keep acceptance criteria, handoffs, and evidence in one current spec. The short record below is sufficient for small tasks; it need not become a separate file.

Use this compact record in the conversation, or a task document for sustained multi-phase work:

```text
Task / use case / phase:
User outcome and non-goals:
Current implementation and decision sources:
Acceptance criteria, including forbidden/failure paths:
Domain: context, aggregate/invariant touched; glossary terms used or added; tagged examples:
Workload / SLO / freshness / cost ceiling (or explicit assumptions):
Owner / allowed write paths / input paths:
Dependencies and specialist handoffs:
Verification commands and expected observable results:
Evidence: commands, outcomes, benchmark environment and limitations:
Reviews: Code Reviewer / Security Engineer / final QA, with verdict and scope:
Status: draft | ready | in progress | in review | blocked | done
Remaining risks / next dependency:
```

Read only the spec named in the handoff; [docs/specs/README.md](specs/README.md) is the index for finding one.

Reviewer report (Code Reviewer, Security Engineer, QA). End every report with this block, so the coordinator can paste the lines into the spec and `scripts/check-spec-verdicts.py` accepts them:

```text
VERDICT: PASS | FAIL | BLOCKED
Scope: files, commits or slices reviewed, and what was not reviewed
Blockers: numbered, each with file:line evidence (none if PASS)
Non-blocking: numbered lows
Commands run: command, exit code, one-line result (a command not run is listed as not run)
Spec line: - <Code Reviewer | Security Engineer | Final independent QA> verdict (<date>, <scope>): PASS | not applicable - <tier reason>
```

A FAIL or BLOCKED reviewer never writes the spec line; the coordinator records PASS only after the re-review.

Only one agent writes a file at a time. Reviewers may inspect the same files concurrently after the author finishes. New fixes invalidate affected review evidence. If independent agents are unavailable, record review as pending; an author changing hats is not independent approval.

## Performance and cost decisions

Optimize for the smallest operating footprint that meets the agreed workload and reliability targets. Record baseline and changed p50/p95/p99, throughput, errors, resource usage, query plans, and test environment when relevant. The use-case SLOs are targets, not measured guarantees.

For infrastructure proposals, estimate compute hours, idle capacity, storage/retention, I/O and egress, embeddings/provider calls, and operational burden. State units, assumptions, and price source/date if pricing is used. Compare scheduled batch with streaming and local execution with managed infrastructure. Use bounded local synthetic data by default; million-row experiments and paid resources are separate scoped work. Do not sacrifice authorization, durability, or correctness for cost.

An optimization needs a baseline, a demonstrated bottleneck, a measured improvement without unacceptable regressions, and a revisit/rollback trigger. Kafka publication and Spark/Delta analytics already exist; additional distributed infrastructure, Databricks, caches, and denormalization require a relevant measured workload. Follow the [current milestone order](development-milestones.md): MongoDB, search/AI, and Kafka workflow refinements precede further Spark/Delta development.

## Validation

Use [local engineering quality](engineering-quality.md) for pure FP, debugging, review, and the `bash scripts/check-local.sh` command, which needs Docker and enforces the merged 85% line-coverage gate. It prints each step's duration as it finishes and a summary of all step durations at the end of a PASS, FAIL or BLOCKED run (the failing step is included, an interrupt prints none); read the summary before choosing what to speed up. There is no CI/CD pipeline; integration and [migration/contract checks](schema-evolution.md) must be run locally when required.

Run `sbt test` for the configured unit suite and `sbt 'IntegrationTest / test'` for live HTTP/disposable MongoDB checks when relevant. Missing Docker is blocked infrastructure, not a passing gate. Run formatting/linting when configured. `sbt run` starts the long-running hiring server; use its probe endpoints and the [startup instructions](../README.md), not process exit, to check health.

Documentation currency (root `AGENTS.md`) is a gate, not a courtesy. Every task that changes a decision lists its documents in the spec's Documentation impact section. The Technical Writer updates `README.md`, `ARCHITECTURE.md`, the owning `docs/*.md` and the `docs/wiki/` pages from verified behavior before the final reviews, stating the decision, its reason and why it matters. `python3 -I scripts/check-docs-current.py` checks the working tree plus every unpushed commit made since the gate was introduced (step 2 of `scripts/check-local.sh`), so a commit that bypassed the hooks (`--no-verify`, an unset hooks path) is still caught before it is pushed; `--staged` checks the next commit and `--range main..HEAD` checks each commit in a range. Commits are gated twice: the Claude Code hook covers agent commits, and the versioned git hook `.githooks/commit-msg` covers every commit in a clone where it is enabled once with `git config core.hooksPath .githooks`. Merge commits are not re-checked. The Claude Code hook sees only the command text: it reads `-m`, heredoc and `-F` messages, defers to the git hook for a message it cannot see (editor, `-C`, `--amend --no-edit`), and recognizes direct, path-qualified, global-option and quoted `sh -c` invocations of `git commit`; git aliases and wrapper scripts are caught only by the git hook. The git hook is versioned code that runs on every commit, so `scripts/guard-harness.py` treats `.githooks/` like `.claude/` and `scripts/`: an agent change to it needs human approval, and any diff to it deserves a human read before it is committed or pulled.

- Decision-bearing paths: production source and configuration (`src/main/`, `analytics/src/main/`), provisioning (`docker/`, `compose*.yaml`, `Dockerfile` and `analytics/Dockerfile`, `.dockerignore`), builds and tooling configuration (`build.sbt`, `project/`, `.scalafmt.conf`, `.sbtopts`, `.jvmopts`, `.mcp.json`, with their `analytics/` copies), operator scripts (`scripts/` except `scripts/tests/`) and the workflow (`AGENTS.md`, `CLAUDE.md`, `.claude/` except personal settings).
- Canonical documentation: `README.md`, `ARCHITECTURE.md` and `docs/**/*.md` except `docs/specs/` and `docs/templates/`. A spec records decision history; it does not replace the documents readers rely on.
- A commit that genuinely needs no canonical change carries `Docs-Impact: none - <reason>` or `Docs-Impact: spec-only - <reason>` (a reason of at least ten characters). Reviewers check that the reason is true.

Why it matters: documentation that lags a decision is how the next agent or operator re-introduces a fixed defect, misconfigures a limit, or keeps paying for capacity the system no longer needs.

Review verdicts are checked too, not only written down. `python3 -I scripts/check-spec-verdicts.py` requires every spec whose `Status:` begins with `done` to carry the three verdict lines of the [feature spec template](templates/feature-spec.md) as `PASS` or `not applicable - <reason>`; which reviews a change needs is the review tier table in `AGENTS.md`. The script runs in step 2 of `scripts/check-local.sh` and from the Claude Code `Stop` and `SubagentStop` hooks, which call `scripts/check-done.sh`. At `Stop` that runs the skills, documentation-currency and verdict checks (seconds, no Docker) and returns exit 2 with the failures so the agent sees them before it finishes; the harness then sets `stop_hook_active`, and the next stop is not re-checked (so a failing gate cannot loop), which makes this a one-time block and a strong reminder, not a lock; at `SubagentStop` it runs the skills and verdict checks only, because a subagent works inside an unfinished task. It exits 0 at once when the hook input has `stop_hook_active`, so a failing gate cannot trap a session in a loop. It does not run `scripts/check-local.sh`, which takes about two hours and needs Docker: on success it only reminds the agent that the local check and independent QA evidence are required when code changed. Why it matters: a verdict that exists only in prose is how a task closes with a review skipped. The check proves the lines exist and say PASS or give a reason, not that the review was good; reviewers challenge a weak reason.

For skill edits, run the project-owned `python3 scripts/check-skills.py`, check local references, and request independent scenario review. Frontmatter validation alone does not prove orchestration behavior or native discovery. No database migration or runtime deployment is needed for documentation-only changes.

## Agent test-run safety

Agents run and seed tests only through `scripts/run-local-tests.sh`; it targets the isolated `compose.test.yaml` stack (random loopback ports, tmpfs data, identity check before every write or cleanup). Tests create their own data in per-case databases inside that stack.

`.claude/settings.json` enforces this with permission rules plus the `scripts/guard-harness.py` PreToolUse hook:

- **Allowed without prompt:** wrapper verbs `start`, `status`, `test`, `analytics`, `interview`, `retention`, `recovery`; `sbt test|compile|scalafmtCheckAll`.
- **Human approval required (cleanup):** `scripts/run-local-tests.sh stop`, `rm`/`rmdir`, `sbt clean`, `find -delete`, any docker lifecycle or cleanup verb.
- **Denied:** `mongosh`/`mongo*`/Kafka CLI tools, `docker exec|run|cp`, compose files other than `compose.test.yaml`, any MongoDB URI that is not the manifest's isolated endpoint (including ports 27017/9092), `sudo`, and writes (Edit/Write, redirects, `cp`/`mv`/`rm` targets) outside the project folder.

Edits to `.claude/`, `scripts/`, `.git/`, build files and compose files need approval, because those run unprompted. The workflow-file list is defined once in `scripts/harness_paths.py` and imported by `guard-harness.py` (approval) and `check-docs-current.py` (documentation gate); `scripts/tests/test_harness_paths.py` fails if a workflow file is covered by one and not the other. Any compose file, not only a fixed set of names, now needs approval. Editing `guard-harness.py` itself needs care: an import that is added after its first use makes the guard fail closed and blocks every tool call until a human restores the file, so add the import first, then the code that uses it. The hook fails closed (exit 2) on malformed input, denies shell substitution in wrapper arguments, docker engine/context flags and `DOCKER_*`/`COMPOSE_*` overrides, and asks for nested shells (`bash -c`, `env`, `xargs`, `python -c`). The wrapper itself unsets Docker engine variables. The agent scratchpad under `/tmp/claude-<uid>` is the only write location outside the project.

Limits: the hook is a guardrail, not a sandbox, so it cannot see inside scripts or interpreters. The wrapper's own automatic release of per-run test namespaces on the isolated stack is not gated per action; this was explicitly accepted by the project owner, because it only touches the identity-verified test stack. sbt, Coursier and Docker keep their caches in the user's home directory and Docker's storage; those are outside the project folder and not controlled by this setup.
