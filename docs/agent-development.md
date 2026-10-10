# Agent-driven development

## Current decisions and implementation

[README](../README.md) describes the Scala 3, Cats Effect, FS2, Sangria, http4s, and MongoDB platform. [Architecture](../ARCHITECTURE.md), [use cases](use-cases.md), and the [current reset specification](specs/pre-mvp-contract-reset.md) describe the active system and its evidence.

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

Canonical Claude Code layout: role skills are real files at `.claude/skills/<role>/SKILL.md` (invoke with the Skill tool or `/<role>`). Delegated roles are subagent definitions at `.claude/agents/<role>.md`; reviewers and QA are read-only by tool list. Root rules live in `AGENTS.md`, imported by `CLAUDE.md`. Shared settings and the skill-layout hook are in `.claude/settings.json`; personal overrides belong in the ignored `.claude/settings.local.json`. Run `python3 -I scripts/check-skills.py` after changing skills or agents.

Model policy: the session and every subagent use Sonnet at medium effort (`model: sonnet`, `effort: medium` in each agent, `model`/`effortLevel` in `.claude/settings.json`), including after plan mode; do not use `opusplan`. Documentation-only work goes to the `technical-writer` agent on Haiku. Do not pass a per-call `model` override to the Agent tool.

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
Workload / SLO / freshness / cost ceiling (or explicit assumptions):
Owner / allowed write paths / input paths:
Dependencies and specialist handoffs:
Verification commands and expected observable results:
Evidence: commands, outcomes, benchmark environment and limitations:
Reviews: Code Reviewer / Security Engineer / final QA, with verdict and scope:
Status: draft | ready | in progress | in review | blocked | done
Remaining risks / next dependency:
```

Only one agent writes a file at a time. Reviewers may inspect the same files concurrently after the author finishes. New fixes invalidate affected review evidence. If independent agents are unavailable, record review as pending; an author changing hats is not independent approval.

## Performance and cost decisions

Optimize for the smallest operating footprint that meets the agreed workload and reliability targets. Record baseline and changed p50/p95/p99, throughput, errors, resource usage, query plans, and test environment when relevant. The use-case SLOs are targets, not measured guarantees.

For infrastructure proposals, estimate compute hours, idle capacity, storage/retention, I/O and egress, embeddings/provider calls, and operational burden. State units, assumptions, and price source/date if pricing is used. Compare scheduled batch with streaming and local execution with managed infrastructure. Use bounded local synthetic data by default; million-row experiments and paid resources are separate scoped work. Do not sacrifice authorization, durability, or correctness for cost.

An optimization needs a baseline, a demonstrated bottleneck, a measured improvement without unacceptable regressions, and a revisit/rollback trigger. Kafka publication and Spark/Delta analytics already exist; additional distributed infrastructure, Databricks, caches, and denormalization require a relevant measured workload. Follow the [current milestone order](development-milestones.md): MongoDB, search/AI, and Kafka workflow refinements precede further Spark/Delta development.

## Validation

Use [local engineering quality](engineering-quality.md) for pure FP, debugging, review, and the `bash scripts/check-local.sh` command, which needs Docker and enforces the merged 85% line-coverage gate. There is no CI/CD pipeline; integration and [migration/contract checks](schema-evolution.md) must be run locally when required.

Run `sbt test` for the configured unit suite and `sbt 'IntegrationTest / test'` for live HTTP/disposable MongoDB checks when relevant. Missing Docker is blocked infrastructure, not a passing gate. Run formatting/linting when configured. `sbt run` starts the long-running hiring server; use its probe endpoints and the [startup instructions](../README.md), not process exit, to check health.

For skill edits, run the project-owned `python3 scripts/check-skills.py`, check local references, and request independent scenario review. Frontmatter validation alone does not prove orchestration behavior or native discovery. No database migration or runtime deployment is needed for documentation-only changes.
