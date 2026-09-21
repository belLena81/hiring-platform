# Agent-driven development

## Current decisions and implementation

[README](../README.md) describes the Scala 3, Cats Effect, FS2, Sangria, http4s, and MongoDB platform. [Architecture](../ARCHITECTURE.md), [use cases](use-cases.md), and the [current reset specification](specs/pre-mvp-contract-reset.md) describe the active system and its evidence.

The current build uses Scala 3.9 LTS, Java 17+, Cats Effect, Sangria/http4s, and MUnit. Unit tests are Docker-independent; the explicit `IntegrationTest` configuration contains live HTTP and disposable MongoDB checks. Inspect the live build before each task.

Application configuration follows the library defaults: `ConfigSource.default` delegates source loading and precedence to Typesafe Config. Use `application.conf` for packaged defaults and the standard `config.file` or `config.resource` selectors for local/test overrides; do not add project-owned filename constants or custom source-merging layers unless a new requirement establishes a different source boundary.

Startup preserves hiring-owned MongoDB data by default. Set `mongo.reset-on-start = true` or
`MONGODB_RESET_ON_START=true` only for an explicitly authorized local reset.

## Entry point and roles

Start a task with:

```text
Use $product-manager to deliver the next Foundation slice. Inspect the current
code and decisions, define acceptance criteria, delegate relevant specialists,
and finish with independent code/security reviews as applicable and final QA.
```

Or ask a specialist directly:

```text
Use $data-engineer to review the UC08 access pattern and index tradeoffs.
Use $big-data-engineer to plan UC11 replay and idempotency with local fixtures.
Use $qa-engineer to validate the final changes against the task criteria.
```

Role skills are stored once as real files at this project's `.agents/skills/<role>/SKILL.md`, where Codex discovers them. This uses the documented [Codex skill mechanism](https://developers.openai.com/codex/skills/). Start a fresh session and check `/skills` after installation. If discovery is unavailable, ask the agent to read the exact `.agents/skills/<role>/SKILL.md` path. These are role instructions used with available delegation tools, not separately provisioned workers or a background scheduler.

Root rules and the canonical `.agents/skills/*/SKILL.md` files define the agent workflow. Use these project skills only; do not fall back to a global installation. There are no duplicate skill stores or links to a home-directory installation. Existing `.codex/config.toml` model/permission preferences remain separate.

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

An optimization needs a baseline, a demonstrated bottleneck, a measured improvement without unacceptable regressions, and a revisit/rollback trigger. Kafka, Spark, Databricks, caches, and denormalization are introduced only when the relevant roadmap workload warrants them.

## Validation

Use [local engineering quality](engineering-quality.md) for pure FP, debugging, review, and the `bash scripts/check-local.sh` command. There is no CI/CD pipeline; integration and [migration/contract checks](schema-evolution.md) must be run locally when required.

Run `sbt test` for the configured unit suite and `sbt 'IntegrationTest / test'` for live HTTP/disposable MongoDB checks when relevant. Missing Docker is blocked infrastructure, not a passing gate. Run formatting/linting when configured. `sbt run` now starts the long-running Foundation server; use its probe endpoints and the [runbook](foundation.md), not process exit, to check health.

For skill edits, run the project-owned `python3 scripts/check-skills.py`, check local references, and request independent scenario review. Frontmatter validation alone does not prove orchestration behavior or native discovery. No database migration or runtime deployment is needed for documentation-only changes.
