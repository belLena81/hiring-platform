# Feature specification template

Copy to `docs/specs/<task-slug>.md` for substantial work. This template is not an active task. Use repository-relative paths in the completed spec; remove fields that do not apply and explain any omitted material gate. See [workflow](../spec-driven-development.md).

## Identity and scope

- Task / use case / roadmap phase:
- Status: draft
- Coordinator / implementation owners:
- User outcome:
- Authorized scope and non-goals:
- Dependencies and blocked work:

## Source context and decisions

- Verified implementation: paths, symbols, configured stack, relevant tests.
- Target requirements: canonical document sections and current user request.
- Assumptions: explicit, with impact and validation method.
- Open questions: identify what each blocks; proceed with independent work.
- Decisions/change notes: choice, basis, affected acceptance criteria.

## Behavior and contracts

Describe actors and ownership, input/output/error semantics, validation, state transitions, and relevant side effects. Specify atomicity, idempotency, migrations/recovery, and compatibility where applicable. Define observable guarantees before choosing design patterns.

Separate pure domain decisions from effectful orchestration and external adapters. For data/API/event changes, apply [schema evolution](../schema-evolution.md): record supported old/new consumers and data shapes, version/deprecation strategy, migration/backfill/concurrent-write ordering, recovery, and local compatibility tests. Mark not applicable with a reason when no contract changes.

## Acceptance and evidence

Keep criterion IDs stable. Add separate rows for relevant forbidden, invalid, concurrent, and failure/recovery cases. Fill evidence only after observing it.

| ID | Given / When / Then | Implementation and test paths | Verification command or method | Actual outcome |
|---|---|---|---|---|
| AC-01 | Define a concrete observable result | Not implemented | Planned check | Not run |

## Performance and cost, when applicable

Record workload size/seed, concurrency, environment, latency/throughput or freshness target, baseline, resource/cost ceiling or explicit unknowns, and measurement method. Compare a simpler approach. Do not treat targets or estimates as benchmark results.

## Implementation handoff

- Ordered slices and relevant AC IDs:
- Agent owners, allowed write paths, and dependency boundaries:
- API/schema/docs updates and integration prerequisites:
- Current risks and recovery approach:

## Checkpoint and review

- Completed criteria and changed files:
- Latest commands/results and their scope:
- Blockers and next concrete action:
- Code Reviewer verdict and scope, when required: pending
- Security Engineer verdict and scope, when required: pending
- Final independent QA verdict: pending

Mark a gate not applicable with a reason rather than claiming it passed. Update this section on meaningful handoffs and reconcile it with current source on resume. No criterion is complete solely because this document describes it.
