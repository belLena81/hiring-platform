# Hiring Runtime Safety

## Current state — 2026-10-05

The scoped runtime safety implementation is present. The later [repository composition](hiring-repository-composition.md) checkpoint records repair of integration consumers and passing integration tests, superseding the compilation blocker recorded below. Its evidence does not retroactively replace this task's BLOCKED QA verdict; current whole-checkout validation belongs to the [development milestones](../development-milestones.md).

The remaining task evidence is retained as a historical record unless explicitly identified as a current source fact. Roadmap sequencing follows [development milestones](../development-milestones.md).

## Identity and scope

- Status: in progress
- Outcome: make required diagnostics and typed report values explicit while preserving hiring, analytics reporting, and Mongo transaction behavior.
- Scope: the ten supplied review points under the accepted narrow-fix choice, root application only.
- Non-goals: GraphQL or stored Mongo shape changes, a second effect runtime, a repository-wide codec or `EitherT` migration, and changes to the separate analytics build.
- Existing unrelated edits: preserve concurrent work in the separate `analytics/` build.

## Original source baseline and decisions

- Root `build.sbt` already has MUnit Cats Effect and Testcontainers; `src/test` and `src/it` contain coverage for lifecycle policies, transaction retries, codecs, and reporting. The review's missing-build/missing-tests claim does not apply to this checkout.
- Main hiring records already use stored case classes and codecs. Mongo commands, filters, updates, validators, and Atlas pipelines remain BSON adapter documents. Stable report reservation/control records are the targeted typed-persistence slice; BSON names and values stay identical.
- `EmailAddress` and `PasswordHash` already are opaque values. Introduce validated report run ID and range fingerprint values at the report reservation port. Keep `IO` and use `RepositoryIO` internally where it simplifies sequential expected errors.
- Production diagnostics defaults are unsafe because omission suppresses failure reporting. `Diagnostics.noop` remains an explicit test fixture only. Logging sink errors remain best effort to avoid recursive diagnostics.
- Root Mongo and GraphQL contracts have one active shape before MVP. This task does not change those contracts or require a migration, backfill, or local data reset.

## Acceptance and evidence

| ID | Given / When / Then | Planned verification | Actual outcome |
|---|---|---|---|
| HRS-01 | A production constructor or factory requires diagnostics, and a test chooses a no-op explicitly | Source search, compile, wiring tests | Implemented; `sbt test` passed |
| HRS-02 | A token, embedding, wakeup, or migration operation throws; the existing public/worker fallback remains and safe cause metadata is emitted once | Focused service, provider, migration, and diagnostics tests | Implemented; focused exception tests and `sbt test` passed |
| HRS-03 | A report run ID or range fingerprint is blank; its boundary rejects it before Mongo. Valid values retain the same BSON fields and reservation behavior | Value tests, codec tests, replica-set report tests | Boundary and codec tests passed; replica-set integration unverified |
| HRS-04 | A receipt is created or a Mongo upsert result is inspected; UUID generation is effectful and Java null is mapped to `Option` | Focused repository tests | Implemented; compile and `sbt test` passed; Mongo integration unverified |
| HRS-05 | An outbox event list contains an invalid entry or failed insert; sequential insertion stops with the same typed error and no later insert occurs | Focused repository tests and Mongo transaction integration | Two focused stop-on-error tests passed; Mongo integration unverified |
| HRS-06 | A lifecycle or transaction-retry decision is evaluated across its finite states; invalid changes remain rejected and retry/rollback rules remain bounded | Existing plus focused exhaustive tests | Lifecycle and expanded retry matrix tests passed |
| HRS-07 | The domain identifier no longer imports `shared`, configuration does not import service search strategy, and docs match the `RepositoryIO` contract | Compile, import search, document review | Implemented; compile and `sbt test` passed |
| HRS-08 | Changed code preserves GraphQL and BSON contracts and passes project gates | `sbt test`, relevant `IntegrationTest`, formatting, `git diff --check`, independent Code Reviewer, Security Engineer, and QA | Unit suite 415/415 and diff check passed; integration and repository-wide formatter gates remain blocked; QA BLOCKED |

## Implementation handoff

- Diagnostics owner: production constructor/factory defaults, failure reporting, and affected root tests; exclude persistence and layering owners' paths.
- Persistence owner: analytics report repository and port, receipt UUID, upsert result, related tests; preserve stored names.
- Layering owner: search strategy and UUID parsing ownership, imports, architecture docs, related tests.
- Coordinator: sequential outbox insertion, codec exception adaptation, spec/evidence integration, final checks and reviews.

## Checkpoint and review

- Current state: scoped implementation complete. Final root `sbt test` passed 415/415 on Java 17. `git diff --check` passed. Changed Scala files were formatted with `scalafmtOnly`.
- `IntegrationTest / compile` failed on 86 errors in stale integration sources, including old `IO[Either]` assumptions, removed setup methods, mongo4cats API calls, and missing explicit diagnostics required by this fix. The integration suite cannot run until that source set is repaired. This gate is unverified; the blockers are not all attributable to an earlier baseline. `scalafmtCheckAll` still flags 13 untouched root files (2 production, 11 test); all changed Scala files pass formatting.
- Code Reviewer: PASS. Security Engineer: PASS. Final QA: BLOCKED on integration compilation and the repository-wide formatter gate; QA found no further scoped code defect.
