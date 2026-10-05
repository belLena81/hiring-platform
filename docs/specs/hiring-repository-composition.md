# Hiring repository and use-case composition

## Current state — 2026-10-05

The transformer composition refactor is implemented. The verification section below records the task run and its then-existing formatting limitation; those counts and limitations are historical, not a fresh whole-checkout gate. Current planning and validation state are tracked in [development milestones](../development-milestones.md).

The remaining task evidence is retained as a historical record unless explicitly identified as a current source fact. Roadmap sequencing follows [development milestones](../development-milestones.md).

Status: complete for the scoped composition refactor; existing repository-wide formatting limitation recorded. Owner: Product Manager / Scala implementation.

## Outcome and contracts

Keep expected failures in `RepositoryIO` / `UseCaseIO` throughout main-backend repository and service composition. Preserve existing error mappings, authorization, transactional identity, retry and cleanup behavior. `UserAuthenticator` changes its Scala return type to `RepositoryIO`; GraphQL, HTTP, Mongo, event and embedding wire contracts do not change.

At the original baseline, ports already exposed transformer aliases, while private Mongo helpers, transaction callbacks, account/job helpers and idempotency/telemetry still contained raw `IO[Either]` composition. Transaction `Resource` and retry handling must observe the underlying result once to select abort, commit or retry. Driver lifts, external adapters, startup migration failures and effect-only worker outcome handling remain deliberate boundaries.

## Acceptance

- AC-01: Internal repository/service composition has no unwrap-and-rewrap pipelines. Audit remaining effect `.value` crossings and their ownership.
- AC-02: Outbox inserts and subject leases remain sequential, ordered, and stop at the first failure; lease conflicts return the already claimed partial batch, empty subjects remain invalid.
- AC-03: Receipts, aggregate writes, outbox and embedding work retain their supplied transaction, rejection/replay semantics and best-effort cleanup. Typed failure and cancellation abort uncommitted work; retry bounds and commit-vs-operation decisions remain intact.
- AC-04: Account validation, token-before-persistence, dummy password checks, authorization normalization, existing wake behavior, telemetry fallback and parallel search results/error mappings remain unchanged.

## Scope and ownership

Root main backend and affected unit/integration tests only. Separate analytics build, existing dirty analytics specification, schemas, queries, indexes, dependencies, migrations and infrastructure are excluded. No commits or deployment.

Implementation is split into disjoint Mongo aggregate helpers, search/outbox helpers, use-case composition, and shared transaction/guard/receipt/analytics helpers. Root serializes sbt tasks. Independent final Code Reviewer, Security Engineer and QA verdicts are required.

## Evidence and next action

Planning inspection and independent data/code review found the bounded type changes feasible. Implementation is complete. Independent Code Reviewer and Security Engineer static verdicts: PASS, including the synchronized receipt-owned cancellation regression. Root unit and integration runtime verification passed.

AC-01 source audit: Mongo composition unwraps only in `MongoRepositorySupport.repositoryGuard` (unexpected driver failure adaptation) and `MongoTransactionRunner` (abort/commit/retry/session control). Migration helpers retain their separate startup `IO[Either[String, ...]]` contract. Service unwrapping remains at HTTP authentication, external adapter constructors, and effect-only search-session/embedding worker outcome handling.

AC-02 focused tests cover sorted sequential leases, first failure, empty input, ordered event insertion, parallel retrieval branches and branch error normalization. AC-03 includes receipt-owned cancellation asserting absence of job, receipt, embedding work and event writes. Existing account, JWT, job, telemetry and idempotency suites cover AC-04.

Targeted Scala formatting completed for implementation and focused tests. Initial compile found an unsupported transformer method, one nested result mapping, and unused imports; corrected before the final checks. Git history moved independently during work; review uses source and the composition baseline rather than working-diff membership. No task agent staged or committed files.

Required checks: focused suites, root `sbt test`, `sbt 'IntegrationTest / test'`, configured formatting checks and `git diff --check`. Record infrastructure blocks and pre-existing failures separately.

Production and unit-test compilation passed. Integration type errors in stale test consumers were corrected with test-only adapter changes: repository observation uses `.value` at the IO harness, migration batch tests call the existing migration owner, Atlas fixture cleanup uses the parameterless driver method, and server fixtures supply diagnostics. No assertions were weakened. Final unused integration-helper imports were removed under `-Werror`.

## Final verification

- Root `sbt test`: PASS, 425 tests, zero failures/errors.
- Root `sbt 'IntegrationTest / test'`: PASS on the corrected final source, 39 tests, zero failures/errors. Actual runtime evidence comprises 18 disposable MongoDB tests, 10 process tests and 9 HTTP/server tests. Two opt-in Compose guards completed with their switches disabled; they provide no live Compose/Kafka/analytics evidence.
- Transaction integration: 8 tests passed, including receipt-owned cancellation after aggregate/embedding/outbox writes. The cancelled fiber leaves no job, receipt, embedding work or outbox event. Outbox subject integration: 6 tests passed. Database probe integration: 4 tests passed.
- A pre-existing raw-BSON command wrapper in two probe fixtures caused an interrupted initial integration run. Replaced only those command inputs with raw Java `Document`, matching production; assertions are unchanged. Full integration rerun passed.
- Source counts against review baseline `3d02e60`: `IO[Either` occurrences 83 to 18; `RepositoryIO` type occurrences 160 to 233. The remaining raw effect signatures are deliberate API/provider boundaries, startup migration/configuration contracts and transformer constructors.
- Global formatting check: FAIL on 12 files byte-unchanged from the review baseline (11 existing unit-test files and `GraphQLFailureCatalog.scala`). No unrelated formatting rewrite is included. Scoped configured `scalafmtCheck`: PASS for 18 implementation files, 4 focused unit files and 8 integration files; `scalafmtSbtCheck`: PASS. Independent QA: PASS for the scoped composition refactor, with the existing global formatting limitation.
- `git diff --check`: PASS. Code Reviewer: PASS. Security Engineer: PASS, including the two probe fixture command corrections.

AC-01 is supported by the source boundary audit; AC-02 by lease/support unit tests and outbox integration; AC-03 by retry/receipt unit tests and transaction integration; AC-04 by account/JWT/job/telemetry/idempotency unit tests and parallel fusion tests. No wire schema, migration, index or dependency changes were made. Separate analytics acceptance and live provider/Compose evidence are outside this scope.
