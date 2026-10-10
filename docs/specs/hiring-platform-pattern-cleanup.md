# Hiring platform pattern cleanup

## Identity and scope

- Task: third maintainability pass, coming out of the 2026-10-10 design-pattern review. Three read-only reviews were run: domain/service/API, persistence/infrastructure/config, and the analytics build. This pass follows [error propagation and library delegation](hiring-platform-error-propagation-and-library-delegation.md) and [quality consolidation](hiring-platform-quality-consolidation.md).
- Status: in progress (2026-10-10). Baseline is HEAD `b12632f`.
- Coordinator: Product Manager.
- Implementation owners:
  - Scala Developer: track D (domain, services, GraphQL) and the cross-boundary phase X.
  - Data Engineer: track P (persistence, Kafka, config).
  - Big Data Engineer: track A (analytics build).
- Reviews: Code Reviewer, Security Engineer, then QA on the final integrated state.
- Outcome: code that is easier to maintain, with three properties:
  - Each piece of logic exists once.
  - Closed states are ADTs rather than Booleans or strings.
  - A pattern stays only where it solves a concrete problem. Examples: a port that has a test double or inverts a dependency, StateT `LifecycleProgram`, `UseCaseIO`/`RepositoryIO`, `GraphQLFailureCatalog`, `Idempotent`, `MongoFilter`/`MongoUpdate`, and the cats-retry runners.
- Non-goals:
  - No change to stored MongoDB document shapes, Kafka event shapes or Delta tables.
  - No change to authorization behaviour.
  - No new dependencies.
  - No GraphQL change apart from PC-02c.
  - Items marked "Skipped with reasons" in the previous spec stay skipped: `SparkBlockingExecution` on `interruptible`, the `foreachBatch`/observer restructure, pureconfig default arguments, the `FailureReason` split, and the shared contracts module (D1).

## User decisions (2026-10-10)

| # | Point | Decision |
|---|---|---|
| 1 | Scope | Full batch in both builds, delivered as slices |
| 2 | Test-only port methods | Remove |
| 3 | Unused analytics report write path in the root build (`reserve`/`publish`, `AnalyticsReportSnapshotPublisher`) | Delete |
| 4 | Search-evaluation harness in `src/main` | Move out of production sources |
| 5 | `LogOutcome` enum | Include. This fixes `OWNERSHIP_LOST`/`RECOVERY_DEFERRED` being logged as `[FILTERED]`. |
| 6 | Interview interval error | Return `INVALID_INTERVIEW_INTERVAL` instead of `INVALID_SEARCH_FILTER`. This is a pre-MVP direct API change: update SDL docs, fixtures and `docs/api.md`. |
| 7 | Typed reason codes | Enums replace free-text step/reason/failure codes and analytics status strings. Stored text stays byte-identical. |

These are the only behaviour changes, and each one needs a test: decisions 5 and 6, and decision 3 because it removes code paths. Everything else is behaviour-preserving.

## Source facts

- Root build: services use `UseCaseIO`, repositories use `RepositoryIO`, and `MongoRepositorySupport.repositoryGuard` translates errors. The integration configuration `it` extends `Test`.
- `MongoInterviewWorkflowRepository.scala` is 2,677 lines and implements three ports.
- `service/Diagnostics.scala:147`: the `LogField.Outcome` allow-list does not contain `OWNERSHIP_LOST` or `RECOVERY_DEFERRED`. Both are emitted at `service/events/SearchSessionHandoff.scala:73,77`. Confirmed by grep.
- `service/search/SearchEvaluation*` and `infrastructure/search/SearchEvaluation*` are referenced only from `src/test` and `src/it`. `docs/search-evaluation.md` runs them through `Test / runMain` and `IntegrationTest / runMain`.
- Analytics build: uses `F[_]` with `Async`, the typed `AnalyticsError`, and an ArchUnit layering spec.

## Acceptance and evidence

Gates that apply to every item:

- Production lines (`src/main`, `analytics/src/main`) are net negative for each track.
- Merged unit + integration JaCoCo line coverage of every touched production file does not fall below the HEAD baseline.
- The merged coverage gate of 85% still passes.
- When a helper replaces copies, the tests of those copies collapse into tests of the helper.

### Track D — domain, services, GraphQL (phase 1)

Write scope: `src/main/.../{domain,service,api,shared}/**`, except port signatures used by `repository/**` or `runtime/**`. Also tests under the same packages, GraphQL SDL/fixtures and `docs/api.md`.

| ID | Criterion | Verification | Outcome |
|---|---|---|---|
| PC-01a | Add `LogOutcome` enum. `SearchSessionHandoff` and the `LogField.Outcome` allow-list both use it, so every emitted value passes `validPublic`. | Unit test covering every `LogOutcome` value | Not run |
| PC-02c | A bad interview interval maps to `InvalidInterviewInterval`, which goes on the wire as `INVALID_INTERVIEW_INTERVAL`. | Service and GraphQL test; fixtures; `docs/api.md` | Not run |
| PC-03 | Scheduling state machine:<br>• it is built on a `move` helper;<br>• identical case pairs and the `RetryExhausted` copies are merged;<br>• the revision guard, notify-key format, millisecond truncation and open-proposal lookup each exist once;<br>• `InterviewMessagePolicy` has no duplicated decisions. | Existing transition and policy specs unchanged and green | Not run |
| PC-04 | `InterviewWorkflowWorker`:<br>• uses `observe`;<br>• has one admin-command loader;<br>• has one advance-result handler;<br>• has one publication-retry block;<br>• has one publication-step enum;<br>• uses `recipient`.<br>Then split it into publication, receive and provider-executor files. | Worker specs green | Not run |
| PC-05 | One copy each of:<br>• the recruiter-manages-application rule (`ActorAuthorization.canManage`);<br>• `AuthorizedJobAccess.manage`;<br>• the embedding staleness check, query-length check and hit-validation block in semantic search;<br>• `wakeAfterCommit`;<br>• the `verify` branch;<br>• the stable-id helper;<br>• the interview visibility lookup.<br>In addition: the `act` callback returns a plain event, and `validateUpdatedJob` no longer round-trips. | Service specs and forbidden-access specs green | Not run |
| PC-06a | Ceremony removed inside the scope:<br>• forwarding `live`/`apply` companions, where `runtime/**` callers are untouched or the change is deferred to phase X;<br>• one-method resolver objects;<br>• `ResolverSupport.rateLimited` and the `toGraphQLFailure` pass-through;<br>• `UseCaseIO as UseCase` aliases.<br>`DurableRetrySettings` becomes a case class, and the always-true `workerReady` is removed. | `sbt test` | Not run |
| PC-07a | The eight GraphQL enum types are generated by a SCREAMING_SNAKE variant of `HiringGraphQLInputs.enumType`. Analytics and coverage types move into their own object. The SDL stays byte-identical. | SDL contract spec | Not run |
| PC-08 | The search-evaluation harness moves to `src/test/scala` under the same packages. Fallback: `test-support`. `IntegrationTest/compile` and both `runMain` entry points still resolve. | `sbt Test/compile IntegrationTest/compile` | Not run |

### Track P — persistence, Kafka, config (phase 1)

Write scope: `src/main/.../{repository,infrastructure,config}/**`, `Main.scala`, `runtime/**`, and their tests and it specs.

| ID | Criterion | Verification | Outcome |
|---|---|---|---|
| PC-10 | `MongoAnalyticsReportRepository.reserve`/`publish` are deleted, together with the it specs that only exercise them. In `latest`:<br>• time comes from `Clock`;<br>• a malformed control document becomes `InvalidStoredData` instead of being dropped by `.toOption`.<br>The port itself is removed in phase X. | Root it specs | Not run |
| PC-11 | The interview repository is split into a command queue, a lifecycle store, a calendar ledger, a notification ledger, a codec, the core and shared fences. `encodeCommand` sits next to `decodeCommandShape`. | Interview it specs; codec specs show identical shapes | Not run |
| PC-12 | One helper each for:<br>• emitted-command insert;<br>• revision update;<br>• `providerFence` increment;<br>• job `Version` increment;<br>• application status write.<br>Also: `MongoStoredDocumentDecoding.repository` is used for application decoding, `EitherT.recoverWith` replaces nested unwrapping, `decodeReceipt` is total, and `.pure`/`EitherT.leftT` replace `fromEither(Right/Left)`. | Interview it specs | Not run |
| PC-13 | Shared helpers replace private copies:<br>• `MongoLeaseQueue` takes `LeaseFields`, so the interview queue keeps its stored field names;<br>• readers come from `MongoDocumentFields`;<br>• field-name constants and the interview collection list each exist once;<br>• `Clock[IO]`/`UUIDGen[IO]` are injected;<br>• caller input errors are not reported as `InvalidStoredData`;<br>• `applyRepair` returns an outcome ADT. | Lease and interview it specs; stored shapes identical | Not run |
| PC-14 | One `scanById` batch helper is used by the migration and audit loops. `collectWithin` uses `MongoBoundedResults.collect`. Step 010 moves onto `MongoMigrationStep` with typed errors, keeping the ledger row shape. | Migration it specs | Not run |
| PC-15 | Interview Kafka:<br>• the receiver returns `RepositoryIO[Unit]`, not `IO[Boolean]`;<br>• the consumer runs under `BackgroundWorker.resource`;<br>• the fencer and retention use `KafkaClientSettings`, `IO.fromCompletableFuture` and a `Resource`-scoped consumer;<br>• there is one `committedConsumer` setting;<br>• there is one 65536 limit;<br>• shared stream helpers live in one object;<br>• the `initializedProducer` forwarder is removed. | Kafka specs and the shared contract fixture | Not run |
| PC-16 | Config:<br>• the interview runtime config has no raw mirror;<br>• there is one `RateLimitConfig` and one reader;<br>• `RawJwtAuthConfig` uses `derives`;<br>• the test-only `parse`/`fromParsed` are removed;<br>• config is loaded once in `Main`;<br>• there is one `"disabled"` check.<br>Redacting `toString` is preserved. | `AppConfigSpec` | Not run |

### Track A — analytics build (phase 1)

Write scope: `analytics/src/**`.

| ID | Criterion | Verification | Outcome |
|---|---|---|---|
| PC-20 | `MongoAnalyticsReportPublisher`:<br>• has one `readControl`, one reservation lookup, one snapshot upsert and one mark-published update;<br>• uses `matchedUpdate`;<br>• shares filters with the erasure queue;<br>• has an `enum ReportState` (stored strings unchanged) and one collection-name constant. | Publisher it spec | Not run |
| PC-21 | Spark:<br>• Gold is rebuilt and extracted in one place;<br>• analysis and classification share their reads;<br>• the redundant `.distinct()` is removed;<br>• `markerFrame`, the quality predicate, the expiry column, skill normalisation and `validateSilverSchema` each exist once;<br>• the two coordinate predicates are merged only if their intent is identical. | Analytics unit and it specs | Not run |
| PC-22 | Errors:<br>• `recover` replaces catch-all `handleErrorWith`;<br>• there is one duplicate-key extractor;<br>• `lakehouseId` and lakehouse error translation each exist once (`LakehouseOperation`);<br>• `MongoSession.resource`, always-`Right` decoders, `runProgram` and the `prepared` alias are removed;<br>• the erasure queue translates errors only once. | Analytics `sbt test` | Not run |
| PC-23 | ADTs:<br>• `BatchAttempt` replaces `isRecoveryAttempt`/`forceDecisionRevision`;<br>• `StreamingJournalState` covers exactly the real states;<br>• `RepairRequest` holds enums;<br>• admission status uses `EventTimeAdmission`;<br>• the marker fingerprint lives in the domain;<br>• corrupt data gets a data-integrity `AnalyticsError` while erasure retry categories stay unchanged;<br>• there is one error-to-text mapping. | `ErasureFailurePolicy` category test; journal and coordinator specs | Not run |
| PC-24 | Journal:<br>• one instant reader;<br>• one decision reader;<br>• one update-and-verify helper.<br>Worker:<br>• `requireApplied`;<br>• the barrier is validated once;<br>• unused port methods are removed. | Analytics specs | Not run |
| PC-25 | Composition:<br>• the streaming per-batch callback lives in `app/StreamingBatchCallback`;<br>• the Spark/Mongo bootstrap lives in `adapter/spark/SparkRuntime`;<br>• stages are wired once, with one `timed` helper;<br>• the checkpoint/lineage control files move out of `SparkHiringAnalyticsStream`;<br>• `HiringGoldTransforms` has its own file;<br>• the key-retirement pure checks are in `service/keyretirement` and the Mongo scan is in `adapter/mongo`. | ArchUnit layering spec; analytics it specs | Not run |

### Phase X — cross-boundary changes (after phase 1 integration)

Owner: Scala Developer, working on the integrated tree.

| ID | Criterion | Verification | Outcome |
|---|---|---|---|
| PC-01b | `Diagnostics` no longer imports `config` and no longer hard-codes Mongo class names. Startup injects the allow-lists. | Diagnostics specs | Not run |
| PC-02 | Typed values:<br>• `SearchSession.searchKind: SearchKind`, with the wire string produced only in the codec;<br>• `jobEvent` takes `JobKind`;<br>• enums replace `RetryExhausted(step)`, `RequireRepair(reason)` and `failureCode`, with persisted text unchanged;<br>• `InterviewCommandResult` and `InterviewResult` merge. | Codec specs show identical stored values | Not run |
| PC-06b | Test-only port methods and their Mongo implementations are removed: `createForOpenJob`, the non-event `updateStatus`, `jobEligibility`/`candidateEligibility`, `findMany` ×2, `canViewUserEmail`, overridden trait defaults, `DurableEmbeddingWorkPublisher.offer`/`EmbeddingWork`/`keyFor`, and the `AnalyticsReportSnapshotPublisher` port. Forwarding companions used by `runtime/**` are removed, along with the `service/port` re-export. | `sbt test`, it specs | Not run |
| PC-07b | `service/port/HiringRepositories.scala` is split into one file per port group. | compile | Not run |
| PC-13b | The interview domain rules (`admitted`, initiator check, `safeFailureCode`) move from the adapter into domain policy. | Unit tests | Not run |
| PC-30 | Both builds pass `scripts/check-local.sh`. Production net is negative. Per-file coverage is at or above the baseline. Docs are updated: `ARCHITECTURE.md`, `docs/api.md`, `docs/search-evaluation.md`, `docs/logging.md`. | Local check plus coverage comparison | Not run |

## Implementation handoff

- Phase 1 tracks D, P and A run in parallel, each in its own detached git worktree with exclusive write scope. There are no branches and no commits.
- Each owner reports:
  - files changed;
  - `git diff --numstat` production totals;
  - the commands run, with their results;
  - skipped items with reasons;
  - any cross-boundary change they needed and deferred to phase X.
- The coordinator integrates D, then P, then A into the main tree, resolves conflicts, and then runs phase X.
- An item that would make production code grow is skipped and reported.

## Checkpoint and review

- 2026-10-10: spec ready. A HEAD baseline is running (both builds, merged unit + integration JaCoCo) in a detached worktree. Next step: launch tracks D, P and A.
- 2026-10-10 baseline at `b12632f`:
  - root: unit 1032, it 288 passed / 10 skipped (opt-in Atlas, scaling and compose-readback gates), merged lines 90.66%;
  - analytics: unit 449, it 42 passed / 6 ignored, merged lines 87.92%.
- 2026-10-10 integrated (D → P → X → A, author-reported):
  - root: unit 1040/1040, it 288 passed / 10 skipped / 0 failed;
  - analytics: unit 449/449, it 42 / 6 ignored / 0 failed.
- Production net:
  - root `src/main`: −1646 (of which 931 lines are the search-evaluation harness moved to `src/test`);
  - `analytics/src/main`: −20 (moves plus ADTs).
- Skipped, with reasons:
  - Injecting `HiringReadService` into `ApplicationService` (grows code).
  - `workerReady` (test seam).
  - `RawJwtAuthConfig` `derives` (kebab-casing breaks `hs256Secret`).
  - `RawInterviewRuntimeConfig` mirror (different shape).
  - About 60 typed `fromEither(Left/Right)` sites (invariance needs ascriptions).
  - Analytics "one error-to-text mapping" (three differing purposes).
  - Journal outcome-string enum.
  - `StreamingBatchCallback`, control-file move and key-retirement moves (boilerplate or test coupling).
- Partial:
  - PC-01b: the project class names and the config import are gone, but `Diagnostics` still allow-lists `com.mongodb.*` driver exception names.
  - PC-02: event constructors still return `Either` because JSON validation can fail.
- Behaviour notes for review:
  - Unknown stored repair-reason text now fails as `InvalidStoredData`.
  - Application repository context variants open their own transaction for `directWrite`, and the unused stale-snapshot retry is removed.
  - Step 010 leaves a "Running" ledger row on precheck failure.
  - Interview Kafka fibers are supervised by `BackgroundWorker` (adds a `RuntimeFailed` log).
  - A malformed analytics snapshot sub-document fails closed.
  - In the analytics erasure queue, a driver JSON-parse failure is now `MalformedMarker`.
  - Analytics corrupt-data errors log `DATA_INTEGRITY_VIOLATION`.
- Committed at the user's request before the final `scripts/check-local.sh`, Code Reviewer, Security Engineer and QA gates. Those gates are pending and follow on the committed state.
- 2026-10-10 gates on `6a08853`:
  - `scripts/check-local.sh` exit 0. Root: unit 1040, it 288 passed / 10 skipped, merged lines 91.08%. Analytics: unit 449, it 42 / 6 ignored, merged lines 87.69%.
  - Security Engineer PASS (static).
  - Code Reviewer PASS with 8 low findings: canonical-UUID tightening for interview command, lease and cleanup fields; erasure-queue parse category; two sources of truth for log allow-lists; partial `ReportState`; per-call retention consumers; barrier validation; spec outcome cells; analytics net is −41, not −20.
  - Root per-file coverage: the split groups have fewer uncovered lines (interview repository 71 → 47, ports 21 → 17). Small regressions in config and codecs are addressed in the follow-up.
- 2026-10-10 root follow-up (user request "continue working on skipped or partially done"):
  - Mongo driver names move to the startup allow-list, and one predicate-based allow-list replaces the `PublicFailureType` marker plus the `classOf` list.
  - Search-event validation is a typed `UseCaseError`.
  - Added `RepositoryIO.pure`/`raise`, converting 156 sites.
  - `HiringReadModel` is injected into `ApplicationService`.
  - `workerReady` removed.
  - Three coverage tests added.
  - Still skipped:
    - `RawJwtAuthConfig` `derives`: the only override is deprecated and fails `-Werror`; the replacement needs `pureconfig-generic-scala3`.
    - `RawInterviewRuntimeConfig` mirror: a 23-field flat raw shape plus cross-field validation, so removing it is no simplification.
  - Root unit 1041/1041 on the merged tree.
- Analytics follow-up (journal enums, shared error text, `StreamingBatchCallback`, `StreamingCheckpointControl`, key-retirement moves, restored `MarkerStorageFailure`, `ReportState` records, validated-only `KafkaRetentionBarrier`) is still in progress in its worktree. Its coverage is being brought to baseline, and it is not in this commit.
- Next: integrate the analytics follow-up, run `scripts/check-local.sh`, re-review the follow-up diff, and run QA on the final state.
