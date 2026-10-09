# Hiring platform error propagation and library delegation

## Identity and scope

- Task: maintainability cleanup from the 2026-10-09 design review (five read-only reviews: Mongo repositories, services/domain, API/config/runtime/infrastructure, analytics adapters, analytics services/config/CLI). Follows [quality consolidation](hiring-platform-quality-consolidation.md).
- Status: in progress (slices 1 and 2 authorized by the user on 2026-10-09; slices 3–7 listed under "Later slices" are not authorized yet).
- Coordinator: Product Manager. Implementation owners: Scala Developer (root build), Big Data Engineer (analytics build). Reviews: Code Reviewer, Security Engineer (slice 2), QA.
- Outcome: code that is cheaper to maintain. Capabilities the configured libraries already provide (cats, cats-effect, fs2, mongo4cats, Delta, Kafka clients) are used directly instead of being re-implemented at each call site. Errors are typed values propagated through Cats Effect and translated once at their owning boundary; no cause is silently dropped.
- Non-goals: any change to GraphQL schema, error codes or messages seen by clients; stored MongoDB document shapes; Kafka topic/event shapes; Delta table shapes; authorization behaviour; new dependencies. The four behaviour decisions listed under "Open decisions" are explicitly out of scope.
- Concurrent work: a second session is editing interview scheduling in this checkout (uncommitted, 2026-10-09). The following paths are locked and must not be edited: every path shown by `git status` at task start, in particular `repository/mongo/MongoInterview*`, `MongoNames.scala`, `MongoMigrationLedger.scala`, `MongoWorkflowIntegrity*.scala`, `MongoHiringIndexSetup.scala`, `MongoHiringMigrations.scala`, `runtime/InterviewSchedulingRuntime.scala`, `service/application/InterviewWorkflowWorker.scala`, `service/port/Interview*.scala`, their tests, and `docs/specs/durable-hiring-workflows.md`. Shared helpers those files call keep their signatures. Root builds use `-Dhiring.test.buildRoot=<scratch>`; a compile failure that originates in a locked file is reported as BLOCKED, never fixed here.

## Source context and decisions

- Verified at review time against HEAD `4c7caa0` plus the locked working-tree changes. Line numbers below are from that state; implementers re-locate by symbol.
- Root build: services use `UseCaseIO = EitherT[IO, UseCaseError, A]` (`service/protocol/UseCaseIO.scala`); repositories use `RepositoryIO` with `RepositoryError`, translated by `MongoRepositorySupport.repositoryGuard`.
- Analytics build: `F[_]` with `Async`; typed `AnalyticsError` with causes (`analytics/.../errors`). The `IO`-in-main / `F[_]`-in-analytics split is kept.
- Interview files call `MongoSessionOperations.{findOne,insertOne,updateOne,updateMany}` (Option-wrapped write results), `RepositoryError.MissingWriteResult` and `MongoOperationalEventInsertion`.
- Decisions:
  - L1: The review's "drop the Option from Mongo write results" change (41 call sites, about −150 lines) is **deferred**. It changes signatures the locked interview files use. It is the first item once that session commits.
  - L2: `MongoOperationalEventInsertion` stays as a one-line delegate to the new shared Mongo error functions until the interview files can move off it.
  - L3: Behaviour-preserving refactors only. Where a typed error replaces `IllegalStateException`/`IllegalArgumentException` at a startup or worker boundary, the logged message stays sanitized and the failure still terminates/restarts exactly as before.
- Shared requirements: [engineering quality](../engineering-quality.md); AGENTS.md "Engineering and GraphQL".

## Behavior and contracts

No external contract changes: GraphQL SDL, operation fixtures, error codes/messages, HTTP responses, stored documents, events and Delta tables stay byte-identical. Schema evolution is not applicable (no data or API shape change). Every refactored failure path keeps its observable outcome (same `RepositoryError`/`UseCaseError`/`AnalyticsError` case, same retry/terminate decision); the only intended differences are internal (typed exception classes instead of `IllegalStateException`, preserved causes, added sanitized log fields).

## Acceptance and evidence

### Slice 1 — library delegation and duplication removal (no behaviour change)

| ID | Given / When / Then | Paths | Verification | Actual outcome |
|---|---|---|---|---|
| EL-01 | Analytics adapters translate unexpected throwables into `AnalyticsError` through one shared extension (e.g. `fa.translating(AnalyticsError.X(_))`); the ~26 copied `adaptError { case e: AnalyticsError => e; case NonFatal(c) => ... }` blocks and `LakehouseErrors.adapt` use it; store-specific pre-cases stay as explicit partial functions. | `analytics/.../errors`, `adapter/**` | analytics `sbt test`; grep shows no remaining copied pattern | Not run |
| EL-02 | Delta table access goes through one `DeltaTables` helper (`read`, `forPath`, `exists`, `readIfExists`) instead of repeated `format("delta").load(SparkPhysicalLocation.resolve(p))`/`isDeltaTable`/`forPath`; behaviour of each caller unchanged. | `adapter/spark/**` | analytics `sbt test` | Not run |
| EL-03 | Analytics Kafka admin/consumer `Properties` are built from `KafkaClientProperties` (no hand-built copies in `AnalyticsEventSources`, `KafkaRetentionBarrier`, `KafkaProducerFencer`); `isolation.level=read_committed` and existing timeouts preserved. | `adapter/kafka/**`, `adapter/spark/AnalyticsEventSources.scala`, `config/KafkaClientProperties` | analytics Kafka specs + shared contract fixture | Not run |
| EL-04 | SHA-256 fingerprints and run ids are built by total constructors (`RangeFingerprint.ofSha256`, `AnalyticsDigest.sha256Hex`, `RunId.prefixed`) without unreachable error branches at the five construction sites. | `analytics/.../domain`, `app/AppModule.scala`, `service/**` | analytics `sbt test` | Not run |
| EL-05 | Analytics ceremony removed: `MongoPublisherStream` forwarders, the five one-field `*Program` wrappers, single-implementation `DeltaReader`/`QuarantineId` (if no test double), `KeyRetirementAuditInputs` 1:1 mirror types, duplicated canonical-UUID checks, identity `unwrap` helpers; hand-written `if/validNec`, `Either.cond(...).toValidatedNec` and effect folds use `Validated.condNec`/`traverse`/`foldM`. | analytics `app/**`, `config/**`, `domain/**`, `adapter/**` | analytics `sbt test` | Not run |
| EL-06 | Root Mongo write-error classification (duplicate key, `TransientTransactionError`, unknown commit) lives in one `MongoErrors` object used by `MongoRepositorySupport`, `MongoTransactionRunner`; one-method mixins `MongoConflictWriteMapping`, `MongoApplicationEventInsertion` become object functions; `MongoOperationalEventInsertion` delegates (L2). | `repository/mongo/**` (unlocked files) | root `sbt test`; Mongo it specs | Not run |
| EL-07 | `MongoAnalyticsRepositoryOperations` forwarding wrapper is removed; the duplicated receipt lookup in `MongoAnalyticsErasureRequestRepository.enqueue` is one helper; the discarded fence read is kept exactly as is (Open decision O3). | `repository/mongo/MongoAnalyticsRepositories.scala` | root `sbt test`; analytics erasure it specs | Not run |
| EL-08 | Repeated Mongo idioms use shared helpers: `MongoSessionOperations.findById`, `MongoLeaseQueue` lease-release update and guarded `matched == 1 else Conflict` transition for the embedding-work, search-session and outbox queues. Stored shapes unchanged. | `repository/mongo/**` (unlocked files) | root `sbt test`; lease/queue it specs | Not run |
| EL-09 | Service call sites use `UseCaseIO.ensure(cond, error)` and `UseCaseIO.found(repo, entity)`; thin renamings of `EitherT` constructors are removed; `JobService.persistUpdatedJob` alias and `OperationalEventKafkaRuntime.saslProperties` forwarder are removed (tests call `KafkaClientSettings.security`). | `service/**`, `infrastructure/kafka/OperationalEventKafkaRuntime.scala` | root `sbt test` incl. SDL/operation fixtures | Not run |

### Slice 2 — typed errors through Cats Effect

| ID | Given / When / Then | Paths | Verification | Actual outcome |
|---|---|---|---|---|
| EL-10 | Startup/infrastructure failures raise typed `NoStackTrace` errors (style of `MigrationError`) instead of `IllegalStateException`/`IllegalArgumentException`: `MongoAdminSeed`, `MongoAtlasSearchSetup`, `MongoMutationReceiptRepository`, `MongoSemanticSearchRepository`, `MongoAtlasSearchAdmin`; the bounded "take(n+1), compare" check is one helper. Messages stay sanitized; startup still fails closed. | `repository/mongo/**` (unlocked) | root `sbt test`; Admin seed / search it specs | Not run |
| EL-11 | Kafka outbox: claim and completion failures raise one typed `OutboxOperationFailed(RepositoryError)` via `rethrowT`; the consumer's receipt failure is no longer collapsed to `false` but propagated as a typed `RecordNotDurable(cause)`, keeping the existing no-commit + resilient-restart behaviour. | `infrastructure/kafka/OperationalEventKafkaRuntime.scala` | root `sbt test`; `OperationalEventKafkaRuntimeSpec`; compose it spec if available | Not run |
| EL-12 | No cause is dropped silently in the touched paths: `MongoProducerGenerationMaintenance` uses the shared guard (cause logged, `Unavailable` returned); `JwtAccessTokenIssuer` logs the sanitized failure before returning `Unavailable`. Client-visible results unchanged. | `repository/mongo/MongoProducerGenerationMaintenance.scala`, `infrastructure/auth/JwtAccessTokenIssuer.scala` | root `sbt test` | Not run |
| EL-13 | `EmbeddingPipeline` repository-failure handling uses `EitherT.leftSemiflatMap` (one `orReport` helper instead of five copies) and a 3-case outcome match; `Identifiers.parse` returns `Option` and callers keep their current error mapping. | `service/search/EmbeddingPipeline.scala`, `domain/model/Identifiers.scala`, callers | root `sbt test`; EmbeddingPipeline specs | Not run |
| EL-14 | Analytics: `MongoHmacKeyRetirementAuthorizationStore` and `DeltaStreamingBatchJournal` keep the cause in the typed error (messages unchanged); catch-all `handleErrorWith { case X => ...; case e => raiseError(e) }` becomes `recover`/`adaptError`; `MongoPublisherStream.one` and `KafkaOffsetRangeSource` raise typed `AnalyticsError` instead of `IllegalStateException`; `KafkaOffsetRangeSource` awaits admin futures with `Async.fromCompletableFuture` in a `Resource` instead of blocking `.get`; `AnalyticsLakehouseIdentity` drops `Try { require }`; `InvalidInput.one(msg)` replaces repeated `NonEmptyChain.one` wrapping. | analytics `adapter/**`, `domain/**`, `app/**`, `service/**` | analytics `sbt test`; Kafka it spec if available | Not run |
| EL-15 | Both builds pass formatting and unit suites; no external contract changed. | all | `scalafmtCheckAll`, `sbt test` in both builds (isolated build root); SDL/operation fixture specs | Not run |
| EL-16 | **Net reduction (user requirement, 2026-10-09).** Per owner, `git diff --numstat` shows: production code (`src/main`, `analytics/src/main`) net negative for slice 1 and for slices 1+2 combined; test code is judged by coverage, not by size: JaCoCo unit line and branch coverage of every touched production file must not fall below the clean-HEAD baseline (`sbt jacoco` on an exported HEAD copy, reports under the scratch build root). A test that adds no covered line or branch beyond the rest of the suite is redundant and is merged or removed; when a helper replaces copies, the tests of those copies collapse into tests of the helper. A new test is added only when it raises coverage or pins a contract (wire bytes, digests, property maps), and it is kept to the minimal case. No parallel old/new variants beyond decision L2. Typed errors reuse existing ADTs/cases before adding new types. Any item that would grow code is skipped and reported. | all | `git diff --numstat` per area, reported by each owner and re-measured by the coordinator | Not run |
| EL-17 | **Coverage gate (user requirement, 2026-10-09).** Both builds fail `sbt jacoco` when unit line coverage is below 60%; `scripts/check-local.sh` runs `jacoco` instead of `test`; reports include `jacoco.xml` for per-file comparison. | `build.sbt`, `analytics/build.sbt`, `scripts/check-local.sh`, `README.md` | Baseline on exported HEAD: application 64.47% lines (846/846 tests), analytics 60.86% lines (446/446 tests); `Test/jacocoReport` with threshold 70 fails with "Required coverage is not met", with 60 passes, in both builds; `scalafmtSbtCheck` passes | PASS (coordinator, 2026-10-09); independent review pending |

## Open decisions (not in this task)

- O1 `EmbeddingCoverageService`/`AnalyticsReportingService` accept an Active Admin without checking `adminSingleton` (unlike `ActorAuthorization.canManage`). Needs Security Engineer review before a shared `requireAdmin` tightens it.
- O2 `SemanticSearchService` (around line 352) maps every repository error, including `AuthorityRevoked`, to `VectorSearchUnavailable`.
- O3 `MongoAnalyticsErasureRequestRepository.enqueue` reads the subject fence and discards it (pre-existing). Likely dead because the request carries `producerRegistry=true`; owner to confirm.
- O4 A short password fails with `BlankField("password")`.

## Later slices (not authorized yet)

3. Config delegation to pureconfig defaults/`emap` readers in both builds (config error text changes). 4. Runtime simplification (`SetupLifecycle`, `EmbeddingCapability`, `RuntimeConfig`, single Argon2 hasher, single config load). 5. Resolver/service shared helpers (`paged`, `withHiring`, `readScope`, semantic-search authorization). 6. `MongoTransactionRunner` on cats-retry's error channel; Spark cancellation bridge and checkpoint write-once helper with RF-08 8b. 7. Decisions O1–O4. Deferred by L1: Mongo write-result `Option` removal.

## Implementation handoff

- Owners and write paths (exclusive):
  - Big Data Engineer: `analytics/src/**` only (EL-01–EL-05, EL-14). Analytics build is independent of the locked files.
  - Scala Developer: root `src/main/**`, `src/test/**`, `src/it/**` excluding locked paths (EL-06–EL-13).
- Order: slice 1 then slice 2 within each owner; the two owners run in parallel (separate builds). Each owner commits nothing.
- Verification: root `sbt -Dhiring.test.buildRoot=<scratch> scalafmtCheckAll test` (Java 17) plus the Mongo/Kafka it specs touched, when Docker is available; analytics `sbt scalafmtCheckAll test` (own target). Missing infrastructure is reported as unverified.

## Checkpoint and review

- Completed criteria and changed files: none yet.
- Latest commands/results: none yet.
- Blockers: locked interview paths (see scope).
- Code Reviewer: pending. Security Engineer (slice 2: auth issuer, Kafka outbox, Admin seed): pending. QA: pending.
