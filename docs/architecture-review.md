# Architecture and Runtime Safety Review

Review date: 2026-10-14. Scope: main application (`src/main/scala/com/example/graphQL/cats`, 226 Scala files), MongoDB persistence, and the separate Spark/Delta build under `analytics/`.

Method: three read-only reviews sampled the source and docs, plus targeted spot-checks. Nothing was built or run, so "tested" means the test exists in source. Findings marked *confirmed* were re-read in code; the rest are agent findings from sampled reads. This is a point-in-time review, not acceptance evidence.

## 1. Design patterns

| Pattern | Where | What it gives |
|---|---|---|
| Ports and adapters (hexagonal) | 17 ports in `service/port`; adapters in `repository/mongo/Mongo*` and `infrastructure/{kafka,embedding,auth}` | Services are tested with fakes; store and broker are swappable. |
| Repository | `MongoJobRepository`, `MongoUserRepository`, others | Persistence stays out of services. |
| Manual composition root | `runtime/MongoHiringRuntime.scala` | Explicit constructor wiring, no DI framework. |
| ADT state machine / transition matrix | `ApplicationLifecycle`, `JobLifecycle`, `InterviewSubjectCleanup` | Illegal transitions are rejected as typed values. |
| State-monad program | `LifecycleProgram = StateT[Either[DomainError,*],S,A]` | Pure decisions compose. |
| Opaque types, smart constructors | `Id[Tag]`, `EmailAddress`, `PasswordHash`, `PageSize` | Typed IDs; invalid values cannot be constructed. |
| Validated accumulation | `ValidatedNel` (about 83 uses) | Config and input report all errors at once. |
| Typed error channels | `UseCaseF[F, A] = EitherT[F, UseCaseError, A]`, `RepositoryF[F, A]` (were `UseCaseIO`/`RepositoryIO` before the [effect-polymorphic services](specs/effect-polymorphic-services.md) migration) | Business failures are values, not exceptions. |
| Batching (DataLoader) | Sangria `Fetcher` / `DeferredResolver` | Avoids N+1 loads on nested relationships. |
| Transactional outbox, inbox, receipts | `event_outbox`, `consumer_receipts`, `mutation_receipts` | At-least-once publication, idempotent consumers and mutations. |
| Lease queues with generation + token | Outbox, embedding work, search work, interview commands | Stale workers cannot overwrite newer work. |
| Strategy | `SearchFusionStrategy`, `RetryBackoff` | Pure, swappable policies. |
| Middleware / decorator | `HttpMiddleware`, `FixedWindowRateLimiter` | Request IDs, admission control, timeouts in one place. |
| Resource, Semaphore, Ref, Deferred | `RequestContext`, Kafka runtimes, `Argon2PasswordHasher` | Cancellation-safe lifetimes, bounded concurrency. |

Absent today, now planned (user-approved 2026-10-14): circuit breakers ([spec](specs/dependency-circuit-breakers.md)), tagless-final services ([spec](specs/effect-polymorphic-services.md); concrete `IO` was the documented choice until then), a literal Saga type ([spec](specs/hiring-saga-orchestration.md); the interview workflow is saga-like).

**Status after refactoring (2026-10-15).** The findings in this review are the 2026-10-14 snapshot and are kept as written. Since then, as verified in source and the specs:

- Circuit breakers (done locally): one process-local breaker each around the Voyage embedding provider, Atlas `$search`/`$vectorSearch` discovery, the Kafka operational-event publisher and Mongo transactional writes. Readiness reports `Unavailable` only while the Mongo-writes breaker is open. Section 7, item 5 is addressed locally; the Kafka broker integration check is unverified ([spec](specs/dependency-circuit-breakers.md)).
- Effect-polymorphic services (done locally): ports and services are written over `F[_]`. `IO` is fixed only in `Main`, `runtime/` wiring, configuration loading, the IO adapters (Mongo, Kafka, auth, embedding) and the Sangria adapter, which is the only unsafe boundary ([spec](specs/effect-polymorphic-services.md)).
- Saga (in review): a pure generic engine in `domain/saga` runs only the interview scheduling workflow. Interview cancel/reschedule was evaluated and not ported. Interview subject cleanup and analytics erasure were not ported by decision and keep their existing pure policies. The analytics build does not use the engine ([spec](specs/hiring-saga-orchestration.md)).

## 2. Pure FP coverage

Strong:

- The domain package has no effect, framework, Mongo, Circe or HTTP imports.
- `var`, `unsafeRun`, `Thread.sleep` and `mutable.` collections: zero hits in main sources.
- One `throw` (intentional complexity rejection inside a Sangria callback, `HiringGraphQLSchemaAssembly.scala:85`) and one `asInstanceOf` (Mongo driver value, `MongoHiringIndexSetup.scala:466`).
- The 14 `println` calls are `IO.println` in CLI entrypoints.

Weak spots:

- `api/admission/FixedWindowRateLimiter.scala:36-40` returns and compares `null` inside a Caffeine lambda and holds a mutable cache.
- `HiringGraphQLSangriaAdapter` uses a request-scoped `ConcurrentLinkedQueue` as an interop collector.

Most-used dependency functions (grep counts across `src/main`):

| Function | Count | Purpose |
|---|---|---|
| `map` / `flatMap` | about 540 / 518 | Effect sequencing |
| `EitherT` | 165 | Typed use-case and repository errors |
| `leftMap` | 123 | Translating errors between layers |
| `traverse` / `traverse_` | 90 / 71 | Batch operations |
| `ValidatedNel` | 83 | Accumulating validation errors |
| `mapN` | 45 | Combining validated values |
| `raiseError` | 33 | Failing effects |
| `attempt` / `handleErrorWith` | 16 / 16 | Error boundaries |
| `timeout` | 12 | Deadlines |
| `Semaphore` / `Deferred` | 10 / 9 | Bounded concurrency, per-request cancellation |

By import count: `cats.effect.IO` (about 100 files), `cats.syntax.all.*` (104), mongo4cats (29), Circe (19), Sangria (17), fs2 / fs2-kafka (13), http4s (12), pureconfig (7), otel4s (6), Caffeine (4). Iron is declared but used in one config file only.

## 3. Architecture: MVC, DDD, OOP

**MVC.** The project is not MVC. It is a layered ports-and-adapters (Clean) design: resolvers play the controller role, services the use-case role, the domain is immutable values.

**Dependency direction** (verified by grep): domain imports nothing outward; `api` imports no `repository` or `infrastructure`; `service` imports no `api`, `repository` or `infrastructure`; only `runtime` and `Main` wire adapters.

**DDD.** Bounded contexts (Hiring, Search and embedding, Analytics) are documented in `docs/domain-language.md`; Analytics is a separate sbt build. Aggregates: Job, Application with history, Interview workflow, User. Value objects are the opaque IDs; pure policies act as domain services; events and ports act as the anti-corruption layer.

**SOLID.** Ports are split per aggregate (ISP), services depend on ports (DIP), 122 sealed traits/enums give exhaustive matching, composition is preferred over inheritance.

Gaps:

- Circe is imported in four `service/` files (`events/OperationalEvent*`, `search/SearchContracts`, `search/SearchSessionRecording`), leaking wire codecs into the service layer.
- No architecture test enforces the layer rules; they rest on docs and review.
- Several Mongo adapters are very large (`MongoInterviewCommandQueue` 793 lines, `MongoOperationalEventRepositories` 716, `MongoSemanticSearchRepository` 642, `MongoHiringMigrations` 629, `MongoHiringCodecs` 610).
- CLI `*Main` objects sit inside `repository.mongo`.
- `ARCHITECTURE.md`'s package tree omits `service/read`, `service/mutation`, `api/admission`, `domain/workflow`, `domain/search`.

## 4. MongoDB data model

**Patterns and why**

- Embed bounded owned values (profile, skills, location, embedding metadata); reference independent entities. Applications reference candidate and job; `application_events` is a separate append-only collection.
- Canonical sidecar fields (`emailCanonical`, `skillsCanonical`, ...) give case-insensitive equality without collation or regex scans.
- Invariants live in the store, not in preflight reads: unique `(candidateId, jobId)`, partial-unique admin singleton, unique email and name, unique receipts.
- Optimistic concurrency via a `version` field with CAS filters on users, jobs and applications' sibling documents.
- Keyset pagination on `(createdAt desc, _id desc)` with paired indexes.
- Partial indexes keep claim scans off completed history; partial TTL indexes delete only published or failed rows.
- Strict `$jsonSchema` validators with fail-closed drift checks at startup.
- 19 ledger-tracked, idempotent, resumable migrations with a custom runner (correctly not Flyway).
- Vector and hybrid search: `$vectorSearch` plus `$search`, application-side RRF fusion, final eligibility re-checked on the authoritative document.

**Enhancements**

1. Add validators for `applications`, `application_events`, `embedding_work`, `search_sessions`, `search_session_work` and both receipt collections.
2. Outbox claim uses `OR`; docs record about 4,200 claim attempts for 500 completions at concurrency 8. A single `claimableAt` field would remove the `OR`.
3. Make read and write concerns explicit (`majority`) rather than relying on URI defaults; `ReadConcern.SNAPSHOT` appears in one repository only.
4. Document a shard key before scale-out. Global unique indexes (email, name, admin, `(candidateId, jobId)`) cannot be enforced on a sharded collection without including the shard key.
5. Consider change streams for outbox latency, keeping polling as the recovery path.
6. Extend inclusion projections to list endpoints.
7. Bound the facet aggregation with `maxTime` or pre-aggregate; it was only observed at 256 jobs.
8. Quantization and `$rankFusion` are unevaluated.
9. Run `$indexStats` before dropping any index; earlier partial-index attempts failed their adoption gates.

## 5. Spark / Delta data model

**Layout.** Bronze, silver, late facts and quarantine; gold tables `application_funnel`, `time_to_hire`, `job_skills`; control tables for manifests, streaming progress, decisions, lineage and the HMAC key registry.

**Patterns and why**

- Medallion layering with idempotent keyed `MERGE` (bronze on topic/partition/offset, silver on `eventId`): batch re-runs and Kafka redelivery are safe.
- Explicit Kafka offset ranges make runs replayable.
- Quarantine takes malformed events and event-ID conflicts and blocks gold from advancing.
- K=10 suppression guards small cohorts in gold output.
- Domain-level 24-hour day watermark routes late facts to a separate table.
- Streaming uses `read_committed` and `failOnDataLoss`; a Delta decision journal is appended before Mongo publication and committed after, with compare-and-set report publishing (effectively-once publication).
- GDPR erasure: HMAC-pseudonymized subject tokens, fence, purge rewrite, gold rebuild, vacuum and log-horizon wait, verify. Raw tables set `dataSkippingNumIndexedCols=0` so raw values do not enter column stats.
- Physical-plan assertions exist in tests but are structural; the docs make no latency claim and record a local bronze p95 of about 40 s as a failure.

**Enhancements**

1. No `partitionBy` and no `OPTIMIZE`, compaction or `optimizeWrite`: each silver `MERGE ... ON eventId` scans the whole table and streaming will create small files. Partition by day and coordinate compaction with erasure (compaction extends the vacuum horizon).
2. Bronze keeps `rawValue` until `expiresAt`; consider storing only the parsed form after validation.
3. Gold is a full overwrite (capped at 10,000 rows); move to `replaceWhere` by day and add Delta CHECK constraints if it grows.
4. Data quality lives in code only; add Delta constraints and quarantine-rate metrics.
5. Record shuffle/spill counters and `PushedFilters` as Phase 15 evidence.
6. Erasure rewrites touch whole datasets when attribution is impossible; partitioning by subject-token bucket would shrink them.
7. Project-recorded open gaps: real-horizon key retirement, healthy-load runs, deployed latency.

## 6. Edge cases, races and load

**Races and same-data access (strong)**

- `MongoTransactionRunner` gives each attempt its own session; transient errors retry up to 3 times with 25-250 ms backoff; an unknown commit result retries the commit alone; abort is owned by a `Resource`, so cancellation is safe.
- Application submit is one transaction: guarded job update (`_id`, version, `Open`), application insert, history insert, outbox insert. A lost guard returns `Conflict`; a duplicate key returns `DuplicateApplication`.
- Status changes filter on the previous status; history and outbox commit in the same transaction.
- Mutation receipts (unique `(operation, actorScope, idempotencyKey)`) make writes idempotent and replayable.
- Outbox and embedding work use lease tokens and generation; publish, retry and fail are lease-filtered.
- Admin is a singleton via a partial unique index plus transactional bootstrap.

**Load and admission**

- 16 concurrent requests, then 503; 5 s request timeout; 64 KB body limit; 64 Ember connections.
- GraphQL depth 16, complexity 1000, page size 100, expensive-root cap (default 4), bounded parsed-document cache.
- Bounded Caffeine rate limiters with trusted-proxy-aware IP resolution.
- Argon2 limited by a semaphore; expensive discovery by 4 permits plus `maxTimeMS`; Mongo pool capped at 10.
- cats-retry with jitter for Kafka restarts; per-request `Deferred` cancels in-flight resolver work.

**Crash prevention**

- `-Wunused:all -Wvalue-discard -Werror` with exhaustive ADTs; typed config validated at startup.
- Sangria exception handler and HTTP layer sanitize errors.
- A record that cannot be made durable stops the stream before its offset is committed.
- No `Await` or `unsafeRun` in production paths.

**Tests present in source**

- Interview cancellation races (`InterviewCancellationRaceIntegrationSpec`).
- Concurrent slot and budget claims, concurrent admin seed, account-deletion fencing.
- Outbox lease reclaim and transaction retry; transaction abort on cancellation.
- Rate limiter concurrency; HTTP admission, size and slow-body limits.

## 7. Residual risks, ranked

1. **Complexity guard and negative `first`** (*code path confirmed, exploit not reproduced*). `connectionComplexity` is `1 + first * child` (`HiringGraphQLSchemaAssembly.scala:41`); `first` is a plain `IntType` (`HiringGraphQLInputs.scala:110`) and page size is validated only later in the resolver. A negative `first` could yield negative cost and offset other fields. The expensive-root cap limits the worst case. Reproduce with a test, then reject negative values at the schema level.
2. **Authorization checked outside the write guard** (*confirmed*). `submitApplication` resolves the candidate first (`ApplicationService.scala:49-50`) while the write guard covers only the job; `changeStatus` follows the same pattern. A user deleted between check and commit could still write. Job ownership is immutable and recruiter deletion closes jobs, which mitigates part of it. No test covers it. `AGENTS.md` asks for actor predicates carried into data access.
3. **Hot-row contention on popular jobs.** Every submit bumps the job version, so concurrent applicants serialize on one document with 3 retries. Existing submit tests are sequential; there is no parallel-apply or close-versus-submit race test.
4. No parallel same-idempotency-key test against real Mongo for the receipt path.
5. No circuit breakers around Mongo, Voyage or Kafka. 16 admission permits against a pool of 10 can time out with `Unavailable` before the 5 s deadline.
6. Readiness covers only Mongo and the embedding worker; it ignores the Kafka publisher, consumers, interview workers and outbox lag. `BackgroundWorker` logs a dead fiber once and does not restart it (some workers restart internally; verify each).
7. Rate limits are process-local; the auth limiter is not fail-closed by default, so a flood of distinct keys can evict counters, and limits multiply across instances.
8. Recruiter deletion closes all open jobs in one transaction, risking Mongo's transaction lifetime or size limits for large accounts.
9. Evidence is mostly local: no load or soak test of the admission path against a real Mongo pool; Atlas, cloud and deployed evidence rows are pending in the docs.

## 8. Suggested order of work

1. Negative-`first` validation plus test.
2. Actor predicate in application write guards plus a deleted-actor race test.
3. Parallel apply, close-versus-submit and idempotency-key integration tests.
4. Validators for the unprotected collections (new migration).
5. Delta day partitioning and compaction coordinated with erasure.
6. Layer-rule architecture test; move Circe codecs out of `service/`.
