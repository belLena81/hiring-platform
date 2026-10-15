# Architecture Review Remediation

## Identity and scope

- Task: close the §3 gaps and §7 residual risks of the [architecture review](../architecture-review.md) (2026-10-14), plus GraphQL latency, throughput and exposure items supplied by the user on 2026-10-15.
- Status: ready (plan approved by the user 2026-10-15). Implementation not started.
- Coordinator: Product Manager. Owners per slice below. Reviewers: Code Reviewer (every slice), Security Engineer (ARR-01, 02, 05, 07, 08, 14, 15, 16, 17, 18), QA Engineer (final state).
- User outcome: every review gap and residual risk is fixed, covered by a test, or closed by a recorded decision; GraphQL reads become cheaper per request without widening access.
- Authorized scope: ARR-01 to ARR-19 below, including production code, tests, SDL, fixtures, configuration examples and canonical docs.
- Non-goals: §7.5 (circuit breakers, already done locally per [dependency circuit breakers](dependency-circuit-breakers.md)); a cross-instance rate limiter; deployed, Atlas or cloud evidence; §4/§5 Mongo validators and Delta partitioning; GraphQL federation, batch mutations, subscriptions (R3), interfaces (R5), directives (R6), field telemetry (R8) and connection facets (R9) of the [graph access spec](hiring-graphql-graph-access.md); role-model or status-transition changes.
- Dependencies: [graph access spec](hiring-graphql-graph-access.md) is the detailed contract for Slice E (its AC-01, AC-02, AC-04, AC-07, AC-10 apply); [hiring observability resilience](hiring-observability-resilience.md) HOR-03/HOR-04 own full degraded-readiness design (ARR-07 is a subset); [engineering quality](../engineering-quality.md); [schema evolution](../schema-evolution.md).

## Source context (read 2026-10-15; not re-run)

| Item | Fact |
|---|---|
| Complexity | `HiringGraphQLSchemaAssembly.scala:41-44` cost uses raw `first`; reducers at `:88-96` (expensive-root budget, depth 16, complexity 1000) run before resolvers. `first` is `IntType` (`HiringGraphQLInputs.scala:110`); range checked only in resolvers (`HiringGraphQLResolverSupport.scala:167-170`, `PageSize.fromInt`). No negative-`first` test. |
| Actor in write | `ActorAuthorization` validates a per-request viewer snapshot. Submit filter (`MongoApplicationRepository.scala:178-199`) covers job `_id`, `version`, `status == Open`; `changeStatus` (`:128-147`) covers application `_id` and previous status. Subject fence pattern: `MongoInterviewWorkflowStore.scala:52-96`. Deletion transaction: `MongoUserRepository.scala:408-458` upserts `outbox_subject_fences`. |
| Races | No parallel same-job apply, close-versus-submit, or parallel same-idempotency-key test against Mongo. Template: `InterviewCancellationRaceIntegrationSpec.scala:276`. Lost submit CAS returns typed `Conflict`, never retried (`MongoRepositorySupport.scala:318-322`); runner retries only transient errors (`MongoTransactionRunner.scala:22-27`). |
| Rate limits | `FixedWindowRateLimiter(failClosedWhenFull = false)` default; auth limiter fail-open, interview limiter hardcoded fail-closed. `RateLimitConfig(windowSeconds, attempts, maxBuckets)`. Limits count requests, not query cost. |
| Introspection | Always enabled; `GET /schema.graphql` serves SDL unauthenticated (`docs/api.md:9-18`); `introspection.graphql` fixture executed by `HiringGraphQLContractSpec.scala:229`. No configuration exists. |
| Readiness | `/ready` = Mongo ping and embedding health and Mongo-writes breaker (`MongoHiringRuntime.scala:344-370`). No outbox-lag query. `BackgroundWorker` observes but never restarts. `SearchSessionHandoff.scala:64-95` and `InterviewSubjectCleanupWorker.scala:101-120` can die unobserved; `InterviewWorkflowWorker.publisher` appears unused. |
| Recruiter deletion | `closeRecruiterJobs` (`MongoUserRepository.scala:482-542`) pages by 1000 but all pages run in the deletion transaction; no total bound. |
| Structure | Circe in `service/events/OperationalEvent*.scala`, `service/search/SearchContracts.scala`, `service/search/SearchSessionRecording.scala`; `payload: Json` and `filter: Json` are service data. CLI mains `ProducerGenerationRetirementMain`, `EmbeddingWorkRepair`, `MongoWorkflowIntegrityAudit` in `repository.mongo`. Five adapters of 610-806 lines. No root architecture test (analytics has `AnalyticsLayeringSpec` with ArchUnit 1.5.1). `ARCHITECTURE.md:95-140` tree stale. |
| GraphQL batching | Request-scoped `usersFetcher`, `jobsFetcher`, `emailVisibilityFetcher` exist (`HiringGraphQLFetchers.scala`). Nested `Job.applications`, `Application.history`, `Application.interview`, `User.applications` do not. |
| Load evidence | No HTTP admission load test against a real Mongo pool. |

## Decisions

- D1 (user, 2026-10-15): scope is §3 + §7 + pasted GraphQL items; items 1-4 are delivered here as Slice E rather than only cross-linked.
- D2 (user): schema exposure is a typed configuration flag, on for local/test and off in the production default; it gates both introspection and `GET /schema.graphql`.
- D3: query complexity and depth limits already exist; the pasted "missing limits" item is closed except the negative-`first` hole (ARR-01).
- D4: projection pushdown uses a pure API-layer `FieldSelection` passed to ports in domain terms; BSON is built only in the Mongo adapter (layer rule), not in resolvers.
- D5: persisted operations default to an allow-list built from fixtures; automatic registration is a separate switch, off by default. GET is accepted only for persisted query operations, never mutations. Only anonymous public reads may be publicly cacheable.
- D6: `/ready` 503 semantics stay as in `dependency-circuit-breakers.md` rule 7; other components are reported as degraded in the body and diagnostics.
- D7: `schema-exposure`, `failClosedWhenFull` and cost budgets are typed configuration with sanitized examples; pre-MVP policy applies (one active API shape, no compatibility code).

## Open questions

| ID | Question | Blocks | Default if unanswered |
|---|---|---|---|
| Q1 | Subject fence for ARR-02: touch only `outbox_subject_fences`, or also `users.version`? | ARR-02 | Decided 2026-10-15: fence document only, plus an in-transaction read of the active actor (needed because a never-fenced user has no fence document). Cost: one recruiter's parallel status changes serialize on their fence (16 parallel: 189-375 ms, 0-3 typed `Conflict`; 16 distinct recruiters: 9-23 ms, 0). Accepted for now; revisit if bulk status changes become a use case. |
| Q2 | ARR-04 submit guard: drop the version match in favour of retried write conflicts? | ARR-04 | Decided 2026-10-15: dropped. `JobSubmissionSnapshot` is only (id, status, revision); the application copies only the job id, so nothing goes stale. Jitter, not the guard change, gave the measured gain. |
| Q3 | HTTP status for cost-budget exhaustion: 429 or 400 with `RATE_LIMITED`? | ARR-14 | 429 with `extensions.code = RATE_LIMITED` and `retryAfter`. |
| Q4 | Sangria version support for Projector and `@defer`/`@stream` (graph access spec A1, Q4). | ARR-17, ARR-19 | Architect spike E0. |
| Q5 | Work storage for remaining recruiter job closures: existing cleanup queue or a new collection? | ARR-08 | Decided 2026-10-15: new `recruiter_job_closure_work` collection (the interview cleanup queue has no lease semantics and a strict interview-specific validator). |

## Behavior and acceptance examples

| ID | Rule | Example (Given / When / Then) |
|---|---|---|
| ARR-01.1 | Negative `first` rejected before execution | Given `jobs(first: -1)`, when executed, then `VALIDATION_FAILED` and zero resolver calls |
| ARR-01.2 | Negative `first` cannot offset cost | Given a costly sibling plus `first: -1000` elsewhere, then the request is rejected before execution |
| ARR-01.3 | Oversized rejected | `first: 101` gives `VALIDATION_FAILED` before execution |
| ARR-02.1 | Deleted candidate cannot submit | Given Ana's deletion and Ana's submit race, then either deletion wins and no application exists, or submit wins and deletion succeeds after; never an application after Ana's tombstone commit |
| ARR-02.2 | Deleted recruiter cannot change status | Same race for Rita's `changeStatus` |
| ARR-02.3 | Non-owner write refused in store | A recruiter who does not own the job gets `FORBIDDEN` even when the service check is bypassed in a test double |
| ARR-03.1 | Parallel apply | 16 candidates apply in parallel to J1: each result is accepted or `Conflict`; history and outbox rows equal accepted count |
| ARR-03.2 | Close versus submit | `closeJob(J1)` and a submit in parallel: no application is created after the close commit |
| ARR-03.3 | Parallel idempotency key | 6 identical submits with one key: one application, one receipt, identical payloads |
| ARR-04.1 | Contention decision | Conflict rate for ARR-03.1 recorded before and after; decision recorded here |
| ARR-05.1 | Auth limiter fails closed | With `maxBuckets` distinct keys filled, a victim's exhausted bucket is not reset by flooding |
| ARR-06.1 | Worker restart | A thrown exception in search-session handoff or subject cleanup is logged and the worker resumes after backoff |
| ARR-07.1 | Degraded detail | A stopped Kafka publisher keeps `/ready` 200 but lists the publisher as degraded; Mongo-writes breaker open still gives 503 |
| ARR-07.2 | Outbox age | Oldest pending outbox age is reported from an indexed query |
| ARR-08.1 | Bounded deletion | A recruiter with 2500 open jobs is deleted: the first transaction closes at most 1000; remaining closures complete through resumable work; none stays open |
| ARR-08.2 | Resume | A crash after the first page resumes without duplicate `JOB_CLOSED` events |
| ARR-09.1 | No Circe in service | Layer test forbids `io.circe` in `service`; event, stored and cursor bytes unchanged in contract specs |
| ARR-10.1 | CLI package | Entry points exist only in `cli` and `Main`; doc `runMain` paths updated |
| ARR-11.1 | Mechanical split | No behavior change: full test suite and contract capture unchanged |
| ARR-12.1 | Layer rules enforced | A forbidden import makes `HiringLayeringSpec` fail |
| ARR-13.1 | Admission under load | 32 concurrent clients against a real pool of 10: only success, 503 or `Unavailable`; no 500; no request past 5 s; percentiles recorded |
| ARR-14.1 | Cost budget | A client within budget succeeds; after spending the budget on expensive queries, the next request is refused with `RATE_LIMITED` and `retryAfter` before any resolver runs |
| ARR-14.2 | Keying | Authenticated callers are keyed by actor ID, anonymous by resolved client IP; flooding distinct keys does not reset a victim |
| ARR-15.1 | Exposure off | With exposure off, `__schema` queries and `GET /schema.graphql` are refused with a sanitized error |
| ARR-15.2 | Exposure on | With exposure on, the introspection fixture and SDL download match the snapshot |
| ARR-16 | Nested fields | Graph access AC-01.1-01.5 and AC-10 |
| ARR-17 | Projection | Graph access AC-02.1-02.3 plus recorded before/after workload |
| ARR-18.1 | Persisted operations | Graph access AC-04.1-04.4 |
| ARR-18.2 | GET only for queries | A persisted mutation hash over GET is refused |
| ARR-18.3 | Cache headers | Anonymous public read gets `Cache-Control: public` and ETag; any authenticated response gets `private, no-store` |
| ARR-19 | Incremental delivery | Graph access AC-07 |

## Acceptance and evidence

Evidence is filled only after observation.

| ID | Implementation and test paths | Verification | Actual outcome |
|---|---|---|---|
| ARR-01 | `HiringGraphQLSchemaAssembly.pageSizeBounds` reducer and clamped cost; `HiringGraphQLSangriaAdapter.execute(rejected)`; `HiringApiRoutesSpec` ARR-01.1-01.3, `HiringGraphQLDiscoveryContractSpec` | `sbt test` | 2026-10-15: 1234 passed, 0 failed. Reviews pending |
| ARR-02 | `repository/mongo/MongoApplicationWriteFence.scala` (in-transaction active-actor read, fence `$inc` upsert, recruiter ownership read), `ApplicationRepository.ApplicationWriteActor`; `ApplicationWriteFenceIntegrationSpec`, `ApplicationServiceSpec` ARR-02.x | `sbt it:test` | 2026-10-15: full IT 290 passed, 25 skipped (Kafka), 0 failed; deletion races 25 rounds each consistent; deterministic snapshot-then-delete tests catch a missing fence. Reviews pending |
| ARR-03 | `src/it/.../ApplicationSubmissionRaceIntegrationSpec.scala` | `sbt "it:testOnly *ApplicationSubmissionRaceIntegrationSpec"` | 2026-10-15 first run: ARR-03.1 PASS, ARR-03.2 and ARR-03.3 FAIL (DEF-1). After DEF-1 fix: 3 of 3 PASS (ARR-03.3 with a millisecond clock, see DEF-2). Reviews pending |
| ARR-04 | `MongoApplicationRepository` submit guard: `status == Open` + `$inc version`, no observed-version match; `MongoTransactionRunner` 5 attempts with full jitter | ARR-03.1 at 16 parallel, local replica set, 3-4 runs | Accepted/conflicts: before 1/15; DEF-1 fix only 4-5/11-12; final 10-14/2-6. Indicative only, no percentiles. Reviews pending |
| ARR-05 | `AuthRateLimiter` on the single overflow design of `FixedWindowRateLimiter` (eviction and fail-closed modes removed); `AuthRateLimiterSpec` ARR-05.1 | `sbt test scalafmtCheck Test/scalafmtCheck` | 2026-10-15: 1295 of 1296 passed; `MongoDatabaseProbeSpec` failed under concurrent load and passed 5/5 when rerun alone (timing-sensitive, untouched code). Reviews pending |
| ARR-06 | `SearchSessionHandoff` restart loop (`pollInterval` delay); `InterviewSubjectCleanupWorker` and `InterviewWorkflowWorker.publisher` under `BackgroundWorker.resource` (cleanup restarts after `restartDelay`; publisher observed, not restarted, because producer fencing must stop it); `SearchSessionHandoffTimingSpec`, `InterviewSubjectCleanupWorkerRestartSpec` ARR-06.1 | `sbt test` | 2026-10-15: 1307 passed. `publisher` kept: used by three IT specs. Reviews pending |
| ARR-07 | `service/WorkerHeartbeats.scala` (components, staleness, pending-work age), `config/ReadinessConfig.scala` (`readiness` section: worker staleness 60 s, outbox age 300 s, closure-work age 1800 s), `HealthService.report`, `/ready` body `{"status":"READY","degraded":[...]}` (503 body unchanged); outbox age via `event_outbox_claim` index (covered, 1 key, 0 docs); closure-work age; `WorkerHeartbeatsSpec`, `ReadinessDegradedSpec`, `ReadinessConfigSpec`, `RecruiterJobClosureWorkerAliveSpec`, `MongoPendingWorkAgeIntegrationSpec`, ARR-07.1 in `HiringPlatformServerSpec`, ARR-07.2 in `RecruiterJobClosureIntegrationSpec` | `sbt test`, `sbt it:test` | 2026-10-15: `sbt test` 1338 passed; full IT 306 passed, 25 skipped (Kafka), 0 failed. Kafka heartbeat wiring untested (needs a broker). Reviews pending |
| ARR-08 | Deletion transaction closes at most one page (`JobClosurePageSize`, 1-1000) and upserts a `recruiter_job_closure_work` row; `MongoRecruiterJobClosure.closeNextPage` (one page per transaction, deterministic `JOB_CLOSED` event ids); `RecruiterJobClosureWorker`; migration `020_recruiter_job_closure_work` (expand-only, strict validator); deletion receipt COMPLETE only when no closure row remains; submit gate requires the job's recruiter to be active; `RecruiterJobClosureIntegrationSpec` | `sbt it:test` | 2026-10-15: 2500 jobs closed 1000 + 1000 + 500, exactly 2500 events; crash rolls back and resume emits 35 events for 35 jobs. Deletion status PENDING until the last page removes the row (tested). Explain (2500 seed): page query IXSCAN `jobs_recruiter_status_created_id`, 1000 keys/docs examined, no blocking SORT; remaining-check and both submit-gate reads 1/1. Full `sbt it:test` 298 passed, 25 skipped (Kafka), 0 failed. Reviews pending |
| ARR-09 | Goldens (step 0) and `shared/crypto/CriteriaDigest` (step 1); `service/events/SearchFilterSnapshot` + `infrastructure/events/SearchFilterWireCodec` (step 2); `OperationalEventEnvelope.payload` typed, `service/events/OperationalEventContract`, `infrastructure/events/OperationalEventWireCodec` (moved verbatim), size check where bytes exist (step 3); glossary terms (step 4) | Golden specs, `sbt test`, `sbt it:test`, grep gate | 2026-10-15: golden literals unchanged and green; `sbt test` 1322 passed; targeted IT 27/27; full `sbt it:test` 301 passed, 25 skipped (Kafka, unverified, including two edited Kafka specs), 0 failed; no `io.circe` under `service/`. Behaviour notes: stored outbox reads skip the size check (checked on write); click verification now compares the normalized `resultId`; stricter filter decode. Reviews pending |
| ARR-10 | Not implemented | `sbt test`, doc check | Not run |
| ARR-11 | Not implemented | Full suite, contract capture | Not run |
| ARR-12 | Not implemented | `sbt test`, negative check | Not run |
| ARR-13 | Not implemented | Opt-in `sbt it:testOnly` | Not run |
| ARR-14 | `api/admission/{FixedWindowRateLimiter,CostBudgetLimiter}`, `costBudget` reducer (last in `queryReducers`), `config/GraphQLConfig{Reader,Validation}`, 429 mapping in `GraphQLHttpRoutes`; `FixedWindowRateLimiterSpec`, `GraphQLCostBudgetSpec`, `CostBudgetConfigSpec` | `sbt test scalafmtCheck Test/scalafmtCheck` | 2026-10-15: 1251 passed, 0 failed. Follow-up: validate `max-cost` >= per-query complexity cap (1000). Reviews pending |
| ARR-15 | `graphql.schema-exposure` (default false, on in `local.conf`); AST check for `__schema`/`__type` in `HiringGraphQLSchema.executeInContext` (FORBIDDEN); `/schema.graphql` routed only when on; `SchemaExposureSpec`, `CostBudgetConfigSpec` (also ARR-14 `max-cost >= 1000`) | `sbt scalafmtCheck Test/scalafmtCheck test` | 2026-10-15: 1260 passed, 0 failed. Reviews pending |
| ARR-16 | Not implemented | `sbt test`, `sbt it:test` with query counting | Not run |
| ARR-17 | Not implemented | `sbt it:test` with command monitoring, workload record | Not run |
| ARR-18 | Not implemented | `sbt test` route tests | Not run |
| ARR-19 | Not implemented | Spike result, route test if enabled | Not run |
| Final | — | `scripts/check-local.sh`, `python3 -I scripts/check-skills.py` | Not run |

## Performance and cost

- ARR-04, ARR-13: local Docker replica set, bounded datasets; record concurrency, p50/p95/p99, errors and conflict rates. No figure here is a result.
- ARR-17: seeded workload per the graph access spec (10 000 jobs, 50 000 applications, 20 000 users; 1 and 16 concurrent clients); compare with a fixed per-root projection before adopting field-aware logic.
- ARR-18: record request bytes and parse time saved against the existing document cache.
- No paid infrastructure. Large benchmarks need separate scope.

## Slice C design (Software Architect, 2026-10-15)

Source facts: Circe is imported in `service/` only by `events/OperationalEvent.scala`, `events/OperationalEventPayload.scala`, `search/SearchContracts.scala` and `search/SearchSessionRecording.scala`. Mongo stores the payload as `payload.noSpaces` and `envelopeBytes`; Kafka publishes the stored `envelopeBytes` unchanged, so byte identity in Mongo also fixes Kafka bytes. The outbox only ever holds validated payloads; quarantine keeps raw bytes. `filterJson` writes three shapes: job criteria `{"city","skills","createdAfter"}`, `{}`, `{"jobId"}`. Layer exceptions today: `ProducerGenerationRetirementMain` imports `runtime` and `infrastructure`; `MongoProducerGenerationMaintenance` imports `infrastructure.kafka`; `shared/HiringHttpPaths` imports http4s `Uri`.

### ARR-09 decisions

- C1 Codec home: `infrastructure.events.OperationalEventWireCodec` (the former `OperationalEventJson` plus payload codecs, moved verbatim). Mongo and Kafka share it; `repository` may depend on `infrastructure.events` only.
- C2 `OperationalEventEnvelope.payload` becomes the `OperationalEventPayload` ADT, with pure `eventType`/`aggregate` accessors. A pure `OperationalEventContract.validate` in `service` holds the semantic rules; the wire codec adds the `MaxEnvelopeBytes` check where bytes exist (outbox record, sequence insert, Kafka decode). Outbox `sameEvent` compares typed payloads. Migrations pattern-match typed payloads with unchanged errors and ledger ids.
- C3 `SearchSession.filter` becomes `SearchFilterSnapshot` (`JobCriteria`, `Unfiltered`, `ForJob`) in `service.events`; codec in `infrastructure.events` with the same key order; decoding accepts only the three written shapes (stricter, accepted under the pre-MVP single-shape policy). The API builds snapshots.
- C4 Cursor fingerprint: `shared/crypto/CriteriaDigest` with primitive parameters, same `Json.arr(...).noSpaces` code moved verbatim; `NearbyJobCursor.fingerprint` calls it after normalization.
- Step order: (0) golden byte spec `OperationalEventWireContractSpec` for all 9 event types, three filter shapes and fingerprint goldens, green on the current code and unchanged afterwards; (1) `CriteriaDigest`; (2) filter snapshot; (3) payload ADT and codec move; (4) glossary terms. Gate: no `io.circe` under `service/`.

### ARR-10 decisions

- Package `com.example.graphQL.cats.cli` with thin IOApps `EmbeddingWorkRepair`, `ProducerGenerationRetirementMain`, `MongoWorkflowIntegrityAuditMain`. `MongoWorkflowIntegrityAudit` keeps logic in `repository.mongo` with a public `run`; a public `MongoEmbeddingWorkMaintenance` facade avoids widening `private[mongo]`. Exit codes and output unchanged. Update `runMain` references in `docs/mongodb-design.md`, `docs/schema-evolution.md`, `docs/big-data-architecture.md`, `README.md`, `ARCHITECTURE.md`.

### ARR-11 decisions

Mechanical, after ARR-09; no behavior or visibility change; callers kept via delegation or Scala 3 `export`; target at most about 500 lines per file. Splits: operational-event repositories one file per class; semantic search pipeline objects to `MongoSearchPipelines`/`MongoSemanticSearchResult`; migration steps to `MongoOutboxSubjectReferenceMigration` and `MongoAnalyticsMigrationSteps`; codecs to `MongoUserCodecs`, `MongoJobCodecs`, `MongoOperationalEventCodecs`, `MongoCodecSupport` behind a `MongoHiringCodecs` export facade; interview command queue to support, notification-repair and attempts components by composition.

### ARR-12 decisions

ArchUnit `1.5.1` (Test) in the root build; `architecture/HiringLayeringSpec` imports main classes only. Rules: domain and service free of outer layers and framework libraries (service Circe rule after ARR-09); api free of repository/infrastructure/runtime/cli and Mongo; infrastructure free of api/repository/runtime/cli; repository free of api/runtime/cli and of infrastructure except `infrastructure.events` (after ARR-09/10; `MongoProducerGenerationMaintenance` either receives the id or is the single named exception); config depends only on domain and shared; nothing depends on `runtime`, `cli` or `Main`; IOApps only in `cli` and `Main`; `shared` free of outer layers (http4s allowed for `HiringHttpPaths`, recorded); `com.example.hiring.saga` uses only the standard library; main classes never use test-support, MUnit or Testcontainers. Negative check: a real (not import-only) Circe use in a service file must fail the spec.

## Implementation handoff

Order (one author at a time on `HiringGraphQLSchemaAssembly.scala`, `hiring.graphql`, `MongoApplicationRepository.scala`):

1. Slice A: ARR-01 → ARR-14 → ARR-15 (Scala Developer, `api/graphql/*`, `api/http/*`, `api/admission/*`, `config/*`, tests, fixtures, `docs/api.md`); ARR-02 → ARR-03 → ARR-04 (Data Engineer then Scala Developer, `repository/mongo/*`, `service/application/*`, `src/it/*`).
2. Slice B: ARR-05, ARR-06, ARR-07, ARR-08 (Scala Developer; Data Engineer for ARR-07 query and ARR-08).
3. Slice C: Architect note for ARR-09 in this spec, then ARR-09 → ARR-10 → ARR-11 → ARR-12 (Scala Developer).
4. Slice D: ARR-13 (QA Engineer with Scala Developer harness).
5. Slice E: E0 Architect spike updates the graph access spec; then ARR-16 → ARR-17 → ARR-18 → ARR-19.
6. Technical Writer consolidates `ARCHITECTURE.md`, `docs/api.md`, `docs/mongodb-design.md`, `docs/architecture-review.md` status.

Risks: ARR-02 fence writes add contention on per-user fence documents; ARR-09 can silently change wire bytes (mitigated by contract specs); ARR-14 can refuse legitimate heavy clients (budget is configuration); ARR-18 caching can leak per-actor data (D5 headers and tests).

Recovery: all items are code or configuration except ARR-08, which may add a work index or collection through a versioned Mongo migration with verification.

## Checkpoint

- 2026-10-15: spec written from the approved plan.
- 2026-10-15: ARR-01 implemented (`sbt test` green). ARR-03 written; it exposed defect DEF-1: Mongo write conflicts (112) inside guarded writes are mapped to `Unavailable` instead of being retried by `MongoTransactionRunner` or mapped to `Conflict`. Next: Data Engineer fixes DEF-1 and measures ARR-04.
- 2026-10-15: ARR-14 implemented (key = verified actor id, else trusted-proxy client address; synchronous in-memory charge in the reducer; fail-closed). Next: ARR-15.
- 2026-10-15: ARR-15 implemented; ARR-14 follow-up closed. Next: independent Code Reviewer and Security Engineer on ARR-01/14/15 while DEF-1/ARR-04 continue.
- 2026-10-15: DEF-1 fixed in `MongoRepositorySupport.guard` (transient errors re-raised to `MongoTransactionRunner`; new `guardCleanup`); breaker still counts only dependency unavailability. ARR-04 implemented. Full `sbt it:test`: 279 passed, 25 skipped, 4 failed (`MongoAccountRegistrationIntegrationSpec` x2, `MainProcessSpec` x2), suspected config regression from ARR-14/15. DEF-2 open: services stamp microsecond `Instant`s but Mongo stores milliseconds, so idempotent replays differ from the first response. Next: fix the IT regression and DEF-2 at runtime wiring.
- 2026-10-15 Security Engineer verdict on ARR-01/14/15: FAIL.
  - F1 (Medium-High): the fail-closed cost-budget cache refuses every unseen key once full; about 10 000 cheap keys per minute (one IPv6 /48 suffices) locks out all new clients, authenticated included. The same design is planned for ARR-05, which must change too. Fix: separate actor and anonymous capacity, plus a bounded striped-counter overflow instead of refusal; lockout test.
  - F2 (Low): anonymous callers share one bucket when the peer address is missing or `trusted-proxy-cidrs` is unset behind a proxy; document it.
  - F3 (Low, exposure on only): aliased `__schema` is cheap; weight introspection fields.
  - F4 (Low-Medium, unverified, pre-existing): nested-fragment expansion costs CPU before any charge; probe and cap document size if slow.
  - F5 (Info): confirm `AuthRateLimiter` covers every password-hashing mutation.
  - Test gaps: alias, `@include` and multi-operation introspection; `first` via variables; "no did you mean" body.
  - Note: `src/main/resources/local.conf` is git-ignored, so the ARR-15 local default is not shipped; docs must point to `GRAPHQL_SCHEMA_EXPOSURE`.
- 2026-10-15 Code Reviewer verdict on ARR-01/14/15: PASS (1260 tests green). Recommended: ARR-01 tests for `first` via variable, fragment, alias and nested connection; same F1 shared-cache concern; non-atomic bucket cap and `cleanUp()` per request when full; single extractor in adapter; docs/api.md to state page-size refusal is 200 `VALIDATION_FAILED`; application.conf comment order; ARR-15.2 test to compare with `hiring.graphql` snapshot; test that a page-size refusal does not charge the budget.
- 2026-10-15: DEF-2 fixed: `shared/MillisecondClock` wraps `Clock[IO]` once in `runtime/MongoHiringRuntime.scala` (including the implicit clocks of `Idempotent` and `InterviewSchedulingService`); `MillisecondClockSpec`; ARR-03 spec reuses it. Remaining full-precision stamps (not service responses): `MongoInterviewWorkflowRepository.live` leases, `InterviewSchedulingRuntime` quarantine, resolver `IO.realTimeInstant`. The four earlier IT failures did not reproduce on the current tree (likely a mid-edit config state). `sbt test` 1263 passed; full `sbt it:test` 283 passed, 25 skipped (Kafka-dependent), 0 failed.
- 2026-10-15: ARR-14/15 fix round done (`sbt test` 1294 passed, format green).
  - F1: `FixedWindowRateLimiter(overflowBuckets)` never evicts tracked buckets; unseen keys beyond capacity share N hashed overflow counters; `CostBudgetLimiter` has separate actor and client limiters; `InterviewActionRateLimiter` moved to overflow; new config keys `max-actor-buckets`, `max-client-buckets`, `overflow-buckets`, `auth.*.overflow-buckets`.
  - F2: proxy note in docs/api.md and README (`http.trusted-proxy-cidrs`).
  - F3: one `weightedComplexity` reducer for cap and charge; +200 per `__schema`/`__type`.
  - F4 probes (`HiringGraphQLDocumentExpansionSpec`, warm JVM): 194 ms, 80 ms, 347 ms, 214 ms, 173 ms; no cap added.
  - F5: only `signUp` and `login` handle passwords; guard test added.
  - Review test gaps, adapter extractor, docs and conf order addressed.
- 2026-10-15: ARR-05 implemented. `MongoDatabaseProbeSpec` is timing-sensitive under parallel sbt load (QA to confirm on a quiet machine).
- 2026-10-15: ARR-02 implemented (see Q1). Next: Code Reviewer and Security Engineer on DEF-1, ARR-04, DEF-2, ARR-02 (repository and service changes).
- 2026-10-15 Security Engineer re-review of ARR-01/05/14/15: PASS (62 targeted tests green). F1-F5 fixed. To fix before close: R1 (Medium) docs overclaim "never a lockout": overflow stripes can be exhausted, so unseen clients can still be refused for a window; R2 (Low-Medium) unseeded stripe hash lets an IPv6 /48 holder target a victim's stripe; R3 tests (all stripes exhausted, capacity overshoot under concurrency); R4 probe with repeated response names across fragment levels. Sent to the ARR-14/15 author.
- 2026-10-15 Security Engineer on DEF-1/ARR-02/ARR-04 (static): FAIL.
  - S1 (Medium): `updateStatusWithSession` replaces the whole document filtered only by `_id` and previous status, so a forged `Application` (victim id, recruiter-owned `jobId`) passes the ownership check and re-parents the victim's application. Not reachable from the API today, but defeats ARR-02.3. Fix: add `jobId` and `candidateId` to the filter (or `$set` status fields only) plus a forged-`jobId` store test.
  - S2 (Low): the interview workflow's status write has its own equivalent fence (`fenceParticipants`); note it in the glossary and consider a shared helper.
  - S3 (Info, pre-existing): NotFound vs Forbidden in `changeStatus` reveals application id existence.
  - Clean: actor provenance, deletion race, Admin singleton, fence upsert rollback, bounded DEF-1 retry, no other application writers.
- 2026-10-15 Code Reviewer on DEF-1/ARR-02/ARR-04/DEF-2: PASS (`sbt test` 1296 passed; 13 targeted IT passed). No non-transactional site turns a transient error into a 500; breaker unaffected; retry sleep at most about 1 s. Items: F1 = S1; F2 `SearchSessionRecording` still uses the full-precision clock; F3 stale test comment; F4 no unit spec for `guard`; F5 unused `JobSubmissionSnapshot.revision`; F6 log noise on retried conflicts; F7 document client retry contract for `CONFLICT` (8 of 16 parallel submits conflicted in this run); F8 race-test delete helper retry is time-sensitive. All sent to the Data Engineer with S1/S2. S3 (NotFound vs Forbidden oracle) is pre-existing and deferred: changing it alters the public error contract.
- 2026-10-15: rate-limiter R1-R4 fixed (`sbt test` 1300 passed, format green). R1 wording corrected in docs/api.md, application.conf and scaladoc; `overflow-buckets` default 64 -> 256. R2 stripe = seeded MurmurHash3 (SecureRandom seed per instance, never logged). R3 tests: all stripes exhausted refuses unseen keys; overshoot <= maxBuckets + racers (4000 keys, 64 racers); stripe balance and per-instance difference. R4 repeated-name 60-level DAG probe: 70 ms warm. Next: Security confirmation of R1/R2 together with S1.
- 2026-10-15: persistence fix round done. S1: status replace filter now also matches stored `jobId` and `candidateId`; forged-id IT added (ARR-02.3). S2 glossary row updated (no shared helper: the interview fence covers several participants and bumps `users.version`). F2 clock, F3 comment, F4 `MongoRepositoryGuardSpec`, F6 retried conflicts no longer logged per attempt (one `transaction.exhausted` log), F8 robust delete helper and fence duplicate key mapped to `Conflict`. F5 kept: `JobSubmissionSnapshot.revision` still validated by the codec; removal is a follow-up touching `domain/model`. F7 retry contract added to docs/api.md "Errors and limits". `sbt test` 1303 passed; targeted IT 38 passed; full `sbt it:test` 291 passed, 25 skipped (Kafka), 0 failed. Next: Security confirmation of S1 and R1/R2, then QA on Slice A.
- 2026-10-15 Security Engineer confirmation: ARR-01/05/14/15 PASS; ARR-02/04, DEF-1, DEF-2 PASS (33 unit + 8 IT targeted tests green). docs/api.md overflow wording re-read by the coordinator: accurate. Slice A code and security gates closed; QA pending (run together with Slice B on a quiet tree to avoid concurrent-compile noise). In progress: ARR-06, ARR-08.
- 2026-10-15: ARR-06 and ARR-08 implemented; ARR-09 steps 0-1 done. Next: Code Reviewer and Security Engineer on ARR-06/08; ARR-09 steps 2-4; then ARR-07 (shares the outbox repository file with ARR-09).
- 2026-10-15 Security Engineer on ARR-06/08 (static): PASS. Deletion cannot report COMPLETE early, the work row holds no PII, the worker backs off 1-30 s, logs carry no user data, and no API path triggers closure. Findings: H1 (Medium-Low) head-of-line blocking: a malformed or permanently conflicting job in the oldest work row stalls every later recruiter closure and keeps that deletion PENDING forever; fix by rotating or cooling down a failing row, or document an operator repair runbook. H2 (Low) record an alert on the age of the oldest `recruiter_job_closure_work` row as a deployment gate (feeds ARR-07). H3 (Info) page size fixed at the maximum 1000.
- 2026-10-15 Code Reviewer on ARR-06/08: PASS with required follow-ups. Runtime re-run blocked by concurrent ARR-09 compile errors (to repeat on a quiet tree). Findings: poison head (same as H1, needs a test); idle polling opens a transaction every second and schedule/page size are not wired to config; submit-gate write-skew for jobs beyond the first page (an application can land on a deleted recruiter's job before the worker closes it; document, do not fence the recruiter); `RuntimeFailed` without detail in one handoff branch; dead interview publisher needs a documented process restart; `null.asInstanceOf` test fixtures; re-enqueue repair has no mechanism; docs/api.md deletion status wording. Fix round sent to the Data Engineer.
- 2026-10-15: ARR-06/08 fix round done (`sbt test` 1322 passed; closure IT 10/10; format green). Work rows carry `attempts` and `notBefore`; a failing head is rescheduled with capped backoff (default 5 s to 15 min) and never dropped; poison-row test shows recruiter B finishes while A stays PENDING. Migration 020 amended in place (unreleased, uncommitted; a developer database that ran the earlier local draft must drop the collection and its ledger row, documented). Idle peek opens no transaction (tested). New typed `recruiter-job-closure` config (no dedicated config test yet). Submit-gate window and manual re-enqueue `mongosh` repair documented in docs/mongodb-design.md; dead publisher needs a process restart (documented); docs/api.md deletion status updated; test stubs replace nulls. Open: config test for the new block; full `sbt it:test` rerun; recommendation to make `InterviewSubjectCleanupWorker.restartDelay` configurable (follow-up).
- 2026-10-15: ARR-09 implemented. Older specs `hiring-event-and-test-reliability.md` and `hiring-retrieval-publication-reliability.md` still name `OperationalEventJson` as historical records (now `OperationalEventWireCodec`). Next: ARR-07; Code Reviewer and Security Engineer on ARR-09 and confirmation of the ARR-06/08 fix round.
- 2026-10-15 Security Engineer on ARR-09: PASS (45 targeted unit tests green; IT not run by the reviewer). Kafka decode checks size before parsing and quarantines with constant reasons; outbox writes validate contract and size; every semantic rule retained; erasure subject ids unchanged; filter snapshot stores no extra data; cursor fingerprint goldens unchanged; click `resultId` comparison equivalent. Observations: add a comment that the stored outbox read relies on write-time size checks; quarantined raw bytes retention is out of scope (pre-existing).
- 2026-10-15 Code Reviewer: ARR-09 PASS; ARR-06/08 fix round PASS (`sbt test` 1322 passed; 21 targeted IT passed). Follow-ups, queued after ARR-07 because files overlap: B1 (required) config spec for `recruiter-job-closure`; A1 test that write-side validation returns `EnvelopeTooLarge`; A4 comment on the stored outbox read; A5 `candidateMatches` golden; B3 drop the optional-`notBefore` legacy branch (pre-MVP one shape); B4 reset `attempts` after a successful page. Correction (A3): the service click check still compares the raw string; only the migration compares the canonical UUID; both equivalent for stored data.
- User instruction 2026-10-15: commit when the in-progress work is done (interpreted as Slices A, B and ARR-09 after ARR-07, its reviews, follow-ups and QA). No push.
- 2026-10-15: ARR-07 implemented. Behaviour change (follows D6): an unhealthy embedding worker no longer makes `/ready` return 503; it is listed as `embedding-worker` in `degraded`, because search must not gate hiring writes. Each `/ready` runs two limit-1 reads (500 ms cap) with no cache. `recruiter_job_closure_work` has no `requestedAt` index (tiny collection; add if it can grow). Next: follow-ups (B1, A1, A4, A5, B3, B4, closure index decision), then Code Reviewer and Security Engineer on ARR-07, then QA.
- 2026-10-15: follow-ups done (`sbt test` 1346 passed; targeted IT 28 passed; full IT 307 passed, 25 skipped, 0 failed). B1 `RecruiterJobClosureConfigSpec` (7 tests; order error only when both values are in range); A1 `validate`/`outboxRecord` take an optional byte limit so `EnvelopeTooLarge` is tested; A4 comment; A5 `candidateMatches` golden (new, existing literals untouched); B3 retry fields required in migration 020 (amended, unreleased); B4 successful page resets `attempts`/`notBefore` (tested); no closure-work index (tiny transient collection; documented with a revisit trigger).
- 2026-10-15 Security Engineer on ARR-07 and follow-ups (static): PASS. Recommended: R7-1 docs note that `/ready` must not be exposed on the public edge (degraded names reveal enabled subsystems, no data); R7-2 short TTL single-flight cache around `HealthService.report` (probes skip admission; each call now costs about 3 cheap Mongo operations), plus bounding the unmetered probe path; R7-3 log volume then bounded per TTL window; R7-5 optionally restrict the byte-limit override to package visibility. Embedding 503-to-degraded change is security-neutral.
- 2026-10-15 Code Reviewer on ARR-07 and follow-ups: ARR-07 PASS conditional on docs fixes; follow-ups PASS (`sbt test` 1346 passed; 25 targeted IT passed; format green). F1 (Medium, docs): a publisher whose sends keep failing still beats (failures are absorbed into retry/Failed), and Failed rows appear nowhere in readiness; outbox-age catches only stalled claims. F2 stale `/ready` embedding text in README, docs/wiki/security-and-operations.md, dependency-circuit-breakers.md. Low: consumer ticker proves stream liveness only (documented); shared handoff beat masks dead parallel workers; duplicate log on concurrent probes; oldest-due approximation; redundant filter; missing beat tests. Fix round (with security R7-1/2/5) sent to the ARR-07 author.
- 2026-10-15: ARR-07 fix round done (`sbt test` 1355 passed; full IT before the last change 307 passed, 25 skipped, 0 failed; `HiringPlatformServerSpec` 10/10 after it). `service/ReadinessCache` single-flight cache (`readiness.report-cache-millis`, default 1000 ms; a state change can take up to the TTL to show). A first-cut probe gate answered 503 when saturated, which would let anyone pull the instance from rotation; it was removed, and 64 concurrent probes now all get the real status with one collection. Handoff heartbeat bug (a store failure counted as progress) fixed and tested. Docs F1/F2 and the internal-only `/ready` note done. R7-5 (narrower byte-limit visibility) skipped as optional. Next: Security confirmation of ARR-07 fixes and QA on the full tree.
- 2026-10-15 Security Engineer confirmation of the ARR-07 fix round: PASS (13 readiness tests green). Single-flight cache has no stranded waiters, failures are not cached, cancellation is safe; `/ready` never refuses for load; TTL bounded 100 ms-10 s; R7-5 skip acceptable. Residual: blocking `/ready` at the edge is a deployment control (unverified here).
- Earlier next step (done): one fix round for ARR-14/15 covering security F1-F3 and review items, after the IT regression/DEF-2 fix lands (shared config files). Then Security re-review. QA verdict: pending.
