# Hiring Platform Functional Refactoring

## Identity and scope

- Task: Current-code review and staged refactoring plan for functional boundaries, correctness, and avoidable duplication.
- Status: in progress.
- Coordinator: Product Manager. Implementation owners: Scala Developer and Data Engineer for persistence work.
- User outcome: Preserve the current Hiring API while making important writes durable, boundaries total and typed, and framework interop smaller and easier to test.
- Authorized scope: Scala/Cats Effect, FS2, http4s, Sangria, MongoDB reactive driver, Circe, PureConfig/Iron, and current tests/docs.
- Non-goals: new dependencies, a generic repository/service framework, effect-system migration, changing public GraphQL names, Kafka/Saga work, a cache, or a broad rewrite of stable custom components.
- Dependencies: P0-A needs replica-set Mongo integration infrastructure and a Data Engineer migration review. Every implementation slice needs current Code Reviewer, Security Engineer, and independent QA verdicts.

## Verified context and decisions

### Current architecture

`HiringApiRoutes` is the http4s boundary; Sangria schema/resolvers bridge effectful fields through the request-scoped Dispatcher; application services own authorization and use repository ports; `MongoHiringRuntime` composes Mongo adapters. Domain models and lifecycle policies are free of HTTP, GraphQL, driver, and configuration dependencies.

The following existing facilities are correctly used and must be retained:

- `Resource` owns Mongo, Ember, Dispatcher, setup, and embedding-pipeline fibers.
- FS2 uses a bounded `Queue` and bounded `parEvalMap` for embedding processing.
- Sangria deferred fetchers batch nested users/jobs, reducers bound depth/complexity, and the execution error limit is set.
- http4s provides entity limiting and request IDs; Cats Effect provides admission, timeouts, cancellation, `Ref`, `Deferred`, and `IOLocal`.
- PureConfig plus Iron own configuration decoding and basic refinements. The custom cross-field/IP/JWT rules are justified, not duplicated parsing to remove.

### Source facts

- Job and candidate changes persist their coalesced `EmbeddingWork` record in the same repository transaction; services only wake the worker after commit: `src/main/scala/com/example/graphQL/cats/repository/mongo/MongoHiringRepositories.scala` and `src/main/scala/com/example/graphQL/cats/service/job/JobService.scala`.
- `EmbeddingWorkPublisher` is wake-only. The durable publisher owns explicit test/support enqueueing, and periodic scanning remains the recovery path for a dropped wakeup: `src/main/scala/com/example/graphQL/cats/service/search/EmbeddingPipeline.scala`.
- Content negotiation chooses the maximum quality across all matching ranges instead of resolving the most-specific range for each offered representation first: `src/main/scala/com/example/graphQL/cats/api/http/HiringApiRoutes.scala:73`.
- BSON codecs can throw for malformed IDs, enum values, missing values, or invalid profiles, while read ports cannot represent a data-integrity failure: `src/main/scala/com/example/graphQL/cats/repository/mongo/MongoHiringCodecs.scala:33` and `src/main/scala/com/example/graphQL/cats/repository/protocol/HiringRepositories.scala:11`.
- Account use cases repeat a narrower version of the principal validation already centralized in `ActorAuthorization`: `src/main/scala/com/example/graphQL/cats/service/auth/UserAccountService.scala:72` and `src/main/scala/com/example/graphQL/cats/service/auth/ActorAuthorization.scala:11`.
- Runtime composition repeats bounded/traced service construction in both vector-search branches: `src/main/scala/com/example/graphQL/cats/runtime/MongoHiringRuntime.scala:116`.
- The reactive bridge's general collection operation requests `Long.MaxValue` and collects into a `Vector`: `src/main/scala/com/example/graphQL/cats/repository/mongo/PublisherBridge.scala:76`.
- `UserPageRequest` keeps `pageSize` as an unvalidated `Int`, unlike the other connection ports, and Mongo passes it directly to `.limit`: `src/main/scala/com/example/graphQL/cats/domain/model/Account.scala:17` and `src/main/scala/com/example/graphQL/cats/repository/mongo/MongoHiringRepositories.scala:272`.
- Embedding-work claim tokens allocate with `IO.randomUUID`, preserving effect ownership: `src/main/scala/com/example/graphQL/cats/repository/mongo/MongoEmbeddingWorkRepository.scala`.
- The pure reciprocal-rank implementation has internal mutable state even though it need not expose mutation: `src/main/scala/com/example/graphQL/cats/shared/search/HybridRankFusion.scala:12`.
- `JobLifecycle` uses `State[Job, Either[DomainError, Job]]` but every service caller immediately runs the transition against that same job: `src/main/scala/com/example/graphQL/cats/domain/policy/JobLifecycle.scala:18` and `src/main/scala/com/example/graphQL/cats/service/job/JobService.scala:55`.

### Workload and cost assumptions

No measured bottleneck, production dataset, concurrent workload, SLO breach, or budget was supplied. P0 work protects correctness, not a performance claim. P2 collection bounds must be measured before selecting a cap; P3 optimization is limited to preserving the current deterministic result. No paid provider or large dataset is authorized.

## Refactoring slices, in severity order

### P0-A: Atomic embedding work handoff

**Problem.** A job write can commit and then report failure when durable embedding work cannot be enqueued. Candidate account/profile writes do not enqueue work at all. A bounded in-memory wakeup is useful for latency but cannot be the durability boundary.

**Smallest viable design.** Add capability-named transactional repository operations that atomically write the changed job or candidate profile and idempotently upsert its `EmbeddingWork` record in the same Mongo session. The service receives the committed aggregate result. After commit, it asks the existing `DurableEmbeddingWorkPublisher` only to wake the worker; a dropped wakeup remains safe because the existing periodic scan finds durable work. Keep the vector-disabled `EmbeddingWorkPublisher.noop` composition.

**Rejected alternatives.** Do not introduce a generic outbox, Saga, Kafka producer, or detached fiber. Do not retain sequential aggregate-write then `publish` calls, because they cannot express the required atomic outcome.

**Data evolution.** Add a forward, resumable setup/backfill step using the existing migration ledger. It must enqueue current jobs and candidate profiles idempotently in bounded batches, tolerate restart/concurrent application writes, verify counts/keys, and never edit an applied migration. Define the recovery point separately from application rollback.

**Plan.**

1. Extend only the affected repository protocols with business-named atomic methods and return typed `RepositoryError` results.
2. Implement aggregate update/insert plus work upsert in the current Mongo transaction runner; preserve optimistic version checks and unique work keys.
3. Refactor `JobService` and `UserAccountService` to call the atomic capability. Make signup, profile update, job create, update, publish, and close declare exactly which embedding work they produce.
4. Retain the current FS2 worker and bounded queue as post-commit notification; make enqueue/worker failures observable without turning a committed mutation into an ambiguous API failure.
5. Add the forward backfill and Mongo setup verification, then update the vector-search docs/spec evidence.

### P0-B: Specificity-correct HTTP representation negotiation

**Problem.** `Accept` handling can let a broad wildcard override a more-specific declaration or explicit exclusion of `application/graphql-response+json`.

**Smallest viable design.** Keep http4s's typed `Accept`, `MediaRange`, and `QValue` types. Add one local pure selector: for each offered type, choose its most-specific matching header range (with header position as a deterministic tie-breaker), honour `q=0`, then select between offered types by quality and existing server declaration order.

**Non-goals.** Do not add a custom parser, alter GraphQL payloads, or change accepted request media types.

**Plan.**

1. Extract the current selection loop into a package-visible pure helper with offered media types as data.
2. Implement specificity before quality and explicit exclusion semantics.
3. Preserve the current default when `Accept` is absent and all existing response headers.

### P1-A: Total stored-document decoding and typed corruption outcomes

**Problem.** Malformed or legacy BSON can escape as an untyped throwable, collapse an entire list/read, and become a generic GraphQL internal error.

**Smallest viable design.** Make `MongoHiringCodecs` decode total into `Either[StoredDocumentError, A]`. Evolve affected read ports coherently to return a typed read result, then map corruption at the Mongo/service boundary to a sanitized availability/data-integrity result. Do not scatter `try/catch` at resolver call sites.

**Compatibility.** Inventory supported historic profile/document shapes before changing decoder behavior. The decoder-first slice must remain compatible with valid legacy shapes. Only a separately approved migration may rewrite data.

**Plan.**

1. Define a non-sensitive stored-document error ADT near the Mongo adapter, including field/category but never raw BSON/PII.
2. Convert UUID, enum, required-field, profile, and numeric reads to total decoders.
3. Change only read protocol paths that need failure representation; propagate typed failures through `HiringReadService`, bounded/traced wrappers, GraphQL payload mapping, and tests as one coherent compile-driven slice.
4. Fail a batch deterministically rather than silently returning a partial list. Emit safe diagnostics with correlation.
5. Add migration preflight checks for malformed legacy records; do not claim data repair without a scoped migration.

### P1-B: One principal-resolution path for account use cases

**Problem.** Account operations repeat active-user checks but omit the stored-role and singleton-admin checks contained in `ActorAuthorization.resolve`.

**Smallest viable design.** Reuse `ActorAuthorization` inside account operations that require an authenticated principal. Keep public typed errors, including `PROFILE_UNSUPPORTED_FOR_ROLE` for Admin profile updates, and do not add a generic authorization framework.

**Plan.**

1. Resolve the actor once at the beginning of `me`, profile update, deletion, and list-users.
2. Apply the operation-specific rule after principal resolution; preserve the current self-only deletion and singleton-Admin contract.
3. Delete only redundant branches made unreachable by the shared resolver.

### P2-A: Focused runtime composition simplification

**Problem.** The two vector configuration branches independently construct the same bounded/traced read, job, application, account, and cursor services, making future behavior drift likely.

**Smallest viable design.** Extract one private, capability-named services assembler. It accepts the optional search service and the correct embedding-work publisher, then applies existing `BoundedHiringServices` and `TracedHiringServices` once. Preserve `Resource` ownership and vector startup validation.

**Non-goals.** No DI container, service locator, generic decorator framework, or public API change.

### P2-B: Explicit reactive-stream collection limits

**Problem.** `PublisherBridge.all` has no cardinality contract and makes unsafe collection easy to introduce later.

**Smallest viable design.** Retain the current cancellation-safe driver bridge because `fs2-reactive-streams` is not a declared dependency. Split `first` from a bounded `collect(maxItems)` API and introduce a paged/streaming adapter only where a real unbounded task requires it. Call sites state their bound.

**Plan.** Audit every `all` call, retain known-small metadata paths with an explicit cap, and use existing keyset/batched reads for scalable paths. Treat a cap breach as typed infrastructure/data error, never a partial success.

### P2-C: Business-owned persistence limits

**Problem.** The HTTP body cap limits a request but domain validators do not bound individual stored strings or collection cardinality. Such input can inflate indexes, writes, and embedding-provider work.

**Smallest viable design.** Define capability-owned pure constants and validation errors for persistent text and collection limits in domain model validation. Reuse the semantic document constraint where the semantics agree; do not use transport limits as a substitute for domain invariants.

**Plan.** Establish limits from the intended Mongo document/index and provider constraints, test exact boundary values, retain sanitized GraphQL errors, and prove rejected writes create no aggregate/event/work record.

### P2-D: Typed account-list pagination at every port boundary

**Problem.** GraphQL validates `first`, but the account service/repository contract can still receive an invalid raw page size and pass it to Mongo.

**Smallest viable design.** Change `UserPageRequest.pageSize` to the existing `PageSize` opaque type and convert at the API boundary. Reuse `MongoKeysetPaging.page` where its sorting/limit semantics match the account list. This removes a divergent guard rather than adding one.

**Plan.** Compile-drive the type through account resolver, cursor helper, account service, Mongo adapter, and fixtures. Add direct protocol-misuse and min/max tests; retain current user ordering and indexes.

### P3-A: Referentially transparent shared algorithms and resolver sources

**Problem.** `HybridRankFusion` uses local mutable state. Separately, resolvers draw time/UUIDs directly from `IO`, which is a valid transport effect boundary but makes deterministic resolver testing unnecessarily awkward.

**Smallest viable design.** First replace rank-fusion mutation with immutable `foldLeft` state only if its tested ordering and allocation cost remain acceptable. Then expose existing Cats Effect time/unique capabilities through the request/runtime composition so tests can supply deterministic values; leave service APIs explicit about time/IDs. Do not create a new custom guard or general capability framework.

**Plan.** Preserve all ordering ties and scores with property/example tests. Add only the minimal request-scoped time/ID dependency necessary for deterministic GraphQL resolver tests. Benchmark only if the immutable rank fusion is on a measured hot path.

### P3-B: Simplify only needless functional machinery and eager effects

`JobLifecycle` is a suitable pure domain boundary, but `State` adds no composition where each caller immediately invokes `runA` with the same aggregate. Replace it with direct total functions such as `publish(job, now): Either[DomainError, Job]`, preserving the current transition matrix and tests. Separately, move embedding-claim UUID creation inside `IO` (or use an injected Cats Effect UUID capability) so construction itself is referentially transparent and testable. Neither item changes public behavior.

### P3-C: Harden isolated Sangria input interop only with evidence

`HiringGraphQLInputs` legitimately adapts Sangria's `Any`-based marshaller, but it contains casts, map indexing, and a non-exhaustive nested job-input match. Keep this interop isolated; make the nested conversion total and add public malformed/nested/nullable input execution tests. Do not replace Sangria coercion or create a second validation framework unless a reproducible public failure appears.

### P3-D: Documentation reconciliation

README and planning text still describe a health-only/Foundation state while source serves account, job, application, cursor, and vector capabilities. Update a capability matrix and mark Atlas/Voyage/live-provider evidence separately as unverified. This is documentation correctness, not a public-contract change.

## Acceptance and evidence

| ID | Given / When / Then | Planned verification | Actual outcome |
|---|---|---|---|
| AC-01 | Given a job/candidate mutation, when the aggregate commits, then exactly one durable embedding-work key exists; when the transaction fails, neither exists. | Replica-set Mongo failure-injection integration tests | Not run |
| AC-02 | Given startup/retry after a committed work key, when the worker runs, then the aggregate is re-embedded once idempotently. | FS2 worker and Mongo integration tests | Not run |
| AC-03 | Given conflicting specific/wildcard `Accept` ranges and `q=0`, when GraphQL responds, then the most-specific allowed representation is selected or 406 is returned. | Focused `HiringApiRoutesSpec` cases | Not run |
| AC-04 | Given malformed BSON, when a read/list executes, then no raw exception/PII reaches GraphQL and the result is typed, deterministic, and diagnosed safely. | Codec/repository/GraphQL unit plus disposable Mongo tests | Not run |
| AC-05 | Given deleted, role-mismatched, or non-singleton-Admin actors, when any account operation runs, then it is denied consistently; valid account flows retain their current result. | `UserAccountServiceSpec`, JWT, and GraphQL access tests | Not run |
| AC-06 | Given enabled and disabled vector search, when runtime resources acquire/release, then common services behave equivalently and every fiber/client finalizes. | Runtime resource/finalization tests | Not run |
| AC-07 | Given a collection path, when result size exceeds its declared cap, then no unbounded accumulation or partial success occurs. | `PublisherBridgeSpec` demand/cancellation/cap tests | Not run |
| AC-08 | Given boundary-size persistent inputs, when validation runs through GraphQL, then accepted values persist and one-over-limit values produce existing sanitized validation payloads with no writes. | Domain, GraphQL, and Mongo integration tests | Not run |
| AC-09 | Given an account list request, when it crosses any port, then page size is a valid `PageSize` and Mongo ordering/limit remain unchanged. | Account resolver/service/repository tests | Not run |
| AC-10 | Given identical rank inputs and lifecycle transitions, when the direct pure functions run, then scores, tie ordering, statuses, and errors match current behavior without mutable state or unnecessary `State`. | `HybridRankFusionSpec` and lifecycle tests | Not run |
| AC-11 | Given embedding claim creation and malformed nested GraphQL input, when construction/execution runs, then IDs allocate only inside effects and invalid input becomes sanitized validation output. | Repository and GraphQL input execution tests | Not run |
| AC-12 | Given documentation and served schema, when local checks run, then capability statements and GraphQL fixtures are consistent with source. | SDL/operation fixtures, link/reference check, `git diff --check` | Not run |

## Implementation checkpoint

- Completed: repository failure ownership moved to `repository.protocol` with a service compatibility alias; obsolete time-less account/job write port overloads removed; malformed durable work keys fail terminally instead of being deleted; Voyage HTTP uses cancellation-aware `sendAsync`; GraphQL resolvers are separated by account, job, application, and search capabilities while schema assembly remains the composition root.
- Focused regression coverage added for malformed durable work keys and Voyage request cancellation/invalid endpoint handling. Existing GraphQL input tests remain in place; the resolver split requires a completed focused GraphQL suite for runtime proof.
- Verification: `git diff --check` passed. Repeated local SBT attempts ended at the execution environment's 30-second observation limit while compiling 14 sources, with no test summary; unit, integration, live Voyage, and Atlas evidence therefore remain unverified.
- Reviews: Security Engineer PASS by static review. Code Reviewer found the stale-source-fact issue above; corrected in this checkpoint. Follow-up review and independent QA remain required after a completed test run.

## Ordered implementation handoff

1. **P0-A** first. Owner: Scala Developer with Data Engineer. Allowed production paths: relevant `service`, `repository/protocol`, `repository/mongo`, `runtime`, setup/migration/docs, and focused tests. Reopen schema-evolution review because stored work/backfill changes.
2. **P0-B** can run independently after its pure selector tests are written. Owner: Scala Developer. Allowed paths: `api/http/HiringApiRoutes.scala` and route tests.
3. **P1-A** follows P0-A because both evolve persistence failure behavior. Owner: Scala Developer with Data Engineer. Its public error mapping requires Architect and Security review before code.
4. **P1-B** can run independently after P0-A's account publisher contract is known. Owner: Scala Developer.
5. **P2-A through P3** are serialized behind the preceding contracts; each must remain a narrow refactor with a focused regression test before broader cleanup. P2-D is independent once its direct protocol tests are in place.

Run `sbt test` after each code slice; run `sbt 'IntegrationTest / test'` for Mongo/GraphQL changes. Mongo transaction proof requires a replica set. Measure any claimed memory/latency change on a documented bounded local workload; missing live Atlas/Voyage infrastructure is unverified, not passing evidence.

## Historical checkpoint and review

The following checkpoint predates the current implementation checkpoint above; its historical review verdicts do not describe the current working tree.

- Implemented and unit-tested: transactional Mongo work upserts and resumable backfill definitions, Accept specificity and `q=0`, shared account-principal resolution, persistent input ceilings, typed account pagination, direct job lifecycle functions, immutable rank fusion, and effect-delayed embedding claim UUID allocation.
- Unit evidence: `sbt test` passed 243 tests, 0 failures, 0 errors. This does not prove replica-set transaction rollback/backfill recovery.
- Independent Code Reviewer: FAIL. Required remaining work: total typed Mongo decoding, bounded reactive collection API, runtime assembler/wakeup preservation, total Sangria nested input interop, and focused integration/regression coverage.
- Independent Security Engineer: FAIL because partial BSON decoding can still turn corrupted stored data into unsanitized runtime failures; the unbounded collector remains a resource-exhaustion risk.
- Independent QA: pending final verdict; static review also identified the missing corruption, collection-bound, and transaction/backfill evidence.
- Next action: complete P1-A and P2-A/B before calling this refactor complete; then add replica-set failure-injection coverage and repeat all independent gates.
