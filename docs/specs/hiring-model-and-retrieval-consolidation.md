# Hiring model and retrieval consolidation

## Identity and scope

- Task / use case / roadmap phase: Phase 8 (MongoDB/vector optimization) and Phase 9 follow-up. Cross-cutting review of the hiring application against data-modelling, functional-design and GraphQL best practice, with a refactoring plan. Touches UC01–UC11 reads and mutations.
- Status: S1 (read path) implemented and locally verified 2026-10-19, independent Code Reviewer, Security Engineer and QA verdicts pending; S2–S5 not started (plan awaiting user decision on which slices to authorize)
- Coordinator: Product Manager. Proposed owners per slice below.
- User outcome: each read request does the minimum number of store round trips and transfers only the fields it needs; each fact is stored and translated once; GraphQL clients can traverse the hiring graph in one request instead of one root query per level.
- Authorized scope: review only so far. No code was changed.
- Non-goals: everything already listed as a non-goal in [schema consolidation](hiring-mongo-schema-consolidation.md); event envelope versioning; frontend; paid infrastructure.
- Dependencies: slices S1–S3 are independent of the in-flight MSC work. S4 (graph access) is the existing draft [graph access spec](hiring-graphql-graph-access.md). S5 depends on MSC-05 (nulls) landing first to avoid conflicting edits in the codecs.

## Source context and decisions

### Method

Three independent read-only reviews (data modelling, FP design, GraphQL utilization) on the `refactor` working tree as of 2026-10-19, plus coordinator spot checks of the high-severity items. Nothing was built or run; "verified" means re-read in source. Explain plans were not captured; items marked *measure* need an `explain` or a counted workload before the claim is relied on.

Already tracked elsewhere and not repeated here: findings F-A–F-V in the schema consolidation spec, section 3 gaps of `docs/architecture-review.md`, and R1–R3 of the graph access spec.

### Findings

Paths are relative to `src/main/scala/com/example/graphQL/cats/`.

#### A. Data model and retrieval flow

| ID | Severity | Finding (verified) | Smallest fix |
|---|---|---|---|
| MR-01 | High | Actor and ownership lookups read whole `users`/`jobs` documents, including the embedding vector and `passwordHash`, with no projection: `MongoUserRepository.find/findVersioned` (`:45-65`), `MongoJobRepository.find` (`:34-41`), `MongoKeysetPaging.page` (`MongoRepositorySupport.scala:531-555`), and the `$lookup` stages in `relatedUsers`/`relatedJobs`. `ActorAuthorization.persisted` runs `users.find` on every authenticated request and decodes the vector. | Add `findActor(id)` projecting `_id, role, accountStatus, adminSingletonKey, name, email`; add `findOwner(jobId)` projecting `recruiterId, status`; exclude `embedding`, `embeddingMeta`, `passwordHash` from list reads and `$lookup` pipelines. Move `embedding` out of the domain `Job`/`User` aggregates into a separate read model (it is derived search-index state). |
| MR-02 | High (*measure*) | `findByCandidate` (`MongoApplicationRepository.scala:43-55`) runs `$match`, then the actor `$lookup`, then `$sort`, with `$limit` appended last, so the sort happens after the join and the `applications_candidate_*` indexes cannot order the page. `findByJob` and `history` correctly sort and limit inside the indexed pipeline. | Reorder to match, sort, limit, then actor check, as `authorizedDiscoveryPipeline` does. Capture `explain` before and after. |
| MR-03 | Medium | Redundant authorization round trips per read. `applicationHistory` (`HiringReadService.scala:44-72`) does `resolve` (users), `applications.find`, `jobs.find`, then `readScope` (users again), then an aggregation that re-checks the actor and access. `jobApplications` (`ApplicationService.scala:97-103`): 3 user reads, 2 job reads. `candidateMatches` reads the full job twice (`SemanticSearchService.scala:150,329`). | Drop pre-checks where the aggregation already enforces access; make `readScope` reuse the resolved `AuthenticatedActor`. Expected: 1 user read plus 1 aggregation per list read. |
| MR-04 | Medium | Search results persisted about 6 times per search: `SearchSession.results`, `SearchPerformed.results` (derivable via `OperationalEvents.searchPerformed`), both in `search_session_work`, then `search_sessions` and the outbox (F-D). A second non-queued `MongoSearchSessionRepository.save` path duplicates the write sequence. | Store only the session in the work row; rebuild the event at `complete`. Remove the non-queued save path if unused in production wiring (*verify in `runtime/`*). |
| MR-05 | Medium | Three writers for application status with three write shapes: `updateStatusWithSession` replaces the whole document (`MongoApplicationRepository.scala:128-160`), `MongoInterviewWorkflowStore.writeApplicationStatus` does `$set`, `commitHiring` (`MongoInterviewWorkflowRepository.scala:98-185`) hand-builds event plus outbox. No check that `applications.status` equals the latest `application_events.newStatus`. | One session-scoped `ApplicationStatusWrite(session, application, event)`: `$set status, updatedAt` guarded by previous status, then event insert, then outbox insert. Both paths call it. Add an integration assertion that current status equals the latest event. |
| MR-06 | Medium | Outbox claim is one transaction per row (`claimPage` then `claimOne`, `MongoOperationalEventRepositories.scala:233-245`), each with a `users` deletion read and a per-subject lease write; about 5 operations per event. `outbox_subject_fences` stores `deleted/deletedAt` while the claim path reads deletion from `users`: two sources for one fact. | Bounded batch lease on `event_outbox`; subject lease only when a batch holds more than one event for a subject. Drop `deleted/deletedAt` from the fence. Rename to `subject_fences` (applications, interviews and admin authorization use it too). |
| MR-07 | Medium | Interview "hiring receipt" inbox document (`MongoInterviewWorkflowRepository.scala:166-172`) duplicates the deterministic `application_events` row (`StableIds.uuid(workflowId:status)`); `hasHiringReceipt` only checks existence. | Query `application_events` by stable `_id`; delete the receipt document type. |
| MR-08 | Medium | Overlapping idempotency stores: `mutation_receipts` for hiring mutations; interview creation keeps its own request receipt inside `interview_workflows` (`request:actor:key`, `MongoInterviewWorkflowCodec.scala:48-60`); plus inbox dedupe, `consumer_receipts`, `interview_notification_receipts`. `interview_workflow_commands` is a second outbox with different lease field names. | Route interview creation through `mutation_receipts`; remove the request-receipt document type. Record the command queue as an accepted second outbox with its reason. |
| MR-09 | Low–Med | Derivable or divergent domain state: `User.adminSingleton` (implied by role), `User.deletedAt` vs `accountStatus`, `Job.closedAt` vs `status` (a `Closed` job without `closedAt` is representable), `ApplicationEvent.actorRole: Option` only for legacy rows (pre-MVP policy forbids legacy shapes). | `AccountStatus.Deleted(at)`, `JobStatus.Closed(at)`, required `actorRole`, drop `adminSingleton` from the domain (keep the unique-index marker in the stored shape only). |
| MR-10 | Low–Med | Duplicated profile mapping: `MongoHiringCodecs.profile` and the inline `StoredProfile` in `storedUser` build the same document, including lowercase sidecars. Field-name sets are repeated in codecs, `MongoSearchEligibilityCodecs.jobFields` and validators. Five Job shapes (`StoredJob`, `Job`, `Versioned[Job]`, `JobSubmissionSnapshot`, eligibility/related/nearby). | One `storedProfile(profile)` used by both; one `JobFields` object owning names, shared by codecs, projections and validators. |
| MR-11 | Low–Med | Read-after-write round trips: `updateProfile`/`updateProfileDirect` do `updateOne` then `find`; `updateEmbedding` reads the full versioned document (with vector) then updates. | `findOneAndUpdate(ReturnDocument.AFTER)`; project `version` only. |
| MR-12 | Low | `jobs` is hot and large: every submission `$inc`s `admissionFence` on a document that also holds a roughly 1536-double vector (*measure*). `embedding_work` stores `kind`/`entityId` already encoded in `_id`. `account_registry` duplicates the admin existence fact enforced by the unique index. | Measure write amplification on a seeded workload before moving vectors to `job_embeddings`. Drop redundant work-row fields. Document `account_registry` as an accepted deviation or fold it into the bootstrap transaction. |

Collection inventory: 35 collections (24 operational, 11 analytics-owned). Six belong to the interview workflow and three to `analytics_report_*`; both groups are merge candidates after MR-07/MR-08.

#### B. Functional design and duplication

| ID | Severity | Finding (verified) | Smallest fix |
|---|---|---|---|
| FD-01 | Medium | `InterviewActionError.{UseCase, Workflow}` (`service/application/InterviewActionError.scala:11`) duplicates `UseCaseError.InterviewRule`; `InterviewSchedulingService` wraps and unwraps (`:226,250,268-285`); `HiringGraphQLInterviewResolvers.scala:158-175` re-implements `mutationResult` and introduces a one-off mapping to `Repository(Unavailable)`. | Delete `InterviewActionError`; use `UseCaseF` plus `InterviewRule`; one `mutationResult` in `HiringGraphQLResolverSupport` driven by failure metadata flags. |
| FD-02 | Medium | The same failure is mapped in several layers: `AuthorityRevoked` becomes `Forbidden` in `UseCaseF.scala:24-27` yet is still matched in `GraphQLFailureCatalog.scala:374,485` (dead or bypassed); `VectorSearchUnavailable` exists as both `RepositoryError` and `SearchError` with conversions in three places; `searchFailure` (`SemanticSearchService.scala:409-413`) collapses `Repository(_)` and discards distinctions `degradable` relies on; `Idempotent.scala:41-58` round-trips `Repository(e)` back to `RepositoryError`. | One total `RepositoryError` to `UseCaseError` function in `UseCaseF`; search views as named extensions in `service/search`; no re-matching on `Repository(_)` elsewhere. |
| FD-03 | Medium | Lease heartbeat/renew loop copied three times with different loss policies: `InterviewPublication.scala:131-161`, `EmbeddingPipeline.scala:198-211`, `OperationalEventKafkaRuntime.scala:215-243`. | `LeaseHeartbeat.guard[F](lease)(renew)(work): F[Either[LeaseLost, A]]` in `service/resilience`. |
| FD-04 | Medium | The authorization chain "resolve actor, load application, load job, canManage" exists three times (`ApplicationService.scala:113-119`, `InterviewSchedulingService.scala:40-44`, `HiringReadService.scala:48-57`); `ensure(cond, Domain(Forbidden))` is written 12 times. `DomainError` holds use-case outcomes (`Forbidden`, stringly typed `NotFound(entity)`, `SearchSessionPending`). | `AuthorizedApplicationAccess.manage(actor, applicationId)` beside `AuthorizedJobAccess`; `UseCaseF.forbidUnless`; move `Forbidden`/`NotFound` to `UseCaseError` with a typed entity enum. Pairs with MR-03. |
| FD-05 | Medium | Lease queue partly shared (`MongoLeaseQueue` is good) but the `claim` bodies for embedding and search-session work differ only in state names and sort; the `leaseUntil.isAfter(now)` guard is written 3 times with two different error types (`InvalidEvent` vs `InvalidStoredData`); the `workerId` length guard is written twice. | `MongoLeaseQueue.claimReady(states, sort)`; one `LeaseRequest.validate` returning a typed error. |
| FD-06 | Medium | Error-translation boilerplate: 32 `leftMap(_ => RepositoryError.X)`, 102 references to `InvalidStoredData`; `MongoSearchSessionWorkCodecs.scala:38,40` take `.leftMap(_.head)` and drop accumulated errors; contract-validation failures become `Repository(InvalidEvent)` in three services. EL-06/EL-08 claimed this consolidation; it is partial. | `decoded.toStored` extension in `MongoRepositorySupport`; one `OperationalEventContractError` to `UseCaseError` function in `service/events`. |
| FD-07 | Low–Med | `RepositoryF.at[IO].fromEither` and `.pure[[A] =>> RepositoryF[IO, A]]` appear 49 times (type-inference workaround noise). 28 hand-rolled `.value.flatMap { case Right/Left }` sites; `EmbeddingPipeline` `Left(_) => Retry` branches drop the cause silently. | Local alias `type R[A] = RepositoryF[IO, A]` with instances; a `RepositoryF#orReport(diagnostics, op)(default)` combinator; use `foldF`/`leftSemiflatTap`. |
| FD-08 | Low | `InterviewSubjectCleanup.scala:74` parses UUIDs with `Try` inside the domain. The `domain/saga` engine carries six type parameters and one instantiation; defensible but speculative. | Use `Identifiers.parse`. Revisit saga arity only if a second saga appears. |

#### C. GraphQL feature utilization

Inventory: 17 query fields, 25 mutations, 0 subscriptions, 48 object types, 25 inputs, 17 enums, 1 interface (`UserError`), 24 unions, 10 scalars.

| ID | Severity | Finding (verified) | Smallest fix |
|---|---|---|---|
| GQ-01 | High | The graph is flat. `Application` exposes only `candidate` and `job`; there is no `Job.applications`, `Application.events`, `Application.interview`, `User.applications` or `InterviewWorkflow.application`. History and lists are root-only, so clients make one round trip per level and the fetcher machinery is idle for them. Tracked as R1 in the draft graph access spec. | Promote the graph access spec to ready; implement connection fields on the existing relation-key fetchers (authorization-safe, batch cap 100). |
| GQ-02 | Medium | No field-level projection: `ctx.astFields` is used only by reducers; `jobs`, `myJobs`, `jobApplications` load full documents for any selection. | Pass a selected-field set from `astFields` into repository projections for list roots. Pairs with MR-01. |
| GQ-03 | Medium | Pagination inconsistent: no `totalCount`, `PageInfo` lacks `startCursor/hasPreviousPage`; `nearbyJobs` hand-builds its connection (`HiringGraphQLSearchResolvers.scala:33-47`) instead of `paged`/`connection`; `jobSearchConnectionType` copies edges from a nested connection; cursors are plain `String` HS256 JWTs with a 900 s TTL (`CursorCodec.scala:15-20,97-107`), so a paused client's cursor expires. | Single `connection` helper for all list fields; a `Cursor` scalar; compact HMAC cursor or longer TTL. Add `totalCount` only where a use case needs it. |
| GQ-04 | Medium | 19 near-identical per-mutation unions `X \| ValidationError \| DomainError`; the `UserError` interface exists but clients cannot share one error fragment. `ErrorCode/code/message` redeclared on four error types. | Keep typed errors; add `interface MutationPayload { errors: [UserError!]! }` or a shared `MutationError` union; declare common error fields once. |
| GQ-05 | Medium | No subscriptions although interview reschedule proposals expire and application status changes are polled via `interviewWorkflow`. Tracked as R3 of the graph access spec. | Decide in the graph access spec; Sangria stream support remains assumption A1/A2 there. |
| GQ-06 | Low–Med | Redundant or derivable fields: `RankedJob`/`RankedCandidate` repeat `searchId` and `model` per hit; `degraded` is implied by `degradedReason`; `coverage` plus `coverageLimit` for one sum type; `InterviewWorkflow` flattens interval pairs and derives `rescheduleRequested` from a timestamp. No shared scoring interface between the two ranked types. | Hoist `searchId/model` to the parent; nested `TimeSlot` objects; a `ScoredHit` interface. |
| GQ-07 | Low | 25 inputs exist as case class plus `deriveInputObjectType` plus a Circe decoder for idempotency payloads plus `InputFieldPaths`; three places to keep in sync. Reducers (about 130 lines) live inside `HiringGraphQLSchemaAssembly`. | Derive the idempotency fingerprint from the Sangria input value instead of a parallel Circe decoder; move reducers to a `QueryAdmission` file. |

Execution controls (depth 16, complexity 1000, page-size reducer, per-actor cost budget, document cache, persisted-query allowlist, introspection gating, SDL snapshot test) are strong and should be kept as-is.

### Preserve

Pure domain with no framework imports; `StateT` lifecycle programs; authorization predicates carried into Mongo pipelines; keyset pagination on `(createdAt, _id)`; `MongoLeaseQueue`, `MongoFilter/MongoUpdate`; `Idempotent.executeFor`, `UseCaseF.ensure/found`, `AuthorizedJobAccess`; fail-closed `noUnexpectedFields` decoding; request-scoped authorization-safe fetchers; exhaustive `GraphQLFailureCatalog.classify`; typed ID scalars, `@oneOf`, committed SDL and persisted-query manifest.

### Assumptions

- Vector width about 1536 doubles per entity (unmeasured). Impact: MR-01 and MR-12 benefit scales with it.
- The queued search-session path is the only production path (MR-04). Validate by reading `runtime/MongoHiringRuntime.scala` wiring before removing `save`.
- `explain` for MR-02 will show an in-memory sort. Validate on the replica-set test container with at least 1000 applications for one candidate.

### Open questions (for the user)

1. Which slices to authorize now: S1–S3 are low-risk and self-contained; S4 reopens the public GraphQL contract (pre-MVP policy allows direct change).
2. MR-09 changes domain ADTs and stored shapes; confirm it is in scope under the pre-MVP "one active shape" policy.
3. MR-06 renames a collection; confirm before touching `outbox_subject_fences`.

## S1 read path: decisions and results (2026-10-19)

Status of the findings: MR-01, MR-02, MR-03, MR-11 and FD-04 are done. GQ-02 (selection-driven field projection from `astFields`) is not part of this change.

Decisions:

- **Embedding stays on the domain `Job` and `User`.** Moving it to a read model touches `EmbeddingPipeline`, `SemanticSearchService`, `SearchEligibility`, both codecs, the validators and about thirty tests, which exceeds one read-path slice. Instead the reads are split by what they return: `users.find` and `jobs.find` (profile or display) never read the vector and report `embedding = None`; `findVersioned` is the single whole-document read and is used only by the embedding pipeline, compare-and-set writes and search ranking. The port documents this. Revisit with MR-09/MR-10 (S5), where the domain ADTs change anyway.
- **Actor projection is `ActorRecord(id, role, accountStatus, adminSingleton)`**, without `name` and `email`: no authorization path reads them, and omitting them keeps personal data out of the per-request read. `AuthenticatedActor.viewer` is now an `ActorRecord`. `ActorAuthorization.resolve` and `readScope` return it, and `HiringReadScope.validated`, `canManage`, `canView` take it (`JobOwner` for the job side). Callers that need the profile use `resolveProfile` (`me`) and search uses `resolveWithEmbedding`/`readScopeWithEmbedding`.
- **`readScope` reuses the request's resolved actor** (`resolve` then `scopeOf`). Search revalidation after a provider or vector call deliberately does not: `currentReadScope` always re-reads (test `MVR-05` keeps that property), because a request-cached actor must not outlive a deleted account.
- **The NotFound versus Forbidden distinction is kept**, so the pre-checks were reduced rather than removed: `jobApplications` still reads `jobs.findOwner` (a projected read of one job, no user read) before the aggregation; `applicationHistory` still reads the application and, for a Recruiter, the job ownership. They are one projected read each instead of full documents, and the actor is read once. The aggregations keep enforcing access independently.
- **`candidateMatches` still reads the full job twice, on purpose.** The second read happens after provider and vector retrieval and revalidates that the owned job is still open and its embedding is current; reading once would drop that concurrency guard. Both reads now go through `findVersioned` explicitly, since `jobs.find` no longer carries the vector.
- **`ApplicationSubmission.create` takes `(candidateId, candidateRole, ...)` instead of a `User`** (one domain edit outside the originally allowed paths), because submission now resolves only an `ActorRecord`.
- Minimal projections decode strictly (exactly the projected fields). Profile, display and list reads reuse the existing full-document decoder, whose unknown-field check is unchanged and still fails closed; no `noUnexpectedFields` set was widened.
- `updateProfile` (both the candidate and the direct path) is one `findOneAndUpdate(ReturnDocument.AFTER)` with the profile projection; a lost guard is `Conflict` as before. `updateEmbedding(id, ...)` reads only `version`.

Accepted risks (Security Engineer residuals):

- **Write authorization trusts the request-cached actor.** Mutations resolve an `AuthenticatedActor` read at the start of the request, so an account deleted mid-request is not seen by the service check. This is bounded by the repository: application writes re-validate the active actor and job ownership inside the write transaction (`MongoApplicationWriteFence`), profile and embedding writes carry an active-account guard in the update filter, and read pipelines re-check the actor (AC-03.3). Search revalidation after long steps always re-reads (AC-03.4).
- **`Forbidden` versus `NotFound` can reveal that an application or job exists.** Pre-existing behavior kept on purpose for the existing error contract; IDs are UUIDs and responses are sanitized. Revisit if enumeration becomes a stated threat.

Not verified, or open:

- No per-request Mongo operation count was captured end to end through GraphQL; AC-02 counts are at the service boundary with counting fakes, and AC-01/AC-03 at the Mongo command boundary. The "bytes per auth read" saving is by projection shape; no byte counts were measured, and the vector width assumption (about 1536 doubles) is unmeasured.
- `me` now costs two small reads (the request's actor projection and the profile read) instead of one whole-user read.
- `MongoRecruiterJobClosure` page reads still use whole job documents; not in scope.

## Behavior and contracts

No behavior change is intended by S1–S3 and S5; they are refactors with identical observable outcomes and fewer round trips and bytes. S4 is additive GraphQL (new fields and interfaces, no removals). Operational Mongo changes (MR-06 rename, MR-07/MR-08 document-type removals) are pre-MVP direct changes: a migration step appended to the ledger, no dual-write. Analytics contracts: not applicable.

## Domain language and examples

- Contexts: Hiring (Application, Job, User aggregates; status-history invariant), Search and embedding (search session), Operational events (outbox).
- Terms used: actor, read scope, admission fence, subject fence, lease, receipt, connection/cursor. Terms to add in `docs/domain-language.md` if S1 is authorized: *actor projection* (the minimal user read used for authorization), *status write* (the atomic status plus event plus outbox write).

| Rule | Example tag | Scenario |
|---|---|---|
| Authorization reads only what it needs | AC-01.1 | Given the projections named by the read paths, then the actor projection names only `_id, role, accountStatus, adminSingletonKey`, profile and list reads exclude `embedding`, `embeddingMeta` and `passwordHash`, and the ownership projection names only `_id, recruiterId, status` (asserted on the projection documents in `MongoReadProjectionsSpec`; the issued commands are captured in AC-01.3 to AC-01.5) |
| Minimal projections fail closed | AC-01.2 | Given a stored actor or ownership projection, when it carries a field outside the projection, lacks a required field or names an unknown enum value, then decoding fails; the Admin singleton marker is set only for the singleton key |
| Issued reads match the projection | AC-01.3 | Given a candidate with a stored embedding and password hash, when the actor, profile, listing and whole-document reads run, then the captured `find` commands project exactly the documented fields and the returned values carry no profile, vector or credential where excluded |
| Job reads | AC-01.4 | Given a stored job with an embedding, when the owner, display and recruiter-list reads run, then the captured projections are `_id, recruiterId, status` and `embedding, embeddingMeta`, and the returned jobs carry no embedding |
| Lookups | AC-01.5 | Given related users and jobs, when `relatedUsers` and `relatedJobs` run, then the captured `$lookup` sub-pipelines exclude the vector and the credential |
| Write then read | AC-01.6 | Given a Recruiter profile update, then one `findAndModify` returns the updated profile and no `find` or second `update` is issued; given an embedding write by id, then only `_id, version` is read first |
| Candidate profile write | AC-01.7 | Given a Candidate profile update, then one `findAndModify` and the embedding-work enqueue commit in one transaction and the returned user has no embedding; a deleted account, a role mismatch or an absent user is a `Conflict` that enqueues nothing |
| Embedding write guards | AC-01.8 | Given an embedding write for an absent document, a deleted account, or an observed revision that has since been bumped, then the result is `Conflict` |
| One actor read per request | AC-02.1 | Given a recruiter viewing `jobApplications`, when the request completes, then one `users.findActor`, one `jobs.findOwner` and one `applications.findByJob` were issued and no whole user or job was read; with an authenticated actor no further user read follows (counting fakes in `AuthorizedReadRoundTripsSpec`) |
| History reads | AC-02.2 | Given the owning Candidate or the managing Recruiter, when `applicationHistory` runs, then the actor, the application, (for a Recruiter) the job ownership and the history aggregation are each read once |
| Access refusals keep their codes | AC-02.3 | Given a non-owning Recruiter, a Candidate or an unknown job, when `jobApplications` runs, then the result is `Forbidden`, `Forbidden` and `NotFound("job")`, and no aggregation runs for the first |
| One access chain | AC-02.4 | Given an application, when `AuthorizedApplicationAccess.manage` runs, then the actor, application and job ownership are read once each; an Admin passes, another Recruiter is `Forbidden`, a missing application or job is `NotFound` |
| No second application read | AC-02.5 | Given `canViewApplication`, then the loaded application is returned and `applications.find` runs once |
| Indexed candidate page | AC-03.1 | Given a candidate with 1000 applications, when they fetch the first page of 20, then `explain` reports 20 documents examined and a winning plan of LIMIT over FETCH over IXSCAN with no `SORT` stage |
| Empty page for a refused actor | AC-03.2 | Given a deleted-account candidate with seeded applications, or a scope of another role, when `findByCandidate` runs, then the page is empty |
| Cached actor boundary | AC-03.3 | Given a scope granted while the account was active, when the account is deleted afterwards, then `findByJob`, `relatedUsers` and `findByCandidate` return nothing because the pipeline re-checks the actor |
| Search revalidation | AC-03.4 | Given candidate matching, when the recruiter is deactivated between retrieval and revalidation, then the call fails and returns no results |
| Status and history agree | AC-04.1 | Given any status mutation path (API or interview commit), when it commits, then `applications.status` equals the latest `application_events.newStatus` |
| One search persisted once | AC-05.1 | Given a completed semantic search, then the results array appears once in `search_sessions` and once in the outbox payload, and the work row holds no `event` copy |
| One error translation | AC-06.1 | Given a repository `AuthorityRevoked` on any mutation, then the GraphQL result carries `FORBIDDEN` and `GraphQLFailureCatalog` has no reachable `Repository(AuthorityRevoked)` branch |
| Graph traversal in one request | AC-07.1 | Given a recruiter, when they query `job(id) { applications(first: 10) { edges { node { candidate { name } events(first: 5) { ... } } } } }`, then it succeeds with at most 4 Mongo round trips and another recruiter's job returns `FORBIDDEN` |

## Acceptance and evidence

| ID | Given / When / Then | Implementation and test paths | Verification | Actual outcome |
|---|---|---|---|---|
| AC-01 | Actor/owner reads are projected (MR-01; GQ-02 not in S1) | `MongoUserRepository`, `MongoJobRepository`, `MongoReadProjections.scala`, `ActorAuthorization`; `MongoReadProjectionsSpec` (unit), `MongoAuthorizedReadProjectionIntegrationSpec` (AC-01.3 to AC-01.6) | `sbt test`; `scripts/run-local-tests.sh test '*MongoAuthorizedReadProjectionIntegrationSpec'` | Pass 2026-10-19: the issued `find` projects exactly `_id, role, accountStatus, adminSingletonKey` for the actor and `_id, recruiterId, status` for ownership; profile, display, list and `$lookup` reads exclude `embedding`, `embeddingMeta`, `passwordHash`; stored vector and hash present in the seeded documents, absent from the returned values. 13 of 13 IT cases (including the cached-actor, deleted-account, Candidate profile write and embedding write guard cases, AC-01.7, AC-01.8, AC-03.2, AC-03.3), 8 of 8 unit cases |
| AC-02 | One actor read per list request (MR-03, FD-04) | `HiringReadService`, `ApplicationService`, `InterviewSchedulingService`, `AuthorizedApplicationAccess`, `UseCaseF.forbidUnless`; `AuthorizedReadRoundTripsSpec` (AC-02.1 to AC-02.4, counting fakes) | `sbt test` | Pass 2026-10-19 (11 of 11): `jobApplications` issues `users.findActor`, `jobs.findOwner`, `applications.findByJob` and nothing else (previously 3 user reads and 2 whole-job reads); with an `AuthenticatedActor` the request issues no further user read; `applicationHistory` issues `users.findActor`, `applications.find`, `applications.history` (plus `jobs.findOwner` for a Recruiter). The count is a unit-level fake count; a Mongo-level count per request was not captured (see Not verified) |
| AC-03 | Candidate page sort is index-backed (MR-02) | `MongoApplicationRepository.findByCandidate`; `MongoAuthorizedReadProjectionIntegrationSpec` AC-03.1 | `explain` `executionStats` before and after on the issued pipeline | Pass 2026-10-19, MongoDB 8.0.32 replica-set test container, 1000 applications for the candidate plus 500 for others, page of 20, indexes from `MongoHiringIndexSetup`. Before (match, actor `$lookup`, `$sort`, `$limit`): `totalDocsExamined` 1000, the `$sort` ran as an aggregation stage after the join over all 1000 rows. After (match, `$sort`, `$limit`, actor `$lookup`): `totalDocsExamined` 20, `totalKeysExamined` 20, winning plan `LIMIT > FETCH > IXSCAN applications_candidate_created_id`, no `SORT` stage (a `SORT` appears only in the rejected plans) |
| AC-04 | Single status write path (MR-05) | `ApplicationStatusWrite`, both callers | integration assertion on every status test | Not run |
| AC-05 | Search results stored once in the work row (MR-04) | `MongoSearchSessionWorkCodecs`, completion path | unit plus integration | Not run |
| AC-06 | Single error translation per failure (FD-01, FD-02, FD-06) | `UseCaseF`, `GraphQLFailureCatalog`, `InterviewActionError` removed | `sbt test`; grep for `Repository(AuthorityRevoked)` | Not run |
| AC-07 | Nested connections with authorization (GQ-01, GQ-03, GQ-04) | per graph access spec | SDL snapshot, forbidden-access tests | Not run |
| AC-08 | Shared lease heartbeat and claim (FD-03, FD-05) | `LeaseHeartbeat`, `MongoLeaseQueue.claimReady` | existing worker tests pass unchanged | Not run |

## Performance and cost

Targets, not results. Seed: 10 recruiters, 200 jobs, 50 candidates, 5000 applications, one candidate with 1000 applications; local replica set via `scripts/run-local-tests.sh`. Measure Mongo operations per request and bytes returned for `me`, `myJobs`, `jobApplications`, `applicationHistory`, `myApplications` before and after S1–S2. Expected: operations per list request from 4–7 down to 2; `users` bytes per auth read reduced by the vector size. No paid resources.

## Implementation handoff

Ordered slices. Each is independently verifiable and leaves the tree green.

| Slice | Scope | Owner | Write paths | Depends on |
|---|---|---|---|---|
| S1 Read path | MR-01, MR-02, MR-03, MR-11, FD-04, GQ-02 | Scala Developer (services, resolvers); Data Engineer (projections, explain evidence) | `service/**`, `repository/mongo/Mongo{User,Job,Application}Repository.scala`, `MongoRepositorySupport.scala`, tests | none |
| S2 Error model | FD-01, FD-02, FD-06, FD-07, FD-08 | Scala Developer | `service/protocol/**`, `service/application/InterviewActionError.scala` (delete), `api/graphql/HiringGraphQLResolverSupport.scala`, `GraphQLFailureCatalog.scala`, repositories' `leftMap` sites | none; serialize with S1 on `service/` files |
| S3 Write path and stores | MR-04, MR-05, MR-06, MR-07, MR-08, MR-12 (work-row fields), FD-03, FD-05 | Data Engineer (migration step, fences); Scala Developer (status write, heartbeat) | `repository/mongo/MongoApplication*`, `MongoInterviewWorkflow*`, `MongoOperationalEventRepositories.scala`, `MongoSearchSession*`, `service/resilience/`, `MongoMigrationLedger.scala`, integration tests | MSC-05 landed (same codec files) |
| S4 Graph access | GQ-01, GQ-03, GQ-04, GQ-05 decision, GQ-06, GQ-07 | Software Architect (promote spec), Scala Developer | `api/graphql/**`, SDL snapshot, `docs/api.md`, `docs/examples/**` | graph access spec ready |
| S5 Domain ADTs | MR-09, MR-10 | Scala Developer; Data Engineer (validators) | `domain/model/**`, codecs, validators | S3 (shared codec files) |

Every slice: `sbt test`, the integration suite where Mongo behavior changes, formatting, independent Code Reviewer and Security Engineer verdicts (S1, S3 and S4 touch authorization and persistence), then QA on the final state. Documentation updates to `docs/mongodb-design.md`, `ARCHITECTURE.md` and `docs/domain-language.md` ride with the slice that changes behavior, not before.

Risks: S1 changes the authorization read shape; a missed field in the actor projection fails closed (decode error), not open. S3 touches the outbox claim path; keep the per-row path behind the existing worker tests until the batch lease has integration evidence. S4 reopens the public contract; the SDL snapshot test and persisted-query manifest regeneration are the guard.
