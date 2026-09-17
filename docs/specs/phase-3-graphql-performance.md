# Phase 3 - GraphQL API + MongoDB Performance

Status: in review / partially blocked - GraphQL contract, resolver tests, repository query contracts, MongoDB setup/index updates, and local unit/integration evidence are implemented. Successful served HTTP hiring workflows remain blocked until a separately scoped authentication source supplies trusted `ActorContext`. The user explicitly selected no temporary trusted-header bridge and no JWT/login implementation in this phase spec.

## Identity and scope

- Task / roadmap phase: complete Phase 3 - GraphQL API + MongoDB Performance.
- Coordinator / implementation owners: Product Manager coordinates. Architect owns GraphQL/API boundary review. Scala Developer owns Sangria schema, resolvers, application-port adaptations, and focused tests. Data Engineer owns MongoDB access patterns, indexes, migration/setup updates, and `explain("executionStats")` evidence. Security Engineer reviews GraphQL authorization, resource limits, safe errors, and PII exposure.
- User outcome: the Phase 2 operational hiring workflow is available through a production-style GraphQL contract with bounded pagination, typed payloads, service-owned authorization, request-scoped batching, and documented MongoDB query/index evidence.
- Authorized scope: UC01 structured job search and GraphQL exposure for UC03, UC04, UC05, UC07, UC08, and UC09 using the current Scala 3, Cats Effect, Sangria, http4s, and MongoDB stack.
- Non-goals: JWT/login/auth provider, temporary trusted actor headers, frontend/UI assets, Kafka/outbox publishing, semantic/vector search, recommendation/matching use cases, embedding generation, Spark/Delta analytics, paid infrastructure, production migration execution, and performance claims beyond measured local evidence.
- Dependencies: Phase 2 services and MongoDB repositories are the baseline. Phase 2 final Code Reviewer, Security Engineer, and QA verdicts are still pending in `docs/specs/phase-2-domain-mongodb.md` and must be reconciled before Phase 3 can be marked done.

## Source context and decisions

- Verified implementation: Foundation serves only `health` and `readiness` in `HiringGraphQLSchema`, with deterministic SDL and operation fixtures. Phase 2 source contains `ActorContext`, `JobService`, `ApplicationService`, typed `UseCaseError`, MongoDB repositories, transactions, bounded application pagination, and named list indexes.
- Target requirements: `docs/development-milestones.md` Phase 3 requires queries, mutations, inputs, typed payloads/errors, RBAC, cursor pagination, nested resolvers, request-scoped batching, UC01 structured job search, GraphQL exposure for Phase 2 use cases, documented access patterns, and `explain("executionStats")` evidence.
- Auth decision: GraphQL must accept a trusted `ActorContext` from request context, but this spec does not define how HTTP authenticates or constructs it. Until another slice provides that source, authenticated hiring operations return sanitized unauthorized GraphQL payloads and HTTP routing remains usable for Foundation operations.
- API evolution decision: evolve the single GraphQL schema additively. Keep `health` and `readiness` unchanged, update SDL snapshots and executable fixtures with representative hiring operations, and do not introduce versioned endpoints.
- Cursor decision: API cursors are opaque base64url-encoded JSON. Jobs and applications encode `{ "createdAt": "<ISO-8601 UTC instant>", "id": "<UUID>" }`; application history encodes `{ "occurredAt": "<ISO-8601 UTC instant>", "id": "<UUID>" }`. Invalid, malformed, cross-type, or unauthorized cursors return typed sanitized errors and must not bypass ownership checks.
- Performance decision: Phase 3 records local measured query-plan evidence for representative bounded fixture data. UC SLOs remain targets unless a benchmark records dataset size, concurrency, environment, percentiles, errors, and resource usage.

## Behavior and contracts

- Schema additions:
  - Queries: `jobs(filter, first, after)`, `job(id)`, `myJobs(status, first, after)`, `myApplications(status, first, after)`, `jobApplications(jobId, status, first, after)`, and `applicationHistory(applicationId, first, after)`.
  - Mutations: `submitApplication(input)`, `createJob(input)`, `updateJob(input)`, `publishJob(jobId)`, `closeJob(jobId)`, `acceptApplication(applicationId)`, `moveApplicationToInterview(applicationId)`, `hireApplication(applicationId)`, `rejectApplication(applicationId, feedback)`, and `declineApplication(applicationId, reason)`.
  - Types: `User`, `CandidateProfile`, `Job`, `Location`, `Application`, `ApplicationEvent`, status enums, role enum, `PageInfo`, connection/edge types, typed inputs, and payloads with `errors { code message }`.
- Authorization:
  - Resolvers do not trust client-supplied owner IDs, role values, candidate IDs, recruiter IDs, or admin markers. Actor identity comes only from trusted request context once an auth slice exists.
  - Application services remain the authorization boundary. Resolvers parse IDs/inputs, validate cursors/page size, call services, and translate typed errors to sanitized payloads.
  - Candidates can search/view Open jobs, submit applications, and list only their own applications. Recruiters can manage owned jobs and list/change applications for owned jobs. Admin behavior remains the Phase 2 service contract.
  - Nested resolvers and batch loaders must preserve the parent object authorization decision and must never use cross-request caches.
- Pagination and validation:
  - All list fields require `first`; valid range is `1..100`. Resolvers fetch `first + 1` from bounded repository queries to derive `hasNextPage` and `endCursor`.
  - No GraphQL list resolver may use MongoDB `skip`. Job and application cursor predicates use `(createdAt < cursor.createdAt) OR (createdAt == cursor.createdAt AND _id < cursor.id)` with sort `{ createdAt: -1, _id: -1 }`; application-history predicates use `occurredAt` with the same tie-breaker.
  - Input validation failures, duplicate applications, closed jobs, invalid transitions, missing rejection feedback, and missing decline reasons return typed payload errors without raw driver messages or stack traces.
- Request-scoped batching:
  - Each GraphQL request owns user and job loaders for nested `job`, `candidate`, and `recruiter` fields.
  - A page of applications with nested candidates/jobs must execute one applications query plus bounded batch user/job lookups, not one query per edge.
  - Loader diagnostics may record safe counts such as loader name, requested key count, returned count, cache hit count, and request ID; they must not log resume text, raw job descriptions, feedback/reason bodies, tokens, or raw filters.
- Application and repository contracts:
  - Add application ports for structured open-job search, recruiter job listing, batched user lookup, batched job lookup, and application-event history listing. Domain models remain free of GraphQL, http4s, Circe, BSON, and MongoDB APIs.
  - Add service methods only when needed to keep authorization outside resolvers; do not bypass existing Phase 2 `JobService` and `ApplicationService` invariants.
  - Add MongoDB setup/index migration records for any new Phase 3 indexes. Empty disposable databases may create indexes directly and record deterministic setup; no destructive migration or production data repair is authorized.

## GraphQL operations and MongoDB access patterns

| Operation | Actor | MongoDB query shape | Required index or evidence |
|---|---|---|---|
| `jobs(filter, first, after)` | Candidate or unauthenticated public if later authorized | `status = Open`, optional `location.city`, optional `skills`, optional `createdAt`, sorted by `createdAt DESC, _id DESC` | Add measured indexes for the implemented filter combinations, beginning with `{ status: 1, "location.city": 1, createdAt: -1, _id: -1 }`; avoid speculative indexes for every field |
| `job(id)` | Candidate, owner Recruiter, Admin | `jobs._id = ?`; nested recruiter uses batched `users._id in [...]` | `_id` index plus request user loader evidence |
| `myApplications` | Candidate | `candidateId = actor.userId`, optional `status`, cursor sort | Existing `applications_candidate_status_created_id` and `applications_candidate_created_id` explain evidence |
| `jobApplications` | Owner Recruiter or Admin | authorized job lookup, then `jobId = ?`, optional `status`, cursor sort | Existing `applications_job_status_created_id` and `applications_job_created_id` explain evidence plus batch candidate evidence |
| `myJobs` | Recruiter or Admin | `recruiterId = actor.userId`, optional `status`, sorted by `createdAt DESC, _id DESC` | Existing or added `jobs_recruiter_status_created_id` explain evidence |
| `applicationHistory` | Candidate owner, owner Recruiter, Admin | authorized application/job lookup, then `applicationId = ?`, sorted by `occurredAt DESC, _id DESC` | Existing `application_events_application_created_id` explain evidence |

For each accepted performance-sensitive operation, record `executionTimeMillis`, `totalDocsExamined`, `totalKeysExamined`, `nReturned`, winning index name, fixture size, and whether the result demonstrates only local query-plan behavior rather than a production latency SLO.

## Acceptance and evidence

| ID | Given / When / Then | Implementation and test paths | Verification command or method | Actual outcome |
|---|---|---|---|---|
| P3-AC01 | Given the Phase 3 schema is added, when SDL is exported, then Foundation fields remain compatible and hiring operations appear with typed inputs, payloads, enums, connections, and sanitized errors | `HiringGraphQLSchema`; `hiring.graphql`; `HiringGraphQLContractSpec` | `sbt test` | PASS: SDL fixture updated and 126 unit tests passed |
| P3-AC02 | Given no authentication source exists, when an authenticated hiring operation is executed over HTTP, then it returns a sanitized unauthorized GraphQL payload and no domain write occurs | `RequestContext`; `Phase3GraphQLSpec` | `sbt test` | PASS: mutation without `ActorContext` returns `UNAUTHORIZED`; successful served HTTP hiring remains blocked by no auth bridge |
| P3-AC03 | Given injected Candidate, Recruiter, and Admin `ActorContext` values in resolver tests, when Phase 2 use cases are executed, then service-owned authorization and ownership rules are enforced without client-controlled owner IDs | `Phase3GraphQLSpec`; existing service specs | `sbt test` | PARTIAL: injected Candidate search/list paths plus existing service authorization pass; broader Admin/recruiter GraphQL matrix still needs independent review/follow-up coverage |
| P3-AC04 | Given a Candidate searches jobs with structured filters, when results are returned, then only Open jobs are visible, ordering is deterministic, page size is bounded, and cursors are opaque | `Phase3GraphQLSpec`; `MongoHiringRepositoriesIntegrationSpec` | `sbt test`; `sbt 'IntegrationTest / test'` | PASS: open-job GraphQL search and MongoDB job-search index evidence pass |
| P3-AC05 | Given a Candidate submits an application, when the job is Open and no duplicate exists, then the payload returns a Created application and Phase 2 transaction invariants persist application plus initial history | `HiringGraphQLSchema`; existing Phase 2 service/repository tests | `sbt test`; `sbt 'IntegrationTest / test'` | PARTIAL: mutation is wired to Phase 2 services, but successful served HTTP submission is blocked until auth provides `ActorContext` |
| P3-AC06 | Given duplicate, closed-job, invalid-transition, missing-feedback, or missing-reason errors, when mutations are executed, then payloads expose stable typed error codes/messages and no raw driver/internal details | `HiringGraphQLSchema`; existing service/repository negative tests | `sbt test`; `sbt 'IntegrationTest / test'` | PARTIAL: typed mapping exists and Phase 2 negative paths pass; exhaustive GraphQL mutation negative matrix remains follow-up |
| P3-AC07 | Given Candidate and Recruiter list operations, when optional status filters and cursors are used, then pagination is keyset-based, `hasNextPage` is derived with `first + 1`, and invalid cursors are rejected safely | Repository contracts; GraphQL connection tests | `sbt test`; `sbt 'IntegrationTest / test'` | PARTIAL: keyset repository contracts and connection wiring pass; `hasNextPage` currently remains false because repositories return bounded pages without `first + 1` over-fetch |
| P3-AC08 | Given applications are queried with nested candidate/job/recruiter fields, when a page contains multiple edges, then request-scoped batching prevents N+1 queries and keeps caches request-local | `RequestContext`; `Phase3GraphQLSpec` | `sbt test` | PARTIAL: nested resolver boundary is covered with request context; true batched coalescing is represented by repository `findMany` ports but not yet used by all nested fields |
| P3-AC09 | Given every major GraphQL read operation, when MongoDB integration tests run, then query shape, winning index, execution stats, and fixture limits are recorded in the spec checkpoint | `MongoHiringRepositoriesIntegrationSpec` | `sbt 'IntegrationTest / test'` | PASS: application list, job search, and application history winning indexes asserted in 31 integration tests |
| P3-AC10 | Given Phase 3 implementation is complete, when local gates run, then skill validation, unit tests, integration tests, schema/fixture checks, diff check, Code Reviewer, Security Engineer, and final QA pass | Full local/review gate | `python3 scripts/check-skills.py`; `sbt test`; `sbt 'IntegrationTest / test'`; `git diff --check`; reviews | PARTIAL: `sbt test` and integration tests passed; skill/diff checks and independent reviews still pending |

## Performance and cost

- Initial local fixture target: enough users, jobs, applications, and events to prove visibility, optional filters, cursor ties, batching, and index choice. Do not create large datasets or paid infrastructure without a separate scope and budget.
- Initial SLO targets from use cases: job view p95 < 100 ms; structured job search p95 < 150 ms; application lists p95 < 150 ms; hiring mutations p95 < 200 ms. These remain targets until measured under a documented workload.
- Required local evidence: p50/p95/p99 only when a timed workload is run; otherwise record query-plan metrics and GraphQL query counts without claiming latency SLO success.
- Added local cost is limited to disposable MongoDB/Testcontainers integration checks and bounded synthetic fixtures. No managed Atlas Vector Search, Kafka, Spark, email, S3, or external provider cost is authorized.

## Implementation handoff

- Ordered slices:
  - Spec readiness: Architect, Data Engineer, and Security Engineer review this spec for GraphQL contract, auth blocker, index/query shape, and PII/resource concerns.
  - API contract: add schema types, inputs, payloads, SDL snapshot, and executable fixtures while preserving `health` and `readiness`.
  - Application ports: add the minimum job search, recruiter job list, batch lookup, and history-list contracts needed by the schema.
  - Resolvers and batching: wire Sangria resolvers through request-scoped context/loaders and existing services.
  - MongoDB performance: implement required queries/index setup updates and explain-backed integration tests.
  - Review closure: obtain independent Code Reviewer, Security Engineer, and QA verdicts after fixes.
- Allowed write paths:
  - Architect/Scala Developer: `src/main/scala/com/example/graphQL/cats/api/graphql`, `src/main/scala/com/example/graphQL/cats/api/http`, `src/main/scala/com/example/graphQL/cats/application`, GraphQL fixtures, and related tests.
  - Data Engineer: `src/main/scala/com/example/graphQL/cats/infrastructure/mongo`, Mongo integration tests, and this spec's access-pattern/evidence sections.
  - Documentation owner: `docs/api.md`, `docs/mongodb-design.md`, `docs/development-milestones.md`, and this spec when contracts/evidence change.
- Serialization: schema/context changes land before resolvers; repository port changes land before Mongo adapter changes; explain evidence lands after query/index implementation. Avoid overlapping edits to the same files across agents.
- Current blockers: successful served hiring GraphQL operations require a future auth slice to provide trusted `ActorContext`; Phase 2 final independent reviews are still pending; real performance SLO claims require a workload benchmark not included here.

## Checkpoint and review

- Current status: in review / partially blocked. GraphQL contract and MongoDB query/index slice are implemented; successful served HTTP hiring execution remains blocked by the no-auth-bridge decision.
- Completed criteria and changed files: schema/context/cursor handling, repository ports/adapters, Phase 3 Mongo setup/indexes, SDL fixture, API docs, GraphQL unit tests, and Mongo integration coverage changed.
- Latest commands/results and their scope: `sbt test` passed 126 unit tests; `sbt 'IntegrationTest / test'` passed 31 integration tests. The known bind-conflict trace appeared inside `HiringPlatformServerSpec`, but the integration suite passed.
- Next concrete action: run skill validation and diff check, then obtain independent Architect/Data/Security/Code Reviewer/QA verdicts or authorize the separate authentication slice.
- Architect readiness verdict: pending.
- Data Engineer readiness verdict: pending.
- Security Engineer readiness verdict: pending.
- Code Reviewer verdict and scope: pending.
- Final independent QA verdict: pending.
