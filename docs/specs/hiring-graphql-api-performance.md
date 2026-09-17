# Hiring GraphQL API + MongoDB Performance

Status: completed for planned development scope - GraphQL contract, HS256 bearer JWT authentication, resolver tests, repository query contracts, MongoDB setup/index updates, local unit/integration evidence, and independent re-review are complete. Production identity-provider/token-issuance integration remains a separate future scope.

## Identity and scope

- Task / business capability: complete Hiring GraphQL API + MongoDB Performance.
- Coordinator / implementation owners: Product Manager coordinates. Architect owns GraphQL/API boundary review. Scala Developer owns Sangria schema, resolvers, application-port adaptations, and focused tests. Data Engineer owns MongoDB access patterns, indexes, migration/setup updates, and `explain("executionStats")` evidence. Security Engineer reviews GraphQL authorization, resource limits, safe errors, and PII exposure.
- User outcome: the domain service operational hiring workflow is available through a production-style GraphQL contract with bounded pagination, typed payloads, service-owned authorization, request-scoped batching, and documented MongoDB query/index evidence.
- Authorized scope: UC01 structured job search and GraphQL exposure for UC03, UC04, UC05, UC07, UC08, and UC09 using the current Scala 3, Cats Effect, Sangria, http4s, and MongoDB stack.
- Non-goals: login/token issuance, external auth provider integration, temporary trusted actor headers, frontend/UI assets, Kafka/outbox publishing, semantic/vector search, recommendation/matching use cases, embedding generation, Spark/Delta analytics, paid infrastructure, production migration execution, and performance claims beyond measured local evidence.
- Dependencies: domain services and MongoDB repositories are the baseline. Their final Code Reviewer, Security Engineer, and QA verdicts are recorded in `docs/specs/phase-2-domain-mongodb.md`.

## Source context and decisions

- Verified implementation: Foundation serves `health` and `readiness`, while hiring schema operations can execute through served HTTP when a valid HS256 bearer JWT maps to a stored user. Domain-service source contains `ActorContext`, `JobService`, `ApplicationService`, typed `UseCaseError`, MongoDB repositories, transactions, bounded application pagination, and named list indexes.
- Target requirements: `docs/development-milestones.md` Hiring GraphQL API requires queries, mutations, inputs, typed payloads/errors, RBAC, cursor pagination, nested resolvers, request-scoped batching, UC01 structured job search, GraphQL exposure for domain service use cases, documented access patterns, and `explain("executionStats")` evidence.
- Auth decision: served HTTP derives `ActorContext` from an HS256 bearer JWT only when `AUTH_JWT_HS256_SECRET` is configured. The token must contain valid `sub`, `iss`, `aud`, and `exp` claims; optional `nbf` is honored. The trusted role is loaded from `UserRepository.find(sub)`, so client-controlled role claims cannot elevate privileges. Login/token issuance and external identity-provider integration remain out of scope.
- API evolution decision: evolve the single GraphQL schema additively. Keep `health` and `readiness` unchanged, update SDL snapshots and executable fixtures with representative hiring operations, and do not introduce versioned endpoints.
- Cursor decision: API cursors are opaque base64url-encoded JSON with a required `kind` discriminator. Jobs encode `{ "kind": "job", "createdAt": "<ISO-8601 UTC instant>", "id": "<UUID>" }`; applications encode `{ "kind": "application", "createdAt": "<ISO-8601 UTC instant>", "id": "<UUID>" }`; application history encodes `{ "kind": "applicationEvent", "occurredAt": "<ISO-8601 UTC instant>", "id": "<UUID>" }`. Invalid, malformed, cross-type, or unauthorized cursors return typed sanitized errors and must not bypass ownership checks.
- Performance decision: Hiring GraphQL API records local measured query-plan evidence for representative bounded fixture data. UC SLOs remain targets unless a benchmark records dataset size, concurrency, environment, percentiles, errors, and resource usage.

## Behavior and contracts

- Schema additions:
  - Queries: `jobs(filter, first, after)`, `job(id)`, `myJobs(status, first, after)`, `myApplications(status, first, after)`, `jobApplications(jobId, status, first, after)`, and `applicationHistory(applicationId, first, after)`.
  - Mutations: `submitApplication(input)`, `createJob(input)`, `updateJob(input)`, `publishJob(input)`, `closeJob(input)`, `acceptApplication(input)`, `moveApplicationToInterview(input)`, `hireApplication(input)`, `rejectApplication(input)`, and `declineApplication(input)`.
  - Types: `User`, `CandidateProfile`, `Job`, `Location`, `Application`, `ApplicationEvent`, status enums, role enum, `PageInfo`, connection/edge types, typed inputs, and payloads with `errors { code message }`.
- Authorization:
  - Resolvers do not trust client-supplied owner IDs, role values, candidate IDs, recruiter IDs, or admin markers. Actor identity comes only from trusted request context once an auth slice exists.
  - Application services remain the authorization boundary. Resolvers parse IDs/inputs, validate cursors/page size, call services, and translate typed errors to sanitized payloads.
  - Candidates can search/view Open jobs, submit applications, and list only their own applications. Recruiters can manage owned jobs and list/change applications for owned jobs. Admin behavior remains the domain service service contract.
  - Nested resolvers and batch loaders must preserve the parent object authorization decision and must never use cross-request caches.
- Pagination and validation:
  - All list fields require `first`; valid range is `1..100`. Resolvers fetch `first + 1` from bounded repository queries to derive `hasNextPage` and `endCursor`.
  - No GraphQL list resolver may use MongoDB `skip`. Job and application cursor predicates use `(createdAt < cursor.createdAt) OR (createdAt == cursor.createdAt AND _id < cursor.id)` with sort `{ createdAt: -1, _id: -1 }`; application-history predicates use `occurredAt` with the same tie-breaker.
  - Input validation failures, duplicate applications, closed jobs, invalid transitions, missing rejection feedback, and missing decline reasons return typed payload errors without raw driver messages or stack traces.
- Request-scoped batching:
  - Each GraphQL request owns user and job loaders for nested `job`, `candidate`, and `recruiter` fields.
  - A page of applications with nested candidates/jobs/recruiters must execute one applications query plus bounded batch user/job lookups, not one query per edge.
  - Loader diagnostics may record safe counts such as loader name, requested key count, returned count, cache hit count, and request ID; they must not log resume text, raw job descriptions, feedback/reason bodies, tokens, or raw filters.
- Application and repository contracts:
  - Add application ports for structured open-job search, recruiter job listing, batched user lookup, batched job lookup, and application-event history listing. Domain models remain free of GraphQL, http4s, Circe, BSON, and MongoDB APIs.
  - Add service methods only when needed to keep authorization outside resolvers; do not bypass existing domain service `JobService` and `ApplicationService` invariants.
  - Add MongoDB setup/index migration records for any new Hiring GraphQL API indexes. Empty disposable databases may create indexes directly and record deterministic setup; no destructive migration or production data repair is authorized.

## GraphQL operations and MongoDB access patterns

| Operation | Actor | MongoDB query shape | Required index or evidence |
|---|---|---|---|
| `jobs(filter, first, after)` | Authenticated Candidate, Recruiter, or Admin | `status = Open`, optional `location.city`, optional `skills`, optional `createdAt`, sorted by `createdAt DESC, _id DESC` | `jobs_open_created_id` for status-only search and `jobs_open_city_created_id` for city-filtered search; avoid speculative indexes for every field |
| `job(id)` | Candidate, owner Recruiter, Admin | `jobs._id = ?`; nested recruiter uses batched `users._id in [...]` | `_id` index plus request user loader evidence |
| `myApplications` | Candidate | `candidateId = actor.userId`, optional `status`, cursor sort | Existing `applications_candidate_status_created_id` and `applications_candidate_created_id` explain evidence |
| `jobApplications` | Owner Recruiter or Admin | authorized job lookup, then `jobId = ?`, optional `status`, cursor sort | Existing `applications_job_status_created_id` and `applications_job_created_id` explain evidence plus batch candidate evidence |
| `myJobs` | Recruiter or Admin | Recruiter: `recruiterId = actor.userId`, optional `status`; Admin: optional `status` across all jobs; both sorted by `createdAt DESC, _id DESC` | `jobs_recruiter_status_created_id` with status, `jobs_recruiter_created_id` without status, and `jobs_created_id` for Admin all-job listing |
| `applicationHistory` | Candidate owner, owner Recruiter, Admin | authorized application/job lookup, then `applicationId = ?`, sorted by `occurredAt DESC, _id DESC` | Existing `application_events_application_created_id` explain evidence |

For each accepted performance-sensitive operation, record `executionTimeMillis`, `totalDocsExamined`, `totalKeysExamined`, `nReturned`, winning index name, fixture size, and whether the result demonstrates only local query-plan behavior rather than a production latency SLO.

## Acceptance and evidence

| ID | Given / When / Then | Implementation and test paths | Verification command or method | Actual outcome |
|---|---|---|---|---|
| HGQL-AC01 | Given the Hiring GraphQL API schema is added, when SDL is exported, then Foundation fields remain compatible and hiring operations appear with typed inputs, payloads, enums, connections, and sanitized errors | `HiringGraphQLSchema`; `hiring.graphql`; `HiringGraphQLContractSpec` | `sbt test` | PASS: SDL fixture updated with mutation input objects and focused schema checks passed |
| HGQL-AC02 | Given missing or invalid authentication, when a hiring operation is executed over HTTP, then it returns a sanitized unauthorized GraphQL payload and no domain write occurs | `JwtActorAuthenticatorSpec`; `HiringApiRoutesSpec`; `HiringGraphQLAccessSpec` | `sbt test` | PASS: missing, malformed, tampered, expired, wrong issuer/audience, future `nbf`, and unknown-user tokens fail closed; role claims are ignored in favor of stored user role |
| HGQL-AC03 | Given Candidate, Recruiter, and Admin actors, when domain service use cases are executed through GraphQL, then service-owned authorization and ownership rules are enforced without client-controlled owner IDs | `HiringGraphQLAccessSpec`; `HiringApiRoutesSpec`; existing service specs | `sbt test` | PASS: injected GraphQL resolver tests and served JWT route tests cover actor-derived authorization and forged-role protection |
| HGQL-AC04 | Given a Candidate searches jobs with structured filters, when results are returned, then only Open jobs are visible, ordering is deterministic, page size is bounded, and cursors are opaque | `HiringGraphQLAccessSpec`; `MongoHiringRepositoriesIntegrationSpec` | `sbt test`; `sbt 'IntegrationTest / test'` | PASS: open-job GraphQL search and MongoDB job-search index evidence pass |
| HGQL-AC05 | Given a Candidate submits an application, when the job is Open and no duplicate exists, then the payload returns a Created application and domain-service transaction invariants persist application plus initial history | `HiringGraphQLSchema`; `HiringApiRoutesSpec`; `MongoHiringRepositoriesIntegrationSpec`; existing domain-service service/repository tests | `sbt test`; `sbt 'IntegrationTest / test'` | PASS: served JWT HTTP route and replica-set Mongo integration cover successful submit, duplicate rejection, lazy setup before first write, and transaction-backed application/history persistence |
| HGQL-AC06 | Given duplicate, closed-job, invalid-transition, missing-feedback, or missing-reason errors, when mutations are executed, then payloads expose stable typed error codes/messages and no raw driver/internal details | `HiringGraphQLSchema`; `HiringGraphQLAccessSpec`; existing service/repository negative tests | `sbt test`; `sbt 'IntegrationTest / test'` | PASS: GraphQL mutation tests cover duplicate, closed-job, invalid-transition, missing-feedback, and missing-reason payload codes; repository tests cover no partial writes |
| HGQL-AC07 | Given Candidate and Recruiter list operations, when optional status filters and cursors are used, then pagination is keyset-based, `hasNextPage` is derived with `first + 1`, and invalid cursors are rejected safely | Repository contracts; GraphQL connection tests | `sbt test`; `sbt 'IntegrationTest / test'` | PASS: keyset repository contracts, `first + 1` connection wiring, malformed cursor handling, cross-type cursor rejection, and recruiter/Admin listing query plans pass |
| HGQL-AC08 | Given applications are queried with nested candidate/job/recruiter fields, when a page contains multiple edges, then request-scoped batching prevents N+1 queries and keeps caches request-local | `RequestContext`; `HiringGraphQLAccessSpec` | `sbt test` | PASS: application pages preload candidate users, jobs, and job recruiter users through request-scoped batch ports; focused GraphQL access tests assert a multi-recruiter user batch |
| HGQL-AC09 | Given every major GraphQL read operation, when MongoDB integration tests run, then query shape, winning index, execution stats, and fixture limits are recorded in the spec checkpoint | `MongoHiringRepositoriesIntegrationSpec` | `sbt 'IntegrationTest / test'` | PASS: application list, structured job search, recruiter job listing with/without status, Admin job listing, and application history winning indexes plus execution stats are asserted in 32 integration tests |
| HGQL-AC10 | Given Hiring GraphQL API implementation is complete, when local gates run, then skill validation, unit tests, integration tests, schema/fixture checks, diff check, Code Reviewer, Security Engineer, and final QA pass | Full local/review gate | `python3 scripts/check-skills.py`; `sbt test`; `sbt 'IntegrationTest / test'`; `git diff --check`; reviews | PASS: `sbt test`, `IntegrationTest / test`, skill validation, diff checks, Code Reviewer, Security Engineer, Architect, Data Engineer, and final QA passed for this planned scope |

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
- Current blockers: no planned-scope blocker remains. Real performance SLO claims require a workload benchmark not included here. Production identity-provider integration and token issuance remain separately scoped, but served HTTP hiring workflows can execute with configured HS256 bearer JWTs.

## Checkpoint and review

- Current status: completed for planned development scope. GraphQL contract, served JWT authentication, MongoDB query/index slice, lazy Mongo setup before first hiring write, and local unit/integration evidence are implemented.
- Completed criteria and changed files: schema/context/cursor handling, JWT request authentication, runtime Mongo service wiring, repository ports/adapters, Hiring GraphQL API Mongo setup/indexes, SDL fixture, API docs, GraphQL unit tests, and Mongo integration coverage changed.
- Latest commands/results and their scope: focused JWT/Mongo command passed 4 JWT unit tests and 8 Mongo integration tests; `sbt test` passed 169/169 unit tests; `sbt 'IntegrationTest / test'` passed 32/32 integration tests. The known bind-conflict trace appeared inside `HiringPlatformServerSpec`, but the integration suite passed.
- Next concrete action: production identity-provider/token issuance, real workload benchmarking, and live deployment verification are separate future scopes.
- Architect verdict and scope: PASS after served JWT/runtime setup changes.
- Data Engineer verdict and scope: PASS after recruiter no-status index and explain evidence changes.
- Security Engineer verdict and scope: PASS after JWT auth, config sanitization, and `local.conf` removal.
- Code Reviewer verdict and scope: PASS after served JWT/runtime setup changes.
- Final independent QA verdict: PASS for planned Phase 2 Domain/MongoDB + Hiring GraphQL API completion scope.
