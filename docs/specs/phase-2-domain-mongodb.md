# Phase 2 — Domain Model + MongoDB

Status: completed for planned development scope — implementation, local verification, and independent completion reviews passed.

## Identity and scope

- Task / roadmap phase: complete Phase 2 — Domain Model + MongoDB.
- Coordinator / implementation owners: Product Manager coordinates. Scala Developer owns domain/application services and focused tests. Data Engineer owns MongoDB repositories, indexes, migration/setup, and replica-set transaction tests. Security Engineer reviews authorization, singleton Admin, user data, and sanitized failures.
- User outcome: transactional hiring operations work through application services and MongoDB repositories before Phase 3 exposes them as served GraphQL operations.
- Authorized scope: complete service + MongoDB support for UC03, UC04, UC05, UC07, UC08, and UC09 using the current Scala 3/Cats Effect/MongoDB stack.
- Non-goals: served hiring GraphQL schema/resolvers, JWT/login/auth provider, Kafka/outbox publisher, semantic/vector search, embedding queue, analytics, frontend, paid infrastructure, production migration execution, and performance/SLO claims.

## Source context and decisions

- Verified current implementation: Foundation serves only health/readiness through HTTP and GraphQL. Phase 2A added domain IDs, user/job/application ADTs, typed domain errors, `ValidatedNel` validation, bounded application page contracts, and pure application lifecycle rules.
- Existing Phase 2A evidence remains valid baseline: `sbt test` passed 99 unit tests; `python3 scripts/check-skills.py` and `git diff --check` passed; Code Reviewer, Security Engineer, and QA passed the Phase 2A scope.
- Target requirements: `docs/development-milestones.md` Phase 2 requires core models, repository ports/adapters, UC03/04/05/07/08/09, ownership, valid transitions, duplicate prevention, closed-job validation, rejection feedback, decline reason, MongoDB duplicate constraint, and atomic application/history writes.
- Scope decision: Phase 2 completion is service + MongoDB only. Served GraphQL operations, request-scoped batching, resolver authorization tests, SDL changes, and consumer operation fixtures are Phase 3.
- Auth decision: services receive a trusted `ActorContext` with actor ID and role. JWT/login/authentication are not implemented in Phase 2, but services must enforce authorization and ownership from this trusted context.
- Admin decision: singleton Admin is enforced at the database boundary and may be created only by explicit seed/setup code. Public signup or runtime admin creation is out of scope.
- Transaction decision: MongoDB transaction acceptance requires disposable replica-set Testcontainers evidence. Missing Docker or replica-set startup is BLOCKED, not a pass.
- Event decision: `application_events` are operational history records in MongoDB. Kafka/event-driven publication is a later phase and must not be required for Phase 2 writes.
- Workflow decision: new implementation follows TDD. Every new Phase 2 behavior requires a focused test prepared before implementation and recorded red/green evidence in this spec checkpoint before the criterion can pass.

## Behavior and contracts

- Domain closed sets remain Scala 3 ADTs/enums. Domain functions stay pure, immutable, infrastructure-free, and use typed errors rather than business exceptions.
- Job aggregate create/update/publish/close transitions must be modeled as pure `cats.data.State[Job, A]` programs. The `State` program must not read clocks, generate IDs, call repositories, log, publish events, or perform I/O.
- Application services sequence effects: validate actor authority, fetch authoritative current state, call pure domain logic, persist atomically, and translate infrastructure failures to typed service errors.
- `ActorContext`:
  - Candidate may submit applications and list only their own applications. Service inputs must not contain candidate IDs for candidate-owned operations.
  - Recruiter may create jobs and manage/list applications only for owned jobs. Client-supplied recruiter/owner IDs are ignored or rejected; ownership is derived from `ActorContext`.
  - Admin may perform authorized full-access service operations only after services resolve the actor ID from `users` and verify role `Admin` plus the seeded singleton marker. Missing, non-Admin, duplicate, disabled, or forged Admin contexts fail with `Unauthorized` or `Forbidden`.
  - Actor IDs always come from `ActorContext`, not client-controlled input. Inputs that try to override candidate, recruiter, or admin identity must be covered by negative tests.
- Phase 2 service contracts:
- Service methods return `F[Either[UseCaseError, A]]`, where `UseCaseError` is a Scala 3 union of original meaningful error types: `DomainError | RepositoryError | AuthenticationError | NonEmptyList[DomainValidationError]`. Services must not translate one meaningful ADT to another solely to fit a service envelope.
  - `JobService.createJob(actor, input, now, jobId): F[Either[UseCaseError, Job]]` creates a Draft or Open job owned by the actor when actor is Recruiter or Admin.
  - `JobService.updateJob(actor, jobId, input, now): F[Either[UseCaseError, Job]]` updates mutable content only for owner Recruiter or Admin and uses a pure `State` transition.
  - `JobService.publishJob(actor, jobId, now): F[Either[UseCaseError, Job]]` moves Draft to Open using pure `State`; already Closed or invalid state returns `DomainError.InvalidJobTransition`.
  - `JobService.closeJob(actor, jobId, now): F[Either[UseCaseError, Job]]` moves Draft/Open to Closed using pure `State`; owner Recruiter or Admin only.
  - `JobService.viewJob(actor, jobId): F[Either[UseCaseError, Job]]` returns visible job details for authorized actors; candidate visibility is limited to Open jobs unless Admin/recruiter owner.
  - `ApplicationService.submitApplication(actor, jobId, applicationId, eventId, now): F[Either[UseCaseError, Application]]` derives candidate from `ActorContext`, verifies Candidate role, reads job authoritatively in the transaction, rejects Closed/nonexistent jobs, and writes application plus initial event atomically.
  - `ApplicationService.myApplications(actor, page): F[Either[UseCaseError, List[Application]]]` derives candidate from `ActorContext` and returns only that candidate's bounded cursor page.
  - `ApplicationService.jobApplications(actor, jobId, page): F[Either[UseCaseError, List[Application]]]` requires owner Recruiter or Admin after authoritative job lookup.
  - `ApplicationService.changeStatus(actor, applicationId, target, feedback, reason, eventId, now): F[Either[UseCaseError, Application]]` requires owner Recruiter or Admin, validates current status, and writes status plus event atomically.
- Safe errors/logs/audit records must not include credentials, tokens, resume text, raw job descriptions, raw feedback/reason bodies, raw Mongo filters, driver exception messages, or stack traces. Audit/logging may include safe actor IDs, aggregate IDs, event/category codes, status codes, and generated correlation IDs.
- MongoDB collections: `users`, `jobs`, `applications`, `application_events`, and `schema_migrations`.
- Persistence documents use UUID `_id`, UTC `Instant` timestamps, and `schemaVersion: 1`. Persistence models/codecs remain under infrastructure and must map to/from domain models; domain models must not import MongoDB or BSON APIs.
- Required document fields:
  - `users`: `_id`, `schemaVersion`, `email`, `emailCanonical`, `name`, `role`, `adminSingletonKey?`, `createdAt`.
  - `jobs`: `_id`, `schemaVersion`, `version`, `recruiterId`, `title`, `description`, `requirements`, `skills`, `location`, `status`, `createdAt`, `updatedAt`, `closedAt?`.
  - `applications`: `_id`, `schemaVersion`, `candidateId`, `jobId`, `status`, `createdAt`, `updatedAt`.
  - `application_events`: `_id`, `schemaVersion`, `applicationId`, `previousStatus?`, `newStatus`, `actorId`, `occurredAt`, `feedback?`, `reason?`.
  - `schema_migrations`: `_id` migration ID, `appliedAt`, `description`, and deterministic checksum or version marker.
- Required MongoDB invariants:
  - unique canonical user email via `users_emailCanonical_unique`
  - singleton Admin via partial unique `users_adminSingleton_unique` on `adminSingletonKey` where role is `Admin`
  - unique `(candidateId, jobId)` via `applications_candidate_job_unique`
  - deterministic cursor ordering by `createdAt DESC, _id DESC`
  - application history separate from applications and append-only
- Required list indexes:
  - `jobs_recruiter_status_created_id`: `{ recruiterId: 1, status: 1, createdAt: -1, _id: -1 }`
  - `jobs_recruiter_created_id`: `{ recruiterId: 1, createdAt: -1, _id: -1 }`
  - `applications_candidate_status_created_id`: `{ candidateId: 1, status: 1, createdAt: -1, _id: -1 }`
  - `applications_candidate_created_id`: `{ candidateId: 1, createdAt: -1, _id: -1 }`
  - `applications_job_status_created_id`: `{ jobId: 1, status: 1, createdAt: -1, _id: -1 }`
  - `applications_job_created_id`: `{ jobId: 1, createdAt: -1, _id: -1 }`
  - `application_events_application_created_id`: `{ applicationId: 1, occurredAt: -1, _id: -1 }`
- Cursor shape is `{ "createdAt": "<ISO-8601 UTC instant>", "id": "<UUID>" }`, encoded opaquely at API boundaries later but represented as a typed value in Phase 2. Repository predicates for the next page use `(createdAt < cursor.createdAt) OR (createdAt == cursor.createdAt AND _id < cursor.id)` with sort `{ createdAt: -1, _id: -1 }`.
- Required transaction boundaries:
  - submit application starts a session transaction, reads the job authoritatively inside the transaction, verifies status Open, performs a conditional job guard write such as incrementing `jobs.version` with predicate `{ _id: jobId, status: Open, version: observedVersion }`, inserts `applications`, inserts initial `application_events`, and maps duplicate key on `(candidateId, jobId)` to `DuplicateApplication`
  - close-job versus submit races are resolved by conflicting conditional job writes on `jobs.version/status`; if closure commits before submit's guard write, submit returns the original meaningful error (`DomainError.JobMustBeOpen` or `RepositoryError.Conflict`) and writes no application/event
  - status change reads current application and owning job in the transaction, guards previous status, updates only if previous status still matches, inserts `application_events`, and returns stale/concurrent writes as `RepositoryError.Conflict`
  - failed validation, duplicate key, closed-job, stale previous status, or transaction fault leaves no partial state, verified by post-failure reads
- Repository query contracts must be bounded. Candidate/recruiter list queries use validated page size and cursor; no unbounded `List` fetch is acceptable.
- Concurrency primitives are allowed only when needed for tests or application/infrastructure coordination: `Ref` for local coordination, bounded `Queue` for explicit handoff, and fibers only with `Resource`, request scope, or structured supervision.

## Acceptance and evidence

| ID | Given / When / Then | Implementation and test paths | Verification command or method | Actual outcome |
|---|---|---|---|---|
| P2A-AC01 | Given invalid user/job input, when validation runs, then independent failures accumulate in `ValidatedNel` | Existing Phase 2A domain model/tests | `sbt test` | PASS: covered in 99-test Phase 2A unit run |
| P2A-AC02 | Given valid job input, when validation runs, then normalized values and ADT status are preserved | Existing Phase 2A domain model/tests | `sbt test` | PASS: covered in 99-test Phase 2A unit run |
| P2A-AC03 | Given application lifecycle changes, when transitions are attempted, then only the explicit matrix succeeds | Existing `ApplicationLifecycleSpec` | `sbt test` | PASS: covered in 99-test Phase 2A unit run |
| P2A-AC04 | Given rejection or decline without required text, when status changes, then typed domain errors are returned | Existing `ApplicationLifecycleSpec` | `sbt test` | PASS: covered in 99-test Phase 2A unit run |
| P2A-AC05 | Given a candidate and open job, when application creation is requested, then a Created application is returned | Existing `ApplicationSubmissionSpec` | `sbt test` | PASS: covered in 99-test Phase 2A unit run |
| P2A-AC06 | Given a non-candidate or closed job, when application creation is requested, then typed domain errors are returned | Existing `ApplicationSubmissionSpec` | `sbt test` | PASS: covered in 99-test Phase 2A unit run |
| P2A-AC07 | Given project skill validation, when checked, then local skill metadata remains valid | Existing project skills | `python3 scripts/check-skills.py` | PASS: 8 project-local skills validated |
| P2A-AC08 | Given new Scala behavior, when implementation starts, then focused tests are prepared first and red/green evidence is recorded when practical | Skill/spec workflow | Skill validation and implementation evidence | PARTIAL: requirement encoded; Phase 2A did not capture a separate red-before-code transcript |
| P2A-AC09 | Given application repository list contracts, when a caller requests applications, then query shape includes cursor and validated bounded page size | Existing application port/tests | `sbt test` | PASS: covered in 99-test Phase 2A unit run |
| P2-AC01 | Given job create/update/publish/close behavior, when pure transitions run, then `cats.data.State` produces the expected job state and rejected transitions leave state unchanged | `JobLifecycleSpec`; `JobServiceSpec` | `sbt test` | PASS: 113 unit tests passed; focused red was captured before adding `JobLifecycle` |
| P2-AC02 | Given Candidate/Recruiter/Admin actors, including forged or non-seeded Admin contexts and inputs that try to override owner IDs, when services are called, then authorization and ownership are enforced from trusted `ActorContext` | `JobServiceSpec`; `ApplicationServiceSpec` with `Ref` fakes | `sbt test` | PASS: trusted actor role/ownership, candidate visibility, forged Admin, non-seeded Admin marker rejection, candidate/recruiter service paths covered |
| P2-AC03 | Given representative users/jobs/applications/events, when Mongo repositories write/read them, then domain values round-trip without leaking persistence models into domain | `MongoHiringRepositoriesIntegrationSpec` | `sbt 'IntegrationTest / test'` | PASS: users/jobs/applications/status history round-trip in disposable MongoDB |
| P2-AC04 | Given repository setup, when migrations run twice, then `schema_migrations` records the setup and all named unique/list/history indexes exist idempotently with expected keys and partial filters | `MongoHiringRepositoriesIntegrationSpec` | `sbt 'IntegrationTest / test'` | PASS: setup reruns, migration record, named indexes, key shapes, unique flags, and admin partial filter are asserted |
| P2-AC05 | Given concurrent duplicate application submissions for the same candidate/job, when both finish, then exactly one application and initial event exist and duplicates return sanitized typed errors | `MongoHiringRepositoriesIntegrationSpec` synchronized replica-set race | `sbt 'IntegrationTest / testOnly com.example.graphQL.cats.repository.mongo.MongoHiringRepositoriesIntegrationSpec'` | PASS: structured fibers plus `Deferred` synchronize duplicate submit attempts; one success, one `DuplicateApplication`, one application, and one initial event |
| P2-AC06 | Given an open job racing with closure, when closure commits before the authoritative submit check, then submit is rejected and writes no application/history | `MongoHiringRepositoriesIntegrationSpec` replica-set closed-job guard | `sbt 'IntegrationTest / testOnly com.example.graphQL.cats.repository.mongo.MongoHiringRepositoriesIntegrationSpec'` | PASS: submit uses an in-transaction open-job/version guard; closed/stale observed jobs return `RepositoryError.Conflict` without application/history writes |
| P2-AC07 | Given valid application status changes, when a recruiter changes status, then current status and immutable history are written atomically | `ApplicationServiceSpec`; `MongoHiringRepositoriesIntegrationSpec` | `sbt test`; `sbt 'IntegrationTest / testOnly com.example.graphQL.cats.repository.mongo.MongoHiringRepositoriesIntegrationSpec'` | PASS: service behavior, stale-status conflict guard, and replica-set transaction rollback evidence covered |
| P2-AC08 | Given invalid transitions, missing rejection feedback, or missing decline reason, when status change is attempted, then no state/history changes are written | `ApplicationServiceSpec`; existing `ApplicationLifecycleSpec`; `MongoHiringRepositoriesIntegrationSpec` | `sbt test`; `sbt 'IntegrationTest / testOnly com.example.graphQL.cats.repository.mongo.MongoHiringRepositoriesIntegrationSpec'` | PASS: service/domain no-write behavior covered; Mongo duplicate-event fault injection proves status rollback leaves current state unchanged |
| P2-AC09 | Given candidate and recruiter list requests, when optional status filters and `{createdAt,_id}` cursors are used, then results are authorization-scoped, bounded, deterministic, and backed by the named compound indexes with `explain(\"executionStats\")` evidence | Service list tests; Mongo index/list integration tests | `sbt test`; `sbt 'IntegrationTest / testOnly com.example.graphQL.cats.repository.mongo.MongoHiringRepositoriesIntegrationSpec'` | PASS: status and no-status candidate/job list queries assert expected winning index names through `explain` |
| P2-AC10 | Given a mid-transaction repository failure, when application submit or status change fails, then the database contains no partial write and the service returns a sanitized typed error | `MongoHiringRepositoriesIntegrationSpec` replica-set fault injection | `sbt 'IntegrationTest / testOnly com.example.graphQL.cats.repository.mongo.MongoHiringRepositoriesIntegrationSpec'` | PASS: duplicate event IDs fail mid-transaction; application submit and status update roll back partial writes and return typed repository errors |
| P2-AC11 | Given Phase 2 completion, when local verification runs, then skills, unit tests, replica-set integration tests, whitespace check, and required independent reviews pass | Full local/review gate | `python3 scripts/check-skills.py`; `sbt test`; `sbt 'IntegrationTest / test'`; `git diff --check`; reviews | PARTIAL: full local gate passed after implementation and security fixes; independent review rerun pending |

## MongoDB setup and recovery

- Add a deterministic MongoDB setup component for Phase 2 collections and indexes. It must be idempotent, locally testable, and must record completion in `schema_migrations`.
- For empty disposable development databases, setup creates indexes directly and records a setup/migration ID. No backfill is required because no persisted hiring data exists yet.
- Destructive operations, production migration execution, and local data deletion are not authorized by this spec.
- Transaction-dependent tests must use a replica-set MongoDB container. Existing standalone readiness tests do not prove transaction readiness.
- Unique-index violations are translated to typed service/repository errors without exposing driver messages, collection names beyond safe codes, raw filters, or credentials.

## Performance and cost

- No Phase 2 SLO is accepted from unit/integration tests alone. Use-case SLOs remain targets for later measured performance work.
- Local fixture sizes stay small and deterministic: enough records to prove uniqueness, ownership, cursor ordering, optional status filtering, and index use.
- Explain/index evidence is required for list-query acceptance, but no large benchmark or paid infrastructure is authorized.
- Added local cost is limited to disposable Testcontainers MongoDB replica-set startup for integration checks.

## Implementation handoff

- **Architect:** review service/MongoDB boundary, GraphQL deferral, `State` use for job transitions, transaction scope, and no Kafka coupling.
- **Data Engineer:** own collection shapes, codecs, index/setup strategy, duplicate/singleton constraints, transaction implementation, replica-set test harness, and index evidence.
- **Scala Developer:** implement job `State` transitions, `ActorContext`, services, union-style typed `UseCaseError`, repository interfaces/adapters, and focused tests.
- **Security Engineer:** review authorization/ownership, singleton Admin, safe errors, PII minimization, and client-controlled input boundaries.
- **Code Reviewer:** inspect final code for architecture, correctness, simplicity, transaction/race behavior, and test mapping.
- **QA Engineer:** independently run final applicable checks and map P2-AC01 through P2-AC11 to observed evidence.

## Checkpoint and review

- Current status: completed for planned development scope. Domain/application service slice, single-node replica-set local Mongo setup, replica-set transaction tests, fault-injection rollback, duplicate race handling, closed-job submit guard, and explain-backed application/job/history list indexes are implemented with full local evidence.
- Completed baseline: Phase 2A domain foundation and evidence remain recorded above.
- Implementation checkpoint (2026-09-16): TDD red/green captured for `JobLifecycleSpec`; focused service red/green captured for missing `ActorContext`, union `UseCaseError`, services, and repository errors. Earlier local commands passed after review fixes: `sbt test 'IntegrationTest / test' && python3 scripts/check-skills.py && git diff --check` passed 113 unit tests, 30 integration tests, 8 skill validations, and whitespace check.
- Refactor checkpoint (2026-09-16): application services now use shared `ActorAuthorization`, `EitherT`, and union-style typed `UseCaseError` without service-error remapping; Mongo adapters now use no-null codecs, explicit standalone/session transaction runners, optimistic job `version` guards, and stale application-status guards. Focused service and Mongo repository checks passed before the full local gate.
- Phase 2 completion checkpoint (2026-09-16): focused red/green captured missing `rejectNextCreateWith`, `createForOpenJob`, no-status index constants, replica-set transaction resource, rollback tests, duplicate race test, and explain evidence before implementation. Focused command passed after security fixes: `sbt 'testOnly com.example.graphQL.cats.service.application.ApplicationServiceSpec' 'IntegrationTest / testOnly com.example.graphQL.cats.repository.mongo.MongoHiringRepositoriesIntegrationSpec'` passed 7 service tests and 7 Mongo integration tests.
- Final local gate checkpoint (2026-09-16): `sbt test 'IntegrationTest / test' && python3 scripts/check-skills.py && git diff --check` passed 114 unit tests, 35 integration tests, 8 skill validations, and whitespace check. The known bind-conflict trace appears inside `HiringPlatformServerSpec`, but the suite passed.
- Latest refresh (2026-09-17): `sbt test` passed 169/169 unit tests; `sbt 'IntegrationTest / test'` passed 32/32 integration tests after adding served JWT/Mongo setup coverage and recruiter no-status job-list index evidence.
- Next concrete action: served GraphQL production identity-provider/token issuance, real workload benchmarking, and deployment verification are separate future scopes.
- Architect readiness verdict: PASS after adding explicit service contracts, actor validation, service errors, MongoDB contract details, and mandatory TDD evidence.
- Data Engineer readiness verdict: PASS after adding document fields, named indexes, migration history, cursor/index predicates, and close-vs-submit `jobs.version` guard-write strategy.
- Security Engineer readiness verdict: PASS after adding Admin actor resolution, forged/non-seeded Admin negative tests, client-controlled owner ID rules, and PII/safe-error requirements.
- Code Reviewer verdict and scope: PASS for Phase 2 completion implementation and GraphQL completion-path fixes.
- Security Engineer verdict and scope: PASS for Phase 2 completion implementation, JWT auth integration, config sanitization, and final setup gate.
- Final independent QA verdict: PASS for planned Phase 2 Domain/MongoDB + Hiring GraphQL API completion scope.
