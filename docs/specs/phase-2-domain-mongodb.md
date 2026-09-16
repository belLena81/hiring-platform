# Phase 2A — Domain Foundation

Status: in progress

## Identity and scope

- Task / roadmap phase: Phase 2 — Domain Model + MongoDB, first pure-domain slice.
- Coordinator / implementation owners: Product Manager coordinates; Scala Developer owns domain code and focused tests. Data Engineer owns MongoDB adapters, indexes, and transaction tests in a later slice.
- User outcome: establish the hiring domain vocabulary and pure rules that later MongoDB and GraphQL slices can call without depending on infrastructure.
- Authorized scope: project-local Scala Developer skill hardening; domain models, ADTs, typed errors, validation helpers, repository ports, and focused tests.
- Non-goals: GraphQL schema changes, MongoDB adapters, migrations, authentication, admin seeding, Kafka/events, vector search, analytics, paid services, and performance claims.

## Source context and decisions

- Verified current implementation: Foundation serves health/readiness only; `docs/specs/phase-1-foundation.md` records P1-AC01 through P1-AC10 as complete and explicitly excludes hiring workflows, Mongo collections/indexes, and authentication.
- Target requirements: `docs/development-milestones.md` Phase 2 names User, CandidateProfile, Job, Application, ApplicationStatus, ApplicationEvent, repository ports, duplicate prevention, closed-job validation, status transitions, and atomic application/history writes.
- Current user request: make the Scala Developer skill require ADTs, domain typed errors, and `ValidatedNel` validation.
- Current user request update: Scala Developer work must follow TDD, with focused tests prepared before code for new behavior.
- Current user request update: job aggregate state changes must use `cats.data.State`; concurrency primitives may be introduced only with bounded Cats Effect ownership; Kafka work must follow event-driven architecture.
- Decision: implement pure domain and ports first. MongoDB uniqueness and transaction guarantees remain specified but unimplemented until the repository-adapter slice introduces replica-set-capable integration tests. Future implementation slices must record the planned failing test or test-first evidence before completing the matching code.

## Behavior and contracts

- Domain closed sets use Scala 3 enums/ADTs: user roles, job status, application status, and typed domain errors.
- Domain identifiers are opaque UUID wrappers for users, jobs, applications, and application events.
- Accumulating validation uses `cats.data.ValidatedNel[DomainValidationError, A]`; sequenced business decisions use `Either[DomainError, A]`.
- Permitted application transitions are `Created -> Accepted | Declined | Rejected`, `Accepted -> Interview`, and `Interview -> Hired | Rejected`.
- Rejection requires non-blank feedback; decline requires non-blank reason. Created applications always start in `Created`. Closed jobs reject new applications.
- Repository ports exist as application-layer contracts only; application list queries require cursor plus validated page size. Adapters, schema migrations, unique indexes, and transaction behavior are pending implementation.
- Future job create/publish/update/close rules must be represented as pure `cats.data.State` transitions over the job aggregate. This slice has not implemented job state transitions beyond the `JobStatus` ADT.
- `Ref`, bounded `Queue`, and fibers are allowed only in application/infrastructure slices with resource ownership, cancellation behavior, and tests; they are not part of this pure-domain slice.
- Kafka/event-driven architecture remains out of scope for Phase 2A implementation, but later event slices must use durable outbox-style publication, versioned event envelopes, idempotent consumers, and replay/recovery tests.

## Acceptance and evidence

| ID | Given / When / Then | Implementation and test paths | Verification command or method | Actual outcome |
|---|---|---|---|---|
| P2A-AC01 | Given invalid user/job input, when validation runs, then independent failures accumulate in `ValidatedNel` | `domain/model`, `DomainValidationSpec` | `sbt test` | PASS: covered in 99-test unit run |
| P2A-AC02 | Given valid job input, when validation runs, then normalized values and ADT status are preserved | `domain/model/Job.scala`, `DomainValidationSpec` | `sbt test` | PASS: covered in 99-test unit run |
| P2A-AC03 | Given application lifecycle changes, when transitions are attempted, then only the explicit matrix succeeds | `domain/service/ApplicationLifecycle.scala`, `ApplicationLifecycleSpec` | `sbt test` | PASS: covered in 99-test unit run with exhaustive matrix assertion |
| P2A-AC04 | Given rejection or decline without required text, when status changes, then typed domain errors are returned | `ApplicationLifecycleSpec` | `sbt test` | PASS: covered in 99-test unit run |
| P2A-AC05 | Given a candidate and open job, when application creation is requested, then a Created application is returned | `ApplicationSubmissionSpec` | `sbt test` | PASS: covered in 99-test unit run |
| P2A-AC06 | Given a non-candidate or closed job, when application creation is requested, then typed domain errors are returned | `ApplicationSubmissionSpec` | `sbt test` | PASS: covered in 99-test unit run |
| P2A-AC07 | Given project skill validation, when checked, then local skill metadata remains valid | `.agents/skills/scala-developer/SKILL.md` | `python3 scripts/check-skills.py` | PASS: 8 project-local skills validated |
| P2A-AC08 | Given new Scala behavior, when implementation starts, then focused tests are prepared first and red/green evidence is recorded when practical | `.agents/skills/scala-developer/SKILL.md`, future specs | Skill validation and implementation evidence | PARTIAL: requirement encoded; this completed pass did not capture a separate red-before-code transcript |
| P2A-AC09 | Given application repository list contracts, when a caller requests applications, then query shape includes cursor and validated bounded page size | `application/port/HiringRepositories.scala`, `ApplicationPageRequestSpec` | `sbt test` | PASS: covered in 99-test unit run |

## Performance and cost

No performance claim is made. This slice uses no external infrastructure and adds no runtime services. Phase 2 MongoDB work will define local fixture size, replica-set requirements, indexes, and transaction evidence separately.

## Implementation handoff

- Ordered slices: test-first pure domain behavior; repository adapter/index/transaction spec; GraphQL schema and authorization; request-scoped batching and pagination.
- Required later Data Engineer work: MongoDB collections, unique `(candidateId, jobId)` index, application/status-history transaction, recovery and verification strategy.
- Required later Security work: authenticated actor context, candidate/recruiter/admin authorization, singleton admin provisioning, and sanitized API errors.

## Checkpoint and review

- Completed criteria and changed files: domain model/services/ports, focused domain tests, Scala Developer skill update, and this spec are implemented locally. AC08 is encoded as future mandatory workflow but remains partial for this pass because no separate red-before-code transcript was captured.
- Latest commands/results and their scope: `python3 scripts/check-skills.py` passed for 8 project-local skills; `sbt test` passed 99 unit tests; `git diff --check` passed. This verifies local skills, whitespace, and Docker-independent unit behavior only.
- Blockers and next concrete action: no concrete blocker remains for this pure-domain slice; next dependency is a separately scoped MongoDB adapter/index/transaction slice.
- Code Reviewer verdict and scope: PASS on rereview after fixing bounded application query contracts and AC08 evidence wording.
- Security Engineer verdict and scope: PASS for Phase 2A security scope; no required fixes, with runtime/deployment gaps explicitly remaining out of scope.
- Final independent QA verdict: PASS on rereview. QA reran `python3 scripts/check-skills.py`, `sbt test`, and `git diff --check`; AC08 remains partial by design because red-before-code evidence was not captured for this completed pass.
