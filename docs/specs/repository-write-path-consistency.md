# Repository Write-Path Consistency

## Identity and scope

- Status: complete
- Coordinator: Product Manager
- Implementation owners: Scala Developer for Mongo/repository and GraphQL changes; Product Manager for this spec and the Scala Developer skill rules
- User outcome: hiring writes retain their current atomic and sanitized behavior while duplicated Mongo paths, discarded optimistic-concurrency inputs, and misleading framework error handling are removed.
- Authorized scope: Mongo user/job write-path consolidation, repository protocol tightening, account-deletion error flow, GraphQL error classification cleanup, internal failure catalog, focused tests, and additive Scala Developer rules.
- Non-goals: GraphQL SDL or response changes, Mongo schema/index changes, migrations, dependency changes, service/domain redesign, or refactoring unrelated repositories and already accepted functional/security mechanisms.
- Dependencies: Scala 3.9 on Java 17+, Cats Effect, Sangria, Mongo reactive streams, MUnit, and replica-set MongoDB for transaction evidence.

## Source context and decisions

- Production services use context-bearing account and job writes, while integration tests also exercise concrete no-context Mongo methods. `MutationWriteContext.noop` is currently rejected by the Mongo context decoder, so deleting overloads without restoring repository-owned transaction semantics would be a regression.
- Job writes have parallel direct, event, embedding, and session implementations. User account creation mirrors that duplication, and several methods bypass local session helpers.
- `JobRepository` defaults currently discard the observed job for CAS operations. No `ApplicationRepository` equivalent exists and that protocol remains untouched.
- Sangria analysis and reducer failures must remain sanitized HTTP 400 results; field failures remain GraphQL errors. The internal failure catalog must preserve every current code, message, and exceptional classification while leaving `code: String!` unchanged.
- Existing Scala Developer rules remain verbatim. New rules are additive and must be validated by realistic independent walkthroughs rather than prose-token checks.
- No data migration or compatibility window applies because stored documents, indexes, SDL, events, and external responses do not change.

## Behavior and contracts

- A repository call with a Mongo receipt context joins that session. A call using the protocol's `MutationWriteContext.noop` opens a repository-owned transaction only when the operation spans atomic writes; single-write paths retain their current direct execution.
- Job creation and CAS replacement each use one session-aware implementation that conditionally adds embedding work and operational events. Account creation uses one session-aware implementation after registry validation; bootstrap retains its singleton-specific path.
- Expected write conflicts and missing write results remain in `Either[RepositoryError, A]`. Unexpected driver failures use the throwable channel only until the repository boundary maps them once.
- GraphQL clients observe no new or changed type, code, message, HTTP status, or exceptional/typed-payload classification.

## Acceptance and evidence

| ID | Given / When / Then | Implementation and test paths | Verification | Actual outcome |
|---|---|---|---|---|
| RWC-01 | Job or account writes include optional embedding work/events | One canonical session-aware core performs the primary write and optional atomic follow-ups without duplicated direct/session families | Focused repository tests; source review | Implemented; focused Mongo/service suites and full unit suite passed; replica-set tests prove atomic job/event/embedding persistence and rollback |
| RWC-02 | A write receives `noop` or a Mongo receipt context | The repository respectively owns the required transaction or joins the supplied session without changing atomicity | Replica-set integration tests | Passed: disposable replica-set tests verified repository-owned and receipt-owned job/event/embedding transactions |
| RWC-03 | Recruiter deletion closes jobs and a CAS or driver result fails | Expected conflict/unavailable results short-circuit through `EitherT`, and the transaction leaves no partial user/job/outbox state | Unit classification and replica-set rollback tests | Passed: deterministic zero-match and missing-result classification tests plus replica-set success and malformed-document rollback scenarios |
| RWC-04 | A `JobRepository` implementation updates a job | It must accept and honor observed state; no shared default or convenience method may discard `expected` | Compile plus focused service/search tests | Passed: unit and IntegrationTest compilation plus job/search focused suites; non-CAS defaults removed |
| RWC-05 | GraphQL validation, depth, complexity, field failures, or deferred email projection execute | Existing HTTP/status, sanitization, authorization, and batching behavior is unchanged without handler rethrows | GraphQL contract/access/HTTP tests | Passed 89/89 focused GraphQL tests and the full unit suite; no SDL change |
| RWC-06 | Any current `UseCaseError` reaches GraphQL | An exhaustive internal catalog returns the exact existing code, message, and exceptional flag, including payload-bearing and duplicate-code cases | Table-driven resolver-support tests | Passed: exhaustive catalog tests preserve codes, messages, and exceptional classification |
| RWC-07 | A Scala Developer handles analogous repository or Sangria work | Project rules require caller inventory, canonical write cores, session-helper reuse, typed expected failures, semantic contract inputs, and localized documented interop | Skill validator and independent scenario walkthrough | Passed: 8 skills validated and independent reviewer passed four realistic repository/Sangria walkthroughs |

## Performance and cost

- This is a maintainability and correctness refactor, not a performance claim. It adds no infrastructure, paid services, dependencies, collections, indexes, or recurring workload.
- Transaction ownership and bounded account-deletion batches remain unchanged. Verification uses the smallest disposable datasets needed for atomicity and rollback evidence.

## Implementation handoff

- Mongo/repository owner: `MongoHiringRepositories.scala`, `MongoMutationReceiptRepository.scala`, `HiringRepositories.scala`, their focused unit/integration tests, and affected test adapters; implement RWC-01 through RWC-04.
- GraphQL owner: failure catalog, schema execution/type comments, resolver support, and focused GraphQL tests; implement RWC-05 and RWC-06 without SDL changes.
- Product Manager owner: this spec and `.agents/skills/scala-developer/SKILL.md`; implement RWC-07 while preserving existing text.
- Code Reviewer and Security Engineer inspect the integrated final state independently; QA runs final acceptance after required fixes.

## Checkpoint and review

- Completed criteria: RWC-01 through RWC-07 are implemented with focused evidence; final review gates remain open.
- Latest evidence: final `sbt test` passed 326/326; focused Mongo unit tests passed 9/9 and replica-set transaction integration passed 3/3. The earlier complete IntegrationTest run passed all 15 repository/server tests but failed one unrelated `MainProcessSpec` assertion (9/10 in that suite) because 17 repeated `RUNTIME_FAILED` records were emitted instead of one; the isolated rerun reproduced that runtime-reporting failure. `git diff --check` and skill validation passed. Security review: PASS. Code rereview after closing two evidence gaps: PASS with no findings.
- Next action: none; implementation and required review gates are complete.
- Code Reviewer verdict: PASS
- Security Engineer verdict: PASS
- Final independent QA verdict: PASS
