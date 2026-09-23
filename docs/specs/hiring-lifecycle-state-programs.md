# Hiring Lifecycle State Programs

## Identity and scope

- Status: in progress
- Coordinator and implementation owner: Product Manager / Scala Developer
- User outcome: lifecycle transitions remain pure and typed while supporting safe sequencing over an in-memory aggregate.
- Scope: Application status changes, Job update/publish/close transitions, lifecycle unit tests, service adaptation, and architecture guidance.
- Non-goals: GraphQL or MongoDB contract changes, changes to authorization/idempotency, new multi-step business mutations, and replacing repository concurrency control or atomic writes.

## Source context and decisions

- `ApplicationLifecycle.changeStatus` returns a pure lifecycle program whose successful run yields the updated Application separately from `StatusChange` metadata; `ApplicationService` supplies actor/time/IDs and persists the Application with its event atomically.
- `JobLifecycle` has pure create/update/publish/close functions; `JobService` supplies time and persists job changes with operational events. Architecture currently requires `cats.data.State` for Job aggregate changes.
- Use `StateT` over `Either[DomainError, *]` for both aggregates. This short-circuits a composed sequence on failure and returns no partially updated aggregate. Successful runs return `(updatedAggregate, result)`; Application transition metadata must not embed a duplicate aggregate. Keep Job initial-state validation (`create`) as a direct typed check because it validates a proposed initial aggregate rather than transitioning existing state.
- State programs receive all actor and time values explicitly. They perform no I/O and do not replace MongoDB CAS/transaction guarantees.

## Acceptance and evidence

| ID | Given / When / Then | Implementation and test paths | Verification | Actual outcome |
|---|---|---|---|---|
| HLS-01 | An allowed Application status change is run against an Application | `ApplicationLifecycle`, `ApplicationLifecycleSpec` | Unit tests assert new aggregate and transition metadata | Passed: full unit suite includes 7 lifecycle tests |
| HLS-02 | Application status programs are composed, including an invalid later change | `ApplicationLifecycleSpec` | Success threads the updated status; failure returns the typed error and no state | Passed: composition success and failure tests |
| HLS-03 | Job update, publish, and close programs are run/composed | `JobLifecycle`, `JobLifecycleSpec` | Unit tests assert content/status/timestamps/closedAt and failure short-circuit | Passed: 7 lifecycle tests include sequencing and typed failure cases |
| HLS-04 | Application and Job services execute lifecycle commands | `ApplicationService`, `JobService` and service specs | Existing authorization, event construction, persistence, and CAS/transaction behavior remains unchanged | Passed: focused service specs (22 tests), including publishJob result and event, and full unit suite |
| HLS-05 | Architecture guidance describes the implemented abstraction | `ARCHITECTURE.md` | Static review confirms `StateT` boundary and effect/persistence ownership | Passed: independent Software Architect review |

## Checkpoint and review

- Implementation: shared domain `LifecycleProgram`, Application/Job lifecycle policies, service callers, lifecycle specs, and architecture guidance updated.
- Verification: `sbt "scalafmtOnly src/test/scala/com/example/graphQL/cats/service/job/JobServiceSpec.scala" scalafmtCheckAll scalafmtSbtCheck test` passed; 349 unit tests passed. The test output included non-failing OpenTelemetry exporter connection-refused warnings for the absent local collector.
- Code Reviewer verdict: PASS after refreshed review. The reviewer confirmed that `statusEvents` preserves event construction and atomic `updateStatusWithEvents`, and the publishJob test asserts result, timestamp, and event.
- Final independent QA verdict: PASS after refreshed QA. Focused lifecycle/service run passed 36/36. Concurrent `MutationWriteOutcome` and runtime edits remain excluded from both lifecycle verdicts and were not changed by this task.
- Architecture review: PASS; it confirmed StateT error semantics and service boundaries, and required clarifying that Job creation is initial-state validation rather than a transition. The spec and architecture guidance reflect that clarification.
- Next action: none for the lifecycle slice. Mongo integration was not rerun because repository transaction/CAS implementations were unchanged.
