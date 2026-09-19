# Hiring Runtime Hardening

Status: in review; disposable-Mongo execution result pending capture

## Identity and Scope

- Task / business capability: preserve safe account access, hiring readiness, and recoverable runtime behavior.
- User outcome: account throttling cannot be bypassed through GraphQL composition; hiring operations fail closed during setup; Mongo and embedding failures have truthful, recoverable outcomes.
- Authorized scope: current Scala/Cats Effect/http4s/Sangria/MongoDB implementation, focused tests, and canonical documentation.
- Non-goals: production migration execution, Atlas provisioning, distributed rate limits, durable event outbox, new dependencies, frontend work, or a package-wide rename.

## Source Context and Decisions

- Verified source: account rate limiting currently classifies only one sensitive operation per GraphQL document; setup runs in a resource-owned background fiber; Mongo setup records Atlas migration history only after provisioning and queryability verification; account service imports the API JWT adapter.
- Decision: a selected operation may contain at most one public account root field: `login`, `signUp`, or `bootstrapAdmin`. A violation is rejected before authentication or resolver execution.
- Decision: readiness remains a GraphQL transport concern. Health/readiness diagnostics are not gated; every persistence-dependent public account operation is.
- Decision: retain Cats Effect `Resource`, `Dispatcher`, `Ref`, bounded `Queue`, and the contained Mongo reactive-stream adapter. They solve concrete ownership or interop boundaries.
- Decision: semantic indexing uses a dedicated Mongo work collection with generation fencing and leases; it is not a generic event outbox or Kafka replacement.
- Assumption: no supported external GraphQL consumer requires a changed schema. Existing payload/error names remain unless a precise new domain error requires a mapped public code.

## Acceptance and Evidence

| ID | Given / When / Then | Implementation and test paths | Actual outcome |
|---|---|---|---|
| HRH-AC01 | Given aliases, fragments, or multiple named operations, when the selected operation contains more than one public account field, then HTTP rejects it before the account service runs | HTTP route spec | Passed: aliases, fragments, cyclic expansion, selected-operation isolation, and bootstrap throttling are covered |
| HRH-AC02 | Given setup is pending or failed, when a public account mutation runs, then it returns `SERVICE_NOT_READY` without service invocation or waiting for setup completion | Mongo runtime and GraphQL resolver/access specs | Passed: runtime readiness is nonblocking while setup is pending; all three public account mutations are denied without account-service calls |
| HRH-AC03 | Given a Mongo migration/index attempt fails, when setup restarts, then incomplete work is not falsely recorded or destructively replaced | Mongo setup and integration checks | Partial: `hiring_migration_ledger` verifies its immutable descriptor/checksum before leasing, persists user-batch checkpoints, and marks `Applied` only after setup verification; it preserves legacy migration history, verifies the exact sparse unique canonical-email key, and fails closed for incompatible Mongo or Atlas definitions. Disposable Mongo coverage includes option/wrong-key preservation, checksum rejection, checkpoints, and active-lease exclusion; the live IT result and Atlas environment evidence remain unverified |
| HRH-AC04 | Given transient transaction or embedding-provider failures, when work is retried, then retries are bounded, resource-owned, and do not duplicate hiring facts | transaction policy and embedding pipeline specs | Partial: pure retry-label decisions and durable work behavior are unit-tested; real `ClientSession` commit retry and Mongo work-adapter behavior require disposable replica-set evidence. |
| HRH-AC05 | Given account creation/token issuance and job creation, when inputs are invalid or a boundary fails, then errors retain their actual domain and no unintended write occurs | service/domain/API tests | Passed by focused domain/service/API coverage |
| HRH-AC06 | Given final changes, when local gates run, then unit tests, applicable integration tests, diff checks, Code Review, Security Review, and QA are recorded | project root checks | `sbt test` passed 242 tests before final review fixes; final focused regression passed 44 tests, `Test / compile` and diff check passed; Code Review and Security re-review passed. Integration compilation passed, but the disposable-container test completion was not captured and remains unverified; QA verdict pending. |

## Implementation Handoff

- Security: selected-operation rate-limit enforcement.
- Scala/domain: token issuer port, password policy, initial job lifecycle, account validation, timestamp propagation.
- Data: Mongo setup/index recovery and transaction semantics.
- Coordinator: runtime composition, availability gate, embedding recovery, contract checks, integration and review evidence.

## Checkpoint and Review

- Baseline before work: `sbt test` passed 224 tests on 2026-09-19.
- Final local unit evidence: `sbt test` passed 242 tests before final review fixes; the final focused regression suite passed 44 tests and `Test / compile` passed on 2026-09-19. `git diff --check` passed.
- Integration evidence: `sbt "IntegrationTest / compile"` passed. A disposable-container integration run was started after final compilation, but its completion result was not captured, so it is unverified.
- Code Reviewer: PASS after independent re-review.
- Security Engineer: PASS after independent re-review.
- QA: pending final independent validation.
