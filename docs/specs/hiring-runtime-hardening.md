# Hiring Runtime Hardening

Status: implementation complete; QA blocked on persistence evidence

## Identity and Scope

- Task / business capability: preserve safe account access, hiring readiness, and recoverable runtime behavior.
- User outcome: account throttling cannot be bypassed through GraphQL composition; hiring operations fail closed during setup; Mongo and embedding failures have truthful, recoverable outcomes.
- Authorized scope: current Scala/Cats Effect/http4s/Sangria/MongoDB implementation, focused tests, and canonical documentation.
- Non-goals: production migration execution, Atlas provisioning, distributed rate limits, durable event outbox, new dependencies, frontend work, or a package-wide rename.

## Source Context and Decisions

- Verified source: account rate limiting currently classifies only one sensitive operation per GraphQL document; setup runs in a resource-owned background fiber; Mongo setup records Atlas migration history before provisioning; account service imports the API JWT adapter.
- Decision: a selected operation may contain at most one public account root field: `login`, `signUp`, or `bootstrapAdmin`. A violation is rejected before authentication or resolver execution.
- Decision: readiness remains a GraphQL transport concern. Health/readiness diagnostics are not gated; every persistence-dependent public account operation is.
- Decision: retain Cats Effect `Resource`, `Dispatcher`, `Ref`, bounded `Queue`, and the contained Mongo reactive-stream adapter. They solve concrete ownership or interop boundaries.
- Assumption: no supported external GraphQL consumer requires a changed schema. Existing payload/error names remain unless a precise new domain error requires a mapped public code.

## Acceptance and Evidence

| ID | Given / When / Then | Implementation and test paths | Actual outcome |
|---|---|---|---|
| HRH-AC01 | Given aliases, fragments, or multiple named operations, when the selected operation contains more than one public account field, then HTTP rejects it before the account service runs | HTTP route spec | Passed: aliases, fragments, cyclic expansion, selected-operation isolation, and bootstrap throttling are covered |
| HRH-AC02 | Given setup is pending or failed, when a public account mutation runs, then it returns `SERVICE_NOT_READY` without service invocation | GraphQL resolver/access spec | Passed: all three public account mutations are denied without account-service calls |
| HRH-AC03 | Given a Mongo migration/index attempt fails, when setup restarts, then incomplete work is not falsely recorded or destructively replaced | Mongo setup and integration checks | Partial: code records Atlas only after provision/queryability and fails closed for incompatible indexes; IT compiled and includes email-index preservation, but the live IT result and Atlas environment evidence are unverified |
| HRH-AC04 | Given transient transaction or embedding-provider failures, when work is retried, then retries are bounded, resource-owned, and do not duplicate hiring facts | repository/pipeline focused tests | Partial: embedding retry/recovery is covered; transaction label behavior is static-reviewed but lacks deterministic driver-label injection evidence |
| HRH-AC05 | Given account creation/token issuance and job creation, when inputs are invalid or a boundary fails, then errors retain their actual domain and no unintended write occurs | service/domain/API tests | Passed by focused domain/service/API coverage |
| HRH-AC06 | Given final changes, when local gates run, then unit tests, applicable integration tests, diff checks, Code Review, Security Review, and QA are recorded | project root checks | Unit suite and diff check passed; Code Review and Security re-review passed; QA is blocked on AC03/AC04 persistence evidence |

## Implementation Handoff

- Security: selected-operation rate-limit enforcement.
- Scala/domain: token issuer port, password policy, initial job lifecycle, account validation, timestamp propagation.
- Data: Mongo setup/index recovery and transaction semantics.
- Coordinator: runtime composition, availability gate, embedding recovery, contract checks, integration and review evidence.

## Checkpoint and Review

- Baseline before work: `sbt test` passed 224 tests on 2026-09-19.
- Final local unit evidence: `sbt test` passed 233 tests on 2026-09-19; `git diff --check` passed.
- Integration evidence: `sbt "IntegrationTest / compile"` passed. A disposable-container integration run was started, but its result was not captured, so it is unverified.
- Code Reviewer: PASS after re-review.
- Security Engineer: PASS after independent re-review.
- QA: BLOCKED, with no unit/static defect found; requires captured disposable-Mongo outcome plus deterministic transaction-label retry evidence.
