# UC04 — Submit application: illustrative draft

**Status: draft example. No feature implementation or review signoff is claimed.**

This demonstrates the [spec workflow](../spec-driven-development.md) using [UC04](../use-cases.md#uc04--submit-application) and the [project invariants](../../AGENTS.md). It is not an instruction to start application development. Source paths below are repository-relative.

## Outcome and boundary

An authenticated Candidate submits an application to an open job and receives its initial status. The application and initial history event must be consistent.

Current source facts: `build.sbt` configures Scala 2.13 and Doobie/PostgreSQL; `src/main/scala/com/example/graphQL/cats/Main.scala` has unfinished startup and scaffold models; `src/main/scala/com/example/graphQL/cats/daos/Dao.scala` exposes the user repository. These are not a working application-submission service. Recheck them before using this example for a real task.

The target roadmap uses Scala 3/MongoDB. The coordinator must first place this feature after its required Foundation/domain/auth work, or explicitly scope prerequisite slices. Do not quietly include a store/language migration. Kafka, Spark, real notifications, embedding calls, and paid services are outside this illustrative slice.

## Contract and open decisions

- Use the canonical `submitApplication` mutation with an input type and typed application/error payload. Actor identity comes from authenticated context, not a client-provided candidate identity.
- UC04 requires initial `Created` status, open-job validation, duplicate protection, and atomic application/history writes. The GraphQL enum representation must follow the actual schema chosen in the prerequisite slice.
- Before readiness, specify the exact input fields, error codes, authenticated-context contract, and relevant persistence/migration approach. Do not infer these from nonexistent implementations.
- Preserve Admin's full authorized access. How Admin submits on behalf of a Candidate is a separate unresolved contract; this example specifies the Candidate route and ordinary Recruiter denial only.
- Data Engineer must define the concurrent job-close/submission guarantee and transaction ordering. Proposed criterion AC-05 requires rejection when closure commits before submission's authoritative open-state check; the overlapping-operation order must be specified and tested before readiness.

## Proposed acceptance criteria

These are planned checks, not current test results. Concrete test names/paths must be supplied by the implementation owner.

| ID | Given / When / Then | Verification | Actual outcome |
|---|---|---|---|
| AC-01 | Given an authenticated Candidate and open job, when valid submission succeeds, return Created and persist exactly one application and its initial history event | Service and DB/GraphQL integration checks | Not run |
| AC-02 | Given unauthenticated or ordinary Recruiter context, when the Candidate mutation is called, deny access and persist no application/history | Service authorization tests plus API integration | Not run |
| AC-03 | Given an attempt to supply another Candidate's identity, when input is processed, it must not change the authenticated owner or authorize another user's submission | Input/context tampering test | Not run |
| AC-04 | Given concurrent submissions for the same Candidate/job, when both finish, at most one succeeds and exactly one application/initial-history pair exists; duplicate failure is sanitized | Real-DB uniqueness and concurrency integration test | Not run |
| AC-05 | Given a closed job, including closure committed before the authoritative open-state check, when submitting, reject without application/history writes | Service validation and synchronized DB race test after ordering is specified | Not run |
| AC-06 | Given a failure between application and history writes, when the operation fails, neither write commits and the API exposes no internal exception | Transaction fault-injection integration test | Not run |
| AC-07 | Given missing/invalid required input or an unknown job, when submitting, return the agreed safe error and write nothing | Validation and GraphQL integration tests after contract definition | Not run |

UC04's p95 target is below 200 ms. Workload, environment, and cost ceiling are not established here; no performance claim is made. Establish them before any optimization or SLO acceptance gate.

## Handoff and evidence state

Product Manager resolves scope/dependencies. Architect handles prerequisite structural changes. Data Engineer defines persistence guarantees and tests; Scala Developer implements the assigned slice; Security Engineer reviews actor/ownership/error handling; Code Reviewer checks implementation; independent QA verifies the final criteria.

No implementation paths, feature test executions, benchmark results, or review verdicts exist for this example. The existing RoleSpec tests do not verify UC04. The next step for an actual UC04 request is to inspect the current prerequisites and turn this draft into a concrete scoped spec; the example itself remains documentation.
