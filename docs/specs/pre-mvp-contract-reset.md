# Pre-MVP Contract Reset

## Identity and scope

- Status: in review
- User outcome: one current, unversioned hiring contract before MVP, with no compatibility code or retained local state.
- Authorized scope: GraphQL, cursors, Mongo documents/setup, events/Kafka, embedding metadata, documentation, fixtures, and tests.
- Non-goals: Git-history rewriting, dependency/toolchain protocol changes, production deployment.

## Decisions

- Startup drops every hiring-owned Mongo collection and recreates only the active shape. No migration ledger, backfill, legacy read, or compatibility window remains.
- API, document, cursor, event, entity, embedding, and search-model version fields are removed. Scala/JDK/dependency versions and required third-party protocol paths remain.
- Concurrent writes use transactions and conditional identity/ownership/status predicates, never stored revisions.
- Events use the unversioned topic and envelope; timestamp plus event ID is their only ordering key.
- The existing uncommitted GraphQL restructuring is the target API and must be reconciled, not discarded. Historical docs/specs are removed; this is the sole active spec.
- Authentication rate limits are enforced by the `login`, `signUp`, and `bootstrapAdmin` field resolvers for each executed field, including aliases and fragments. Exhaustion is a sanitized HTTP 200 GraphQL error with `RATE_LIMITED` and positive `retryAfter` seconds extensions.

## Acceptance and evidence

| ID | Given / When / Then | Evidence | Actual outcome |
|---|---|---|---|
| PCR-01 | Startup runs against any existing hiring database | Owned collections are empty then recreated without a migration ledger | Implemented; integration source compiles |
| PCR-02 | Clients use GraphQL, cursors, or events | Only one unversioned contract is emitted and accepted | Implemented; unit contract tests pass |
| PCR-03 | Concurrent lifecycle writes occur | Conditional transactional writes preserve authorization, transitions, and duplicate/closed-job rules without revisions | Implemented; focused service/repository tests pass |
| PCR-04 | Repository docs and fixtures are inspected | No historic migration/version compatibility material remains | Implemented; retired specs, plans, and fixtures removed |
| PCR-05 | Unit and disposable integration checks run | Compile, test, integration, and static checks pass | Unit suite and integration compilation pass; live Docker execution pending |
| PCR-06 | A client executes multiple public account fields or uses aliases/fragments | Each field consumes the address/operation bucket independently and limited fields return GraphQL retry metadata | Implemented; HTTP route and rate-limiter unit tests pass |

## Review

- Code Reviewer: pending
- Security Engineer: pending
- QA: pending
