# Pre-MVP Contract Reset

## Identity and scope

- Status: in review
- User outcome: one current, unversioned hiring contract before MVP, with no compatibility code; local state is retained unless an explicit reset is enabled.
- Authorized scope: GraphQL, cursors, Mongo documents/setup, events/Kafka, embedding metadata, documentation, fixtures, and tests.
- Non-goals: Git-history rewriting, dependency/toolchain protocol changes, production deployment.

## Decisions

- Startup drops every hiring-owned Mongo collection and recreates only the active shape. No migration ledger, backfill, legacy read, or compatibility window remains.
- API, document, cursor, event, entity, embedding, and search-model version fields are removed. Scala/JDK/dependency versions and required third-party protocol paths remain.
- Concurrent writes use transactions and conditional identity/ownership/status predicates, never stored revisions.
- Events use the unversioned topic and envelope; timestamp plus event ID is their only ordering key.
- The existing uncommitted GraphQL restructuring is the target API and must be reconciled, not discarded. Historical docs/specs are removed; this is the sole active spec.
- Authentication rate limits are enforced by the `login`, `signUp`, and `bootstrapAdmin` field resolvers for each executed field, including aliases and fragments. Exhaustion is a sanitized HTTP 200 GraphQL error with `RATE_LIMITED` and positive `retryAfter` seconds extensions.
- Authenticated GraphQL execution resolves the stored viewer once per request and reuses that verified snapshot for service authorization and nested email visibility. There is no cross-request actor cache; a successful account deletion invalidates the request snapshot.
- Parsed GraphQL documents use a process-local cache of at most 256 raw query texts for 60 seconds. A cache hit skips repeated static validation only; variables, authentication, rate limits, depth/complexity reducers, and field execution remain per request.
- Cursor signing uses the project JWT library with a cursor-specific derived key and explicit request time; it is safe across Cats Effect fibers, expires after the configured cursor TTL, and rejects legacy compact cursors. Argon2id work remains process-bulkheaded to the JVM-visible processor count. JWT expiry is represented by the public `Instant` scalar.

## Acceptance and evidence

| ID | Given / When / Then | Evidence | Actual outcome |
|---|---|---|---|
| PCR-01 | Startup runs against any existing hiring database | Owned collections are empty then recreated without a migration ledger | Implemented; integration source compiles |
| PCR-02 | Clients use GraphQL, cursors, or events | Only one unversioned contract is emitted and accepted | Implemented; unit contract tests pass |
| PCR-03 | Concurrent lifecycle writes occur | Conditional transactional writes preserve authorization, transitions, and duplicate/closed-job rules without revisions | Implemented; clean unit suite and targeted Mongo observed-state CAS integration tests pass |
| PCR-04 | Repository docs and fixtures are inspected | No historic migration/version compatibility material remains | Implemented; retired specs, plans, and fixtures removed |
| PCR-05 | Unit and disposable integration checks run | Compile, test, integration, and static checks pass | Unit suite and integration compilation pass; live Docker execution pending |
| PCR-06 | A client executes multiple public account fields or uses aliases/fragments | Each field consumes the address/operation bucket independently and limited fields return GraphQL retry metadata | Implemented; HTTP route and rate-limiter unit tests pass |
| PCR-07 | An authenticated operation contains multiple roots or nested user emails | The viewer is loaded once, service authorization remains enforced, and deletion prevents later authenticated fields in that request | In progress; focused unit and GraphQL checks pending |
| PCR-08 | Repeated valid GraphQL documents execute within the cache window | The second execution reuses the document with static validation skipped, while invalid documents are not cached and complexity/authentication still run | In progress; focused cache and HTTP checks pending |
| PCR-09 | Password, cursor, and token operations execute under load | Argon2 work is permit-bounded, cursor JWT signing is fiber-safe and expiring, legacy/tampered cursors are rejected, and JWT claims/Instant expiry remain contract-compatible | Implemented; focused cursor/config/GraphQL unit checks pass; integration runtime gate remains blocked by unrelated classpath failures |

## Review

- Code Reviewer: pending final implementation review
- Security Engineer: pending final implementation review
- QA: pending final implementation validation
