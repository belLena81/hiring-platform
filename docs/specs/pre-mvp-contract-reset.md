# Pre-MVP Hiring Contract and Aggregate Revisions

## Identity and scope

- Status: in progress
- User outcome: one active public/API and event contract before MVP, with local operational data retained unless an explicit reset is enabled; Mongo user/job writes reject stale snapshots through internal revisions.
- Authorized scope: GraphQL, cursors, Mongo documents/setup and migration, events/Kafka, embedding metadata, documentation, fixtures, and tests.
- Non-goals: Git-history rewriting, dependency/toolchain protocol changes, production deployment.

## Decisions

- Startup preserves hiring data by default. Explicit reset drops every hiring-owned Mongo collection; normal startup runs a restartable, versioned migration before strict user/job decoding.
- User and job documents carry a non-negative Long version. New documents start at 0; every user/job write increments it atomically, while guarded writes compare _id and expected version. This is storage concurrency metadata, not a schema/API/event version.
- Concurrent writes use transactions and conditional identity/ownership/status predicates together with aggregate revisions where snapshots are written back.
- Migration 001_user_job_revisions backfills missing user/job revisions in bounded _id batches, verifies BSON type and non-negative value, then records completion in hiring_migration_ledger. Startup skips backfill and verification when that ledger entry is already Complete. A partial or concurrent run is safe to repeat and cannot reset an advanced revision; failed verification aborts startup. The durable embedding worker waits for setup completion before claiming work. Old binaries must be stopped before migration because strict old codecs reject the added field.
- Events use the unversioned topic and envelope; timestamp plus event ID is their only ordering key.
- The existing uncommitted GraphQL restructuring is the target API and must be reconciled, not discarded. The operational contract remains the active compatibility boundary; the [Hiring Analytics Lakehouse](hiring-analytics-lakehouse.md) spec owns derived analytics behavior and evidence.
- Authentication rate limits are enforced by the `login`, `signUp`, and `bootstrapAdmin` field resolvers for each executed field, including aliases and fragments. Exhaustion is a sanitized HTTP 200 GraphQL error with `RATE_LIMITED` and positive `retryAfter` seconds extensions.
- Authenticated GraphQL execution resolves the stored viewer once per request and reuses that verified snapshot for service authorization and nested email visibility. There is no cross-request actor cache; a successful account deletion invalidates the request snapshot.
- Parsed GraphQL documents use a process-local cache of at most 256 raw query texts for 60 seconds. A cache hit skips repeated static validation only; variables, authentication, rate limits, depth/complexity reducers, and field execution remain per request.
- Cursor signing uses the project JWT library with a cursor-specific derived key and explicit request time; it is safe across Cats Effect fibers, expires after the configured cursor TTL, and rejects legacy compact cursors. Argon2id work remains process-bulkheaded to the JVM-visible processor count. JWT expiry is represented by the public `Instant` scalar.

## Acceptance and evidence

| ID | Given / When / Then | Evidence | Actual outcome |
|---|---|---|---|
| PCR-01 | Startup runs against existing user/job documents without revisions | Revisions are backfilled without deleting data; rerunning or racing startup preserves completed state, skips backfill and verification after completion, and worker claims wait for setup | Implemented; replica-set backfill/CAS race, completed-ledger scan-skip, and setup-gated worker tests |
| PCR-02 | Clients use GraphQL, cursors, or events | Only one unversioned contract is emitted and accepted | Implemented; unit contract tests pass |
| PCR-03 | Concurrent lifecycle, embedding, or submission writes use an observed aggregate | One write from a given revision succeeds; every user/job write advances the revision once and preserves transaction behavior | Implemented; replica-set CAS and migration integration tests |
| PCR-04 | Repository docs and fixtures are inspected | Public contracts remain unversioned; internal revisions and migration recovery are documented | Implemented; SDL and codec checks |
| PCR-05 | Unit and disposable integration checks run | Compile, test, integration, and static checks pass | Unit suite passes (352/352); Mongo replica-set specs pass (9/9); full integration is partial (28/29) because `MainProcessSpec.P1-AC09` observed 17 `RUNTIME_FAILED` records where it expects 1 |
| PCR-06 | A client executes multiple public account fields or uses aliases/fragments | Each field consumes the address/operation bucket independently and limited fields return GraphQL retry metadata | Implemented; HTTP route and rate-limiter unit tests pass |
| PCR-07 | An authenticated operation contains multiple roots or nested user emails | The viewer is loaded once, service authorization remains enforced, and deletion prevents later authenticated fields in that request | In progress; focused unit and GraphQL checks pending |
| PCR-08 | Repeated valid GraphQL documents execute within the cache window | The second execution reuses the document with static validation skipped, while invalid documents are not cached and complexity/authentication still run | In progress; focused cache and HTTP checks pending |
| PCR-09 | Password, cursor, and token operations execute under load | Argon2 work is permit-bounded, cursor JWT signing is fiber-safe and expiring, legacy/tampered cursors are rejected, and JWT claims/Instant expiry remain contract-compatible | Implemented; focused cursor/config/GraphQL unit checks pass; integration runtime gate remains blocked by unrelated classpath failures |

## Review

- Code Reviewer: PASS; revision writer inventory, CAS behavior, migration race protection, and setup-gated embedding worker reviewed
- Security Engineer: PASS; no remaining security defects in the versioned write and migration changes
- QA: BLOCKED; full integration is 28/29 because `MainProcessSpec.P1-AC09` observed 17 `RUNTIME_FAILED` records instead of 1; the change is outside this refactor, but no clean baseline comparison was run
