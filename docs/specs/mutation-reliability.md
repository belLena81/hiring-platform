# Mutation Reliability

## Identity and scope

- Status: in progress
- User outcome: GraphQL mutation clients receive typed expected outcomes, can safely retry a timed-out mutation with the same idempotency key, and never receive false confirmation for interaction telemetry.
- Authorized scope: mutation error contract, idempotency receipts, search-session handoff, Mongo setup, tests, and canonical API/schema documentation.
- Non-goals: dependency changes, deployment, Kafka protocol changes, paid infrastructure, or a generic workflow framework.

## Decisions

- Mutation payload unions use typed Scala 3 outcomes. `ValidationError` and `DomainError` remain concrete union members and implement additive `UserError { code, message }`.
- Validation, client-visible domain rejections, repository duplicate/conflict results, and account workflow rejections are typed payload outcomes. Authentication, authorization, availability, repository unavailability, search-provider failures, and rate limiting remain sanitized GraphQL errors.
- Every mutation input carries `idempotencyKey: UUID!`. Receipts retain a SHA-256 request fingerprint and non-secret outcome metadata. Retries rehydrate authorized entity results and issue a fresh authentication token rather than persisting credentials or tokens.
- GraphQL mutation fingerprints hash compact Circe JSON with recursively sorted object keys. Full mutation input fields remain represented; arrays preserve order and absent optional values encode as `null`. Application status actions use a structured object with application ID, uppercase status, feedback, and reason.
- Fingerprint serialization cuts over immediately. Receipts created with the prior string-rendering format can mismatch on replay until MongoDB's TTL process deletes them; the transition does not add a compatibility fallback or rewrite stored hashes.
- Search-session materialization is durable and asynchronous. A request persists work before returning search results; an application-owned worker persists the session plus outbox event. An owned interaction before materialization receives `SEARCH_SESSION_PENDING` and must retry.
- The user selected setup-only addition for the new Mongo collections and indexes. Local data is preserved unless the existing explicit reset setting is enabled.

## Acceptance and evidence

| ID | Given / When / Then | Implementation and test paths | Verification | Actual outcome |
|---|---|---|---|---|
| MR-01 | A mutation resolves success, validation, or expected domain/account conflict | GraphQL model, types, resolvers, SDL tests | `sbt test` | Not run |
| MR-02 | Authentication, authorization, availability, and unavailable storage failures occur | GraphQL error mapping tests | `sbt test` | Not run |
| MR-03 | A client retries any mutation with the same key after a timeout | receipt service/repository and HTTP integration tests | `sbt test`; `sbt 'IntegrationTest / test'` | Not run |
| MR-04 | A key is reused with different input or while the original is active | receipt unit and integration tests | `sbt test` | Not run |
| MR-05 | Search session handoff is pending, succeeds, retries, fails, or shuts down | worker, interaction, diagnostics tests | `sbt test`; `sbt 'IntegrationTest / test'` | Not run |
| MR-06 | Mongo setup runs against retained local data | setup/index integration checks | `sbt 'IntegrationTest / test'` | Not run |
| MR-07 | A mutation input is fingerprinted or a status action is replayed | Circe encoders and sorted-key compact JSON produce stable fingerprints; old-format receipt keys may conflict during the cutover TTL window | `HiringGraphQLResolverSupportSpec`; `MongoHiringRepositoryTransactionIntegrationSpec`; `sbt test`; `sbt 'IntegrationTest / test'` | Focused resolver spec 6/6; focused Mongo receipt integration spec 6/6 (including prior-format receipt conflict); full unit suite 354/355 (unrelated `AuthRateLimiterSpec` failure); full integration suite 28/29 (`MainProcessSpec.P1-AC09` observed 17 `RUNTIME_FAILED` records instead of 1; run before the new focused receipt case) |

## Recovery and limits

- Receipts are unique by caller scope, mutation name, and idempotency key; request fingerprint mismatch is rejected without executing a write.
- Search-session workers use batch size 16, parallelism 4, 30-second leases, three attempts, 250 ms to five-second backoff, and seven-day terminal retention. These are local defaults, not performance claims.
- If durable search handoff cannot be persisted, search data still returns and a sanitized correlated diagnostic is emitted; interaction attribution for that search is unavailable.

## Review

- Code Reviewer: pending final implementation review
- Security Engineer: pending final implementation review
- QA: pending final implementation validation
