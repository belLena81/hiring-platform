# API reference

This is a backend-only GraphQL API. The GraphQL equivalent of an OpenAPI contract is its schema (SDL), complemented by introspection and executable operations. The server exports the schema directly; no UI or Node toolchain is included in this repository.

Default base URL: `http://127.0.0.1:8080`.

| Endpoint | Purpose |
|---|---|
| POST `/graphql` | Execute a JSON-encoded GraphQL operation, including introspection |
| GET `/schema.graphql` | Download the current SDL without querying MongoDB |
| GET `/health` | Application liveness, HTTP 200 with UP |
| GET `/ready` | MongoDB connectivity, HTTP 200 READY or 503 NOT_READY |

## Browse and track the contract

An external GraphQL API client can load `/graphql` through introspection or import the SDL from `/schema.graphql`. An HTTP-only client can execute the examples below. Do not use OpenAPI generation as a substitute for the GraphQL schema: GraphQL operations select their own fields and share one execution endpoint.

The checked schema snapshot is [hiring.graphql](../src/test/resources/graphql/hiring.graphql). Backend contract tests compare it with the live schema definition, verify the download endpoint matches, and execute [health](../src/test/resources/graphql/health.graphql), [readiness](../src/test/resources/graphql/readiness.graphql), and [standard introspection](../src/test/resources/graphql/introspection.graphql) fixtures, and validate the [embedding coverage operation](../src/test/resources/graphql/embedding-coverage.graphql) against the live schema. Update schema and consumer fixtures together when contracts evolve; a schema diff alone does not establish compatible behavior.

The schema includes Hiring GraphQL operations for jobs, applications, status transitions, cursor connections, application history, and account lifecycle. `User.id`, `Job.id`, and `Application.id` use `UserID`, `JobID`, and `ApplicationID`; UUID-shaped search and interaction inputs use the named `UUID` scalar. Every mutation input requires an `idempotencyKey: UUID!`; reusing a key with the same caller scope and input replays the authoritative result, while a different input is rejected. GraphQL enum labels use SCREAMING_CASE, while domain and event values retain their existing internal representation. Queries return direct values: `me` and `job` are nullable, while connections remain non-null and contain no error field. Expected mutation domain outcomes are operation-specific unions containing the success type, `ValidationError`, and/or `DomainError`; clients must select union members with inline fragments. Authentication, authorization, availability, repository, account, and search failures are real GraphQL errors with `errors[].extensions.code`, and are not encoded in mutation payloads; the seven interview cancel and reschedule mutations are the exception for authorization and rate-limit refusals (see the interview section). Connection cursors are opaque signed JWTs carrying the keyset position, connection kind and expiration. `User.profile` is the nullable `UserProfile` union of `CandidateProfile` and `RecruiterProfile`; it is null for the singleton Admin and profile-less deleted accounts, while active Candidates and Recruiters expose only their matching union member. Admin provisioning uses explicit startup `auth.admin-seed` configuration; there is no public `bootstrapAdmin` mutation. `signUp` accepts only Candidates and Recruiters with exactly one matching profile, while Admin signup returns `ADMIN_BOOTSTRAP_ONLY`. Public `signUp` name collisions return generic `REGISTRATION_FAILED` rather than `NAME_TAKEN`. `login` issues short-lived HS256 bearer tokens with the required `AUTH_JWT_HS256_SECRET`. Login and signup are protected by process-local token buckets configured under `auth.rate-limit`; exhausted fields return a GraphQL error with `extensions.code = RATE_LIMITED` and a positive `extensions.retryAfter` value in seconds. The limiter uses the TCP peer by default and accepts trusted RFC 7239 `Forwarded` or fallback `X-Forwarded-For` client addresses only when that peer is in `http.trusted-proxy-cidrs`; configure only the actual trusted proxy networks in [application settings](../src/main/resources/application.conf). `me`, `updateMyProfile`, and initial `deleteMyAccount` calls derive identity from the token subject and stored user role. The deletion mutation returns a durable `DeletionReceipt` with `PENDING`; query `accountDeletionStatus(receiptId: ID!)` requires a verified token for the receipt owner and returns only that owner's `PENDING`, `COMPLETE`, or `NOT_FOUND` status. The deleted account can use a still-valid verified token to check status and replay the same deletion result; other operations still require active-user authorization. Credential storage uses Argon2id; password-bearing authentication request fingerprints are HMAC-protected with a key derived from `AUTH_RECEIPT_FP_SECRET` (the JWT secret when unset). Every token-producing receipt replay rechecks the referenced active account, canonical name and password. Changing the receipt key (or the JWT secret when no separate receipt secret is set) invalidates existing authentication receipt matches; use a new idempotency key. Email is optional and is not a login identity.

Cursor values are signed HS256 JWTs containing a connection kind, keyset timestamp, UUID tie-breaker, issuer/audience markers, and a configurable expiration (`auth.jwt.cursor-ttl-seconds`, 15 minutes by default). They are opaque to clients, validated with the cursor-specific derived key, and never accepted after expiration. `nearbyJobs` result cursors use the same signed codec with their own cursor kind, carry the distance, job id and a digest of the center, radius, filters and ordering they are bound to, and are rejected with `INVALID_CURSOR` when reused with different criteria; presenting a connection cursor to `nearbyJobs` (or the reverse) returns `WRONG_CURSOR_KIND`. Unlike the earlier unsigned nearby cursors, nearby cursors now expire after the same TTL as connection cursors, and a malformed nearby cursor reports the generic "Invalid cursor" message. Search filter validation reports every violated filter field in one `INVALID_SEARCH_FILTER` error message.

## Embedding coverage report

`embeddingCoverage(expectedModel: String): EmbeddingCoverageReport!` is an Admin-only, read-only aggregate query. The service rechecks the live Admin record (role Admin, account Active); any other actor receives `UNAUTHORIZED`. When vector search is disabled the query returns the typed `VECTOR_SEARCH_UNAVAILABLE` error rather than zero counts. `expectedModel` is optional, is trimmed, must be non-blank and at most 128 characters (`VALIDATION_FAILED` otherwise), and no model name is built in; without it `MODEL_MISMATCH` is never produced.

The report separates two dimensions. `freshness` (`CURRENT`, `CONTENT_CHANGED`, `MODEL_MISMATCH`, `NOT_EMBEDDED`) says whether the stored embedding matches the entity now; `repairState` (`NO_QUEUED_WORK`, `WAITING`, `RETRYING`, `IN_PROGRESS`, `LEASE_EXPIRED`, `FAILED`, with a separate `failureReason` for `FAILED`) says whether a repair is scheduled. `cells` holds one non-zero count per kind, freshness, repair state and failure reason; `kinds` reports searchable and scanned counts, `truncated` and `coverageShare` per kind. The population follows production eligibility: Open jobs, and Active Candidates with a profile (recruiter search opt-in is not applied). The response contains no ids, names, text, vectors or provider payloads.

`checks` contains `NO_ORPHANED_GAP` (a non-current entity has no queued work, so it will never be repaired) and `NO_STUCK_WORK` (queued work older than three durable retry caps past its availability), each `PASSED`, `FAILED` or `INCONCLUSIVE` with an offending count. The scan is bounded per kind by a fixed cap and `maxTime`; when the cap is reached `truncated` (or `queueTruncated`) is true, the affected checks are `INCONCLUSIVE` and never `PASSED`, and the counts are partial. Coverage share, lag percentiles and ages are informational. The entity scan needs snapshot read concern; a deployment without it (for example a standalone MongoDB server) fails the query with the repository-unavailable error. At most one `embeddingCoverage` root is accepted per request (complexity). Only jobs record a change time, so `lagSeconds` and `oldestNotCurrentAgeSeconds` cover the kinds listed in `lagEntityKinds` (jobs).

## Interview scheduling, cancellation and rescheduling

Fixtures: [scheduling operations](../src/test/resources/graphql/interview-scheduling.graphql) and [cancellation and reschedule operations](../src/test/resources/graphql/interview-cancellation.graphql), both validated against the live schema in the unit suite; the SDL snapshot above carries the types. The change is additive: nothing public was removed or renamed (`rescheduleInterview` was never exposed).

### Schema

```graphql
input InterviewActionInput { workflowId: UUID!, expectedRevision: Long!, idempotencyKey: UUID! }
input ProposeInterviewRescheduleInput {
  workflowId: UUID!, expectedRevision: Long!, startsAt: Instant!, endsAt: Instant!, idempotencyKey: UUID!
}
union InterviewActionResult = InterviewWorkflow | ValidationError | DomainError

type Mutation {
  cancelInterview(input: InterviewActionInput!): InterviewActionResult!
  requestInterviewReschedule(input: InterviewActionInput!): InterviewActionResult!
  dismissInterviewRescheduleRequest(input: InterviewActionInput!): InterviewActionResult!
  proposeInterviewReschedule(input: ProposeInterviewRescheduleInput!): InterviewActionResult!
  withdrawInterviewReschedule(input: InterviewActionInput!): InterviewActionResult!
  acceptInterviewReschedule(input: InterviewActionInput!): InterviewActionResult!
  declineInterviewReschedule(input: InterviewActionInput!): InterviewActionResult!
}
```

`InterviewWorkflow` gains nullable `pendingStartsAt`, `pendingEndsAt` (the replacement held after the candidate accepted), `cancelledAt`, `proposedStartsAt`, `proposedEndsAt`, `proposalExpiresAt` (the open proposal) and `rescheduleRequested: Boolean!`. A workflow is visible only to its candidate, its owning recruiter and Admin, and nothing identifies who proposed: the inputs and the type carry no actor, role, proposer or initiator. `cancelInterview` has no text field; cancelling records system-generated feedback chosen by the trusted initiator (candidate, recruiter or administrator).

`progress` is the workflow phase: `ReservationPending`, `StatusCommitPending`, `CompensationPending`, `NotificationsPending`, `Completed`, `RepairRequired` (scheduling); `ProposalPending` (a proposal awaits the candidate; the old slot is untouched); `CancelPending`, `CancelNotificationsPending`, `Cancelled` (cancellation); `RescheduleHoldPending`, `RescheduleSwapPending`, `RescheduleCancelOldPending`, `RescheduleNotificationsPending`, `RescheduleCompensationPending` (an accepted proposal being booked). Mutations return the workflow as stored right after the decision; provider calls and notifications follow asynchronously, so poll `interviewWorkflow` for the later phases.

### Authorization

The actor comes only from the authenticated request. A workflow the caller does not participate in is indistinguishable from a missing one (`NOT_FOUND`); a participant refused an action receives `FORBIDDEN`. Refusals are typed `DomainError` payloads for these seven mutations (so one refused alias does not discard the others); a missing or invalid token is still the `UNAUTHORIZED` GraphQL error.

| Action | Candidate on the application | Owning recruiter | Admin | Any other candidate or recruiter |
|---|---|---|---|---|
| `cancelInterview` (rejects the application) | allow | allow | allow | `NOT_FOUND` |
| `requestInterviewReschedule` | allow | `FORBIDDEN` | `FORBIDDEN` | `NOT_FOUND` |
| `dismissInterviewRescheduleRequest` | `FORBIDDEN` | allow | allow | `NOT_FOUND` |
| `proposeInterviewReschedule` | `FORBIDDEN` | allow | allow | `NOT_FOUND` |
| `withdrawInterviewReschedule` | `FORBIDDEN` | allow | allow | `NOT_FOUND` |
| `acceptInterviewReschedule` / `declineInterviewReschedule` | allow | `FORBIDDEN` | `FORBIDDEN` | `NOT_FOUND` |

Consent to a new time is the candidate's alone, so Admin cannot accept or decline. The candidate gains no other status power: `rejectApplication` and the other status mutations remain for the owning recruiter and Admin.

### Error codes (`DomainError.code`)

`NOT_FOUND`, `FORBIDDEN`, `STALE_REVISION` (the workflow moved since `expectedRevision`), `INVALID_STATUS_TRANSITION` (the application is no longer `Interview`, or the action does not fit the current state), `INTERVIEW_ALREADY_STARTED`, `INTERVIEW_ALREADY_CANCELLED`, `INTERVIEW_NOT_SETTLED` (an earlier cancel or reschedule is still being carried out), `PROPOSAL_ALREADY_OPEN`, `NO_OPEN_PROPOSAL`, `PROPOSAL_EXPIRED`, `NO_RESCHEDULE_REQUEST` (dismissing a request that was never made), `INVALID_INTERVIEW_INTERVAL` (the proposal must start in the future and end after it starts), `RESCHEDULE_INTERVAL_UNCHANGED`, `CONFLICT` (an idempotency key reused for a different operation or input; also a lost concurrent write) and `RATE_LIMITED`. Storage failures and unexpected states surface as the generic `UNAVAILABLE` GraphQL error. Messages are fixed text: no workflow, user or application identifiers, times or feedback text.

### Idempotency

Every action takes an `idempotencyKey`, scoped to the acting user. Repeating the same operation with the same input returns the stored workflow without a second effect. A key reused with a different operation or input is a `CONFLICT`. Exception: a `requestInterviewReschedule` whose flag is already set (a no-op) writes nothing, not even a receipt, so repeating it with any key is harmless but a key first used for such a no-op is not remembered for conflict detection.

### Notification guarantee

Cancel and reschedule notifications (cancelled, proposed, declined, withdrawn, expired, requested, rescheduled, could not be booked) are sent after the change is durable and are at-least-once on a non-local provider: a crash after the provider accepted a message but before the receipt is stored causes a resend, so a duplicate is possible and a silent loss is not (an exhausted notification becomes visible command-level repair for Admin). The local ledger provider keeps exactly one receipt per key.

### Rate limit and request bounds

Each authenticated actor may perform at most `auth.interview-action-rate-limit.attempts` interview actions (any of the seven mutations) per `window-seconds` (packaged values 30 per 60 seconds and 10000 users, process-local; the section is required, so a missing or invalid value fails startup; see [application settings](../src/main/resources/application.conf)). The check uses the verified token claims and runs before any storage work. Every aliased or batched action takes its own unit, so a request that exceeds the allowance has its surplus actions answered with `DomainError { code: "RATE_LIMITED" }` while the others keep their results; the message states no limit or count. This differs from the login and sign-up limit, which returns a top-level GraphQL error (`extensions.code = RATE_LIMITED` with `extensions.retryAfter` seconds, keyed by client address): an interview action is refused with a `DomainError` payload in `data` and no retry hint, keyed by the authenticated user. If more users than `max-buckets` are active in one window, further unseen users are refused until a window expires (fail closed) rather than evicting a counted user. Each action also weighs 100 in the query-complexity budget (1000 per request), so a single request can carry at most nine of them.

## Execute operations

```bash
curl -sS http://127.0.0.1:8080/schema.graphql
curl -sS http://127.0.0.1:8080/graphql \
  -H 'Content-Type: application/json' \
  --data '{"query":"query PlatformHealth { health { status } readiness { status } }"}'
```

With MongoDB available:

```json
{"data":{"health":{"status":"UP"},"readiness":{"status":"READY"}}}
```

During a database outage the same executed GraphQL operation still returns HTTP 200, with `readiness.status` equal to `NOT_READY`. Liveness does not access MongoDB.

By default, GraphQL execution has a five-second HTTP deadline and a four-second resolver-service deadline. Because Sangria leaf actions are `Future`-based, an HTTP timeout cannot cancel a resolver already running after the Cats Effect to Future bridge. The service bound limits cooperative `IO` work, but a timed-out mutation may still have completed; clients must determine the outcome through a subsequent authorized read instead of blindly retrying.

Named operations and variables:

```json
{
  "query": "query Readiness($include: Boolean!) { readiness @include(if: $include) { status } }",
  "operationName": "Readiness",
  "variables": {"include": true}
}
```

Send `Content-Type: application/json`; GraphQL responses negotiate between `application/graphql-response+json` and `application/json` and echo the selected media type. An absent `Accept` header prefers `application/graphql-response+json`; unsupported response media returns 406. GraphQL request envelopes are decoded by http4s/Circe, while GraphQL document parsing remains owned by Sangria.

`query` is required. Missing/null `variables` means an empty object, and missing/null `operationName` means no selected name. When the document contains several operations, provide an operation name. Supply required variables explicitly; the pinned Sangria version has a documented limitation with omitted required-variable defaults.

## Errors and limits

Errors use a sanitized `errors` array. The active 32-character hexadecimal OpenTelemetry trace ID is the request correlation ID; W3C `traceparent` propagation and response headers are owned by the HTTP server middleware. Invalid syntax/schema/variables/operation selection or query budgets produce 400; unsupported method/media produce 405/406/415; oversized body 413; overload 503; deadline 504; unexpected failures 500. GraphQL field failures, including rate-limited login/signup fields, use HTTP 200 with `errors[].extensions.code`; rate-limit errors additionally include `errors[].extensions.retryAfter` in seconds. Expected mutation domain outcomes are selected from operation-specific unions in `data`. Stable codes such as `UNAUTHORIZED`, `FORBIDDEN`, `NOT_FOUND`, `DUPLICATE_APPLICATION`, `INVALID_STATUS_TRANSITION`, `PROFILE_ROLE_MISMATCH`, and `PROFILE_UNSUPPORTED_FOR_ROLE` never expose raw driver messages or stack traces.

The [HTTP configuration](../src/main/resources/application.conf) and [GraphQL routes](../src/main/scala/com/example/graphQL/cats/api/http/GraphQLHttpRoutes.scala) define request admission, request size, depth/complexity and deadline bounds. Introspection uses those same limits. Run `sbt test` for contract checks and `sbt 'IntegrationTest / test'` for real HTTP/database checks.
