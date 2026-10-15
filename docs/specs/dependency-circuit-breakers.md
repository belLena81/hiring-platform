# Dependency Circuit Breakers

## Identity and scope

- Status: done (local). The spec was reopened on 2026-10-14 for an eligibility regression with no `DiscoveryQueryPolicy` (CBR-04.5). The regression was fixed and QA re-checked it. Still open: Kafka integration (CBR-05.3) is blocked, and CBR-06.3 is unverified. Final whole-checkout gates run with the refactoring's final QA.
- Outcome: when an outbound dependency is failing, hiring requests and background work fail fast with the existing sanitized errors instead of waiting for timeouts. The dependency recovers through bounded probing, and no hiring write is rejected because of a business conflict.
- Context: Hiring, plus Search and embedding (`docs/domain-language.md`). Aggregates touched: none. Breakers guard adapters and never change aggregate invariants.
- Scope: one reusable breaker, applied to four dependencies:
  - the Voyage embedding provider;
  - Atlas `$search` / `$vectorSearch` discovery queries;
  - the Kafka operational-event publisher;
  - Mongo transactional writes.
- Non-goals:
  - new GraphQL error codes, fields or readiness values;
  - "degraded" readiness reporting (owned by HOR-03 in [hiring observability resilience](hiring-observability-resilience.md));
  - distributed or shared breaker state across instances;
  - breakers in the analytics build;
  - a resilience library dependency.
- Effect style: the breaker is written over `F[_]` so the [effect-polymorphic services](effect-polymorphic-services.md) migration does not have to change it.

## Source facts (verified 2026-10-14)

- No breaker exists, and `build.sbt` has no resilience library. Available building blocks: `Ref`, `Clock`, `cats-retry`, `RetryBackoff.exponentialMillis` (`domain/policy/RetryBackoff.scala`).
- Voyage (`infrastructure/embedding/VoyageEmbeddingService.scala:59-81`) has a timeout and maps failures to `EmbeddingError.ProviderUnavailable`, which reaches GraphQL as `PROVIDER_UNAVAILABLE`. `EmbeddingPipeline` retries durably with backoff.
- Discovery (`repository/mongo/DiscoveryQueryPolicy.scala`) takes a `Semaphore` permit and applies `maxTime`. Its failures map to `RepositoryError.Unavailable`.
- The Kafka publisher (`infrastructure/kafka/OperationalEventKafkaRuntime.scala`) claims outbox waves. A failed batch is released for retry, and the stream restarts with jittered backoff. A fenced producer stops the stream.
- Mongo transactions (`repository/mongo/MongoTransactionRunner.scala`) retry transient errors and an unknown commit result 3 times each. Errors map through `MongoErrors.toRepositoryError`: write conflict and transient exhaustion become `Conflict` (or the caller's choice), and everything else becomes `Unavailable`.
- Readiness (`runtime/MongoHiringRuntime.scala:285-301`) decides the status from the Mongo ping and the Mongo-writes breaker only; since ARR-07 embedding-worker health is listed in the `degraded` body array and `/ready` stays 200. `ProbeResult` is `Ready | Unavailable | AuthenticationFailed`.

## Rules

1. **Counting failures.** A breaker counts only failures classified as dependency unavailability, within a sliding window. Business outcomes are never counted, and neither are caller-side validation or query-dependent failures such as `maxTime` expiry (one user could otherwise open the breaker for everyone). Cancellation is ignored, except a Mongo write cancelled after one of its own attempts already failed as unreachable (retries can outlast the request timeout).
2. **Opening.** When counted failures reach the threshold within the window, the breaker opens until `now + openDuration`. Repeated openings back off exponentially, up to `maxOpenDuration`.
3. **While open.** Calls are rejected without touching the dependency, and each rejection maps to the error that dependency already returns.
4. **Half-open.** Once the open period ends, at most `halfOpenProbes` calls go through. A success closes the breaker and resets the backoff; a counted failure reopens it.
5. **Mongo writes:**
   - the breaker classifies by exception type and cause chain, never by label: `MongoTimeoutException` (server selection, pool wait), `MongoSocketException` including read timeout, not-primary, node-recovering and pool-cleared. The driver labels these `TransientTransactionError` inside a transaction, so a label does not exempt them. They are counted only after the runner's own retries are exhausted and then map to `Unavailable`;
   - a write conflict that exhausts its retries stays `Conflict` and is not counted; command errors (112, 11000, 50) are never counted;
   - only a committed result closes the breaker: a typed rejection (`Right(Left(_))`) is an ignored outcome, because it may not have touched the database;
   - write conflict, duplicate key, CAS `Conflict`, `AuthorityRevoked` and validation are never counted;
   - an open breaker rejects before a session starts, so no receipt or partial write exists;
   - the readiness ping never goes through the breaker.
6. **Kafka publisher.** Only transport and broker-availability errors count: timeouts, network, disconnect, not-leader, broker-not-available and other `RetriableException`s. Per-record errors (record too large, serialization, invalid topic), authorization errors and producer fencing never count, so one poison event cannot stop publication. While the breaker is open the publisher claims nothing and waits until the breaker is half-open; a probe claims one event. A send refused by the breaker is released for retry and is not a delivery attempt. Outbox rows stay durable, so nothing is lost and nothing is published twice beyond the existing at-least-once contract.
7. **Readiness.** An open Mongo-writes breaker makes `/ready` report `Unavailable`. The other breakers do not change readiness.
8. **Diagnostics.** Every state change (`Closed → Open`, `Open → HalfOpen`, `HalfOpen → Closed`, `HalfOpen → Open`) is emitted once, with the breaker name and no request data.
9. **Configuration.** Typed and validated at startup, per breaker: `enabled`, `failure-threshold ≥ 1`, `window-ms`, `open-ms ≤ max-open-ms`, `half-open-probes ≥ 1`. A disabled breaker passes every call through.

## Acceptance and evidence

| ID | Given / When / Then | Planned verification | Actual outcome |
|---|---|---|---|
| CBR-01 | Pure transitions. CBR-01.1: threshold 3, three counted failures within the window, so the breaker opens. CBR-01.2: two failures, then the window passes, then one more, so it stays closed. CBR-01.3: an ignored failure while closed leaves the count unchanged. CBR-01.4: open with the time not yet reached, so an attempt is rejected with `retryAt`. CBR-01.5: open with the time reached, so the attempt is admitted as half-open. CBR-01.6: half-open success closes and resets `consecutiveOpens`. CBR-01.7: half-open failure reopens with a doubled duration, capped at the maximum. CBR-01.8: half-open with all probes in flight, so the attempt is rejected. | `CircuitBreakerPolicySpec` | Pass: `CircuitBreakerPolicySpec`, 12 tests (CBR-01.1 to 01.9, cycle-tagged probes) |
| CBR-02 | Effectful breaker under `TestControl`. CBR-02.1: concurrent callers hitting the threshold open the breaker exactly once. CBR-02.2: a cancelled protected call releases its half-open slot and is not counted. CBR-02.3: an open breaker does not evaluate the protected effect. CBR-02.4: a disabled breaker passes everything through. CBR-02.5: each state change is emitted once. CBR-02.6: a failing transition listener neither fails the call nor leaks a probe slot. CBR-02.7: a probe of an earlier cycle finishing late cannot close or reopen the current cycle. | `CircuitBreakerSpec` | Pass: `CircuitBreakerSpec` (9 tests, `TestControl`, including failing listener and stale-cycle probe) and `DependencyReadinessSpec` (02.4, 02.5 diagnostics once per change) |
| CBR-03 | Voyage. CBR-03.1: after threshold provider failures, `semanticJobSearch` returns `PROVIDER_UNAVAILABLE` without calling the provider. CBR-03.2: a 4xx validation-type response other than 429 is not counted. CBR-03.3: after the open period, one successful probe restores normal calls. CBR-03.4: a breaker rejection (`EmbeddingError.CircuitOpen`, surfaced publicly as `PROVIDER_UNAVAILABLE`) is retried durably with `chargeAttempt = false` and does not burn the in-memory retry loop. | Embedding service and search service specs with a fake client | Pass: `VoyageEmbeddingServiceSpec` CBR-03.1 to 03.3 (rejection is `CircuitOpen`), `EmbeddingPipelineSpec` CBR-03.4 (six calls, no attempt charged) |
| CBR-04 | Discovery. CBR-04.1: repeated network and server-selection failures open the breaker, and discovery returns `Unavailable` before taking a permit. CBR-04.2: a query-shape or validation error is not counted. CBR-04.3: `maxTime` expiry is not counted. CBR-04.4: the discovery guard re-raises only unreachable-deployment errors to the policy and maps the rest in place. | `DiscoveryQueryPolicy` spec | Pass: `DiscoveryQueryPolicySpec` CBR-04.1 to 04.4 |
| CBR-05 | Kafka publisher. CBR-05.1: send failures open the breaker, after which no outbox rows are claimed while it is open. CBR-05.2: producer fencing is not counted and still stops the stream. CBR-05.3: after recovery the outbox drains with no lost events. | Publisher runtime spec; Kafka integration | Unit pass: `OperationalEventKafkaRuntimeSpec` CBR-05.1 to 05.4 (claim gating from one state read, fencing and poison events not counted, broker-error classification). Kafka integration UNVERIFIED: `OperationalEventComposeIntegrationSpec` skipped, no Kafka service in this environment |
| CBR-06 | Mongo writes. CBR-06.1: N parallel duplicate `submitApplication` calls for one candidate and job, so one succeeds, the others return `DuplicateApplication` or `Conflict`, and the breaker stays closed. CBR-06.2: repeated server-selection or timeout failures open it, and a mutation then returns `Unavailable` without inserting a receipt. CBR-06.3: the same idempotency key retried after recovery succeeds exactly once. CBR-06.4: `/ready` reports `Unavailable` while the breaker is open, and the readiness ping bypasses it. CBR-06.5: a labelled timeout, socket-read-timeout or not-primary error that exhausts its retries opens the breaker and returns `Unavailable`; an exhausted write conflict stays `Conflict`. CBR-06.6: a call cancelled after an unreachable attempt counts, and one cancelled without such an attempt does not. CBR-06.7: a typed rejection cannot close a half-open breaker. CBR-06.8: a transient failure that the runner's own retries absorb is not counted. | Runner unit spec with fault injection; Mongo integration | Pass (runner level): `MongoTransactionRunnerSpec` CBR-06.1, 06.2, 06.4 to 06.8 (fault-injected sessions, `TestControl`); `DependencyReadinessSpec` CBR-06.4; `MongoWriteBreakerIntegrationSpec` CBR-06.1 against real Mongo. CBR-06.2 "no receipt" is shown as "no session started", not through the mutation or receipt path. **CBR-06.3 unverified:** the runner test shows only that a write is accepted again after a successful probe. Exactly-once retry by idempotency key after an induced outage has not been tested |
| CBR-07 | Configuration. CBR-07.1: valid defaults load. CBR-07.2: `open-ms > max-open-ms` and `failure-threshold = 0` are both reported in a single startup error. CBR-07.3 (rule 8): a disabled breaker logs one warning-level line with its name and no configuration values. | Config reader and validation specs | Pass: `ResilienceConfigSpec` CBR-07.1, 07.2; `DependencyReadinessSpec` CBR-07.3 |
| CBR-08 | Contracts and gates. No GraphQL SDL, BSON or event change. `sbt test`, the relevant `IntegrationTest`, and formatting pass. Code Reviewer, Security Engineer and QA verdicts are recorded. | Gates and reviews | Pass, with Kafka integration blocked. QA's own runs before the eligibility fix (pre-fix figures):
- `sbt -batch test`: 1143 passed, 0 failed.
- `IntegrationTest` across 8 specs (write breaker, job submission, transaction, discovery, embedding work, probe, interview cancellation race, outbox retry): 47 passed, 0 failed.
- `scalafmtCheckAll` and `scalafmtSbtCheck`: clean.
- `OperationalEventComposeIntegrationSpec`: skipped, no Kafka service, so **BLOCKED and unverified**.
- No GraphQL, BSON or event file changed. Checked statically only; no SDL snapshot test exists.
- Fix after the effect-polymorphism review (2026-10-14): `MongoSemanticSearchRepository.authorizedEligibility` had started failing closed (`Unavailable`) when no `DiscoveryQueryPolicy` was configured, which broke `MongoSearchAuthorizationIntegrationSpec`. With a policy it keeps permit, breaker and `discoveryGuard`; with `None` it runs through `discoveryGuard` and maps unreachable-deployment failures to `Unavailable` in place (`MongoSemanticSearchRepository.unmetered`). The search roots (`searchJobs`, `recommendedJobs`, `candidateMatches`) keep `None` -> `Unavailable`. New `EligibilityWithoutDiscoveryPolicySpec` (3 tests, tagged CBR-04.5). `MongoSearchAuthorizationIntegrationSpec` assertions are unchanged.
- Full `sbt -batch IntegrationTest/test` after the fix: 305 total, 280 passed, 0 failed, 25 skipped (capability-gated: Atlas production fusion and rerank gates in `AtlasHiringSearchIntegrationSpec`, `MongoEmbeddingCoverageIntegrationSpec` ECR-10, `OperationalEventComposeIntegrationSpec` and Kafka restart specs that need Compose Kafka or Atlas). Skipped tests are unverified, not passed.
- Correction to the line above: an SDL snapshot test does exist. `HiringGraphQLContractSpec` DHW-31 compares the SDL with `src/test/resources/graphql/hiring.graphql`.
- QA re-check after the fix (QA's own runs):
  - `IntegrationTest` SearchAuthorization, JobDiscovery and WriteBreaker: 11 passed, 0 failed.
  - Unit tests `EligibilityWithoutDiscoveryPolicySpec` and `DiscoveryQueryPolicySpec`: 8 passed, 0 failed.
  - The developer's full `IntegrationTest` count above was not re-run by QA.
  - The final whole-checkout `sbt test`, formatting and full integration gates belong to the final refactoring QA, because the working tree now also holds the effect-polymorphism slices.
- Verdicts: Code Reviewer PASS, Security Engineer PASS, Data Engineer PASS (on re-review), QA PASS (re-checked after the eligibility fix). |

## Accepted behaviour and risks

- Connection-pool wait timeouts (`MongoTimeoutException`) count as unavailability: failing fast under pool saturation is intended. Consequence: sustained saturation opens the Mongo-writes breaker, `/ready` reports `Unavailable` until the open period ends, and readiness can flap while saturation persists.
- Writes outside the breaker by design: the outbox repository's `markPublished`, `releaseForRetry`, `markFailed` and claim-page find, and the embedding-work, consumer-receipt and quarantine repositories. They keep their own error handling, so a Mongo outage can still surface there while the breaker is closed.
- `semanticSearch.authorizedEligibility` was not under the discovery permit before; it now runs through the same admission and breaker as the other search queries.
- Voyage 429 and 5xx responses now emit `EmbeddingProviderFailed`, as other provider failures do.
- Breaker timing uses wall-clock time (`Clock.realTimeInstant`); a clock step can shorten or lengthen an open period.
- Disabled breakers are logged at startup (warning, name only).
- The discovery breaker no longer counts server `maxTime` expiry, so a single caller cannot trip it for everyone. Tradeoff: a deployment that stays reachable but hung, where queries only time out, does not open the discovery breaker. Those requests still end at the per-query `maxTime` and the bounded permits.
- A search request takes its search permit and then its eligibility permit one after the other, never both at once. There is no deadlock, but each request adds more queueing on the shared discovery semaphore.
- The Kafka breaker counts every `RetriableException`, which includes broker-side conditions such as not-enough-replicas. These are treated as broker unavailability.
- Re-review verdicts (2026-10-14): Data Engineer PASS (reversing the earlier FAIL), Security Engineer PASS, Code Reviewer PASS.

## Implementation handoff

- Owner: Scala Developer.
- Write paths:
  - the breaker core and wrapper in `domain/policy/` and `service/resilience/`;
  - the four wrap points;
  - runtime wiring and configuration models, readers and validation;
  - `application.conf`;
  - tests under `src/test` and `src/it`.
- Reviews: Data Engineer reviews the Mongo error classification. Security Engineer reviews the Mongo-write and readiness behaviour. Code Reviewer, then QA.

## Checkpoint

- 2026-10-14: spec ready; implementation started.
- 2026-10-14: CBR-01 to CBR-07 implemented (first pass). Pure core `domain/policy/CircuitBreakerPolicy.scala`, `F[_]: Temporal` wrapper `service/resilience/CircuitBreaker.scala`, wiring in `runtime/DependencyBreakers.scala` and `MongoHiringRuntime`. `cats-effect-testkit` added as a Test dependency for `TestControl`. Decisions: readiness uses `admits` (not "is open") so it recovers once the open period ends; discovery repositories re-raise only dependency-unavailability errors to the policy boundary (`MongoRepositorySupport.discoveryGuard`); a send rejected by the Kafka breaker is released without counting as a delivery attempt. Open: CBR-05.3 Kafka integration unverified; reviews (Data Engineer, Security, Code, QA) pending.
- 2026-10-14: review fixes applied. Mongo network-class errors are classified by type regardless of the transient label and map to `Unavailable`; cancelled writes that saw an unreachable attempt count (effectful classifier); discovery counts only unreachable deployments (maxTime dropped) and eligibility queries now share its admission; Kafka counts broker-availability errors only and reads breaker state once; `EmbeddingError.CircuitOpen` defers durable work without charging; probes carry a cycle token; listener failures are swallowed; typed rejections cannot close the breaker; disabled breakers are logged; `CircuitBreakerName` enum replaces the string constants. Open: CBR-05 Kafka integration unverified, independent re-review pending.
