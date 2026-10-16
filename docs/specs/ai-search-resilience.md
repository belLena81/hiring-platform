# AI Search Resilience and Cost Transparency

## Identity and scope

- Task: close gaps G1-G3 and G5-G7 from the AI-features-as-GraphQL-fields research (2026-10-15): AI search fields fail independently, degrade to keyword search, never fail because of analytics recording, and show callers their cost budget.
- Status: done locally (S1-S4); AIR-07.1 unverified until run against Atlas Local.
- Coordinator: Product Manager. Owners: Software Architect (design below), Scala Developer (S1, S2, S4, service part of S3), Data Engineer (repository part of S3). Reviewers: Code Reviewer and Security Engineer per slice; QA on the final state.
- User outcome: a screen that mixes ordinary data with AI search still renders when the embedding provider or vector search is down; candidates and recruiters get keyword results instead of an error; partner developers can see their remaining query budget.
- Non-goals: token-bucket budget (deferred, see D4); `RateLimit` HTTP headers (draft syntax unverified); plan tiers; `@cost` directives; structured fallback for `recommendedJobs`; operational event contract change (no `mode` field); pagination of ranked results (D2 of the contract-consistency spec stands); LLM features and streaming.
- Dependencies: [contract consistency](hiring-graphql-contract-consistency.md) (ErrorCode enum, `UserError`, D2), [architecture review remediation](architecture-review-remediation.md) (ARR-14 limiter properties), [dependency circuit breakers](dependency-circuit-breakers.md).

## Source facts (Software Architect, read 2026-10-15)

- AI roots return `RankedJobResults` / `RankedCandidateResults` (`api/graphql/HiringGraphQLSearchResolvers.scala:85,105,122`; facets `:63`); every use-case error goes through `raiseOnUseCaseError` (`HiringGraphQLResolverSupport.scala:27`) and becomes a top-level error; the non-null roots then null `data`.
- Mutations already map non-exceptional failures to data through `mutationResult` (`HiringGraphQLResolverSupport.scala:33-44`) and `MutationOutcome[+A] = A | ValidationError | DomainError` (`HiringGraphQLModel.scala:53`).
- (Before this change) PROVIDER_UNAVAILABLE, VECTOR_SEARCH_UNAVAILABLE, FACETS_UNAVAILABLE, MISSING_EMBEDDING, STALE_EMBEDDING and INPUT_TOO_LARGE are `exceptional = true` in `GraphQLFailureCatalog`; `.exceptional` is read only by mutation paths.
- Atlas `$search` retrieval exists in `MongoSemanticSearchRepository` (`lexicalJobs` `:422-439`, `candidateLexicalPipeline` `:209-222,:416`), private and behind the same discovery breaker as `$vectorSearch`.
- `recommendedJobs` makes no provider call (stored profile embedding), so a provider outage does not affect it.
- `recordSearch` (`HiringGraphQLResolverSupport.scala:83-101`) runs in the field's effect chain.
- `FixedWindowRateLimiter.acquireNow` returns `Either[RateLimited, Unit]`; the cost charge happens in the synchronous `costBudget` reducer (`HiringGraphQLSchemaAssembly.scala:168-176`).

## Decisions

- D1 (AIR-1): each AI root returns a nullable union: `SemanticJobSearchResult`, `RecommendedJobsResult`, `CandidateMatchesResult` = ranked results `| SearchUnavailable | ValidationError | DomainError`; `JobDiscoveryFacetsResult = JobDiscoveryFacets | SearchUnavailable | ValidationError`. New `SearchUnavailable implements UserError { code, message, retryable }`. A typed `SearchRefusal` attribute on each catalog entry decides the mapping (the `exceptional` flag keeps its mutation meaning): provider, vector-search (including not configured), facets, missing and stale embedding become `SearchUnavailable` (retryable); input and filter errors become `ValidationError` with `field`; non-exceptional domain errors become `DomainError`; UNAUTHORIZED, FORBIDDEN, SERVICE_NOT_READY, repository UNAVAILABLE and INTERNAL stay top-level, and the nullable root confines them to that field. RATE_LIMITED stays HTTP 429. Rejected: non-null unions (forbidden still blanks siblings); nullable roots without unions (no deliberate client state).
- D2 (AIR-2): session recording is best-effort. Results and `searchId` are returned; the failure is logged (search kind, request id, error class only); cancellation still propagates; no detached retry. A later click or view on that `searchId` gets the existing `SEARCH_SESSION_UNAVAILABLE`/`PENDING` domain error.
- D3 (AIR-3): lexical fallback for `semanticJobSearch` and `candidateMatches` when the provider or `$vectorSearch` fails (not for missing or stale embedding). New lexical-only repository port methods with a `LexicalSearchQuery`; eligibility variants that keep every live check (open status, filters, active candidate, profile, consent) except embedding freshness. `SearchMode` gains `LEXICAL`; hit `model` becomes nullable; ranked results gain `degraded: Boolean!` and `degradedReason: ErrorCode`. If lexical also fails, return `SearchUnavailable`. `recommendedJobs` and facets have no fallback. Fallback sessions record `model = None`; the event envelope is unchanged.
- D4 (AIR-4): keep the fixed window. `acquireNow` returns a grant (`limit`, `remaining`, `resetsInSeconds`) computed from the caller's own bucket; responses carry `extensions.cost { requested, limit, remaining, resetsInSeconds }`, also on 429 with `remaining = 0`. Keys routed to shared overflow stripes get no cost object (it would reveal other callers' consumption). Values are per process. AI root SDL descriptions state the cost formula and caps. Token bucket deferred until edge-of-window abuse is measured or a partner needs smoothing; ARR-14 tests stay the invariant suite.

## Open points (defaults applied)

| ID | Question | Default |
|---|---|---|
| Q1 | Keep `DomainError` as an AI result member? | Yes, mirrors mutations. |
| Q2 | Fallback for `recommendedJobs`? | No (same search deployment); revisit a structured `FILTER` fallback. |
| Q3 | Does the analytics reader accept `model: null` with results? | Answered 2026-10-16: the model is in neither the event envelope nor the Mongo session codec, and the Spark validator reads only `searchId`, `searchKind`, `results`; no change needed. |
| Q4 | Does `Diagnostics` have a counter for recording failures? | Answered: no counter facility; log event only. |

## Acceptance examples

| ID | Given / When / Then |
|---|---|
| AIR-01.1 | Provider breaker open, fallback disabled in the fake: `{ me {…} semanticJobSearch(…) { … on SearchUnavailable { code retryable } } }` returns `me` data and `SearchUnavailable{PROVIDER_UNAVAILABLE, true}`, no top-level error |
| AIR-01.2 | Stale candidate embedding: `recommendedJobs` returns `SearchUnavailable{STALE_EMBEDDING}` |
| AIR-01.3 | Query over the size limit: `ValidationError` with `field = "query"` |
| AIR-01.4 | Recruiter on another recruiter's job: top-level FORBIDDEN, `candidateMatches: null`, siblings intact, provider call count 0 |
| AIR-02.1 | Unauthenticated: UNAUTHORIZED stays a top-level error |
| AIR-03.1 | SDL snapshot, fixtures and persisted-query manifest regenerated; union selection complexity equals the previous value for the same `first` |
| AIR-04.1 | Recording fails: results and `searchId` returned, one log event without result ids |
| AIR-04.2 | Later click on that `searchId`: `DomainError{SEARCH_SESSION_UNAVAILABLE}` |
| AIR-05.1 | Provider fails: `semanticJobSearch` returns `degraded = true`, `degradedReason = PROVIDER_UNAVAILABLE`, hits `searchMode = LEXICAL`, only open jobs, filter honoured |
| AIR-05.2 | `candidateMatches` fallback keeps consent parity: private filters (country, city, availability) match only opted-in candidates, opted-out candidates are never matched on private data, and deleted or non-Candidate users are never returned |
| AIR-05.3 | Job closed between retrieval and re-check is dropped |
| AIR-05.4 | A vector query cut by its time budget, refused by an open breaker (`RepositoryError.Overloaded`) or failing with a whole-store outage never falls back; only `RepositoryError.VectorSearchUnavailable` (lexical branch answered) degrades |
| AIR-06.1 | Lexical also unavailable: `SearchUnavailable{VECTOR_SEARCH_UNAVAILABLE}` |
| AIR-06.2 | Fallback session recorded with `model = None`; wire contract spec unchanged |
| AIR-07.1 | (IT, Atlas Local) `$search` fallback ranks jobs while the provider fake throws |
| AIR-08.1 | Charged query carries `extensions.cost`; two queries show `remaining` decreasing by `requested` |
| AIR-08.2 | 429 body carries the cost object with `remaining = 0`; `Retry-After` unchanged |
| AIR-08.3 | Key on an overflow stripe: no cost object; ARR-14.2 tests still pass |
| AIR-09.1 | AI root descriptions state the cost formula and caps |

## Acceptance and evidence

| ID | Paths | Verification | Outcome |
|---|---|---|---|
| AIR-01..03 (S1) | `SearchUnavailable`, `SearchOutcome`/`FacetsOutcome` unions, nullable roots, `searchResult`/`facetsResult` classifier, typed `SearchRefusal` catalog attribute decides data versus top-level (the six codes keep `exceptional = true` for mutation paths), `field` paths; `HiringGraphQLSearchOutcomeSpec`; fixtures, SDL snapshot and manifest regenerated; docs/api.md "AI search outcomes" | `sbt scalafmtCheck Test/scalafmtCheck test` | 2026-10-16: 1450 passed. Union complexity takes the max alternative (115 with or without error fragments). Code Review, Security and QA PASS |
| AIR-04 (S2) | `recordSearch` best-effort (non-fatal failures caught, cancellation propagates), `RequestContext.reportSearchRecordingFailure`, `LogEvent.SearchRecordingFailed` (request id, search kind, error code or class only); `HiringGraphQLSearchRecordingSpec` (5) | `sbt test` | 2026-10-16: S2 tests pass; full run 3 failures from the stale SDL snapshot (S3 `LEXICAL`) and the `MongoDatabaseProbeSpec` timing flake. A click after a failed recording can also get `NOT_FOUND` when nothing was queued (documented). Code Review, Security and QA PASS |
| AIR-05..07 (S3) | Backend done: `SearchMode.LEXICAL`; `SemanticSearchRepository.lexicalJobs/lexicalCandidates` (`LexicalSearchQuery`, `$search` behind `admitted`/discovery breaker); `SearchEligibilityPolicy.jobLexical/candidateLexical`; `SemanticSearchService.semanticJobSearch`/`candidateMatches` returning `SearchResults` with `DegradedSearch` (the strict methods were removed in the fix round); `SemanticSearchLexicalFallbackSpec` (14), `MongoLexicalFallbackSpec` (3); AIR-07.1 IT written, not run (Atlas Local not started). API: resolvers use the single public search methods; `degraded`, `degradedReason`, nullable `model`, `LEXICAL` in SDL; snapshot and manifest regenerated; AIR-05.1/06.1 resolver tests. `sbt test` 1475 passed. AIR-07.1 IT not run. Code Review, Security and QA PASS | `sbt test`, `sbt it:test` (Atlas Local for AIR-07.1) | `sbt test` 1488 passed (2026-10-16); AIR-07.1 not run |
| AIR-08..09 (S4) | `Grant` ADT (`Tracked`/`Shared`) from `FixedWindowRateLimiter.acquireNow`; request-scoped `CostReceipt` set by the `costBudget` reducer; `extensions.cost` merged in `HiringGraphQLSchema.execute`; 429 body cost with `remaining = 0`; no cost object for shared stripes or pre-charge refusals; cost formula in five root descriptions; `GraphQLCostBudgetSpec`, `CostBudgetLimiterSpec`, `HiringGraphQLCostDescriptionSpec` | `sbt scalafmtCheck Test/scalafmtCheck test` | 2026-10-16: 1488 passed. Code Review, Security and QA PASS |
| Final | — | Component gates of `scripts/check-local.sh` | 2026-10-16 QA: format clean; `sbt test` 1501 passed; `sbt it:test` 308 passed, 26 skipped (Atlas Local incl. AIR-07.1, Kafka, opt-in measurement: unverified), 0 failed; skills check passed. `check-local.sh` itself not run |

## Implementation handoff

Order: S1 → S2 → S3; S4 after S1 (shared schema files); regenerate the SDL snapshot and manifest last. Glossary terms "degraded search" and "lexical fallback" in S3. Risks: client contract change for four roots (allowed pre-MVP); fallback relevance mistaken for normal results (hence `degraded`); fallback skipping consent (explicit tests); overflow cost leak (AIR-08.3).

## Checkpoint

- 2026-10-15: spec written from the architect's design note.
- 2026-10-16: S1 done. Notes: facets have no `DomainError` member, so a domain refusal there stays top-level; a Candidate calling `candidateMatches` gets `DomainError{RECRUITER_REQUIRED}` (Q1); facets lose `extensions.reason`. Next: S2 (resolver files) and the S3 repository/service part in parallel.
- 2026-10-16: S3 backend done (additive service API; existing use cases unchanged until the resolvers switch). AIR-05.2 wording corrected to the existing consent rule. Open: `SDL` snapshot needs `LEXICAL`; `MongoDatabaseProbeSpec` failed in the last run (investigate after S2's Diagnostics change); AIR-07.1 needs Atlas Local.
- 2026-10-16: S3 API exposure done; `MongoDatabaseProbeSpec` made deterministic (it validates the error type against the production log allow-list, which already permits the driver timeout class; `Reason` distinguishes the two timeouts). Another session is concurrently removing the `shared/saga` source root and changing saga files; those changes are not part of this spec. Next: S4.
- 2026-10-16: S4 done. Next: Code Reviewer and Security Engineer on S1-S4, then QA.
- 2026-10-16 Security Engineer on S1-S4 (static): PASS. No new disclosure (authorization precedes any data; FORBIDDEN stays top-level); fallback runs only after authorization with consent parity, closed-job re-check, BSON-parameterized bounded `$search` text and the same admission/breaker; recording logs only allow-listed fields; cost values come only from the caller's own bucket. Low items: (a) fallback after a `maxTime` expiry can double worst-case latency; skip fallback on timeout or admission rejection; (b) callers without a peer address or behind an untrusted proxy share one tracked bucket and see its aggregate `remaining`; return `Shared` for `Client(None)` or document; (c) a caller reusing an existing `searchId` silently loses their own recording; (d) pre-existing NOT_FOUND vs FORBIDDEN existence oracle on `candidateMatches` (now NOT_FOUND is data). AIR-07.1 not runtime-verified.
- 2026-10-16 Code Reviewer on S1-S4: PASS (`sbt test` 1488 passed, format green). Findings: M1 `RateLimited` default `Grant.Shared` hides a forgotten grant; M2 the old non-fallback search methods remain public (pre-MVP one shape); L3 extra candidate read in the job fallback; L4 classification by hard-coded code strings and an unreachable branch; L5 latent: `DeletedAccountAuthenticationRequired` is a non-exceptional `UNAUTHORIZED` (not produced on search roots today; same rule as mutations; left as is); L6 docs should say the fallback shares the discovery breaker; L7 test tags. Fix round (with security a and b) sent to the developer; c and d recorded as accepted/pre-existing.
- 2026-10-16: review fix round done (`sbt test` 1501 passed; `IntegrationTest/compile` ok; IT not run). `RateLimited` grant explicit; one public search shape (fallback methods renamed to `semanticJobSearch`/`candidateMatches`, strict methods removed); `RepositoryError.Overloaded` for `maxTime` expiry and open breaker, never degraded; `Client(None)` gets `Grant.Shared`; candidate scope reused; typed `SearchRefusal` classification; docs on the shared breaker; test tags fixed. Another session (hiringplatform-05/08) edited the same search files concurrently and added `RepositoryError.VectorSearchUnavailable` as the only degrading repository signal. Next: confirmation review and QA.
- 2026-10-16 Security Engineer confirmation of the fix round: PASS (39 targeted tests green). No fallback after time-budget expiry, open breaker or whole-store outage; authorization precedes fallback; `Client(None)` and overflow keys get `Shared` with no numbers; authentication failures stay top-level. Gap: breaker and `maxTime` behaviour verified by unit tests and static review only.
- 2026-10-16 QA: PASS for AIR-01..09 except AIR-07.1 (unverified: Atlas Local not started). Every example maps to a tagged test.
- 2026-10-16 review finding fixes (developer, `sbt test` 1505 passed; `IntegrationTest/compile` ok; IT not run): driver operation timeouts and `maxTimeMS` expiry map to `Overloaded` before the breaker check (AIR-05.4); `RepositoryError.VectorSearchUnavailable` is produced only by the hybrid fuse when a vector branch failed next to an answering lexical branch, and a plain `Unavailable` fails fast (native `$rankFusion` is one pipeline, so its failures never degrade); candidate lexical retrieval applies a live `$match` between `$search` and `$limit` (AIR-05; Atlas IT case added, unrun); authentication failures including the deleted-account `UNAUTHORIZED` stay top-level on every root; a rate-limited search root returns `DomainError` with `retryAfterSeconds`; embedding, provider and vector failures are exceptional again for mutations while search roots keep deciding data versus top-level from the failure's search kind first. Not done: recording-failure telemetry event (`OperationalTelemetryService` only records job views and clicks and the operational event envelope has no such event; adding one is an event-contract decision).
