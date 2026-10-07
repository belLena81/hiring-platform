# Hiring Discovery and Search Quality

## Identity and scope

- Roadmap: Phase 10 in [development milestones](../development-milestones.md).
- Status: in progress. The implementation handoff supplies the local radius and exact structured-facet contract below; Atlas lexical/vector execution and ranking adoption remain deployment/evaluation gates.
- Outcome: candidates find relevant eligible jobs through structured geographic discovery and richer lexical/hybrid search; recruiters improve matching within their existing authorization and privacy boundaries.
- Coordinator: Product Manager. Architect owns contracts, Data Engineer owns Mongo/Atlas access and schema, Scala Developer owns services and API. Independent Code Reviewer, Security Engineer and QA own final verdicts.
- Dependencies: measured Phase 8 access baseline and Phase 9 held-out evaluation in [MongoDB optimization](mongodb-vector-retrieval-optimization.md) and [Hiring Search Enhancements](hiring-search-enhancements.md#search-evaluation-and-embedding-architecture).
- Non-goals: geocoding, real resume extraction, LLM explanations/agents, automatic hiring decisions, new role/tenant model, paid provider procurement, frontend assets or replacing current search by default.
- Completing this capability does not declare full MVP or weaken Phase 6/7 activation gates.

## Verified source context

Inspected October 6, 2026. Local source additions are distinguished from executed acceptance evidence below.

| Boundary | Current source fact |
|---|---|
| Search use cases | [SemanticSearchService](../../src/main/scala/com/example/graphQL/cats/service/search/SemanticSearchService.scala) provides hybrid text search, vector recommendations and owned-open-job candidate matching with optional query/filter branches. Missing/stale query-entity embeddings are typed failures. |
| Search input/results | [SearchContracts](../../src/main/scala/com/example/graphQL/cats/service/search/SearchContracts.scala) has city/skills/date job filters and ranked lists, plus minimized candidate match DTOs. It now adds validated geographic criteria, criteria-bound cursors, nearby results and bounded exact structured facets. |
| GraphQL | [HiringGraphQLSearchResolvers](../../src/main/scala/com/example/graphQL/cats/api/graphql/HiringGraphQLSearchResolvers.scala) wires current search use cases and telemetry, plus `nearbyJobs` and `jobDiscoveryFacets`; optional GeoPoint input/output are wired through authorized job mutations. |
| Retrieval selection | [SearchFusionStrategy](../../src/main/scala/com/example/graphQL/cats/domain/search/SearchFusionStrategy.scala) and `MongoSemanticSearchRepository` support existing application/native fusion selection. Native capabilities remain gated by the actual deployment. |
| Embedding freshness | `EmbeddingPipeline`, `MongoJobRepository.updateEmbedding` and `MongoUserRepository.updateEmbedding` use current model/source hashes and observed aggregate revisions. Aggregate `version` is a concurrency token, not an embedding-model version. |
| Existing evidence seams | `SemanticSearchServiceSpec`, `MongoSearchFusionPipelineSpec`, `MongoSemanticSearchResultSpec` and the evaluation harness support future tests; no result here certifies new discovery features. |

Requirements come from [planned discovery use cases](../use-cases.md#planned-discovery-extensions-phase-10), [MongoDB design](../mongodb-design.md), [engineering quality](../engineering-quality.md), and [schema evolution](../schema-evolution.md).

## Desired behavior

### Actors and authorization

Candidates search only visible open jobs and obtain recommendations for their trusted authenticated identity. Recruiters request matches for owned open jobs; preserve the current explicitly authorized Admin behavior. Validate role/ownership before query embedding, search or facet calls. Nested/direct-ID access and cache keys obey the same policy. Search results remain advisory; ranking never applies, accepts or rejects an application.

Private candidate residence, availability and consent remain owner-only values. Matching retains the HSE semantics: opted-in candidates satisfy supplied private filters, opted-out/missing-consent candidates bypass only those private filters, and all candidates satisfy active-account, role, required-skills and model predicates. Do not use private filter values as new ranking features, facet dimensions or explanations merely because they are available in persistence.

All new retrieval paths must reuse the Phase 8 authoritative eligibility recheck and bounded short-result behavior. A deleted/closed/ineligible hit is discarded before output; failure to recheck yields sanitized unavailability. A concurrent update after the final read does not create a response-lifetime snapshot guarantee. The API must document this boundary.

### Radius discovery

The caller supplies structured center coordinates and radius; the service does not infer their location, geocode text, or persist a candidate tracking history. Validate finite latitude/longitude and positive bounded radius with typed values before database/provider work. Define units, maximum radius, coordinate order and boundary inclusion in the accepted contract before implementation; reject impossible coordinates rather than clamping or guessing units.

Radius combines with existing job filters and visibility using conjunction. Jobs without accepted structured coordinates cannot satisfy a radius filter; they remain eligible for searches that omit it. Preserve existing city-only queries. A radius query must never silently fall back to city text or ignore distance when the geo index is missing. Empty geographic matches return an empty page.

Distance is calculated by the selected Mongo/Atlas geographic capability, with tested agreement at the agreed boundary/tolerance. Exercise identical coordinates, just-inside/on/outside radius, negative longitude, poles/antimeridian and missing coordinates. Do not implement unbounded all-job distance calculations in application memory.

Radius results require deterministic cursor pagination with a unique ID tie-breaker, including equal-distance/rank cases. The selected sort, encoded cursor coordinates, query fingerprint and concurrent-index limitation must be explicit in the contract decision. Reject a cursor bound to different filters or ranking policy. Stable data must page without gaps or duplicates; concurrent edits/index refresh may reorder results unless a separately accepted snapshot mechanism is introduced. No generic snapshot store is implied.

Distance exposure, ordering precedence between relevance and distance, remote-job semantics and multiple job locations are product choices below. Implementation must not invent them from the existing free-text city field. New coordinates are supplied through authorized recruiter/Admin job mutation inputs after validation; candidate queries cannot rewrite a job's location.

### Lexical search and facets

Support an evaluated lexical alternative alongside current vector/hybrid behavior, using explicit index fields/analyzers and bounded query inputs. Fixed-path query builders accept typed values; caller text cannot supply search operators or arbitrary field paths. Keep query language/tokenization choices visible in the evaluation fixture set, including punctuation, casing, empty input and multilingual cases if the corpus actually covers them.

Facet counts must correspond to the selected query's eligible visibility/authorization set and agreed facet-filter semantics. No count may reveal closed jobs, deleted entities, another recruiter's restricted data or candidate private attributes. Counts over only the returned top-K cannot be labeled corpus totals. A stale index cannot be trusted for authorization-sensitive totals; select a provably authorized aggregation strategy or return facets unavailable rather than exposing unreconciled counts. Do not hydrate an unbounded candidate set to repair counts.

Bound facet dimensions, bucket count and request cost; sort bucket ties deterministically. Missing values are omitted or placed in an explicit bucket only after the facet contract chooses that policy. Facets and hits use the same query/filter identity. If a consistent exact count cannot be provided, an explicitly accepted approximate/unavailable contract must explain it; never silently call an estimate exact.

Missing/incompatible indexes, unsupported Atlas capabilities and timeout return typed sanitized unavailability. No silent fallback changes lexical analyzers, fusion algorithm or count meaning. Local Community Mongo checks validate core workflows but cannot certify actual Atlas lexical/vector/facet execution.

### Recommendations, matching and personalization

Establish a fixed Phase 9 relevance baseline before evaluating a new personalized ranking. Start from the simplest candidate: existing application RRF/vector behavior and deterministic skill evidence. Proposed features must be authorized, necessary and available at ranking time. Do not add sensitive profile inference, protected attributes, private residence/availability signals, or behavioral tracking without a separately specified purpose, consent/retention policy and review.

Separate candidate generation from pure ranking. The ranking function accepts a bounded authorized hit list and typed allowed features, returns stable scores/order and deterministic evidence, and performs no I/O. Retain original retrieval score where the existing contract does so. Missing permitted features produce an explicit baseline behavior, not invented values. Tie order must be stable and described in evaluation records.

Evaluate overall and use-case/filter subgroups on held-out fabricated human judgments. Include cold profiles, missing skills, sparse matches and counterfactual fixtures where only a disallowed/private attribute changes; such changes cannot affect ranking. Do not turn the score into a probability of hiring success or expose model-generated personal rationale.

Adopt only if pre-agreed relevance, latency, privacy and cost gates pass. Otherwise record an explicit defer decision and retain existing default ranking. A defer outcome can close the experimental-personalization decision criterion; it does not close unimplemented radius, lexical or facet criteria. The current strategy selector remains the configuration seam; add a concrete algorithm only after its evaluation justifies it.

### Freshness, provider failures and recovery

Use current model and source-hash checks for recommendation/match query embeddings and authoritative result validation. Preserve version-guarded writes and the Phase 9 shared preparation contract. No new embedding version field is implied; if model/schema identity changes, specify it independently from aggregate revision and update the one active contract.

Provider timeouts, malformed vectors, cancellation and stale-write conflicts keep typed outcomes and existing ownership. A user search failure does not block operational hiring transactions. Durable embedding repair resumes after restart, and a revoked/deleted entity cannot be resurrected by delayed provider results. Keep one bounded retry owner per operation; no retry of validation or authorization failures and no hidden provider fallback.

## APIs, persistence and code boundaries

The approved local additions are optional `GeoPointInput` on job location, `nearbyJobs(center, radiusKm, filter, first, after)` returning job, kilometre distance and cursor plus `hasNextPage`, and `jobDiscoveryFacets(filter, center?, radiusKm?)`. Facet center and radius must appear together. Existing semantic search fields retain their contracts. Invalid coordinates/radius/filter/page size and mismatched cursors return sanitized typed errors. Discovery is authorized for active Candidates and singleton Admin; Recruiters are denied.

Before MVP, update the single active GraphQL/cursor/event shape directly with its consumers/fixtures; do not introduce legacy readers, versioned endpoints or dual writes. Geographic operational Mongo storage and indexes require a new restartable migration/verification plan, bounded scanning, concurrent-write behavior and recovery checks. Never infer coordinates from city strings or backfill private information. Missing old coordinates must have an explicit allowed representation. Old binaries that replace job records may be incompatible; define stop/cutover conditions before new writes. Existing migrations remain immutable.

- Domain: immutable geographic criteria and ranking inputs, smart constructors, pure validation/decisions, exhaustive ADTs and typed IDs. Pass time/identity explicitly; no Mongo/GraphQL/Circe/provider dependencies.
- Application: retain `UseCaseIO`/`RepositoryIO` over `IO`; services sequence authorization, query execution, authoritative checks and result assembly. Preserve typed errors through private helpers.
- Adapters: Mongo/Atlas owns geospatial/query stages, analyzers, projections, counts and cursor execution; GraphQL owns coercion and public error mapping; provider adapters own network/vector validation.
- Validation: `ValidatedNec` accumulates independent input/configuration errors, then `Either` sequences decisions. Use `Option` for absence; no nulls, business exceptions or partial `.get`.
- Runtime: existing `Resource` owners release clients/cursors/workers under cancellation; bound branch budgets, facet buckets, batches and concurrency using existing FS2/Cats Effect tools.
- Maintainability: small typed filter builders suffice; Composite needs accepted nested Boolean criteria. A new Strategy needs a concrete evaluated algorithm. Avoid generic discovery frameworks, caches and new infrastructure without measured need.

## Alternatives and decision records required

### Local delivery decisions (2026-10-06)

- Job location has one optional validated latitude/longitude point, stored as GeoJSON `[longitude, latitude]`; existing records without coordinates remain valid. Remote jobs and records without coordinates do not match radius queries.
- Startup migration `006_job_geo_points` verifies existing point shapes in bounded, restartable batches, preserves missing points, applies the optional-point validator, and creates the named `2dsphere` index before runtime readiness. Stop incompatible old job writers before starting this migration; the migration does not rewrite or infer coordinates.
- `nearbyJobs` uses a maximum radius of 500 km, MongoDB `$geoNear` first-stage execution and `2dsphere`; distance is kilometers and ordering is full-precision distance then job ID. Cursors bind the center, radius, normalized filters and ordering. Facets are exact conjunctive counts over the full structured-filter set before pagination, for skills/country/city/remote, at most 20 buckets per dimension, count descending then value.
- These choices authorize local implementation only. Atlas lexical/vector/hybrid comparison, production migration cutover, and live performance acceptance remain pending. Human relevance judgments are pending review; personalization is deferred under the conservative policy.
- Historical checkpoint before the current cursor/aggregation/integration extensions: optional location coordinates, radius discovery, radius-aware exact structured facets, and criteria-bound list cursors are implemented in the domain, job service, Mongo adapter, and GraphQL schema/resolvers. `Test / compile` passed on that earlier source; 26 focused tests passed across domain validation, discovery contracts, Mongo codecs, and geo-point validation. `git diff --check` passed. The Mongo aggregation has not been exercised against MongoDB; index readiness, authorization/race integration, explain plans, the 128-job workload, and Atlas comparisons remain unverified. Current eligibility uses `status=Open` plus the requested structured filters, matching existing `findOpen`; recruiter account state is not independently checked. Stop incompatible old job writers before migration startup.

The user approved the geographic/facet choices and conservative personalization defer policy on October 6. Atlas analyzer changes and paid provisioning remain undecided and unimplemented.

| Choice | Options and decision evidence | Owner / blocked dependent work |
|---|---|---|
| Geographic job representation | Approved: one optional finite latitude/longitude point; absent historical points preserved; remote jobs excluded from radius hits. | Product Manager + Data Engineer; local implementation, executions and independent reviews passed; deployed acceptance remains pending. |
| Radius contract and execution | Approved: Mongo `$geoNear`/`2dsphere`, kilometre response/metre maximum distance, radius `(0,500]`, distance then ID ordering, criteria-bound cursors and one-metre numerical tolerance. | Architect + Product Manager; local implementation, executions and independent reviews passed; deployed acceptance remains pending. |
| Facet dimensions and meaning | Approved: exact authoritative structured-filter counts; skills/country/city/remote; conjunctive filtering; 20 buckets/dimension; no shared hit/facet snapshot promise. | Product Manager + Security/Data Engineer; local implementation, executions and independent reviews passed; deployed acceptance remains pending. |
| Lexical behavior | Existing analyzer baseline versus a specific changed analyzer/boost policy justified by held-out queries; verify deployment capability and relevance regression. | Data Engineer + Product Manager; blocks analyzer/index replacement and ranking default changes. |
| Personalization | Approved: defer personalization under the conservative policy; human relevance labels remain pending review. | Product Manager + Architect + Security Engineer; blocks algorithm activation, not offline fixture preparation. |
| Atlas/provider environment | Existing authorized disposable environment versus blocked live evidence; record capabilities, resource envelope, price assumptions and cleanup owner. | Deployment owner + Product Manager; blocks live Atlas/provider acceptance and any spend. |

## Workload and verification

Reuse Phase 8 environment/run coordinates and Phase 9 held-out corpus; record intentional geographic/facet corpus additions separately. Existing ceilings (10,000 entities, 100 queries, concurrency 1/8) do not authorize spend or production-scale claims. Set actual local corpus size, warmup, duration, geographic/filter skew and numerical acceptance limits before tuning.

Compare query plans/counts, keys/documents examined, latency percentiles, throughput/errors, index build/read/write/storage cost, provider requests/tokens and observed billing. Record unavailable metrics explicitly and label calculated estimates. Compare the simplest current behavior with every selected extension; include empty/selective filters and cold profiles. Adoption cannot be based only on average relevance or a compiler pass.

| ID | Given / When / Then | Verification target/method | Actual outcome |
|---|---|---|---|
| HDQ-01 | Given accepted valid geographic criteria, when combined with existing filters, then only eligible jobs within the agreed distance boundary appear. | Pure criteria tests, service tests, real selected Mongo/Atlas geographic execution. | Local Mongo PASS: 4 discovery integrations plus real-service workload; Atlas geographic/index deployment acceptance remains pending. |
| HDQ-02 | Given invalid/nonfinite/out-of-range coordinates, excessive radius or wrong-bound cursor, when requested, then typed validation rejects before external work. | GraphQL operations and service direct-call tests. | Local PASS: recording service-port tests and executed GraphQL queries reject invalid coordinates/radius/filter/cursor before discovery storage, preserve public errors and enforce Candidate/Admin access. Functional follow-up passed 24 discovery tests; see HWR-07 in the recovery specification. |
| HDQ-03 | Given static tied radius results, when paged, then cursor ordering yields no gaps/duplicates; concurrent refresh follows the documented limitation. | Real repository pagination and synchronized update scenarios. | Local PASS: 24 nonzero tied distances paged without gaps/duplicates; closure and deletion overlap queries, with subsequent reads proving committed exclusion. |
| HDQ-04 | Given lexical/vector/hybrid cases, when executed in Atlas, then the accepted analyzer/filter semantics and bounded branch limits hold. | Atlas integration plus held-out comparison records. | Atlas execution pending; fixed-field lexical adapter and bounded authorized capture tooling covered by the 9-test fusion pipeline suite. |
| HDQ-05 | Given facets and stale/restricted records, when counted, then counts follow accepted semantics without leaking ineligible/private data or silently approximating totals. | Authorized-count oracle, Atlas lag/deletion scenarios, forbidden-access tests. | Local structured PASS: complete filtered counts, skill deduplication, 20-bucket bounds/truncation, radius exclusions, city/skill/date conjunction; real-service Recruiter and Deleted candidate denials. Lexical-query facets and Atlas lag scenarios pending. |
| HDQ-06 | Given consent/private-field counterfactuals and non-owner requests, when matching/ranking runs, then existing consent rules, minimized DTOs and authorization remain unchanged. | Service, GraphQL and real branch tests. | Local service/GraphQL/authoritative Mongo checks PASS; live Atlas branch checks unverified. See [HRP-07–09](hiring-retrieval-publication-reliability.md). |
| HDQ-07 | Given permitted features and tied scores, when a proposed ranker runs, then pure deterministic ordering and cold/missing-feature baseline behavior are reproducible. | Pure ranking/property examples and evaluation fixtures. | Deferred by approved conservative policy; no new ranker adopted. |
| HDQ-08 | Given held-out baseline and pre-agreed thresholds, when personalization is evaluated, then an evidence-backed adopt/defer decision is recorded without weakening other criteria. | Relevance/latency/privacy/resource reports and independent reviews. | Decision: defer personalization. Human relevance labels and Atlas comparison remain pending review/execution; no adoption claim. |
| HDQ-09 | Given timeout, cancellation, stale embedding or restart, when provider/index work fails or resumes, then failures sanitize, resources close and durable repair cannot restore stale/deleted data. | Worker/service resource tests and Mongo restart/race integration. | Local worker/resource/Mongo recovery checks PASS. Real-provider acceptance remains external. See [HRP-01/02/04/14](hiring-retrieval-publication-reliability.md). |
| HDQ-10 | Given accepted location/index schema changes, when migration is interrupted or writers race, then restart/verification converges without invented coordinates or constraint loss. | Disposable Mongo migration tests; actual Atlas index readiness checks. | Local PASS: incompatible legacy point fails cutover with Running ledger; invalid writer rejected by installed validator; exact point repair followed by repeated setup reaches Complete and verifies the geo index. Production writer cutover and Atlas deployment remain pending. |
| HDQ-11 | Given an adopted query/index change, when paired workloads execute, then agreed read/relevance gains and write/storage/cost limits are met. | Source-qualified before/after plans and measurements. | Local measured workload recorded below, including actual GEO_NEAR_2DSPHERE, sort/limit, commands, latency and flushed index/storage growth. No before/after latency improvement or SLO is claimed; Atlas relevance/cost acceptance remains pending. |

## Implementation handoff and checkpoint

1. Product Manager/Architect close geographic and facet contract decisions; Data Engineer confirms query/index feasibility. Record schema/API examples and migration stop/restart conditions before dependent code.
2. Implement radius as a bounded vertical slice through domain, service, repository and GraphQL (HDQ-01/02/03/10); Security Engineer reviews authorization and coordinate retention.
3. Implement accepted lexical/facet behavior with authoritative count semantics (HDQ-04/05/06); do not ship facets whose privacy correctness is unresolved.
4. Evaluate recommendation/matching refinements against fixed fixtures (HDQ-07/08). Retain baseline on defer; introduce a Strategy only for an accepted concrete algorithm.
5. Exercise outages, cancellation, migration and restart; capture paired workload evidence (HDQ-09/11). Run root `sbt test`, relevant `IntegrationTest / test`, configured formatting and schema/operation checks. Obtain independent Code Reviewer/Security Engineer and final QA verdicts.

Assign exclusive paths by layer per slice; serialize shared domain/SDL contracts and repository edits. [Durable Hiring Workflows](durable-hiring-workflows.md) may later extend repair orchestration; it must not defer correctness required here. [Observability](hiring-observability-resilience.md) owns broader dashboards, while this feature must already provide safe diagnostics and testable errors.

- Documentation checkpoint: source baseline, desired behavior, boundaries, choices and acceptance mapping prepared. No HDQ criterion is complete; no new feature or benchmark execution occurred in this documentation task.
- Final local discovery integrations/workload and the Java 17 full unit/integration commands passed. Independent Code Reviewer, Security Engineer and final independent QA: PASS for the authorized local scope. Atlas execution and human relevance judgments remain pending. Documentation-delivery evidence is owned by the [roadmap checkpoint](../development-milestones.md#specification-ownership-and-readiness).

### Current discovery implementation checkpoint

- Mongo `$geoNear` is first, with explicit `location.point` key, authoritative `Open` predicate, remote exclusion and maximum radius in metres. A `distanceMultiplier` of 0.001 returns the exact kilometre value used for sorting and cursor comparison, avoiding a kilometres-to-metres cursor round-trip. Cursor fingerprints use canonical structured JSON to avoid delimiter collisions and bind normalized city/skill filters, date, center, radius and ordering; malformed job identities fail closed.
- Structured facets apply conjunctive filters before `$facet`. Skill arrays use `$setUnion` before unwind so duplicate stored array values count only once per job. The real Mongo fixture now writes a duplicate `Scala` array entry directly and verifies corpus and radius totals remain 26 and 24; this regression passed in the later 4-test discovery run. Each dimension reads at most 21 buckets, returns 20 and reports truncation. Hits and separate facets have no shared snapshot promise.
- Added `MongoJobDiscoveryIntegrationSpec` for tied-distance pagination, missing/remote/closed/deleted exclusions, complete-corpus facets, conjunction, bucket bounds, restartable setup and Mongo distance boundary tolerance. Added `HiringGeographicDiscoveryEvaluationIntegrationSpec` for 128 jobs, 32 candidates, 20 fixed queries and concurrency 1/8; artifacts go to ignored `.local/data/discovery-evaluation/`. Measurements cover service authorization through Mongo, requests/errors/throughput/latency, cgroup resource samples and before/after index/storage size; HTTP/GraphQL overhead and SLO acceptance are excluded from this measurement boundary.
- Existing `HiringSearchEvaluationRetrieval` supports actual Atlas lexical captures alongside vector and fusion branches. `MongoSearchFusionPipelineSpec` now explicitly verifies fixed public job fields (`title`, `description`, `requirements`, `skills`), eligibility before limits and exclusion of private candidate attributes. Atlas capture execution remains pending.
- Verification on October 6, 2026: Java 17 main/Test compilation and deterministic SDL capture passed; the focused unit command passed 51 tests, including 6 discovery contracts, 9 fusion pipeline contracts and 1 executable discovery GraphQL operation. The discovery integration command passed all 5 tests (4 Mongo behavior/migration tests plus the workload); a follow-up passed all four behavior cases with a raw duplicate-skill fixture. Final formatting and all 526 units passed. The full integration command succeeded with 85 executed cases, four skipped Atlas cases and two disabled older operational Compose checks. Logs: ignored `.local/logs/discovery-local-verification.log`, `.local/logs/discovery-local-final.log`, `.local/logs/hiring-capabilities-final-unit.log` and `.local/logs/hiring-capabilities-final-integration.log`. Independent Code Reviewer, Security Engineer and final independent QA: PASS for the authorized local scope.

### Local geographic workload evidence

Artifact: ignored `.local/data/discovery-evaluation/geographic-1791295827579.json`. MongoDB 8.0.32 replica set, Java 17.0.20.1; 128 synthetic jobs, 32 active Candidates and one Recruiter, 20 fixed queries, 32 samples/query, no warmup. Each level executes 640 service requests; the boundary includes authoritative account lookup and Mongo aggregation and excludes HTTP/GraphQL. Recruiter discovery/facets and Deleted candidate discovery are denied before measured requests; the synthetic candidate is restored before measurement.

| Concurrency | Requests / errors | p50 / p95 / p99 milliseconds | Requests/second | Mongo commands | Mongo CPU delta |
|---|---|---|---|---|---|
| 1 | 640 / 0 | 10.429 / 21.879 / 34.002 | 84.820 | 1,280 | 2.750 seconds |
| 8 | 640 / 0 | 16.295 / 32.149 / 45.799 | 401.180 | 1,280 | 3.382 seconds |

- Cgroup memory samples: concurrency 1, 350.310 to 346.837 MB; concurrency 8, 347.165 to 350.700 MB. These are boundary snapshots for the disposable Mongo container, including probe processes; they are not peak memory or whole-application resource measurements.
- Flush with `fsync` before each physical storage sample: job storage 4 to 28 KiB; total job indexes 32 to 184 KiB; geographic index 4 to 20 KiB. These figures measure seeded-data growth, not comparative write overhead or a latency improvement from the new index.
- Representative production-shaped first-query explain: `GEO_NEAR_2DSPHERE` on `jobs_location_point_2dsphere`; 116 keys and 173 documents examined, 31 radius/filter-eligible rows before the full-precision distance/ID sort and 21 after limit. Sort reports no disk use or spill. Mongo execution time is 3 ms in this one explain observation.
- The earlier immediate, unflushed capture (`geographic-1791295319477.json`) reported unchanged physical sizes and is superseded for storage evidence. Captures are local observations under concurrent development load, with no measured SLO, production capacity, Atlas acceptance or human relevance claim.

- Follow-up discovery verification: all 4 `MongoJobDiscoveryIntegrationSpec` cases passed again in `.local/logs/hiring-discovery-recovery-ack.log`, including the raw duplicate-skill fixture. That same command passed 6 workflow repository cases but failed a scheduling recovery fixture and both standalone Kafka drills; those outcomes are separate workflow gates and are not represented as a pass for the combined command. The discovery production source is unchanged from the measured capture.

## Functional validation follow-up

[Workflow recovery and discovery validation](hiring-workflow-recovery.md) tracks shared pure normalization for direct service and GraphQL calls, typed cursor validation preserving the wire contract, strict aggregation decoding and executable resolver evidence. Existing live relevance, provider and deployed workload gates remain unchanged.
