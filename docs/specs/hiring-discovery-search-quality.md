# Hiring Discovery and Search Quality

## Identity and scope

- Roadmap: Phase 10 in [development milestones](../development-milestones.md).
- Status: draft. Behavior and safety requirements below are specified; radius storage/API details, facets and ranking adoption remain blocked by the explicit decisions below.
- Outcome: candidates find relevant eligible jobs through structured geographic discovery and richer lexical/hybrid search; recruiters improve matching within their existing authorization and privacy boundaries.
- Coordinator: Product Manager. Architect owns contracts, Data Engineer owns Mongo/Atlas access and schema, Scala Developer owns services and API. Independent Code Reviewer, Security Engineer and QA own final verdicts.
- Dependencies: measured Phase 8 access baseline and Phase 9 held-out evaluation in [MongoDB optimization](mongodb-vector-retrieval-optimization.md) and [Hiring Search Enhancements](hiring-search-enhancements.md#search-evaluation-and-embedding-architecture).
- Non-goals: geocoding, real resume extraction, LLM explanations/agents, automatic hiring decisions, new role/tenant model, paid provider procurement, frontend assets or replacing current search by default.
- Completing this capability does not declare full MVP or weaken Phase 6/7 activation gates.

## Verified source context

Inspected October 5, 2026; intended extensions are not represented as current capabilities.

| Boundary | Current source fact |
|---|---|
| Search use cases | [SemanticSearchService](../../src/main/scala/com/example/graphQL/cats/service/search/SemanticSearchService.scala) provides hybrid text search, vector recommendations and owned-open-job candidate matching with optional query/filter branches. Missing/stale query-entity embeddings are typed failures. |
| Search input/results | [SearchContracts](../../src/main/scala/com/example/graphQL/cats/service/search/SearchContracts.scala) has city/skills/date job filters and ranked lists, plus minimized candidate match DTOs. It has no geographic radius criterion or facet result contract. |
| GraphQL | [HiringGraphQLSearchResolvers](../../src/main/scala/com/example/graphQL/cats/api/graphql/HiringGraphQLSearchResolvers.scala) wires current search use cases and telemetry. New fields require schema and executable operation updates, not resolver-only additions. |
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

Public additions are **proposed, not accepted wire shapes**: structured geographic job input/search criterion, a deterministic radius connection, and a facet result contract. Existing search fields and result names remain until a scoped contract decision specifies the exact addition. The decision record must include units/nullability/error semantics, cursor rules, operation fixtures, SDL and clients/examples affected. Do not implement placeholders for unresolved contracts.

Before MVP, update the single active GraphQL/cursor/event shape directly with its consumers/fixtures; do not introduce legacy readers, versioned endpoints or dual writes. Geographic operational Mongo storage and indexes require a new restartable migration/verification plan, bounded scanning, concurrent-write behavior and recovery checks. Never infer coordinates from city strings or backfill private information. Missing old coordinates must have an explicit allowed representation. Old binaries that replace job records may be incompatible; define stop/cutover conditions before new writes. Existing migrations remain immutable.

- Domain: immutable geographic criteria and ranking inputs, smart constructors, pure validation/decisions, exhaustive ADTs and typed IDs. Pass time/identity explicitly; no Mongo/GraphQL/Circe/provider dependencies.
- Application: retain `UseCaseIO`/`RepositoryIO` over `IO`; services sequence authorization, query execution, authoritative checks and result assembly. Preserve typed errors through private helpers.
- Adapters: Mongo/Atlas owns geospatial/query stages, analyzers, projections, counts and cursor execution; GraphQL owns coercion and public error mapping; provider adapters own network/vector validation.
- Validation: `ValidatedNec` accumulates independent input/configuration errors, then `Either` sequences decisions. Use `Option` for absence; no nulls, business exceptions or partial `.get`.
- Runtime: existing `Resource` owners release clients/cursors/workers under cancellation; bound branch budgets, facet buckets, batches and concurrency using existing FS2/Cats Effect tools.
- Maintainability: small typed filter builders suffice; Composite needs accepted nested Boolean criteria. A new Strategy needs a concrete evaluated algorithm. Avoid generic discovery frameworks, caches and new infrastructure without measured need.

## Alternatives and decision records required

Every choice remains draft until the named owner records options, selection, evidence and affected criterion IDs here. Independent exploration of source/fixtures can proceed now.

| Choice | Options and decision evidence | Owner / blocked dependent work |
|---|---|---|
| Geographic job representation | One optional structured point as simplest proposal; multiple sites/remote semantics only if use cases require them. Compare existing job inputs, data quality, mutation rights and index cost. | Product Manager + Data Engineer; blocks stored schema, migration and radius endpoint. |
| Radius contract and execution | Mongo geospatial query versus supported Atlas geographic prefilter; select ordering, units/cap, inclusivity/tolerance and cursor binding from representative relevance/distance cases. | Architect + Product Manager; blocks API/cursor implementation and geo index adoption. |
| Facet dimensions and meaning | Small approved non-sensitive job dimensions with exact authorized counts; alternatively explicitly approximate/unavailable counts with product justification. Choose conjunctive versus disjunctive filtering and prove stale-index safety. | Product Manager + Security/Data Engineer; blocks facet API and aggregation implementation. |
| Lexical behavior | Existing analyzer baseline versus a specific changed analyzer/boost policy justified by held-out queries; verify deployment capability and relevance regression. | Data Engineer + Product Manager; blocks analyzer/index replacement and ranking default changes. |
| Personalization | Retain baseline/defer versus a bounded evaluated feature-based ranking; establish allowed feature list, relevance/latency thresholds and cost ceiling before adoption. | Product Manager + Architect + Security Engineer; blocks algorithm activation, not offline fixture preparation. |
| Atlas/provider environment | Existing authorized disposable environment versus blocked live evidence; record capabilities, resource envelope, price assumptions and cleanup owner. | Deployment owner + Product Manager; blocks live Atlas/provider acceptance and any spend. |

## Workload and verification

Reuse Phase 8 environment/run coordinates and Phase 9 held-out corpus; record intentional geographic/facet corpus additions separately. Existing ceilings (10,000 entities, 100 queries, concurrency 1/8) do not authorize spend or production-scale claims. Set actual local corpus size, warmup, duration, geographic/filter skew and numerical acceptance limits before tuning.

Compare query plans/counts, keys/documents examined, latency percentiles, throughput/errors, index build/read/write/storage cost, provider requests/tokens and observed billing. Record unavailable metrics explicitly and label calculated estimates. Compare the simplest current behavior with every selected extension; include empty/selective filters and cold profiles. Adoption cannot be based only on average relevance or a compiler pass.

| ID | Given / When / Then | Verification target/method | Actual outcome |
|---|---|---|---|
| HDQ-01 | Given accepted valid geographic criteria, when combined with existing filters, then only eligible jobs within the agreed distance boundary appear. | Pure criteria tests, service tests, real selected Mongo/Atlas geographic execution. | Not run |
| HDQ-02 | Given invalid/nonfinite/out-of-range coordinates, excessive radius or wrong-bound cursor, when requested, then typed validation rejects before external work. | GraphQL operations and service direct-call tests. | Not run |
| HDQ-03 | Given static tied radius results, when paged, then cursor ordering yields no gaps/duplicates; concurrent refresh follows the documented limitation. | Real repository pagination and synchronized update scenarios. | Not run |
| HDQ-04 | Given lexical/vector/hybrid cases, when executed in Atlas, then the accepted analyzer/filter semantics and bounded branch limits hold. | Atlas integration plus held-out comparison records. | Not run |
| HDQ-05 | Given facets and stale/restricted records, when counted, then counts follow accepted semantics without leaking ineligible/private data or silently approximating totals. | Authorized-count oracle, Atlas lag/deletion scenarios, forbidden-access tests. | Not run |
| HDQ-06 | Given consent/private-field counterfactuals and non-owner requests, when matching/ranking runs, then existing consent rules, minimized DTOs and authorization remain unchanged. | Service, GraphQL and real branch tests. | Not run |
| HDQ-07 | Given permitted features and tied scores, when a proposed ranker runs, then pure deterministic ordering and cold/missing-feature baseline behavior are reproducible. | Pure ranking/property examples and evaluation fixtures. | Not run |
| HDQ-08 | Given held-out baseline and pre-agreed thresholds, when personalization is evaluated, then an evidence-backed adopt/defer decision is recorded without weakening other criteria. | Relevance/latency/privacy/resource reports and independent reviews. | Not run |
| HDQ-09 | Given timeout, cancellation, stale embedding or restart, when provider/index work fails or resumes, then failures sanitize, resources close and durable repair cannot restore stale/deleted data. | Worker/service resource tests and Mongo restart/race integration. | Not run |
| HDQ-10 | Given accepted location/index schema changes, when migration is interrupted or writers race, then restart/verification converges without invented coordinates or constraint loss. | Disposable Mongo migration tests; actual Atlas index readiness checks. | Not run |
| HDQ-11 | Given an adopted query/index change, when paired workloads execute, then agreed read/relevance gains and write/storage/cost limits are met. | Source-qualified before/after plans and measurements. | Not run |

## Implementation handoff and checkpoint

1. Product Manager/Architect close geographic and facet contract decisions; Data Engineer confirms query/index feasibility. Record schema/API examples and migration stop/restart conditions before dependent code.
2. Implement radius as a bounded vertical slice through domain, service, repository and GraphQL (HDQ-01/02/03/10); Security Engineer reviews authorization and coordinate retention.
3. Implement accepted lexical/facet behavior with authoritative count semantics (HDQ-04/05/06); do not ship facets whose privacy correctness is unresolved.
4. Evaluate recommendation/matching refinements against fixed fixtures (HDQ-07/08). Retain baseline on defer; introduce a Strategy only for an accepted concrete algorithm.
5. Exercise outages, cancellation, migration and restart; capture paired workload evidence (HDQ-09/11). Run root `sbt test`, relevant `IntegrationTest / test`, configured formatting and schema/operation checks. Obtain independent Code Reviewer/Security Engineer and final QA verdicts.

Assign exclusive paths by layer per slice; serialize shared domain/SDL contracts and repository edits. [Durable Hiring Workflows](durable-hiring-workflows.md) may later extend repair orchestration; it must not defer correctness required here. [Observability](hiring-observability-resilience.md) owns broader dashboards, while this feature must already provide safe diagnostics and testable errors.

- Documentation checkpoint: source baseline, desired behavior, boundaries, choices and acceptance mapping prepared. No HDQ criterion is complete; no new feature or benchmark execution occurred in this documentation task.
- Next action: specialist contract review and geographic/facet decisions; fixture preparation can proceed independently.
- Future implementation Code Reviewer: pending; Security Engineer: pending; final QA: pending. Documentation-delivery evidence is owned by the [roadmap checkpoint](../development-milestones.md#specification-ownership-and-readiness).
