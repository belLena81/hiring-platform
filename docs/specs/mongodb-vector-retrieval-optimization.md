# MongoDB Access and Vector Retrieval Optimization

## Identity and scope

- Roadmap: Phase 8 in [development milestones](../development-milestones.md).
- Status: draft; local query inventory can proceed, measured tuning awaits the workload and Atlas decisions below.
- Coordinator: Product Manager. Design: Software Architect and Data Engineer; Scala Developer owns services/adapters; independent Code Reviewer, Security Engineer and QA own verdicts.
- Outcome: candidates discover eligible open jobs and recruiters retrieve permitted matches with bounded database and application work; evidence explains the quality, latency and cost of every adopted optimization.
- Scope: operational access inventory, typed query shaping, projections, pagination, batching, index verification and bounded vector retrieval comparison.
- Non-goals: new discovery APIs, altered consent, new embedding model, production migration execution, paid provisioning, caching without evidence, or analytics tuning.
- Dependencies: existing hiring/search contracts; [Hiring Search Enhancements](hiring-search-enhancements.md) owns HSE behavior and Phase 9 evaluation. Existing Phase 6/7 acceptance remains independent.

## Verified source context

Inspected on October 5, 2026. These are source observations, not runtime or performance results.

| Boundary | Current implementation and implication |
|---|---|
| Search orchestration | [SemanticSearchService](../../src/main/scala/com/example/graphQL/cats/service/search/SemanticSearchService.scala) resolves trusted actors, validates owned open jobs for recruiter matches, and checks the query entity's model/source hash. Admin follows its existing authorized branch. |
| Retrieval | [MongoSemanticSearchRepository](../../src/main/scala/com/example/graphQL/cats/repository/mongo/MongoSemanticSearchRepository.scala) builds bounded vector/lexical branches, app RRF and selectable native fusion. `collectWithin` rejects over-limit materialization. Inspect each branch independently when changing limits. |
| Query types | [SearchContracts](../../src/main/scala/com/example/graphQL/cats/service/search/SearchContracts.scala) contains `JobSearchFilter`, `CandidateMatchFilters`, `VectorSearchQuery` and minimized `CandidateSearchHit`. Ranked search returns bounded lists today, not cursor connections. |
| Index setup | [MongoHiringIndexSetup](../../src/main/scala/com/example/graphQL/cats/repository/mongo/MongoHiringIndexSetup.scala) declares ordinary indexes; [MongoAtlasSearchSetup](../../src/main/scala/com/example/graphQL/cats/repository/mongo/MongoAtlasSearchSetup.scala) owns search definition/readiness validation. |
| Correctness baseline | `MongoJobRepository`, `MongoApplicationRepository`, `MongoEmbeddingWorkRepository` and `MongoOperationalEventRepositories` own persistence and work claims. Unique constraints and transactional status/ownership checks must survive tuning. |
| Existing evidence seams | `MongoSearchFusionPipelineSpec`, `MongoSemanticSearchResultSpec`, `SemanticSearchServiceSpec`, `MongoHiringRepositoryTransactionIntegrationSpec`, and `SearchEvaluationAtlasRunner` provide test locations. Their existence does not prove this spec's acceptance. |

Canonical requirements: [MongoDB search design](../mongodb-design.md#planned-vector-search-optimization), [use cases](../use-cases.md), [engineering quality](../engineering-quality.md), and [schema evolution](../schema-evolution.md).

## Desired behavior and contracts

### Operational access inventory

Create one reproducible inventory attached to this specification during implementation. Each row identifies the owning use case, production repository method, actor predicate, optional filters, projection, deterministic ordering, page/batch bound, current index, execution count and explain evidence. Include:

| Capability | Required behavior |
|---|---|
| Candidate job discovery | Only visible open jobs; compose city/skills/date filters before paging and preserve the existing public ordering. |
| Recruiter jobs and applications | Restrict ordinary recruiters to their own jobs and applications; ownership is part of database selection, including nested loads. |
| Candidate applications | Select by trusted candidate identity; direct IDs and batch loads cannot bypass the restriction. |
| Application history | Ordered append-only history remains bounded by the existing pagination contract; optimization cannot weaken atomic state/history writes. |
| Embedding work/outbox | Due-time, claim identity, lease and revision predicates remain atomic; inspect oldest/due work access and publication queues without changing delivery guarantees. |
| Admin lists | Preserve explicit Admin authorization and bounded ordering; no shared request cache may mix Admin and ordinary-user results. |

Measure optional-filter and empty-result variants, not only the most selective happy path. Preserve keyset tie-breakers and avoid deep offset pagination. Remove repeated hydration only when inventory evidence identifies it; request loaders batch within the actor's authorization context.

### Retrieval eligibility and stale indexes

1. Validate actor, inputs, requested count and query-embedding freshness before provider/search work. Preserve current typed missing/stale embedding and unavailable-provider outcomes.
2. Apply supported visibility, active-account, model, skill and consent predicates inside each retrieval branch before its candidate limit. Do not relax predicates or exceed the configured retrieval cap to fill a page.
3. Candidate private filters retain the exact existing rule: opted-in profiles must match supplied residence/availability predicates; false or absent opt-in bypasses only those private predicates. Required skills, active status, role and model apply to everyone. Missing private values fail supplied private filters only for opted-in candidates.
4. Before returning selected hits, recheck authoritative Mongo eligibility in a bounded batch at the repository/service boundary. Re-evaluate current visibility/status, model/source freshness and candidate consent/filter applicability; recheck the request actor and owned open job as relevant to the operation. Do not expose deleted or invalidated results solely because the search index still contains them.
5. Rechecking establishes eligibility at the authoritative read; it does not promise a snapshot lasting after the response. Tests synchronize changes before that read. A failure to obtain that evidence returns sanitized search unavailability rather than stale hits.
6. Retrieve validation candidates up to the configured `branchResultLimit`, recheck them in ranked order, and return at most `first` eligible results. This is a single bounded over-fetch, not a refill loop; if fewer eligible results survive the cap, return the short or empty list.
7. Preserve deterministic ranking within the returned eligible set. Dropped hits do not trigger hidden fallback to another algorithm or loosened filters. Search-index changes between independent requests can change ranking; existing ranked lists do not provide snapshot pagination.
8. Project only authorized fields. Candidate match results exclude residence, availability, consent, email, resume references and vectors. Private data used to recheck eligibility stays inside the owning boundary and is never added to result DTOs or telemetry.

### Candidate budgets, errors and cancellation

Distinguish final `first`, each branch's returned-hit limit and ANN exploration `numCandidates` in validated configuration and benchmark records. Existing configuration remains active until a measured change is selected. Invalid budgets fail startup/input validation before database work; every branch, fusion input, validation batch and final output has a finite bound.

Retain application RRF as default and its deterministic ties. Evaluate native fusion/reranking only when explicitly selected and supported by the actual deployment. Definition mismatch, readiness timeout, unsupported stages and malformed scores produce sanitized typed unavailability; they do not silently select a different algorithm. No performance tuning retries an authorization or validation error.

Cancellation must stop provider/search calls and release cursors/clients through their existing owners. Bound concurrency at the orchestration point; no per-hit fibers, detached workers or unlimited materialization. Query execution failure does not affect hiring writes or enqueue speculative repair from a read path.

### Public and persistence contracts

No GraphQL field rename, new connection, new event or embedding-model change is required. If internal criteria need a distinct branch limit, add a validated internal value and preserve existing callers/defaults until paired evidence selects it. Update API fixtures only for an explicitly accepted public change, applying the single-active-shape pre-MVP policy.

Operational Mongo index/schema changes use a new repeatable migration/setup step with definition verification, restart behavior and recovery documentation. Do not edit applied migrations or remove integrity indexes. Prefer a separately named candidate index for a comparison; keep the current index usable until cutover verification. Incompatible same-name definitions fail closed. Record concurrent writer behavior, build write/storage cost and explicit cleanup ownership; rollback of application code does not undo stored data changes.

## Code style and ownership

- Domain and ranking policies use immutable values, typed IDs, exhaustive ADTs and total functions. Pass policy inputs explicitly; no BSON, GraphQL, Circe, clock, provider SDK or driver access in those functions.
- Use `ValidatedNec` for independent configuration/input failures and `Either` for sequenced decisions. Legitimate absence is `Option`; expected failures never become thrown business exceptions or unchecked `.get`.
- Main application ports retain `UseCaseIO` and `RepositoryIO` (`EitherT[IO, ...]`). Keep typed errors through helpers and unwrap only at driver/effect-control boundaries.
- Services own authorization, batched recheck sequencing and request lifetime. Mongo adapters own typed criteria-to-BSON conversion, selective projection, explain capture and cursor cleanup.
- Extract small named query builders for real reusable predicates. Fixed field paths and typed values prevent caller-controlled BSON operators; avoid a generic query language or repository framework.
- Prefer indexes, batching and measured allocation reduction over caches. A cache proposal must establish actor-aware keys, invalidation, model/source freshness and deletion behavior before adoption.
- Preserve `Resource` ownership, FS2 backpressure, injected clock/ID effects and bounded concurrency. Configuration uses the existing HOCON/PureConfig boundary.

## Workload, alternatives and unresolved choices

All new runs are **not run**. Existing limits of 10,000 entities, 100 queries and concurrency 1/8 are upper bounds from HSE, not authorization for a paid or large run. Start with the smallest synthetic corpus that reproduces a query-plan issue. Record seed, size, skew, selectivity buckets, warmup, duration, warm/cold mode, server/index/model/dimensions/similarity and source revision.

For identical before/after inputs report query counts, keys/documents examined versus returned where available, p50/p95/p99, throughput, errors, CPU/memory, build time and index bytes, write latency/storage overhead and provider requests/tokens. Distinguish collection index bytes from Atlas vector index bytes; unavailable telemetry stays unavailable. Report Recall@K against exact neighbors using identical filters/model/metric separately from human relevance. Never infer quality from latency alone.

| Decision | Alternatives and evidence | Owner and blocked work |
|---|---|---|
| Atlas environment and budget | Existing authorized disposable Atlas versus defer live runs. Compare capability/version, isolation, usage cost and telemetry access. No provisioning is implied. | Product Manager and deployment owner; blocks live benchmark and native-stage acceptance, not query inventory/local tests. |
| Query targets and representative load | Use observed workload distributions versus a clearly labeled bounded synthetic approximation; set numerical latency/recall/write-overhead limits before selecting a winner. | Product Manager + Data Engineer; blocks performance acceptance and tuning adoption. |
| Vector optimization | Keep current settings versus measured candidate-budget adjustment; compare unquantized with supported scalar/binary quantization only after deployment compatibility is verified. | Architect + Data Engineer; blocks quantization/index contract changes until recall, memory, build and latency results exist. |
| Search capacity | Existing shared capacity versus dedicated search capacity only after a measured bottleneck and total recurring-cost ceiling. | Deployment owner + Product Manager; blocks capacity changes. |

No guessed vendor support/version or pricing becomes an acceptance fact. Verify current official deployment documentation when implementing a selected option.

## Acceptance and evidence

Proposed test locations below name capability suites to extend or introduce during implementation; none constitutes a result.

| ID | Given / When / Then | Verification method / target | Actual outcome |
|---|---|---|---|
| MVR-01 | Given each operational use case, when its filter variants run, then inventory records authorization, limits, projection, order, query counts and reproducible plans. | Repository integration workloads and sanitized explain records. | Not run; source inventory and Mongo explain/query-count capture remain outstanding. |
| MVR-02 | Given another user's IDs or nested relations, when accessed directly/batched, then forbidden data is never returned and query predicates remain scoped. | Hiring GraphQL/service and real Mongo authorization suites. | Partial: semantic search rechecks the persisted actor and recruiter job ownership; focused service and GraphQL access suites passed. Nested relationship scopes and the real Mongo authorization matrix remain outstanding. |
| MVR-03 | Given tied order values and page boundaries, when paginated on unchanged data, then no duplicate/missing eligible records occur and query work stays bounded. | Repository pagination integration tests with optional filters. | Not run |
| MVR-04 | Given opted-in/out/absent consent and missing private fields, when any vector/lexical/native branch runs, then the documented private-filter rule holds before branch truncation. | `MongoSearchFusionPipelineSpec` plus actual Atlas branch execution. | Partial: service-level consent/private-field truth table and local filter pipeline assertions passed; consent was not exercised against the production Mongo/Atlas branches. |
| MVR-05 | Given stale indexed hits and synchronized deletion, closure, consent or source/model changes, when authoritative validation runs, then revoked/stale hits disappear and short/empty results remain valid. | `SemanticSearchServiceSpec`, Mongo integration and Atlas lag scenario. | Partial: 16 focused service tests cover closure after retrieval, an inactive cached actor, candidate source/profile changes with stale embedding metadata, consent/private-field/filter eligibility, wrong model, survivor ordering, and closed-job removal. Deletion, ownership reassignment, authoritative read failures, cancellation cleanup and Atlas lag remain outstanding. |
| MVR-06 | Given final and branch budgets, when broad/selective/empty searches execute, then validation candidates stay within `branchResultLimit`, final results stay within `first`, and no iterative refill or per-hit fetch occurs. | Repository instrumentation, pipeline tests and live search queries. | Partial: repository output/fusion bounds and service final truncation now use the configured `branchResultLimit` and requested `first` respectively; regression tests cover stale/ineligible top hits with lower-ranked eligible survivors. Broad/selective/empty runtime counts and actual Atlas queries remain outstanding. |
| MVR-07 | Given identical bounded inputs, when ANN and exact search are paired, then fidelity, latency, errors and environment are captured without a human-relevance claim. | `SearchEvaluationAtlasRunner` and reproducible records. | Not run; `ATLAS_TEST_URI` is absent. The report now includes `branchResultLimit`; paired Atlas observations and generated records remain outstanding. |
| MVR-08 | Given an adopted optimization, when the same workload is rerun, then pre-agreed quality/latency limits and write/storage constraints pass against baseline. | Paired query plans/resource measurements; independent review. | Not run |
| MVR-09 | Given index mismatch, timeout or unavailable authoritative reads, when search initializes/executes, then it fails closed with sanitized errors and preserves existing indexes. | Setup and repository failure tests; Atlas readiness evidence. | Partial: four focused pure index-definition comparison tests passed; `MongoHiringRepositoryTransactionIntegrationSpec` passed 9 tests against disposable replica-set Mongo, exercising ordinary index initialization. Real mismatch preservation and Atlas readiness evidence remain outstanding. |
| MVR-10 | Given cancellation or migration interruption, when retried/restarted, then owned resources close and index/setup verification resumes without weakening constraints. | Resource tests and disposable Mongo migration integration. | Not run; interruption/restart and cancellation cleanup scenarios remain outstanding. |

## Implementation handoff and checkpoint

1. Complete the operational access inventory and bounded local Mongo execution-statistics capture (MVR-01/02/03).
2. Complete synchronized authoritative-read coverage for deletion, reassignment, unavailable reads and bounded batches; execute consent coverage against production Mongo branches (MVR-04/05/06).
3. Run repeat/interruption/index-mismatch setup tests against disposable Mongo and verify preserved integrity indexes (MVR-09/10).
4. When `ATLAS_TEST_URI` is available, exercise actual production branches and readiness, then run paired broad/selective/empty ANN-versus-exact evaluation. Keep Atlas telemetry limitations explicit (MVR-04/05/06/07).
5. Agree numerical quality, latency, write and storage thresholds before adopting any optimization. MVR-08 remains open until paired evidence meets those targets.

Implementation owners receive source/test paths for their assigned boundary; serialize edits to shared repository/query types. No agent receives migration, deployment or paid-service authority merely through this handoff.

- Implementation checkpoint (October 5, 2026): added authoritative post-retrieval validation for current candidate actors, owned open jobs, visible open job hits, active candidate hits, embedding metadata/source hashes, required skills and consent-aware private filters. Candidate-hit validation uses one bounded `findMany` call and preserves result order. Search repositories return validation candidates up to `branchResultLimit`; service paths, including candidate matching without query text, validate then cap the response at `first`. Branch-result limits are separately configured and validated against the maximum page size and ANN exploration budget. Present malformed or non-finite scores fail as invalid stored data. Ordinary Mongo index setup now preflights and verifies ordered keys, unique/sparse flags, partial filters and TTL values without dropping or rewriting definitions.
- Review follow-up (October 5, 2026): repository search/fusion outputs, including candidate vector, lexical, fused, and reranked results, are bounded at `branchResultLimit`; services validate the ranked candidates and apply `first` afterward. Regression tests cover a closed top job and a filter-ineligible top candidate with lower-ranked eligible survivors. This addresses underfilled pages within the configured cap while keeping the no-refill-loop and finite-work constraints.
- Focused evidence: `SemanticSearchServiceSpec` (16), `HiringGraphQLAccessSpec` (37), `AppConfigSpec` (36), `MongoSearchFusionPipelineSpec` (8), `MongoSemanticSearchResultSpec` (6), `MongoHiringIndexSetupSpec` (4), and `SearchEvaluationHarnessSpec` (1) passed. The complete unit suite passed 444 tests; `scalafmtCheckAll`, `scalafmtSbtCheck`, and `git diff --check` passed. The complete `IntegrationTest / test` suite exited successfully with 42 tests passed. Independent Code Reviewer, Security Engineer, and QA reviews returned PASS for this review follow-up.
- Source observations only: job search results currently hydrate full job documents; candidate aggregation uses a privacy-minimized projection. No query counts, explain statistics, index build time/storage measurements, or Atlas resource telemetry have yet been captured. The Atlas runner now defaults to the bounded seed-20261005/128-document/20-query/page-7/concurrency-1 workload, cycles broad/selective/empty filters, uses a fresh nonce database, restricts output to `.local/data`, and removes its owned database/collection after the run.
- Environment: `ATLAS_TEST_URI` was absent at implementation time; the Atlas runner was invoked and stopped with its explicit missing-URI error before connecting. Atlas tests and paired runs are unverified, not passed. MVR-08 targets remain unspecified, so optimization acceptance and tuning adoption stay open.
- Remaining acceptance: operational query inventory/explain evidence, real Mongo authorization and pagination matrix, setup mismatch/restart/interruption tests, actual Atlas branch/readiness/paired evaluation, measured baselines and agreed MVR-08 thresholds. The local correctness and baseline-runner slice is implemented; Phase 8 optimization acceptance remains open.
