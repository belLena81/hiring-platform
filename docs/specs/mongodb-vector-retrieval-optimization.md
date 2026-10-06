# MongoDB Access and Vector Retrieval Optimization

## Identity and scope

- Roadmap: Phase 8 in [development milestones](../development-milestones.md).
- Status: local correctness and baseline remediation verified; actual Atlas evidence and agreed optimization thresholds remain open acceptance gates.
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

The local operational baseline has run; live Atlas runs remain **blocked** without access. Existing limits of 10,000 entities, 100 queries and concurrency 1/8 are upper bounds from HSE, not authorization for a paid or large run. Start with the smallest synthetic corpus that reproduces a query-plan issue. Record seed, size, skew, selectivity buckets, warmup, duration, warm/cold mode, server/index/model/dimensions/similarity and source revision.

For identical before/after inputs report query counts, keys/documents examined versus returned where available, p50/p95/p99, throughput, errors, CPU/memory, build time and index bytes, write latency/storage overhead and provider requests/tokens. Distinguish collection index bytes from Atlas vector index bytes; unavailable telemetry stays unavailable. Report Recall@K against exact neighbors using identical filters/model/metric separately from human relevance. Never infer quality from latency alone.

| Decision | Alternatives and evidence | Owner and blocked work |
|---|---|---|
| Atlas environment and budget | Existing authorized disposable Atlas versus defer live runs. Compare capability/version, isolation, usage cost and telemetry access. No provisioning is implied. | Product Manager and deployment owner; blocks live benchmark and native-stage acceptance, not query inventory/local tests. |
| Query targets and representative load | Use observed workload distributions versus a clearly labeled bounded synthetic approximation; set numerical latency/recall/write-overhead limits before selecting a winner. | Product Manager + Data Engineer; blocks performance acceptance and tuning adoption. |
| Vector optimization | Keep current settings versus measured candidate-budget adjustment; compare unquantized with supported scalar/binary quantization only after deployment compatibility is verified. | Architect + Data Engineer; blocks quantization/index contract changes until recall, memory, build and latency results exist. |
| Search capacity | Existing shared capacity versus dedicated search capacity only after a measured bottleneck and total recurring-cost ceiling. | Deployment owner + Product Manager; blocks capacity changes. |

No guessed vendor support/version or pricing becomes an acceptance fact. Verify current official deployment documentation when implementing a selected option.

## First iteration latency plan

The deployment owner requested latency planning for the first iteration on October 5, 2026. The first iteration establishes a reproducible reference rather than adopting tuning from assumed targets:

1. Run the deterministic local operational corpus (seed `20261005`, 128 jobs, 32 candidates, two recruiters and singleton Admin), with tied dates and bounded applications/history/work. Capture each production method's command count and sanitized selection/plan, then measure concurrency 1 and 8 separately.
2. Record first-pass and warmed client conditions, sample count, elapsed duration, p50/p95/p99, completed operations/second and errors. Label unavailable CPU/memory telemetry; ordinary collection index bytes do not represent Atlas vector storage.
3. Run 20 paired ANN/exact queries at `k=7` with broad/selective/empty filters on the authorized nonce-scoped Atlas database. Record client warmup without claiming control of server caches. Keep repository retrieval timing separate from service authoritative validation and provider latency; fabricated vectors incur no provider requests.
4. Compare observations with the existing initial use-case latency targets: UC01 structured job search p95 < 150 ms and p99 < 300 ms (excluding external network latency), UC02 semantic/hybrid search p95 < 500 ms, UC03 job detail p95 < 100 ms, UC05 candidate applications and UC08 recruiter applications p95 < 150 ms, and UC10 candidate search p95 < 500 ms. Repository-only measurements are not end-to-end service SLO proof. Use observed p95/p99 and throughput distributions to propose the next iteration’s optimization acceptance budgets. Require explicit agreement on Recall@7 and write/storage overhead, and the workload/scope of latency gates, before selecting budget/index changes; the supplied response requests latency planning rather than those missing numerical limits.
5. Treat errors, unsupported native stages, missing access and unobserved asynchronous index lag as distinct blocked gates. Synchronized Mongo mutation after retrieval proves authoritative eligibility independently of index-lag observation.

No production search budgets, embedding settings or application RRF defaults change in this baseline iteration. MVR-08 remains open until agreed limits and paired evidence exist.

## Acceptance and evidence

The table separates executed local proof from live Atlas and optimization acceptance. Earlier checkpoints below retain their historical results; the final October 6 record is authoritative for this source.

| ID | Given / When / Then | Verification method / target | Actual outcome |
|---|---|---|---|
| MVR-01 | Given each operational use case, when its filter variants run, then inventory records authorization, limits, projection, order, query counts and reproducible plans. | Repository integration workloads and sanitized explain records. | Local focused PASS: 22 production capabilities × 20 calls at concurrency 1/8; sanitized commands, plans, hydration, latency, throughput, errors, build time and ordinary index bytes recorded. Final post-retry full-suite baseline PASS with 880 requests and zero errors. |
| MVR-02 | Given another user's IDs or nested relations, when accessed directly/batched, then forbidden data is never returned and query predicates remain scoped. | Hiring GraphQL/service and real Mongo authorization suites. | Local scoped reads implemented; GraphQL access 37 tests and real search-authorization integration passed. Application/history/nested real Mongo matrix PASS on final full suite. |
| MVR-03 | Given tied order values and page boundaries, when paginated on unchanged data, then no duplicate/missing eligible records occur and query work stays bounded. | Repository pagination integration tests with optional filters. | Tied timestamp application/event keyset pages, optional status filters, empty pages and separate actor scopes added to real Mongo matrix; final full-suite execution PASS. |
| MVR-04 | Given opted-in/out/absent consent and missing private fields, when any vector/lexical/native branch runs, then the documented private-filter rule holds before branch truncation. | `MongoSearchFusionPipelineSpec` plus actual Atlas branch execution. | Local production Mongo candidate predicate and projected BSON true/false/absent consent truth table PASS. Actual Atlas vector/lexical/native branch truth table remains BLOCKED without ATLAS_TEST_URI. |
| MVR-05 | Given stale indexed hits and synchronized deletion, closure, consent or source/model changes, when authoritative validation runs, then revoked/stale hits disappear and short/empty results remain valid. | `SemanticSearchServiceSpec`, Mongo integration and Atlas lag scenario. | Local search service 22 tests and real Mongo authorization gate PASS: deletion, closure, reassignment, consent/private/source/model changes, active cached actor, unavailable reads, cancellation and survivor order. Actual Atlas lag observation remains BLOCKED. |
| MVR-06 | Given final and branch budgets, when broad/selective/empty searches execute, then validation candidates stay within `branchResultLimit`, final results stay within `first`, and no iterative refill or per-hit fetch occurs. | Repository instrumentation, pipeline tests and live search queries. | Local bounded projection/overflow/empty-batch and service ranking tests PASS. Runtime operational projection counts recorded; actual Atlas broad/selective/empty retrieval remains BLOCKED. |
| MVR-07 | Given identical bounded inputs, when ANN and exact search are paired, then fidelity, latency, errors and environment are captured without a human-relevance claim. | `SearchEvaluationAtlasRunner` and reproducible records. | BLOCKED: ATLAS_TEST_URI absent. Paired broad/selective/empty ANN/exact runner and explicit production branch capability suite implemented; no live retrieval or recall claim. |
| MVR-08 | Given an adopted optimization, when the same workload is rerun, then pre-agreed quality/latency limits and write/storage constraints pass against baseline. | Paired query plans/resource measurements; independent review. | OPEN: first iteration latency plan references existing initial use-case SLOs; repository baseline is not end-to-end proof. Recall@7 and write/storage constraints plus adopted optimization paired evidence remain unagreed/unmeasured. |
| MVR-09 | Given index mismatch, timeout or unavailable authoritative reads, when search initializes/executes, then it fails closed with sanitized errors and preserves existing indexes. | Setup and repository failure tests; Atlas readiness evidence. | Local focused index recovery 7 tests PASS, including ordered key, uniqueness, sparse, partial and TTL mismatch preservation. Projection decoder/read failures fail closed. Four Atlas index readiness gates implemented but live evidence BLOCKED. |
| MVR-10 | Given cancellation or migration interruption, when retried/restarted, then owned resources close and index/setup verification resumes without weakening constraints. | Resource tests and disposable Mongo migration integration. | Local focused repeat/interrupted setup and unique-constraint preservation PASS; service cancellation cleanup PASS. Deterministic transaction-label/suppression retry tests added; final full-suite execution PASS. Live Atlas cleanup remains BLOCKED. |

## Implementation handoff and checkpoint

1. Completed locally: operational access inventory and bounded Mongo execution-statistics capture (MVR-01/02/03); evidence below.
2. Completed locally: synchronized authoritative-read coverage for deletion, reassignment, unavailable reads and bounded batches; consent execution against production Mongo predicates (MVR-04/05/06). Actual Atlas execution remains open.
3. Completed locally: repeat/interruption/index-mismatch setup and preserved integrity indexes against disposable Mongo (MVR-09/10).
4. When `ATLAS_TEST_URI` is available, exercise actual production branches and readiness, then run paired broad/selective/empty ANN-versus-exact evaluation. Keep Atlas telemetry limitations explicit (MVR-04/05/06/07).
5. Agree numerical quality, latency, write and storage thresholds before adopting any optimization. MVR-08 remains open until paired evidence meets those targets.

Implementation owners receive source/test paths for their assigned boundary; serialize edits to shared repository/query types. No agent receives migration, deployment or paid-service authority merely through this handoff.

- Implementation checkpoint (October 5, 2026): added authoritative post-retrieval validation for current candidate actors, owned open jobs, visible open job hits, active candidate hits, embedding metadata/source hashes, required skills and consent-aware private filters. Candidate-hit validation uses one bounded `findMany` call and preserves result order. Search repositories return validation candidates up to `branchResultLimit`; service paths, including candidate matching without query text, validate then cap the response at `first`. Branch-result limits are separately configured and validated against the maximum page size and ANN exploration budget. Present malformed or non-finite scores fail as invalid stored data. Ordinary Mongo index setup now preflights and verifies ordered keys, unique/sparse flags, partial filters and TTL values without dropping or rewriting definitions.
- Review follow-up (October 5, 2026): repository search/fusion outputs, including candidate vector, lexical, fused, and reranked results, are bounded at `branchResultLimit`; services validate the ranked candidates and apply `first` afterward. Regression tests cover a closed top job and a filter-ineligible top candidate with lower-ranked eligible survivors. This addresses underfilled pages within the configured cap while keeping the no-refill-loop and finite-work constraints.
- Focused evidence: `SemanticSearchServiceSpec` (16), `HiringGraphQLAccessSpec` (37), `AppConfigSpec` (36), `MongoSearchFusionPipelineSpec` (8), `MongoSemanticSearchResultSpec` (6), `MongoHiringIndexSetupSpec` (4), and `SearchEvaluationHarnessSpec` (1) passed. The complete unit suite passed 444 tests; `scalafmtCheckAll`, `scalafmtSbtCheck`, and `git diff --check` passed. The complete `IntegrationTest / test` suite exited successfully with 42 tests passed. Independent Code Reviewer, Security Engineer, and QA reviews returned PASS for this review follow-up.
- Environment: `ATLAS_TEST_URI` was absent at implementation time; the Atlas runner was invoked and stopped with its explicit missing-URI error before connecting. Atlas tests and paired runs are unverified, not passed. MVR-08 targets remain unspecified, so optimization acceptance and tuning adoption stay open.

- Gap-remediation checkpoint (October 6, 2026): validated persisted-actor scopes, relationship-aware nested selection, projected search eligibility with a pure policy, synchronized mutation/failure/cancellation tests, real Mongo instrumentation and index recovery checks are verified locally. The opt-in production Atlas suite is implemented; absent `ATLAS_TEST_URI` remains a blocked external gate.

- Review findings resolved: Security identified the race between actor/job prechecks and final hit selection; scoped Mongo selection and synchronized service/real-Mongo tests now cover it. Code Review identified the outbox retry boundary defect exposed by the baseline; session-only transient pass-through and deleted-subject suppression retry tests now pass. Both independent reviewers returned PASS.

## Final local remediation evidence — October 6, 2026

- Final command: `sbt scalafmtAll test 'IntegrationTest / test' scalafmtCheckAll scalafmtSbtCheck` exited successfully: **454 unit tests passed; 55 integration tests passed; four Atlas tests skipped because access is absent**. Skips remain blocked acceptance. `scalafmtSbt` ran successfully separately, and `git diff --check` passed. The full unit rerun includes the corrected HTTP workflow fixture for scoped nested loaders.
- Real Mongo evidence includes ten hiring repository transaction tests, one scoped search authorization test, two operational/consent tests, seven index recovery tests and two deterministic outbox retry tests. All passed on the final source. The synchronized service suite has 22 passing tests; projected decoder suite has four. Cancellation tests exercise owned effect cleanup; transaction resources retain existing cleanup tests.
- Local baseline: MongoDB 8.0.32, seed `20261005`, 128 jobs, 32 candidates, two recruiters, singleton Admin, 128 applications, 384 history records, 64 embedding work records and 64 outbox records. Twenty requests for each of 22 capabilities at concurrency 1/8 produced 44 rows / **880 requests / zero errors**. Non-worker/publisher repository p95 ranged from **2.17 to 15.59 ms** in this run. Twenty samples are a small synthetic baseline; these timings do not establish end-to-end SLOs or a production optimization winner.
- Ordinary index build on the seeded local corpus took **2892 ms**. Collection ordinary index storage totaled **651264 bytes**. Vector index bytes and server CPU/memory remain unavailable, and provider requests were zero. Reports contain per-capability p50/p95/p99, elapsed time, sample count, no dedicated warmup, throughput, returned counts, command shapes/counts, minimized/full hydration observations and sanitized examined/returned statistics. Listed explain index names include both winning and rejected plans.
- Evidence: `.local/data/mongodb-access-evaluation/operational-access-baseline.json` (ignored), UTC `2026-10-06T04:38:09Z`, source files SHA-256 `b265f11def38660996433b5b3e2b045b7a82032754e8592b61dd6050e48d03d3`. This hashes source files in the runner's recorded scope, not documentation. The pre-retry run remains separately preserved at `operational-access-before-outbox-retry.json`: it recorded 17/20 outbox failures at concurrency 8. That run led to the corrected active-session transient-label boundary and deterministic claim/deleted-subject suppression retry tests. No general budget/index tuning was adopted.
- Independent Code Reviewer and Security Engineer returned **PASS** after their findings were fixed. Independent final QA returned **PASS** for the local remediation and evidence record. Overall Phase 8 optimization acceptance remains open.
- Explicit Atlas execution: `sbt 'IntegrationTest / testOnly *AtlasHiringSearchIntegrationSpec'` reported **0 passed / 4 skipped**; the bounded `SearchEvaluationAtlasRunner` command (`128` documents, `20` queries, `k=7`, concurrency `1`, seed `20261005`) exited **1** with the missing-`ATLAS_TEST_URI` message before connection. Both outcomes are **BLOCKED**, not successful live gates; concurrency-8 paired observations likewise remain unverified.
- **Open gates:** actual Atlas vector/lexical/native fusion/reranking execution, four-index readiness and cleanup on Atlas, paired ANN/exact Recall@7 and latency at concurrency 1/8, observed asynchronous index lag, vector/resource telemetry, agreed MVR-08 quality/write/storage constraints and paired optimization adoption. Missing infrastructure, unobserved lag and unsupported native stages remain blocked/open; local correctness does not close Phase 8 optimization acceptance. Functional embedding Template/Pipeline extraction remains Phase 9.
