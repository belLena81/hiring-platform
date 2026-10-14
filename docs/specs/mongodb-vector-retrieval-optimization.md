# MongoDB Access and Vector Retrieval Optimization

## Identity and scope

- Roadmap: Phase 8 in [development milestones](../development-milestones.md).
- Status: local correctness and bounded operational measurements are complete for the evaluated workloads. Overall retrieval acceptance remains partial. As of 2026-10-13 each Atlas-dependent criterion is split into an Atlas-compatible local gate (`.L`, evidence pending on current HEAD) and a deferred Atlas cloud gate (`.C`); see [Local closure split](#local-closure-split-2026-10-13), which supersedes the "BLOCKED without ATLAS_TEST_URI" wording in the table below. The local write/storage limits are recorded below.
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

The table separates executed local proof from live Atlas and optimization acceptance. Earlier checkpoints below retain their historical results; the latest local measurement refinement records the current follow-up and does not supersede unexecuted Atlas gates.

| ID | Given / When / Then | Verification method / target | Actual outcome |
|---|---|---|---|
| MVR-01 | Given each operational use case, when its filter variants run, then inventory records authorization, limits, projection, order, query counts and reproducible plans. | Repository integration workloads and sanitized explain records. | Historical baseline PASS: 880 requests and zero errors. Extended focused PASS: 30 read and four write/claim capabilities at concurrency 1/8, eight warmup plus 100 measured attempts per row; 6,800 measured attempts with zero unexpected errors. Query-plan context, scoped resources and durable outbox reconciliation are captured separately from the baseline. |
| MVR-02 | Given another user's IDs or nested relations, when accessed directly/batched, then forbidden data is never returned and query predicates remain scoped. | Hiring GraphQL/service and real Mongo authorization suites. | Local scoped reads implemented; GraphQL access 37 tests and real search-authorization integration passed. Application/history/nested real Mongo matrix PASS on final full suite. |
| MVR-03 | Given tied order values and page boundaries, when paginated on unchanged data, then no duplicate/missing eligible records occur and query work stays bounded. | Repository pagination integration tests with optional filters. | Tied timestamp application/event keyset pages, optional status filters, empty pages and separate actor scopes added to real Mongo matrix; final full-suite execution PASS. |
| MVR-04 | Given opted-in/out/absent consent and missing private fields, when any vector/lexical/native branch runs, then the documented private-filter rule holds before branch truncation. | `MongoSearchFusionPipelineSpec` plus actual Atlas branch execution. | Local production Mongo candidate predicate and projected BSON true/false/absent consent truth table PASS. Actual Atlas vector/lexical/native branch truth table remains BLOCKED without ATLAS_TEST_URI. |
| MVR-05 | Given stale indexed hits and synchronized deletion, closure, consent or source/model changes, when authoritative validation runs, then revoked/stale hits disappear and short/empty results remain valid. | `SemanticSearchServiceSpec`, Mongo integration and Atlas lag scenario. | Local search service 22 tests and real Mongo authorization gate PASS: deletion, closure, reassignment, consent/private/source/model changes, active cached actor, unavailable reads, cancellation and survivor order. Actual Atlas lag observation remains BLOCKED. |
| MVR-06 | Given final and branch budgets, when broad/selective/empty searches execute, then validation candidates stay within `branchResultLimit`, final results stay within `first`, and no iterative refill or per-hit fetch occurs. | Repository instrumentation, pipeline tests and live search queries. | Local bounded projection/overflow/empty-batch and service ranking tests PASS. Runtime operational projection counts recorded; actual Atlas broad/selective/empty retrieval remains BLOCKED. |
| MVR-07 | Given identical bounded inputs, when ANN and exact search are paired, then fidelity, latency, errors and environment are captured without a human-relevance claim. | `SearchEvaluationAtlasRunner` and reproducible records. | BLOCKED: ATLAS_TEST_URI absent. Paired broad/selective/empty ANN/exact runner and explicit production branch capability suite implemented; no live retrieval or recall claim. |
| MVR-08 | Given an adopted optimization, when the same workload is rerun, then pre-agreed quality/latency limits and write/storage constraints pass against baseline. | Paired query plans/resource measurements; independent review. | OPEN: no vector-retrieval optimization has been adopted and no paired retrieval comparison closes this criterion. ANN/exact Recall@7 and latency, representative retrieval workload, and Atlas branch behavior remain OPEN/BLOCKED. The local submission-projection, publication-scheduling, and cleanup-index measurements in [search/publication reliability](hiring-search-publication-reliability.md) and [event/test reliability](hiring-event-and-test-reliability.md) are contextual operational evidence only; they do not satisfy MVR-08. Candidate outbox partial indexes were rejected after paired regressions. |
| MVR-09 | Given index mismatch, timeout or unavailable authoritative reads, when search initializes/executes, then it fails closed with sanitized errors and preserves existing indexes. | Setup and repository failure tests; Atlas readiness evidence. | Local focused index recovery 7 tests PASS, including ordered key, uniqueness, sparse, partial and TTL mismatch preservation. Projection decoder/read failures fail closed. Four Atlas index readiness gates implemented but live evidence BLOCKED. |
| MVR-10 | Given cancellation or migration interruption, when retried/restarted, then owned resources close and index/setup verification resumes without weakening constraints. | Resource tests and disposable Mongo migration integration. | Local focused repeat/interrupted setup and unique-constraint preservation PASS; service cancellation cleanup PASS. Deterministic transaction-label/suppression retry tests added; final full-suite execution PASS. Live Atlas cleanup remains BLOCKED. |

## Implementation handoff and checkpoint

1. Completed locally: operational access inventory and bounded Mongo execution-statistics capture (MVR-01/02/03); evidence below.
2. Completed locally: synchronized authoritative-read coverage for deletion, reassignment, unavailable reads and bounded batches; consent execution against production Mongo predicates (MVR-04/05/06). Actual Atlas execution remains open.
3. Completed locally: repeat/interruption/index-mismatch setup and preserved integrity indexes against disposable Mongo (MVR-09/10).
4. When `ATLAS_TEST_URI` is available, exercise actual production branches and readiness, then run paired broad/selective/empty ANN-versus-exact evaluation. Keep Atlas telemetry limitations explicit (MVR-04/05/06/07).
5. Use the selected local write/storage ceilings below and agree the remaining retrieval-quality/workload decisions before adopting any optimization. MVR-08 remains open until paired evidence meets the relevant targets.

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
- **Open gates at this baseline checkpoint:** actual Atlas vector/lexical/native fusion/reranking execution, four-index readiness and cleanup on Atlas, paired ANN/exact Recall@7 and latency at concurrency 1/8, observed asynchronous index lag, vector/resource telemetry, agreed MVR-08 quality/write/storage constraints and paired optimization adoption. The local measurement refinement below supersedes the write/storage decision and local resource limitations; Atlas evidence and paired optimization adoption remain open. Functional embedding Template/Pipeline extraction remains Phase 9.

## Local measurement refinement

The October 6 follow-up addresses the local evidence limitations above without an Atlas connection, paid infrastructure, production/index changes or a fabricated optimization winner. Existing acceptance IDs remain authoritative.

- MVR-01: retain the historical baseline and add a separately named report with bounded warmup, larger measured samples, useful filter/page variants, stage-context explain metrics and separate winning/rejected index evidence. Assert zero unexpected errors for every capability, including embedding work and outbox. The outbox contract permits empty/partial claims on contention; record those attempts separately from completed publications and reconcile published, remaining and in-flight records plus released subject leases after each phase.
- MVR-08 preparation: measure complete authenticated structured GraphQL operations against disposable MongoDB, distinguish HTTP/service timings from repository timings, and compare observations with the existing use-case targets descriptively. Capture representative transactional write latency, ordinary index/storage cost and explicitly scoped local resource samples. Unavailable measurements stay unavailable.
- Acceptance decisions: on October 6 the deployment owner selected no more than 10% write p95 regression and 25% ordinary index-byte growth on identical before/after inputs for local operational optimizations. Evaluate each measured write capability at concurrency 1 and 8 separately, using completion-only latency and its disclosed sample count for claims/publications rather than mixing empty attempts into write latency. Compare the same seeded corpus and index-storage scope, and require zero unexpected errors. Existing use-case latency targets remain the latency references. Workload representativeness beyond this bounded local corpus and Recall@7 remain separate decisions/evidence gates; the chosen overhead limits do not establish production or Atlas acceptance.
- Adoption: select an operational optimization only when a reproducible bottleneck justifies it; require paired plans, read/write latency, storage and resource evidence before adoption. Keeping the current queries/indexes is valid when the evidence does not justify a change, and does not itself close overall MVR-08.
- Verification: focused measurement integrations, full root unit/integration suites, configured formatting checks, independent Code Reviewer and Security Engineer verdicts, then independent final QA. No live Atlas result is implied by these local checks.

Focused runtime checkpoint, October 6:

- `sbt scalafmtAll 'IntegrationTest / testOnly *MongoOperationalAccessEvaluationIntegrationSpec *HiringStructuredAccessEvaluationIntegrationSpec' scalafmtCheckAll scalafmtSbtCheck` exited successfully: four focused tests passed. `sbt test` passed all 454 unit tests earlier in this follow-up; no production or unit-test source changed afterward.
- Operational report: `.local/data/mongodb-access-evaluation/operational-access-measurements.json`, 68 rows / 6,800 measured attempts / zero unexpected errors, plus separate first-pass and eight-request warmup observations. All 68 rows captured available cgroup boundary samples. The unchanged historical baseline remains separately preserved; changed workload separation and sample conditions prevent treating the two reports as a paired optimization comparison.
- Structured HTTP report: `.local/data/mongodb-access-evaluation/structured-http-1791267909639-c7d58dda-d016-4cf5-84bd-588f2c6fe3c2.json`, 12 rows / 1,200 measured requests / zero unexpected errors, including 400 expected forbidden responses. Job discovery includes both UC01 p95 and p99 references; detail and application lists include their existing p95 references. Timings include loopback transport, complete response decoding/assertions, authentication, GraphQL, services and Mongo; they are not deployed SLO proof.
- Both focused reports match current Scala-source fingerprint `6a80150bf998e8297d5645b6ef71c8ece60f19014598cdccfcb58ec09b0fe1bc`. Outbox records reconcile across first-pass, warmup and measured phases, including distinct/shared-subject cases; empty claims remain attempts and are excluded from completion-only write percentiles.
- The initial focused failure was an invalid harness assumption that each concurrent outbox attempt must return an event. Production `MongoOperationalEventOutboxRepository.claim` intentionally converts expected conflicts to empty/partial claims. The corrected test preserves zero-error checks and verifies progress plus durable completion/state/lease counts; independent review confirmed the contract interpretation. Its initial observation is preserved in `operational-claim-attempts-observation.json`.

- Independent Code Reviewer: **PASS**. Separate Security Engineer: **PASS**. Both verified the final source, focused test/format results, report counts, durable outbox reconciliation, forbidden outcomes and matching source fingerprints.

### Final local validation

The final Java 17 command explicitly unset `ATLAS_TEST_URI` for its process, then ran `sbt test 'IntegrationTest / test' scalafmtCheckAll scalafmtSbtCheck`. It exited **0**: **454 unit tests passed; 57 integration tests passed; four Atlas tests skipped**. The skipped gates remain deferred/unverified. `git diff --check` passed, and generated artifacts remain ignored with none staged. The complete log is `.local/logs/mongodb-access-final-validation.log`.

The full suite regenerated `operational-access-measurements.json` at UTC `2026-10-06T06:38:29Z` and `structured-http-1791268444925-729b2bea-52b8-48cb-ad09-95b5920c900b.json`. Both match the source fingerprint above. Their final counts remain 6,800 operational attempts and 1,200 HTTP requests with zero unexpected errors. All operational rows have available scoped boundary samples. Distinct/shared-subject outbox concurrency-8 workloads completed 41/39 publications from 100 attempts; the remaining 59/61 attempts were legitimate empty claims. Published/remaining totals and released leases reconcile in every phase. These completion sample counts accompany completion-only latency rather than pretending 100 successful publications occurred.

Final HTTP observations in milliseconds:

| Capability | p95, concurrency 1 | p95, concurrency 8 | Existing p95 reference |
|---|---:|---:|---:|
| Structured job discovery | 89.44 | 33.52 | < 150 |
| Job detail | 18.67 | 25.81 | < 100 |
| Candidate applications | 23.93 | 36.46 | < 150 |
| Recruiter applications | 24.36 | 50.72 | < 150 |

UC01 p99 was 112.85 / 34.37 ms at concurrency 1/8 against its < 300 ms reference. These are bounded warmed synthetic loopback observations, with only 100 samples per row; they do not establish a production SLO or paired improvement. The operational read-corpus ordinary index bytes were 598,016; its queue/write workload is now isolated, so the earlier 651,264-byte baseline is not an identical-storage comparison or an index-storage saving.

Independent final QA: **PASS** for the scoped local measurement refinement, after independently checking final command exit/results, generated reports, source fingerprints, durable reconciliation, resource limits, forbidden responses and documentation. Code Reviewer and separate Security Engineer verdicts remain **PASS**.

Status: local measurement refinement complete with full local validation and independent reviews passed. Paired operational measurements and rejected-index comparisons are now cross-referenced in MVR-08. Actual Atlas gates, ANN/exact Recall@7, representative retrieval workload decisions and vector optimization evidence remain open. No production query, index, embedding budget or ranking change was adopted from the descriptive retrieval measurements.

## Local closure split (2026-10-13)

Historical sections above are unchanged. Where they say Atlas parts are "BLOCKED without ATLAS_TEST_URI", this section governs from 2026-10-13.

### Decisions

1. Each Atlas-dependent criterion (MVR-04, 05, 06, 07, 08, 09, 10) keeps its parent ID and is split into `.L` (Atlas-compatible local gate, evidence from Atlas Local) and `.C` (Atlas cloud gate, deferred to the Atlas integration slice). Terms are defined in [domain language](../domain-language.md).
2. Atlas Local (`compose.atlas-local.yaml`, mongot, 127.0.0.1:27018, $0) is the `.L` environment. `DisposableAtlas.deployment` labels loopback URIs `local-container`; those figures are never Atlas acceptance. Prior local run: [search evaluation](../search-evaluation.md#local-atlas-compatible-run) (`AtlasHiringSearchIntegrationSpec` 3 of 4 passed, native `$rerank` absent; `SearchEvaluationAtlasRunner` 500 documents / 20 queries ANN/ENN; closure lag observed). That run predates this table and is not evidence for it.
3. Cloud-only items appear only in `.C` rows: native `$rerank`, Atlas-scale recall/latency, index memory and storage, telemetry, dedicated capacity.
4. MVR-08 retrieval limits (deployment owner, 2026-10-13), evaluated per filter bucket (broad, selective, empty) at concurrency 1 and 8 on identical corpus, seed, filters, model and metric:
   - Recall@7 of ANN against exact (ENN) >= 0.95.
   - ANN p95 <= baseline p95 + 10%.
   - Existing ceilings: write p95 regression <= 10%, index-byte growth <= 25% (see Local measurement refinement), zero unexpected errors.
5. Only MVR-08.C can adopt a change to `numCandidates`, quantization or index definition. MVR-08.L can only conclude "retain current settings" or "candidate for cloud confirmation".

### Criteria

Evidence for every `.L` row is **Pending evidence run on current HEAD**; no `.L` row is claimed as passed. Tests carry the tag in their name (for example `MVR-04.L ...`). `.C` rows are DEFERRED to the Atlas integration slice; none implied by this section.

| ID | Given / When / Then | Method and expected test tag | Evidence |
|---|---|---|---|
| MVR-04.L | Given opted-in/out/absent consent and missing private fields, when vector and lexical branches run on Atlas Local, then the private-filter rule holds before branch truncation. | `AtlasHiringSearchIntegrationSpec`, tag `MVR-04.L` | Pending evidence run on current HEAD |
| MVR-04.C | Given the same truth table, when vector, lexical and native fusion/`$rerank` branches run on cloud Atlas, then the rule holds. | Same suite against cloud, tag `MVR-04.C` | DEFERRED |
| MVR-05.L | Given indexed hits and synchronized deletion, closure, consent or source/model change, when authoritative validation runs against Atlas Local, then revoked/stale hits disappear; index-closure lag is recorded as observed or not observed. | Atlas Local lag scenario, tag `MVR-05.L` | Pending evidence run on current HEAD |
| MVR-05.C | Given the same changes, when run on cloud Atlas, then revoked hits disappear and cloud index lag is measured. | Tag `MVR-05.C` | DEFERRED |
| MVR-06.L | Given final and branch budgets, when broad/selective/empty searches run on Atlas Local, then validation candidates <= `branchResultLimit`, results <= `first`, and no refill or per-hit fetch occurs. | No instrumented Atlas Local test exists yet; evidence comes from the runner's per-bucket bounded result counts, tag `MVR-06.L` | Pending evidence run on current HEAD; stays OPEN if those counts cannot show the validation-candidate bound |
| MVR-06.C | Given the same budgets, when run on cloud Atlas including native stages, then the same bounds hold. | Tag `MVR-06.C` | DEFERRED |
| MVR-07.L | Given identical bounded inputs, when ANN and ENN are paired per filter bucket at concurrency 1 and 8 on Atlas Local, then Recall@7, latency, errors and environment are recorded, labeled `local-container`, with no human-relevance claim. | `SearchEvaluationAtlasRunner`, record tag `MVR-07.L` | Pending evidence run on current HEAD |
| MVR-07.C | Given the same inputs, when run on cloud Atlas, then Atlas-scale Recall@7, latency, errors and telemetry are recorded. | Runner against cloud, tag `MVR-07.C` | DEFERRED |
| MVR-08.L | Given the rehearsal sweep below on Atlas Local, then the outcome is "retain current settings" or "candidate for cloud confirmation" with per-bucket figures against the decision-4 limits. No change is adopted. | Sweep records, tag `MVR-08.L` | Pending evidence run on current HEAD |
| MVR-08.C | Given the same sweep on cloud Atlas, when limits in decision 4 are met against the cloud baseline and independent review passes, then the change may be adopted; otherwise current settings stay. | Sweep records plus telemetry, tag `MVR-08.C` | DEFERRED |
| MVR-09.L | Given index mismatch, readiness timeout or unavailable authoritative reads on Atlas Local, then search fails closed with sanitized errors and existing indexes are preserved; the four index readiness gates pass. | Setup tests, tag `MVR-09.L` | Pending evidence run on current HEAD |
| MVR-09.C | Given the same faults on cloud Atlas, then fail-closed behavior and readiness hold, including native-stage unsupported outcomes. | Tag `MVR-09.C` | DEFERRED |
| MVR-10.L | Given cancellation or interrupted setup against Atlas Local, when retried, then owned resources and evaluation databases/indexes are cleaned and setup resumes without weakening constraints. | Resource tests, tag `MVR-10.L` | Pending evidence run on current HEAD |
| MVR-10.C | Given the same interruption on cloud Atlas, then cleanup of cloud-side search indexes and databases is verified. | Tag `MVR-10.C` | DEFERRED |

Tag scope: bare tags (`MVR-04`, `MVR-05`, `MVR-06`, `MVR-09`, `MVR-10`) are plain-MongoDB local evidence for the parent rows; `.L` tags are reserved for Atlas Local runs. `RetrievalLimitsSpec` (tests named `MVR-08 retrieval limits rule ...`) unit-tests the decision rule and is not `.L` evidence.

Parent criteria MVR-04..10 close only when both `.L` and `.C` are recorded; until then the parent status is "local pending/partial, cloud deferred". `.L` closure does not satisfy `.C`.

### MVR-08.L rehearsal procedure and decision rule

1. Start Atlas Local; use the runner's isolated `search_evaluation_` database and output beneath `.local/data`. Fix seed `20261005`, one corpus, `--dimensions`, `--documents`/`--queries` as in the prior local run, `--page-size 7`.
2. Baseline: the current setting (`--num-candidates 100` with `--branch-limit 100`, matching production `num-candidates = 100` in `src/main/resources/application.conf`). Run with `--concurrency 1` and `8`, `--temperature cold` then `warm`.
3. Sweep `numCandidates` in {current, half, double}; `half` must still be >= `--branch-limit` (lower `--branch-limit` equally if needed and record it; then compare against a baseline re-run at that same branch limit so only one variable changes). Optionally repeat with `--quantization scalar` at the same three values.
4. Per filter bucket (broad/selective/empty) and concurrency record Recall@7 versus ENN, ANN p50/p95/p99, errors, index build time and bytes where available, and write p95 if an index change is involved.
5. Decision: a setting is a candidate for cloud confirmation only if every bucket and concurrency meets Recall@7 >= 0.95 and ANN p95 <= baseline p95 + 10%, and it improves at least one measured dimension (latency, index bytes or build time) over current. Otherwise the outcome is "retain current settings". Write the outcome in this spec with record paths and `local-container` labels.

### Cloud runbook for `.C` rows (deferred)

Authorization required before any run, none implied by this spec: a named disposable cloud Atlas deployment (isolated, with version and tier recorded), an explicit budget ceiling and cleanup owner, and credentials supplied only through the environment variable the runner reads (via `DisposableAtlas.authorizedUri`), never committed or logged. `.C` runs must use the direct cloud URI, never a loopback tunnel or port-forward: loopback hosts are labelled `local-container`, which skips the native `$rerank` gate and would mislabel cloud evidence. Absent these, `.C` rows stay DEFERRED.

Run the integration suite for MVR-04/05/06/09/10 against the cloud URI, then the runner per bucket and concurrency. Exact flags: `--database` (must start `search_evaluation_`), `--output` (beneath `.local/data`), `--source-revision`, `--documents`, `--queries`, `--dimensions`, `--num-candidates`, `--branch-limit`, `--stored-source`, `--quantization none|scalar`, `--page-size`, `--concurrency 1|8`, `--temperature cold|warm`, `--seed`.

```
sbt 'IntegrationTest / runMain com.example.graphQL.cats.repository.mongo.SearchEvaluationAtlasRunner \
  --database search_evaluation_cloud --output .local/data/search-evaluation-cloud \
  --source-revision <git HEAD> --seed 20261005 --page-size 7 \
  --num-candidates <current|half|double> [--quantization scalar] \
  --concurrency 1|8 --temperature cold|warm'
```

Run the baseline and each sweep point at both concurrencies and both temperatures; use the corpus size authorized for the budget (runner bounds: documents <= 10000, queries <= 100). Telemetry to capture where Atlas exposes it, otherwise record "unavailable": vector index memory and storage bytes, index build time, index status/readiness and replication lag, CPU and memory of the search nodes, query latency percentiles, provider/Atlas usage cost, and native `$rerank` availability and result. Apply decision-4 limits against the cloud baseline; only a pass with independent review adopts a change, and any adopted index change follows the migration and recovery rules in Public and persistence contracts. Dedicated search capacity needs a separately measured bottleneck and cost ceiling.

### Local closure checkpoint (2026-10-13)

- Commit `e2349f0`: split, tags, rerank skip and `RetrievalLimits` rule. Independent Code Reviewer PASS (after tag-accuracy fixes) and Security Engineer PASS.
- `bash scripts/check-local.sh` exit 0 on that commit: root 1051 units; root integration 297 total, 288 passed, 9 explicit external/opt-in skips (not passes); analytics 458 units and 102 integrations, 4 opt-in ignored; 8 skills and 7 agent definitions validated. Log: ignored `.local/logs/mvr-local-closure-check-local.log`.
- Independent QA (2026-10-13): PASS for the local scope on the current tree (`check-local` exit 0; root 1083 units, integration 290 passed / 14 skipped). Log: ignored `.local/logs/qa-local-closure-check.log`.
- BLOCKED: the Atlas Local evidence run for all `.L` rows and the MVR-08.L decision (container not started). `.C` rows remain DEFERRED.
