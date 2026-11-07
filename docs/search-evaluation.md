# Search evaluation and embedding experiments

This procedure separates relevance, retrieval quality, latency, and provider cost. A passing unit test for the metric functions does not establish a live Atlas performance result.

The [current roadmap](development-milestones.md) places MongoDB/vector optimization in Phase 8, search evaluation and embedding architecture in Phase 9, and AI discovery in Phase 10. Existing metrics and the disposable runner provide a baseline; they do not complete those phases.

The [search evaluation and embedding specification](specs/hiring-search-enhancements.md#search-evaluation-and-embedding-architecture) owns the detailed contracts and acceptance criteria. It preserves HSE-06/HSE-07 and separates deterministic synthetic retrieval checks from human-authored relevance judgments over fabricated hiring data. Local preparation and typed assessment do not authorize Atlas/provider spending. [MongoDB/vector optimization](specs/mongodb-vector-retrieval-optimization.md) owns access-plan and ANN tuning; [discovery](specs/hiring-discovery-search-quality.md) owns adoption of evaluated product changes.

## Bounded retrieval comparison

Use an authorized disposable Atlas deployment configured through `ATLAS_TEST_URI` and a database named with the `search_evaluation_` prefix. `SearchEvaluationAtlasRunner` creates a uniquely named disposable collection, seeds up to 10,000 deterministic synthetic vectors in batches, builds a vector index, checks readiness, runs paired ANN/ENN queries with category filters at concurrency 1 or 8, writes a JSON run record, and drops only the collection it successfully created. It does not read production records or use an embedding provider. Synthetic judged relevance is generated independently from ENN using a second deterministic topic label; these judgments test retrieval behavior, not human semantic relevance.

Run it only against a disposable Atlas database, once for each concurrency/temperature combination you want to compare:

```bash
sbt 'IntegrationTest / runMain com.example.graphQL.cats.repository.mongo.SearchEvaluationAtlasRunner --source-revision <working-tree-id> --database search_evaluation_benchmark --output .local/data/search-evaluation --documents 10000 --queries 100 --dimensions 1024 --num-candidates 100 --page-size 20 --concurrency 8 --temperature cold --seed 20260923'
```

Add `--vector-storage float64-array` for the pre-migration `028` array baseline; the default `float32-binary` stores the BSON binary vector the application writes. Use `--concurrency 1` and `--temperature warm` for the other runs. `ATLAS_TEST_URI` must point to a disposable test deployment; the runner does not print the URI. The output includes Atlas version, workload, collection index size when `collStats` permits it, paired ANN/ENN latency and errors, ANN-vs-ENN recall, independent synthetic judged ranking metrics, and explicit nulls where Atlas Vector Search index bytes or CPU/memory/query telemetry are not available to the driver. `collectionIndexBytes` is not Atlas Vector Search index size. Add vector-index size and Atlas CPU/memory/query metrics from the Atlas metrics export before comparing resource costs. The recorded local evidence covers compilation; it does not include execution against Atlas.

Compare the configured application-side RRF baseline with the selected experimental retrieval strategy. For ANN ground truth, run the same query/filter set with ENN. Record dataset and index sizes, Atlas/server versions, embedding model and dimensions, indexes and quantization, filters, `numCandidates`, page size, query count, concurrency (1 and 8), warmup, warm/cold run, duration, and timestamp. Capture Recall@K against ENN, NDCG@K against the judged IDs, throughput, errors, p50/p95/p99 latency, CPU/memory, provider request counts, and Atlas query/index metrics. Billing telemetry is the source for observed costs; calculations from request counts or storage are estimates and must be labeled as such.

Write one JSON run record per strategy containing those inputs and measurements. Keep raw query text synthetic. Do not claim a performance improvement without paired results under the same environment and workload. Use measured relevance and resource tradeoffs to decide whether an experimental fusion mode should be enabled.

The evaluation core validates typed corpus/run observations and produces an immutable numerical report; the infrastructure JSON adapter renders it. Ranking failures and exact-reference failures are separate. Recall/NDCG quality means cover successful rankings only, with explicit denominators and attempted/success/failed/unavailable counts. Fidelity requires both retrieval and exact-reference success and has its own denominator. Successful empty eligible sets or queries without positive judgments retain the zero metric convention and are counted explicitly. Missing telemetry includes a reason; absent retrieval latency or billing is never inferred from fixture replay.

### Write latency option

`--write-samples <n>` adds client-observed single-document write latency to a `SearchEvaluationAtlasRunner` run. `n` is an integer from 0 to 2000 (`SearchEvaluationWriteLatency.MaxSamples`). The default 0 turns the option off; the queries, explain capture and JSON records are then unchanged. A non-numeric or out-of-range value fails argument parsing before any connection is opened.

When `n` is greater than 0, the runner measures writes after the query phase, the `collStats` capture and the explain capture, and before the reports are written. The extra documents therefore take part in no query or explain. The measurement uses only the runner's own disposable collection:

- `n` single-document inserts, then `n` single-field `$set` updates of `embedding` on the documents just inserted. Documents have `_id` `write-NNNNN` (five-digit index), `category` 99, which no query filter matches, a `judgedTopic` value, and the run's `--vector-storage` form. Each update writes a different vector to the same document.
- A warmup of `min(n, 10)` operations per kind runs first. Warmup operations are excluded from every figure.
- Operations are sequential on one client, so `--concurrency` does not apply to this phase, and the record states concurrency 1. Writes use the application write concern from `MongoConcernPolicy` (majority, journaled, `wtimeout` 1500 ms). Reads of the runner's collection use `local`, as for the query phase.
- An update is a successful sample only when it matches and modifies exactly one document. Any other result is an error.
- Failed operations are counted in `errors` and left out of the p50, p95 and p99 figures. Percentiles use the nearest rank over successful operations and are `null` when none succeeded. `count` is the number of measured operations attempted, so it equals `n`.
- The client's socket read timeout is 2 seconds (`MongoDatabaseProbe.effectiveSettings`). A write slower than that becomes an error rather than a latency value, so a comparison is valid only when both runs report `errors` of 0.
- Latency is measured around the client call. It excludes asynchronous mongot vector-index ingestion, which the runner does not wait for.

The figures appear as a `writeLatency` object in the `criterion-evidence-search-<concurrency>-<temperature>-<timestamp>.json` record. The `search-evaluation-*.json` report is unchanged. The object is absent when the option is 0. Its fields are `criterion` (`MVR-07.L`), `vectorStorage`, `concurrency` (always 1), `warmup`, and `insert` and `update`, each with `count`, `errors`, `p50Millis`, `p95Millis` and `p99Millis`.

Checks for option parsing, the warmup bound, nearest-rank percentiles, error exclusion, the update acceptance rule and the record shape are in `SearchEvaluationWriteLatencySpec` (`src/it`, tagged `MVR-07.L`). Because the spec sits in `src/it`, it runs under `sbt 'IntegrationTest / test'`, not under the unit suite (`sbt test`). It needs no Atlas and establishes no latency result.

Known limit: the criterion record's `unavailableTelemetry` list is a fixed list in `AtlasLocalCriterionEvidence` and still includes `write p95`, even when `--write-samples` is greater than 0. Records with a `writeLatency` object therefore list `write p95` as unavailable. Read `writeLatency` and ignore that entry. Follow-up: remove the entry when `--write-samples` is greater than 0. No measured write latency is recorded in this document; the MVR-07.L write-latency results, including the 2026-11-07 Atlas Local runs, are recorded in the [vector retrieval optimization specification](specs/mongodb-vector-retrieval-optimization.md#float32-binary-storage-decision-2026-11-07). The acceptance limits and the storage-form comparison procedure are owned by the [canonical job filters and binary vectors gates](specs/mongodb-vector-retrieval-optimization.md#canonical-job-filters-and-binary-vectors-atlas-local-gates-msc-08-msc-09).

## Local fixture preparation

The approved local workload contains eight fabricated jobs, eight candidate summaries and twelve queries (four per use case), equally split between tuning and held-out sets. The binary rubric uses intended role and required skills. The rubric is user approved, but generated per-query labels are provisional until domain review; approving the rubric does not make these labels human reviewed. Freeze corpus/judgment digests and the split before any future tuning.

Offline replay supplies authored vector/lexical branch rankings to the actual application RRF functions and the same typed evaluator/renderer used by the Atlas adapter. It checks reproducibility, numerical/report behavior and existing ranking ties. It does not execute lexical/vector retrieval, measure live search latency or establish human relevance. Recommendations remain vector-only; lexical/hybrid recommendation combinations are reported unavailable. No provider calls or paid requests are made. Reports belong under ignored `.local/data/search-evaluation/` and logs under `.local/logs/`.

Run from the repository root with Java 17+ and an explicit label for the working tree being measured:

```bash
sbt 'Test / runMain com.example.graphQL.cats.service.search.SearchEvaluationFixtureReplay --source-revision <working-tree-id>'
sbt 'Test / runMain com.example.graphQL.cats.service.search.SearchEvaluationFixtureReplay --source-revision <working-tree-id> --concurrency 8'
```

Both entrypoints separately fingerprint the actual Scala source tree, including fixtures. The caller's revision label and fingerprint are distinct: a Git HEAD alone cannot identify uncommitted changes. The replay defaults to K=7 and concurrency 1; its bounded concurrency-8 run and fixture tests check deterministic execution. Each replay writes a separate run directory to preserve earlier artifacts.

Quality averages alone cannot authorize adoption. The conservative evaluation policy is approved; reviewed held-out labels and paired live relevance, reliability, latency and cost evidence remain pending. Eligibility or privacy violations block acceptance irrespective of relevance scores or reliability averages.

## Paired ranking assessment

`SearchEvaluationAssessment.assess` takes two validated reports, an explicitly scoped policy, a digest-bound human label attestation and separately observed privacy/eligibility/billing evidence for each run. It recomputes canonical measurements before trusting a report. The numerical core remains separate from JSON; `SearchEvaluationReportJson.renderAssessment` renders the decision and reasons. An assessment is an evaluation recommendation and never changes the configured search strategy.

The conservative policy approved by the user on October 6 requires no held-out Recall@K or NDCG@K regression in any selected use-case/filter group, no failed supported queries (including tuning), and no p95 or provider-request increase. The assessment separately requires no observed billing cost increase and may add an absolute billed-cost ceiling; no absolute currency amount has been chosen. Provider-request counts do not establish billing cost. Selected use cases need complete supported-query coverage and held-out groups; unsupported combinations remain explicit and cannot silently shrink the accepted scope.

Human label review must identify the exact corpus digest and judgment identity. Provisional labels, an unagreed policy, authored rankings, synthetic embeddings, missing measurements or missing observed privacy/eligibility evidence defer recommendation. Recorded privacy or eligibility failure rejects it. Invalid policy/coordinates or modified numerical summaries fail typed validation. An observed Atlas ranking using fabricated vectors is still tooling evidence; positive semantic-quality recommendation requires genuine provider-generated embeddings and the other observed acceptance evidence.

The user approved the conservative evaluation policy and explicitly kept labels **pending human review**. The curated runner records that agreed policy; general policy construction defaults to proposed. Local tests use fabricated label attestations and observed-evidence values only to exercise the assessment; they are not human signoff or billing observations.

## Curated judgment review and retrieval capture

Export the complete review packet without a provider or database connection:

```bash
sbt 'Test / runMain com.example.graphQL.cats.service.search.SearchEvaluationJudgmentReview'
```

The packet is written under ignored `.local/data/search-evaluation/judgment-review/`, named by the corpus digest. It includes all sixteen fabricated entities and twelve queries, their fixed tuning/held-out split, eligibility, binary labels, intended roles/required skills and every eligible entity's rationale. Review the packet, adjudicate ambiguity and record the human reviewer against its exact corpus digest and judgment identity. Exporting or automatically checking the packet leaves its human review status pending.

The separate curated runner captures supported queries through the production service and Mongo retrieval code using isolated, deterministic fixture vectors. It requires both explicit authorization and a securely configured disposable `ATLAS_TEST_URI`; it cannot target a caller-selected application database. Run only after the disposable Atlas execution has been authorized:

```bash
sbt 'IntegrationTest / runMain com.example.graphQL.cats.repository.mongo.HiringSearchEvaluationAtlasRunner --authorize-disposable-atlas --output .local/data/search-evaluation --source-revision <working-tree-id> --concurrency 1'
```

Use concurrency 8 for the paired execution. The runner owns a generated `search_evaluation_` nonce database and its cleanup, verifies four production index definitions/readiness, and seeds closed/deleted/inactive/stale-source and consent/private-filter fixtures. Supported job-search and recruiter combinations capture lexical, vector and application RRF results; recommendations support only broad vector queries because their current API has no filters. Unsupported modes and filtered recommendation cases stay explicit in the twelve-query corpus.

These are **observed retrieval with synthetic embeddings** when actually executed. They do not constitute semantic relevance acceptance, human signoff, production activation or billing observations. Provider requests are zero; Atlas cost remains unknown. Local compilation and controlled adapter tests are preparation evidence; absence of an executed live run remains unverified.

## Automated Embedding comparison

The application continues to write Voyage embeddings through its durable work queue. Automated Embedding is only a separate Atlas preview experiment: create a disposable collection populated with synthetic documents, configure an isolated AutoEmbed index and query path, then measure indexing and query latency and provider/billing telemetry against the manual-embedding baseline. Record the Atlas preview status and version. Delete only this disposable collection and index after recording results. Never dual-write application collections or move the application read path as part of this experiment.

## Local Atlas-compatible run

`compose.atlas-local.yaml` starts a pinned MongoDB Atlas Local image on `127.0.0.1:27018` (tmpfs, no provisioning or spend). The same image is available as the opt-in `atlas-local` service of `compose.yaml`: `docker compose --profile atlas up -d --wait atlas-local` is an alternative to the standalone file (`docker compose -f compose.atlas-local.yaml up -d --wait`); stop either with `down --volumes` using the same file and profile. Point the existing disposable-Atlas guard at it with `ATLAS_TEST_URI='mongodb://127.0.0.1:27018/?directConnection=true' ATLAS_TEST_DISPOSABLE=true ATLAS_TEST_ALLOWED_HOSTS=127.0.0.1:27018`, then run `sbt 'IntegrationTest/testOnly *AtlasHiringSearchIntegrationSpec'`. This is correctness evidence only: local ANN recall, latency and index size are not Atlas figures, and it does not replace the disposable-Atlas gates above.

Recorded run (October 8, 2026, Atlas Local reporting MongoDB 8.3.11): a direct `$search`, `$vectorSearch` (ANN with filter and exact), `$rankFusion` and `$scoreFusion` probe executed against READY indexes. `AtlasHiringSearchIntegrationSpec` passed 3/4 (consent truth table, `MongoRankFusion` and `MongoScoreFusion` capability gates); the native `$rerank` gate failed with its explicit BLOCKED assertion because the local image does not provide it. Both Atlas runners initially terminated with a `StackOverflowError` because they passed `mongo4cats.bson.Document.fromJava(...)` to `runCommand`; they now use `MongoAccessEvaluationSupport.command` and complete locally. `HiringSearchEvaluationAtlasRunner` wrote its three curated capture files (semantic quality and privacy acceptance remain unobserved), and `SearchEvaluationAtlasRunner` completed a 500-document, 20-query synthetic ANN/ENN run (20/20 successful, no errors; the recorded latency and recall figures are local-container values, not Atlas results).

### Criterion evidence on Atlas Local

With the same three environment variables, the `.L` gates are:

```bash
sbt 'IntegrationTest/testOnly *AtlasHiringSearchIntegrationSpec *AtlasHiringSearchBoundsIntegrationSpec *AtlasSearchIndexSetupIntegrationSpec *AtlasLexicalQueryFacetsIntegrationSpec *AtlasDiscoveryIntegrationSpec'
sbt 'IntegrationTest/runMain com.example.graphQL.cats.repository.mongo.HiringSearchEvaluationAtlasRunner --authorize-disposable-atlas --output .local/data/search-evaluation --source-revision REV --concurrency 1'   # and 8
sbt 'IntegrationTest/runMain com.example.graphQL.cats.repository.mongo.SearchEvaluationAtlasRunner --database search_evaluation_sweep --output .local/data/search-evaluation --source-revision REV --concurrency 1 --temperature cold'   # and warm, 1 and 8
```

Each runner also writes a `criterion-evidence-*.json` record beside its report (not read by `SearchEvaluationRecordTable`): `criterion-evidence-search-<concurrency>-<temperature>-<ts>.json` names MVR-07.L, HSE-06.L and MVR-08.L with per-bucket Recall@K against exact search, ANN p50/p95/p99, errors, throughput and the `local-container` label; `criterion-evidence-curated-<concurrency>-<ts>.json` names SEB-02.L and SEB-04.L, the capture files and the fixture tags. For MVR-08.L run the baseline first, then each sweep point with `--baseline <baseline criterion-evidence-search file>` (same concurrency and temperature, same seed, corpus and page size; only `--num-candidates` or `--quantization` differs; the rule compares branch limit, vector storage, stored source, page size, documents, queries, dimensions and seed and retains current settings if any differs or if both swept settings differ). The record then states `retain current settings` or `candidate for cloud confirmation` under the decision-4 limits; the setting counts as a candidate only when the records for concurrency 1 and 8, cold and warm, all say so. No change is adopted from `.L` evidence. `AtlasDiscoveryIntegrationSpec` (HDQ-01.L, 04.L, 05.L, 06.L, 11.L) also writes `facets-index-lag.json` (HDQ-05.L, lag observed or not) and `criterion-evidence-discovery-<ts>.json` (HDQ-11.L paired `$geoNear` versus `$geoWithin` plans, counts and p95, outcome `retain current` or `candidate for cloud confirmation`); both carry the `local-container` label.

## Tabulating recorded runs

`sbt 'Test / runMain com.example.graphQL.cats.service.search.SearchEvaluationRecordTable --input .local/data/search-evaluation'` reads the recorded `curated-*.json` and `search-evaluation-*.json` files without opening any client and prints one row per run: strategy, deployment label, outcome counts, recall@K, NDCG@K, recall against exact search (the ANN-versus-exact drift) and latency percentiles. Runners label loopback deployments `local-container`; the table repeats that label and states that such rows are correctness evidence, not Atlas figures. Records written before 2026-10-08 labelled local runs as Atlas and should be regenerated.

## Current evidence

`SearchEvaluationMetrics` implements Recall@K and NDCG@K for offline run analysis. The recorded evidence contains no authorized live Atlas benchmark or Automated Embedding run. It establishes no live latency, recall, throughput, resource, or cost result; verify environment availability before any newly authorized benchmark.

## Production hiring retrieval gate

The opt-in `AtlasHiringSearchIntegrationSpec` uses a fresh `search_evaluation_hiring_` nonce database and fabricated hiring vectors. It exercises the production vector and application RRF branches, consent true/false/absent, missing private fields, inactive accounts, model/skill predicates, bounded eligibility projections and production service validation. Separate native rank/score fusion and reranking tests are capability gates: unsupported stages fail their gate rather than selecting a fallback. Setup verifies definitions and readiness for all four job/candidate vector/lexical indexes.

```bash
sbt 'IntegrationTest / testOnly com.example.graphQL.cats.repository.mongo.AtlasHiringSearchIntegrationSpec'
```

Configure `ATLAS_TEST_URI` securely before this explicit gate. Its absence skips the live tests and remains blocked acceptance, even when the ordinary local integration suite succeeds. Each test drops only its own nonce database, including on failure or cancellation. Initial ingestion polling is separate from the synchronized post-retrieval Mongo closure check; that check does not prove observed asynchronous Atlas index lag. A separate 30-second bounded observation records whether a stale hit and its later removal were observed in `.local/data/search-evaluation/closure-lag-*.json`; immediate removal or timeout leaves that lag gate open. Actual native execution and index-lag observation remain open until actual supported deployment runs supply evidence.

The [first iteration latency plan](specs/mongodb-vector-retrieval-optimization.md#first-iteration-latency-plan) defines concurrency 1/8, `k=7`, 20 paired queries, first-pass/warmup reporting and telemetry limits. Numerical optimization targets will be proposed from the baseline and agreed before adoption.
