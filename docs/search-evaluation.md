# Search evaluation and embedding experiments

This procedure separates relevance, retrieval quality, latency, and provider cost. A passing unit test for the metric functions does not establish a live Atlas performance result.

The [current roadmap](development-milestones.md) places MongoDB/vector optimization in Phase 8, search evaluation and embedding architecture in Phase 9, and AI discovery in Phase 10. Existing metrics and the disposable runner provide a baseline; they do not complete those phases.

The [search evaluation and embedding specification](specs/hiring-search-enhancements.md#search-evaluation-and-embedding-architecture) owns the detailed target contracts and acceptance criteria. It preserves HSE-06/HSE-07 and separates deterministic synthetic retrieval checks from human-authored relevance judgments over fabricated hiring data. The commands below describe the existing runner; they do not establish implementation of that later evaluation stage or authorize Atlas/provider spending. [MongoDB/vector optimization](specs/mongodb-vector-retrieval-optimization.md) owns access-plan and ANN tuning; [discovery](specs/hiring-discovery-search-quality.md) owns adoption of evaluated product changes.

## Bounded retrieval comparison

Use an authorized disposable Atlas deployment configured through `ATLAS_TEST_URI` and a database named with the `search_evaluation_` prefix. `SearchEvaluationAtlasRunner` creates a uniquely named disposable collection, seeds up to 10,000 deterministic synthetic vectors in batches, builds a vector index, checks readiness, runs paired ANN/ENN queries with category filters at concurrency 1 or 8, writes a JSON run record, and drops only the collection it successfully created. It does not read production records or use an embedding provider. Synthetic judged relevance is generated independently from ENN using a second deterministic topic label; these judgments test retrieval behavior, not human semantic relevance.

Run it only against a disposable Atlas database, once for each concurrency/temperature combination you want to compare:

```bash
sbt 'IntegrationTest / runMain com.example.graphQL.cats.repository.mongo.SearchEvaluationAtlasRunner --source-revision <working-tree-id> --database search_evaluation_benchmark --output .local/data/search-evaluation --documents 10000 --queries 100 --dimensions 1024 --num-candidates 100 --page-size 20 --concurrency 8 --temperature cold --seed 20260923'
```

Use `--concurrency 1` and `--temperature warm` for the other runs. `ATLAS_TEST_URI` must point to a disposable test deployment; the runner does not print the URI. The output includes Atlas version, workload, collection index size when `collStats` permits it, paired ANN/ENN latency and errors, ANN-vs-ENN recall, independent synthetic judged ranking metrics, and explicit nulls where Atlas Vector Search index bytes or CPU/memory/query telemetry are not available to the driver. `collectionIndexBytes` is not Atlas Vector Search index size. Add vector-index size and Atlas CPU/memory/query metrics from the Atlas metrics export before comparing resource costs. The recorded local evidence covers compilation; it does not include execution against Atlas.

Compare the configured application-side RRF baseline with the selected experimental retrieval strategy. For ANN ground truth, run the same query/filter set with ENN. Record dataset and index sizes, Atlas/server versions, embedding model and dimensions, indexes and quantization, filters, `numCandidates`, page size, query count, concurrency (1 and 8), warmup, warm/cold run, duration, and timestamp. Capture Recall@K against ENN, NDCG@K against the judged IDs, throughput, errors, p50/p95/p99 latency, CPU/memory, provider request counts, and Atlas query/index metrics. Billing telemetry is the source for observed costs; calculations from request counts or storage are estimates and must be labeled as such.

Write one JSON run record per strategy containing those inputs and measurements. Keep raw query text synthetic. Do not claim a performance improvement without paired results under the same environment and workload. Use measured relevance and resource tradeoffs to decide whether an experimental fusion mode should be enabled.

The evaluation core validates typed corpus/run observations and produces an immutable numerical report; the infrastructure JSON adapter renders it. Ranking failures and exact-reference failures are separate. Recall/NDCG quality means cover successful rankings only, with explicit denominators and attempted/success/failed/unavailable counts. Fidelity requires both retrieval and exact-reference success and has its own denominator. Successful empty eligible sets or queries without positive judgments retain the zero metric convention and are counted explicitly. Missing telemetry includes a reason; absent retrieval latency or billing is never inferred from fixture replay.

## Local fixture preparation

The approved local workload contains eight fabricated jobs, eight candidate summaries and twelve queries (four per use case), equally split between tuning and held-out sets. The binary rubric uses intended role and required skills. The rubric is user approved, but generated per-query labels are provisional until domain review; approving the rubric does not make these labels human reviewed. Freeze corpus/judgment digests and the split before any future tuning.

Offline replay supplies authored vector/lexical branch rankings to the actual application RRF functions and the same typed evaluator/renderer used by the Atlas adapter. It checks reproducibility, numerical/report behavior and existing ranking ties. It does not execute lexical/vector retrieval, measure live search latency or establish human relevance. Recommendations remain vector-only; lexical/hybrid recommendation combinations are reported unavailable. No provider calls or paid requests are made. Reports belong under ignored `.local/data/search-evaluation/` and logs under `.local/logs/`.

Run from the repository root with Java 17+ and an explicit label for the working tree being measured:

```bash
sbt 'Test / runMain com.example.graphQL.cats.service.search.SearchEvaluationFixtureReplay --source-revision <working-tree-id>'
sbt 'Test / runMain com.example.graphQL.cats.service.search.SearchEvaluationFixtureReplay --source-revision <working-tree-id> --concurrency 8'
```

Both entrypoints separately fingerprint the actual Scala source tree, including fixtures. The caller's revision label and fingerprint are distinct: a Git HEAD alone cannot identify uncommitted changes. The replay defaults to K=7 and concurrency 1; its bounded concurrency-8 run and fixture tests check deterministic execution. Each replay writes a separate run directory to preserve earlier artifacts.

Quality averages alone cannot authorize adoption. The separate reliability threshold, relevance/latency/cost conditions, reviewed held-out labels and paired live evidence remain pending. Eligibility or privacy violations block acceptance irrespective of relevance scores or reliability averages.

## Automated Embedding comparison

The application continues to write Voyage embeddings through its durable work queue. Automated Embedding is only a separate Atlas preview experiment: create a disposable collection populated with synthetic documents, configure an isolated AutoEmbed index and query path, then measure indexing and query latency and provider/billing telemetry against the manual-embedding baseline. Record the Atlas preview status and version. Delete only this disposable collection and index after recording results. Never dual-write application collections or move the application read path as part of this experiment.

## Current evidence

`SearchEvaluationMetrics` implements Recall@K and NDCG@K for offline run analysis. The recorded evidence contains no authorized live Atlas benchmark or Automated Embedding run. It establishes no live latency, recall, throughput, resource, or cost result; verify environment availability before any newly authorized benchmark.

## Production hiring retrieval gate

The opt-in `AtlasHiringSearchIntegrationSpec` uses a fresh `search_evaluation_hiring_` nonce database and fabricated hiring vectors. It exercises the production vector and application RRF branches, consent true/false/absent, missing private fields, inactive accounts, model/skill predicates, bounded eligibility projections and production service validation. Separate native rank/score fusion and reranking tests are capability gates: unsupported stages fail their gate rather than selecting a fallback. Setup verifies definitions and readiness for all four job/candidate vector/lexical indexes.

```bash
sbt 'IntegrationTest / testOnly com.example.graphQL.cats.repository.mongo.AtlasHiringSearchIntegrationSpec'
```

Configure `ATLAS_TEST_URI` securely before this explicit gate. Its absence skips the live tests and remains blocked acceptance, even when the ordinary local integration suite succeeds. Each test drops only its own nonce database, including on failure or cancellation. Initial ingestion polling is separate from the synchronized post-retrieval Mongo closure check; that check does not prove observed asynchronous Atlas index lag. A separate 30-second bounded observation records whether a stale hit and its later removal were observed in `.local/data/search-evaluation/closure-lag-*.json`; immediate removal or timeout leaves that lag gate open. Actual native execution and index-lag observation remain open until actual supported deployment runs supply evidence.

The [first iteration latency plan](specs/mongodb-vector-retrieval-optimization.md#first-iteration-latency-plan) defines concurrency 1/8, `k=7`, 20 paired queries, first-pass/warmup reporting and telemetry limits. Numerical optimization targets will be proposed from the baseline and agreed before adoption.
