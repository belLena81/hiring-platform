# Search evaluation and embedding experiments

This procedure separates relevance, retrieval quality, latency, and provider cost. A passing unit test for the metric functions does not establish a live Atlas performance result.

## Bounded retrieval comparison

Use an authorized disposable Atlas deployment configured through `ATLAS_TEST_URI` and a database named with the `search_evaluation_` prefix. `SearchEvaluationAtlasRunner` creates a uniquely named disposable collection, seeds up to 10,000 deterministic synthetic vectors in batches, builds a vector index, checks readiness, runs paired ANN/ENN queries with category filters at concurrency 1 or 8, writes a JSON run record, and drops only the collection it successfully created. It does not read production records or use an embedding provider. Synthetic judged relevance is generated independently from ENN using a second deterministic topic label; these judgments test retrieval behavior, not human semantic relevance.

Run it only against a disposable Atlas database, once for each concurrency/temperature combination you want to compare:

```bash
sbt 'IntegrationTest / runMain com.example.graphQL.cats.repository.mongo.SearchEvaluationAtlasRunner --database search_evaluation_benchmark --output .local/data/search-evaluation --documents 10000 --queries 100 --dimensions 1024 --num-candidates 100 --page-size 20 --concurrency 8 --temperature cold --seed 20260923'
```

Use `--concurrency 1` and `--temperature warm` for the other runs. `ATLAS_TEST_URI` must point to a disposable test deployment; the runner does not print the URI. The output includes Atlas version, workload, collection index size when `collStats` permits it, paired ANN/ENN latency and errors, ANN-vs-ENN recall, independent synthetic judged ranking metrics, and explicit nulls where Atlas Vector Search index bytes or CPU/memory/query telemetry are not available to the driver. `collectionIndexBytes` is not Atlas Vector Search index size. Add vector-index size and Atlas CPU/memory/query metrics from the Atlas metrics export before comparing resource costs. The runner was compiled locally but not executed against Atlas in this task.

Compare the configured application-side RRF baseline with the selected experimental retrieval strategy. For ANN ground truth, run the same query/filter set with ENN. Record dataset and index sizes, Atlas/server versions, embedding model and dimensions, indexes and quantization, filters, `numCandidates`, page size, query count, concurrency (1 and 8), warmup, warm/cold run, duration, and timestamp. Capture Recall@K against ENN, NDCG@K against the judged IDs, throughput, errors, p50/p95/p99 latency, CPU/memory, provider request counts, and Atlas query/index metrics. Billing telemetry is the source for observed costs; calculations from request counts or storage are estimates and must be labeled as such.

Write one JSON run record per strategy containing those inputs and measurements. Keep raw query text synthetic. Do not claim a performance improvement without paired results under the same environment and workload. Use measured relevance and resource tradeoffs to decide whether an experimental fusion mode should be enabled.

`SearchEvaluationHarness.report` turns one captured paired run into the JSON record shape used for comparison. Supply, per query, ANN and ENN rankings, judged relevant IDs, both observed latencies, and any error; also supply workload and environment values captured by the Atlas driver. The report calculates mean Recall@K against ENN, mean NDCG@K against judgments, success/error counts, throughput, and separate p50/p95/p99 latency for ANN and ENN. It carries Atlas query metrics, collection/vector index sizes, CPU, memory, and provider request counts through unchanged so missing measurements remain explicit `null` values. The pure report builder does not connect to Atlas or invent environment measurements.

## Automated Embedding comparison

The application continues to write Voyage embeddings through its durable work queue. Automated Embedding is only a separate Atlas preview experiment: create a disposable collection populated with synthetic documents, configure an isolated AutoEmbed index and query path, then measure indexing and query latency and provider/billing telemetry against the manual-embedding baseline. Record the Atlas preview status and version. Delete only this disposable collection and index after recording results. Never dual-write application collections or move the application read path as part of this experiment.

## Current evidence

`SearchEvaluationMetrics` implements Recall@K and NDCG@K for offline run analysis. This repository has no authorized Atlas benchmark environment configured, so there are no live latency, recall, throughput, resource, or cost claims from this change. The isolated Automated Embedding procedure is not run here.
