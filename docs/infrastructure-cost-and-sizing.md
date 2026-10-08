# Infrastructure cost and sizing

Decision-support estimates for hosting search (MongoDB Atlas Search and Vector Search, embeddings) and analytics (Kafka, Spark, Delta Lake). Research date: 2026-10-08.

This note authorizes nothing. It provisions no Atlas, EC2, Kafka or EMR resource and changes no code. Provisioning, paid benchmarks and provider changes remain separately scoped tasks (see [MongoDB design](mongodb-design.md#planned-vector-search-optimization), [search evaluation](search-evaluation.md), [development milestones](development-milestones.md)). All figures are list-price estimates; billing telemetry is the only source of observed cost.

## How to read this document

- **Verified** prices come from the vendor page named in the sources, fetched on the research date.
- **Aggregator** prices come from third-party tables that were not cross-checked against the vendor page. Confirm them before budgeting.
- **Assumption** marks a workload or sizing figure chosen here, not measured.
- Region is AWS us-east-1 unless stated. Month = 730 hours.
- No query plan, latency, recall or throughput was measured for this note. Savings from index or file-layout changes are directions to measure, not results.

## Workload assumptions

| Item | Assumption |
|---|---|
| Searchable corpus | 60,000 documents (50,000 candidate profiles, 10,000 jobs), about 500 tokens each = 30M tokens one time |
| Queries | 100,000 a month, about 30 tokens each = 3M tokens |
| Vector index | 1024 dimensions, float32 (about 240 MB raw vectors) |
| Kafka | 1M events a month, about 1 KB each = about 1 GB a month; 7-day retention; 64 KiB maximum record |
| Delta | 3–5 GB across Bronze, Silver, Gold and quarantine |
| Local acceptance workload | 100,000 records (already recorded in the analytics specification) |

Re-run the tables with measured figures before any deployment decision.

## Current decisions checked

| Area | Current decision (source) | Cost or fault-tolerance effect |
|---|---|---|
| Embedding provider | App-side Voyage, `voyage-4-lite`, 1024 dimensions (`application.conf`, `vector-search.voyage`) | Cheapest Voyage tier at $0.02 per 1M tokens. Provider is swappable. |
| Vector search | Disabled by default (`vector-search.enabled = false`) | No search or embedding spend until enabled. |
| Embedding workflow | Durable queue, leases, retries, model and source-hash provenance | Provider outages do not lose work. |
| Fusion and rerank | `applicationRrf`; rerank disabled | No paid rerank or Atlas-native fusion dependency. |
| Unavailable search | Fails closed with `VECTOR_SEARCH_UNAVAILABLE` | Safe; no keyword fallback if `mongot` is down. |
| Local search | `compose.atlas-local.yaml` (`mongodb-atlas-local`, includes `mongot`) | $0; correctness only. |
| Kafka | KRaft, 1 broker, RF=1, `min.insync.replicas=1`, 1 partition per topic, 7-day retention, transactional producers (`compose.yaml`) | Cheapest shape. Broker loss would lose data; confirm each topic is replayable from a Mongo outbox before relying on this. |
| Spark and Delta | Spark 4.0.1, Delta 4.0.0, Scala 3.7.4 with 2.13 artifacts, Java 17 (`analytics/build.sbt`); local one-shot batch over an explicit offset range; opt-in streaming with a 10 s trigger | Batch is idempotent via run manifest and mutex, so it suits spot or serverless compute. Streaming is always-on cost. |
| Delta properties | `dataSkippingNumIndexedCols`, log and deleted-file retention, `checkpointInterval=100`; Bronze column statistics disabled for privacy | Sensible. No scheduled compaction or VACUUM was found by search; confirm in source. |

The pre-MVP policy (one active schema, no migration code) makes dimension or quantization changes cheap now: the cost is re-embedding.

## Search: price list

### MongoDB Atlas (verified, mongodb.com/pricing, updated 2026-09-29)

| Tier | Price | Notes |
|---|---|---|
| Free (M0) | $0 | 512 MB, shared; 100 ops/s |
| Flex | $8–30 a month by ops/s | Up to 5 GB, shared |
| M10 / M20 / M30 (AWS) | $0.08 / $0.20 / $0.54 per hour (about $58 / $146 / $394 a month) | M10: 2 GB RAM, 2 vCPU; M20: 4 GB; M30: 8 GB. M10 and M20 are burstable. The hourly price appears to cover the replica set; one third-party guide claims 3× — confirm in the Atlas calculator. |
| Search Nodes, AWS high CPU | S20 $0.12/hr (4 GB), S30 $0.24/hr (8 GB) per node | At least two nodes assumed; confirm. Billed in full hours. |

Storage beyond the default, backups and data egress are billed separately.

### Embeddings (verified for Atlas and Voyage; aggregator for others)

| Option | Price per 1M tokens | Free allowance |
|---|---|---|
| Atlas Automated Embedding: voyage-4-lite / voyage-4 / voyage-4-large | $0.02 / $0.06 / $0.12 | 200M tokens per model, one time, shared across the organization |
| Voyage API: voyage-3.5 / voyage-3.5-lite | $0.06 / $0.02 | 200M tokens per account |
| OpenAI text-embedding-3-small | $0.02 (aggregator) | none |
| Cohere Embed v4 | about $0.12 (aggregator) | none |

Embedding token cost for the assumed corpus on voyage-4-lite is about $0.60 to embed once, inside the free allowance. Even a 1M-document corpus (500M tokens) costs under $100 on any option above.

## Search: monthly totals

| Scenario | Atlas | Embeddings | Total |
|---|---|---|---|
| Thesis or development | M0 or Flex, $0–30 | $0 (free tokens) | **$0–30** |
| Small production, search on the cluster | M10 ≈ $58 | ≈ $1–2 | **≈ $60** |
| Production, dedicated search | M30 $394 + 2×S20 $175 | ≈ $1–2 | **≈ $570** |
| Same with 2×S30 | M30 $394 + 2×S30 $350 | ≈ $1–2 | **≈ $745** |

Hosting dominates; tokens are a rounding error.

## Search: alternatives compared

| # | Option | Cost | Pros | Cons |
|---|---|---|---|---|
| 1 | Keep app-side Voyage embeddings (current) | tokens only | Full control of provenance, consent and deletion; swappable provider; durable queue exists | You operate the worker and API key |
| 2 | Atlas Automated Embedding (`autoEmbed`) | same token rates | No worker or provider client; query text embedded server-side | **Preview** — docs advise against production use; Voyage models only; 32k-token context; hides model, hash and freshness state; duplicates the existing pipeline; candidate text goes to Atlas-hosted models; storage auto-scaling must be on (disk full pauses embedding) |
| 3 | Cheaper model (OpenAI small, voyage-4-lite) | saves cents | Lowest per-token | Changes vector space and dimension contract; needs re-embedding and re-evaluation |
| 4 | Shared search on the cluster | $0 extra | Cheapest | Competes with transactions for RAM and CPU; M10 and M20 can throttle |
| 5 | Dedicated Search Nodes | +$175–350 | Isolation; independent scaling | Roughly doubles a small bill |
| 6 | Self-hosted `mongod` + `mongot` on EC2 | see below | Large RAM for the money; no Atlas lock-in | You run patching, backups, upgrades, TLS and recovery; Community `mongot` requires MongoDB 8.2+ and Automated Embedding is Preview |
| 7 | Non-MongoDB engines (Qdrant, pgvector, OpenSearch) | $20–60 on a VPS | Cheap | Loses Atlas Search and `$vectorSearch`; rewrite of retrieval adapters and fusion |

### Self-hosting on EC2 (aggregator prices)

| Instance | $/hr | $/month |
|---|---|---|
| t3.large (2 vCPU, 8 GiB) | 0.0832 | 60.74 |
| t3.xlarge (4 vCPU, 16 GiB) | 0.1664 | 121.47 |
| m7g.large (Graviton, 8 GiB) | 0.0816 | 59.57 |
| r6g.large (Graviton, 16 GiB) | 0.1008 | 73.58 |

EBS gp3 is $0.08 per GB-month; snapshots $0.05 per GB-month. A one-year savings plan lowers t3.large to about $0.06/hr (≈ $44 a month).

| Setup | Atlas | Self-hosted | Difference |
|---|---|---|---|
| Small | M10 ≈ $58 (2 GB RAM, 3 nodes) | one t3.large + 50 GB gp3 ≈ $65 (8 GB RAM, one node) | ≈ $0, four times the RAM, no failover |
| Small, committed | n/a | ≈ $48 | ≈ $10 saved |
| HA production | M30 + 2×S20 ≈ $570 | 3 database nodes ≈ $180 + 2 `mongot` hosts ≈ $120 + EBS ≈ $30 ≈ $330 | ≈ $240 (about 40%) |
| Single node | $570 | one m7g.xlarge-class host ≈ $130–150 | ≈ $420, no failover |

Self-hosting saves little against an M10 and a meaningful amount only against M30 plus Search Nodes. A few hours of operator time a month offsets the small-scale saving.

### When Automated Embedding becomes worthwhile

- It reaches general availability and the docs describe regeneration on a model change.
- Several new embedded fields or document types make the custom pipeline cost more than it saves.
- A recall and latency comparison against the current setup, using the [search evaluation](search-evaluation.md) harness, shows no regression.
- Security and legal approve sending candidate text to Atlas-hosted models.
- Cheap test: an M0 cluster with a payment method (lifts the 3 requests-per-minute query limit) and voyage-4 on fabricated data, inside the free tokens.

### Search cost levers, ranked

1. **Smaller vectors.** Test 512 dimensions and scalar or binary quantization. Index storage and RAM could fall by 2× to 4× or more, possibly avoiding dedicated Search Nodes. Requires Recall@K against the 1024-dimension float baseline. Whether `voyage-4-lite` supports 512 dimensions is unconfirmed.
2. Host `mongot` yourself (option 6).
3. Defer dedicated Search Nodes until measured contention.
4. Keep Voyage from the application (option 1).
5. Use Flex or M0 for demos after checking their search-index limits (unverified against your four indexes).
6. Pause or stop non-production capacity; savings plan for always-on hosts.
7. Leave rerank and Atlas-native fusion off.

### Search fault-tolerance tiers

| Level | Setup | Approx. monthly | Failure behavior |
|---|---|---|---|
| Cheapest | one EC2 with `mongod` + `mongot`, EBS snapshots | $50–65 | Single point of failure; restore from snapshot |
| Middle | Atlas M10, shared search | ≈ $58 + backup | Database fails over; search shares RAM |
| Stronger | 3 EC2 database nodes + 1–2 `mongot` hosts | $250–330 | Survives one node loss |
| Managed | Atlas M30 + 2×S20 | ≈ $570 | Managed HA; most expensive |

A keyword-only fallback would keep search usable during a `mongot` outage more cheaply than more replicas, but it is a code change and out of scope here.

## Analytics: price list

### Kafka (verified or aggregator as marked)

| Option | Price | Source quality |
|---|---|---|
| Self-hosted single KRaft broker on EC2 | host cost (a t3.medium-class host ≈ $30, t3.large ≈ $61; t3.medium is an unsearched recollection) | AWS list |
| MSK provisioned, kafka.t3.small | $0.0456/hr per broker; 3 brokers ≈ $100 + $0.10 per GB-month | aggregator matches AWS example |
| MSK provisioned, kafka.m7g.large | $0.204/hr per broker | aggregator |
| MSK Serverless | $0.75 per cluster-hour (≈ $547), $0.0015 per partition-hour, $0.10 per GiB in, $0.05 per GiB out, $0.10 per GiB-month | aggregator |
| Confluent Cloud Basic | first eCKU free, then $0.14 per eCKU-hour; $0.05 per GB in/out; $0.08 per GB-month | vendor page (via search) |
| Redpanda Serverless | about $0.10 per GB in, out and retained; $0.003 per partition-hour (an older blog quotes lower rates; the two conflict) | vendor FAQ vs blog |
| Aiven Startup | $200 a month, 3 VMs, 90 GB, networking included. The free plan (250 KiB/s, 3-day retention) does not meet your 7-day retention. | vendor page |

### Spark compute and storage

| Item | Price | Source quality |
|---|---|---|
| EMR Serverless | $0.052624 per vCPU-hour + $0.0057785 per GB-hour (AWS example, region unnamed); 1-minute minimum | AWS page, example |
| EMR on EKS uplift | $0.01012 per vCPU-hour + $0.00111125 per GB-hour; EKS $0.10 per cluster-hour | AWS page, example |
| Databricks Jobs Compute | DBU rate on top of VM cost; reported $0.07–0.30 per DBU depending on cloud, tier and date | aggregator, conflicting |
| S3 Standard | $0.023 per GB-month; PUT $0.005 per 1,000; GET $0.0004 per 1,000 | aggregator |

The sizing used below is a 6 vCPU, 24 GB job, about $0.45 an hour on EMR Serverless.

## Analytics: monthly totals

| Option | Kafka | Spark | Lakehouse | Total |
|---|---|---|---|---|
| A. Self-hosted | single broker on EC2 ≈ $30–61 | local-mode batch on a t3.xlarge, 1 hr a day ≈ $5 | S3 ≈ $0.12 | **≈ $35–70** |
| B. Pay-per-use managed | Confluent Basic ≈ $0–5 | EMR Serverless, daily batch ≈ $2–3 | ≈ $0.12 | **≈ $5–10** |
| C. AWS HA | MSK 3×t3.small ≈ $106 | EMR Serverless ≈ $3–11 | ≈ $0.12 | **≈ $110–120** |
| D. MSK Serverless | ≈ $547 + partitions | as above | as above | **≈ $550+** |
| E. Aiven Startup | $200 | as above | as above | **≈ $205+** |
| F. Redpanda Serverless | ≈ $7–10 | as above | as above | **≈ $10–15** |

Notes: MSK t3.small is a development size. Option B needs a check of ACL and transactional-ID support on Confluent Basic, and of the EMR Spark 4 runtime.

### Streaming versus scheduled batch

| Mode | Commits a month | S3 PUTs (about 4 per commit) | Spark compute (EMR Serverless) |
|---|---|---|---|
| 10 s trigger, always on | ≈ 259,200 | ≈ 1M ≈ $5 | ≈ $330 (24/7) |
| `AvailableNow` every 15 min | ≈ 2,880 | ≈ 11,500 ≈ $0.06 | ≈ $44 (2 min per run) |
| `AvailableNow` hourly | ≈ 720 | ≈ 2,900 | ≈ $11 |
| `AvailableNow` daily | 30 | ≈ 120 | ≈ $2–3 (10 min per run) |

`AGENTS.md` prefers batch when freshness permits. Forum reports say `maxOffsetsPerTrigger` may not be honored under `AvailableNow`; test it on Spark 4.0.1 before relying on it for cost control.

## Analytics: native optimizations

### Delta Lake (open source, per Delta documentation)

| Feature | Since | Setting | Note |
|---|---|---|---|
| `OPTIMIZE` compaction | 1.2 | `OPTIMIZE table [WHERE ...]` | Fixes small files; idempotent |
| Auto compaction | 3.1 | `delta.autoOptimize.autoCompact` | Runs after writes; needs a minimum file count |
| Optimized write | 3.1 | `delta.autoOptimize.optimizeWrite` | Off by default; adds a shuffle, can raise write latency |
| Z-order | 2.0 | `OPTIMIZE ... ZORDER BY` | Not idempotent; weakens with more columns |
| Data skipping | 1.2 | `delta.dataSkippingNumIndexedCols` | Already used; unavailable on Bronze because statistics are disabled for privacy |
| Log compaction | 3.0 | `.compact.json` files | Reduces checkpoint overhead |
| VACUUM | — | retention settings | Reclaims storage; retention also gates the erasure proof |
| Deletion vectors | — | table feature | Avoids file rewrites on delete, but rows stay in files until a purge rewrite; privacy review required given the physical-erasure design |
| Liquid clustering | reportedly 4.0 | — | Not confirmed from the release notes |

Do not partition tables at this size; the common guideline is to partition only around 1 TB or more.

### Spark

1. Local or small-cluster mode; distributed processing needs a documented workload benefit (`AGENTS.md`).
2. Adaptive query execution (on by default since 3.2): coalesces shuffle partitions, handles skew, converts joins to broadcast. Set `spark.sql.shuffle.partitions` low rather than 200.
3. Spot or Graviton instances; explicit offset ranges plus the run manifest make an interrupted batch safe to retry.
4. Zstd Parquet compression usually saves storage (assumed, not cited).

### Kafka

1. Keep partitions minimal; serverless products bill per partition-hour.
2. Producer batching (`linger.ms`, `batch.size`) and compression (lz4 or zstd); whether a vendor bills compressed or uncompressed bytes varies.
3. Retention of 7 days is also the erasure wait; shorter retention erases sooner but shortens replay.
4. Tiered storage is not worth it at this volume.
5. RF=3 with `min.insync.replicas=2` only when broker fault tolerance is required; cost rises to about $105+.

## Spark 4 status

The analytics build already uses Spark 4.0.1 and Delta 4.0.0 on Java 17. Open constraints:

- Spark 4.0's Scala 2 reflection encoders misread Scala 3 case classes (open as AC-RV-10); keep using DataFrames with explicit schemas.
- The analytics driver cannot move to Scala 3.9 until Spark's runtime allows it; the main application stays on 3.9.
- Spark and Delta versions move together; check the Delta compatibility table before any Spark upgrade.
- Deployment: whether EMR Serverless offers a Spark 4.0 runtime was not confirmed (a custom image is the usual route); check the Databricks runtime's Spark and Delta versions before using it; do not enable Delta table features that other engines on the table cannot read.

## Recommendations

1. Stay on `compose.atlas-local.yaml` and local Kafka and Spark until a deployment is scoped.
2. Move analytics to scheduled `AvailableNow` batches. This is the largest saving and needs no vendor change.
3. Test 512 dimensions and quantization with the evaluation harness before sizing search hosting.
4. For a first deployment, a single self-hosted EC2 for `mongod` + `mongot` (about $60) plus option A or B for analytics (about $5–70) is the cheapest working shape. Do not describe it as highly available.
5. Add Search Nodes, RF=3 Kafka or a managed Atlas tier only on measured need.
6. Revisit Automated Embedding after it leaves Preview.

## Open items and unverified claims

- Whether the Atlas hourly price covers the whole replica set; whether Search Nodes need at least two nodes.
- Search-index limits on M0 and Flex against your four indexes.
- Whether `voyage-4-lite` supports 512 dimensions.
- Whether every Kafka topic is replayable from a Mongo outbox.
- ACL, SASL and transactional-ID support on Confluent Basic and Redpanda.
- EMR Spark 4 runtime availability; Delta safe concurrent writes on S3 for the chosen Delta version.
- Delta 4.0 liquid clustering details; whether `AvailableNow` honors `maxOffsetsPerTrigger`.
- Databricks cost per job (DBU consumption per instance was not found).
- All aggregator prices above.
- Any latency, recall, throughput or cost claim: none is measured here. The recorded local Bronze p95 failure (40,085 ms against a 30,000 ms limit) is unresolved and tracked for the final phase.

## Sources

- MongoDB pricing — https://www.mongodb.com/pricing.md
- Atlas Automated Embedding — https://www.mongodb.com/docs/vector-search/crud-embeddings/automated-embedding/ and https://www.mongodb.com/docs/vector-search/crud-embeddings/automated-embedding/models/
- Voyage AI pricing — https://docs.voyageai.com/pricing
- MongoDB Embedding and Reranking API — https://www.mongodb.com/products/updates/now-in-public-preview-embedding-and-reranking-api-on-mongodb-atlas
- OpenAI text-embedding-3-small — https://developers.openai.com/api/docs/models/text-embedding-3-small
- Cohere Embed v4 listing — https://cloudprice.net/models/cohere-embed-v4-0
- EC2 prices — https://www.doit.com/compute/spot/us-east-1/t3.large, https://www.doit.com/compute/spot/us-east-1/t3.xlarge, https://calculator.holori.com/aws/ec2/m7g.large, https://calculator.holori.com/aws/ec2/r6g.large/us-east-1
- EBS pricing — https://cloudchipr.com/blog/aws-ebs-pricing
- Amazon MSK — https://aws.amazon.com/msk/pricing/, https://www.automq.com/blog/understanding-aws-msk-pricing, https://www.automq.com/blog/msk-serverless-pricing-costs-limits-and-alternatives
- Confluent Cloud — https://confluent.io/confluent-cloud/pricing
- Redpanda Serverless — https://redpanda.com/redpanda-cloud/serverless
- Aiven Kafka — https://aiven.io/pricing/kafka
- Amazon EMR — https://aws.amazon.com/emr/pricing/
- Databricks pricing — https://www.doit.com/blog/databricks-pricing-explained-dbus-tiers-cost-control
- Delta Lake optimizations — https://docs.delta.io/latest/optimizations-oss.html
- Delta Lake 4.0 — https://delta.io/blog/2025-09-25-delta-lake-40/
- Spark AQE — https://docs.aws.amazon.com/prescriptive-guidance/latest/spark-tuning-glue-emr/using-adaptive-query-execution.html
- Amazon S3 pricing — https://www.cloudforecast.io/amazon-s3-pricing-and-optimization-guide/
