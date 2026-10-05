# Hiring Management Platform

Backend-first hiring management platform built with **Scala 3, Sangria GraphQL, Cats Effect, and MongoDB**.

A practical playground for functional Scala, GraphQL API design, MongoDB data modeling/query optimization, and AI-powered semantic search & candidate/job matching via MongoDB Vector Search.

## Agent-driven development

Start with `$product-manager` to coordinate specialist work and independent reviews. See [agent workflow and usage](docs/agent-development.md) and [project rules](AGENTS.md). Local skills cover Product Manager, Software Architect, Scala Developer, Data Engineer, Big Data Engineer, QA Engineer, Security Engineer, and Code Reviewer.

The build uses Scala 3.9 LTS and Java 17+, with Cats Effect/FS2, Sangria/http4s Ember, Circe, MongoDB reactive driver, fs2-kafka, Logback, and MUnit/Testcontainers core. It serves the current hiring API with a resource-managed MongoDB client.

## Local build

`sbt run` forks a separate JVM so Cats Effect `IOApp` owns the application lifecycle. Start MongoDB locally, then run the application on `127.0.0.1:8080`:

```bash
docker compose up -d mongodb
sbt run
```

Event publication can also use the local Kafka broker:

```bash
docker compose up -d mongodb kafka
KAFKA_SASL_SECURITY_PROTOCOL=SASL_PLAINTEXT sbt run
```

The local Compose broker is published only on loopback and uses SASL without TLS. The application defaults to `SASL_SSL` whenever SASL credentials are configured; use the explicit `SASL_PLAINTEXT` override only for this trusted local broker. Remote brokers should use `SASL_SSL`.

Kafka publishes to `hiring.operational-events` with seven-day broker retention. MongoDB readiness and HTTP startup do not depend on Kafka availability; operational mutations write a transactional Mongo outbox first and the background publisher retries broker delivery.

### Local analytics batch

The analytics batch is an opt-in, one-shot Compose profile. It reads one explicit Kafka partition/offset range, writes local Delta data under the ignored `.local/data/analytics/` directory, and exits. It is not a streaming daemon and does not run with the default application stack.

The separate analytics build uses Scala 3.7.4 with Spark 4.0.1 and Delta 4.0.0 artifacts compiled for Scala 2.13. Its batch and erasure worker load typed settings from the packaged `application.conf` through PureConfig; environment values are read only as HOCON substitutions. The batch run ID and offset range are configuration inputs, not command-line arguments. Configuration overrides may use the standard `config.file` or `config.resource` selectors. Cats Effect `IO` and `Resource` own blocking work and Spark/Mongo lifetimes, while Cats `ValidatedNec` and Iron refinements validate independent settings before startup. The main application remains on Scala 3.9.

```bash
docker compose up -d mongodb kafka
ANALYTICS_RUN_ID=local-001 \
  HIRING_ANALYTICS_HMAC_SECRET_BASE64="[REDACTED_SECRET]" \
  ANALYTICS_PARTITION=0 \
  ANALYTICS_START_OFFSET=0 \
  ANALYTICS_END_OFFSET_EXCLUSIVE=100 \
  docker compose --profile analytics run --rm analytics-batch
```

The batch reads up to 100,000 pending account-erasure markers before it mutates Delta data. With active markers it purges marked Delta rows, rebuilds Gold, and leaves the report hidden until the erasure worker completes the full lifecycle.

Continuous Hiring Analytics implementation is underway with an opt-in `analytics-streaming` Compose profile and a fail-closed resource-managed query runtime. The runtime requires an explicitly selected immutable, time-bounded Mongo activation grant; it stops when that grant expires. Production activation requires Phase 6 real-horizon retention, writer exclusion, guarded HMAC retirement, audit, and independent signoff. The isolated synthetic test environment can use independently reviewed accelerated physical retention and explicitly simulated calendar boundaries. See [the Phase 7 specification](docs/specs/continuous-hiring-analytics.md). Default application startup never provisions a grant; an authorized isolated proof run explicitly provisions one immutable grant after independent readiness validation.

The streaming profile reads `.local/config/analytics-streaming.conf`, selects its grant with `activation-grant-id`, and requires explicit starting offsets for every Kafka partition. Its trigger defaults to 10 seconds. Start it only with a valid selected grant and the applicable independently reviewed Phase 6 evidence. Production requires genuine deployed horizons; test activation must bind unused nonce-scoped resources. A durable lakehouse registration keeps ordinary batch ingestion disabled after stream shutdown. The checked-in [sanitized configuration example](analytics/analytics-streaming.conf.example) intentionally uses a single example partition and must be adjusted to the broker's actual partition set.

The local proof launcher supports `diagnose` for optional bounded Spark cost measurements and `run` for the unchanged healthy-load acceptance workload. Both require a fresh staged nonce and independently reviewed activation evidence. `stage-maintenance` stages a separate test fixture with 60-second progress retention; its `maintenance` execution checks natural checkpoint/journal pruning and continuation, without claiming production elapsed retention or healthy-load freshness. Successful maintenance must occur within five minutes under the accepted local load; sanitized observations include deferrals and monotonic maximum age. Each new proof uses a small non-overlapping task network and preserves existing proof data.

### Local analytics erasure worker

Run the API at least once so MongoDB migrations and analytics control collections are initialized. Keep the API process running, then start the worker in another terminal:

```bash
docker compose up -d mongodb kafka
# In one terminal, start the API with `sbt run`.
docker compose --profile analytics-erasure up -d --build analytics-erasure-worker
docker compose logs -f analytics-erasure-worker
```

The worker shares `.local/data/analytics/` with batch runs. It requires the same HMAC secret and Kafka reader credentials as the batch, plus `KAFKA_FENCER_PASSWORD` in the ignored root `.env` for a principal scoped to the publisher transactional-ID prefix. `deleteMyAccount` commits the deletion and returns a durable receipt with `PENDING`; a verified token for the receipt owner can query `accountDeletionStatus(receiptId: "<receipt-id>")` for `PENDING`, `COMPLETE`, or `NOT_FOUND`, including while that account is tombstoned. The worker fences every publisher process recorded by the deletion transaction before it captures the Kafka barrier or purges outbox rows. It may then wait through Kafka retention and Delta's seven-day vacuum safety horizon before verifying snapshots, rebuilding the report, and atomically publishing the snapshot and completion ledger. A completed receipt remains queryable after the request marker expires. The current transactional publisher uses `hiring_publisher_v2` with `KAFKA_PUBLISHER_V2_PASSWORD`; the pre-transactional `publisher` identity has its topic, cluster-idempotence, and transactional-ID ACLs removed by ACL initialization. The regular local broker was cut over on 2026-09-25: its KRaft data was preserved in `.local/data/kafka`, its legacy PLAIN login was removed, and its stale ACL grants were revoked. The scoped regular-broker check verifies v2 publisher writes and fencing, legacy authentication rejection, and reader denials. The maintained live mutation-to-Compose-worker proof passes through `DeltaPurged/PENDING`; final deployed retention completion and guarded key retirement remain operational gates. See the [analytics specification](docs/specs/hiring-analytics-lakehouse.md).

Worker failures persist a fixed operational category and retry count; exhausted or invalid-state requests stay `PENDING` and require local repair. With the analytics HOCON and credentials configured, inspect them using `cd analytics && sbt 'runMain com.example.hiring.analytics.cli.AnalyticsErasureRepairMain inspect 50'`. After repairing the underlying cause, requeue one request with `sbt 'runMain com.example.hiring.analytics.cli.AnalyticsErasureRepairMain requeue <request-id> <observed-attempt-count>'`. The command refuses stale attempt counts or requests with a live lease and resumes the saved phase.

Run `scripts/run-analytics-erasure-smoke-proof.sh` to stage an isolated Compose flow check using synthetic events. The topic initially retains seed events for 15 minutes, then switches to one-minute retention after the post-barrier tail; Delta uses one-minute data and log retention. The script prints a nonce; after the next UTC day plus one minute, run `scripts/run-analytics-erasure-smoke-proof.sh resume <nonce>`. The resume stage restarts the worker, waits for the broker's actual earliest offsets, and checks report/receipt completion plus captured-file absence. Project-scoped named volumes persist between stages. Together with simulated-clock boundary tests, this accelerated physical proof supports local Phase 6 implementation closure. The seven-day Kafka and 7/30-day Delta proof, external-writer exclusion, guarded key retirement, and operational signoff remain required before production Phase 7 activation. The test-only `calendar-resume` command advances calendar boundaries after actual shortened Kafka and Delta physical retention; its evidence must remain labelled simulated. Current safety repairs and their verification are tracked in [Analytics erasure safety](docs/specs/analytics-erasure-safety.md).

`GET /health` reports application liveness; `GET /ready` reports MongoDB connectivity. `POST /graphql` accepts `{"query":"{ health { status } readiness { status } }"}`. MongoDB outages leave HTTP running and readiness reports `NOT_READY`. `GET /schema.graphql` exports the current schema; GraphQL introspection supports API documentation/testing clients. See the [API reference](docs/api.md).

See the [API reference](docs/api.md) and the [current reset specification](docs/specs/pre-mvp-contract-reset.md) for the active contract and verification evidence.

See [diagnostic logging](docs/logging.md) for searchable markers, request tracing, masked defaults, and explicitly gated local payload capture.

For project-only sbt JDK selection, create an ignored `.sbtopts` file in this root with `-java-home /path/to/jdk-17` (this machine: `/usr/lib/jvm/java-17-openjdk-amd64`). The installed sbt launcher reads it for commands such as `sbt compile` and `sbt test`; it does not change your shell's Java or other projects. Keep machine-specific paths out of Git. IntelliJ's JDK settings remain separate. The local-check script's Java prerequisite check still uses `JAVA_HOME`, so set that variable for the script as described below.

Use Java 17+ and sbt 1.11.1. Set `JAVA_HOME` to your installed JDK when the default Java is older, then run:

```bash
bash scripts/check-local.sh
```

The command validates local skills and runs the Docker-independent MUnit suite. Run `sbt 'IntegrationTest / test'` separately for real HTTP lifecycle and disposable MongoDB tests; Docker is required for the database tests. These commands are local checks, not deployment or performance certification.

## Test coverage

JaCoCo coverage is available for both SBT builds. Run `sbt jacoco` from the repository root for main-project unit coverage; run `sbt 'IntegrationTest / jacoco'` to run its integration tests with coverage and merge their results. Reports are written under `target/scala-<version>/jacoco/report/test/` and `.../report/it/`, with the merged report under `.../report/merged/`. Run the same commands from `analytics/` to generate analytics unit and integration coverage. Integration coverage requires the same Docker-backed services as the integration test suite. Coverage thresholds are not configured yet; reports provide measurement without imposing a baseline.

## Documentation

- [Architecture](ARCHITECTURE.md), [MongoDB design](docs/mongodb-design.md), and [use cases](docs/use-cases.md)
- [Agent workflow](docs/agent-development.md) and [project rules](AGENTS.md)
- [Spec-driven development](docs/spec-driven-development.md), [spec template](docs/templates/feature-spec.md), and [worked draft](docs/examples/submit-application-spec.md)
- [Pure FP and local quality checks](docs/engineering-quality.md) and [data/API/schema evolution](docs/schema-evolution.md)

## Goals

- Clean separation between domain, application, API, and infrastructure code (Clean Architecture / Ports & Adapters)
- Functional effect management with Cats Effect
- MongoDB data modeling optimized for GraphQL access patterns
- High read performance with minimal unnecessary data duplication
- Simple enough for an MVP, but swappable infrastructure components

## Planned Features

### Hiring
- Candidate, Recruiter, and Admin roles (RBAC)
- Job posting creation/management, filtering, search, cursor-based pagination
- Candidate application submission & tracking
- Recruiter application management
- Application lifecycle: `Created → Accepted → Interview → Hired / Rejected / Declined`
- Application status history & recruiter feedback
- GraphQL subscriptions for live application updates
- Admin statistics & hiring analytics

### GraphQL
- Sangria API with queries, mutations, subscriptions
- Typed inputs/enums, nested resolvers, cursor-based pagination
- Batched relationship loading (N+1 prevention)
- Typed domain/API errors, query depth & complexity limits

### MongoDB
Collections modeled around GraphQL access patterns, not relational normalization: `users`, `jobs`, `applications`, `application_events`.

- Compound & unique indexes, cursor/keyset pagination
- Aggregation pipelines, transactions
- Query analysis via `explain()` and index tuning

### AI & Vector Search
- Semantic job search combines the current vector and lexical retrieval paths
- Recruiter candidate matching supports natural-language and structured filters with explicit candidate consent for residence/availability filters
- Deterministic `matchedSkills` evidence is derived from candidate/job skill sets
- Candidate/job embeddings are generated asynchronously; MongoDB fusion, reranking, and Automated Embedding remain isolated opt-in experiments

## Tech Stack

| Area | Technology |
|---|---|
| Language | Scala 3 |
| Functional Effects | Cats Effect 3 |
| Streaming | FS2 |
| GraphQL | Sangria |
| HTTP | http4s |
| Database | MongoDB (+ Atlas Vector Search) |
| JSON | Circe |
| Auth | JWT |
| Testing | MUnit + Cats Effect, Testcontainers |
| Infra | Docker Compose |
| Logging | SLF4J + Logback |
| AI Integration | Embedding/LLM API behind service interfaces |

## High-Level Architecture

```text
                GraphQL API (Sangria + http4s)
                            │
                            ▼
                 Application Layer (Services)
                            │
                       Domain Ports
                            │
           ┌────────────────┼────────────────┐
           ▼                ▼                 ▼
     Repository Ports   Search Port     Embedding Port
           │                │                 │
           ▼                ▼                 ▼
              Infrastructure: MongoDB · Vector Search · LLM API
```

Dependencies point inward (`transport → service → domain`); the domain layer stays free of Sangria, MongoDB, http4s, Circe, JWT, and AI-SDK dependencies.

Current package layout: `api/` (GraphQL, HTTP, auth adapters) → `service/` (use-case protocols, outbound ports, and job/application/search use cases) → `domain/` (model, pagination, error, pure policy). `repository.mongo/` and `infrastructure/` contain adapters that implement service ports. Event and search contracts live with their service capabilities; pagination contracts live in the domain because domain page requests use the validated page size. `shared/` contains only neutral utilities.

## MVP Use Cases (13)

Structured & semantic job search, job details, application submission/tracking, candidate application recommendations, job management, recruiter application review & status changes, semantic candidate search, domain event publishing, hiring funnel analytics, time-to-hire analytics.

## Roadmap

```text
Foundation
   ↓
Domain + MongoDB
   ↓
GraphQL + Performance
   ↓
Vector / Hybrid Search
   ↓
Event Architecture
   ↓
Lakehouse
   ↓
Spark Batch Analytics
   ↓
Structured Streaming
   ↓
Search Evaluation
   ↓
Scale
   ↓
Observability & Resilience
   ↓
Production Hardening
   ↓
Discovery Intelligence & Search Quality
```

Status markers: `[x]` complete, `[~]` in progress, `[ ]` planned. Complete means the capability is implemented in the local application; it does not claim production rollout or external-service certification.

- `[x] Phase 1 — Foundation:` app skeleton, resource-managed runtime, health/readiness, Mongo connectivity, and basic GraphQL schema.
- `[x] Phase 2 — Domain + MongoDB:` core hiring entities, persistence, account roles, and job/application lifecycle use cases.
- `[x] Phase 3 — GraphQL + Performance:` hiring operations, authorization, bounded pagination, batching, and query/resource limits.
- `[x] Phase 4 — Vector / Hybrid Search:` semantic and hybrid job/candidate search with MongoDB Vector Search integration.
- `[x] Phase 5 — Event Architecture:` operational events, transactional outbox, Kafka publication, idempotent consumption, and quarantine.
- `[x] Phase 6 — Lakehouse + Spark Batch Analytics:` all HAL-01–HAL-14 pass final independent local QA, covering batch/Admin, account deletion, recovery and shortened physical retention with explicitly simulated calendar boundaries. Genuine deployed retention, writer exclusion, guarded key retirement and operational signoff remain production gates in the [lakehouse specification](docs/specs/hiring-analytics-lakehouse.md).
- `[~] Phase 7 — Structured Streaming:` seven process-loss boundaries and native maintenance pruning pass on their recorded sources. Restored source `1d997` passes 440 unit tests, 45 executed integrations and formatting (six ignored bodies excluded), including recovery, cancellation and guarded key retirement. Independent final functional QA passes on exact `1d997`. A bounded native journal UPDATE candidate passes ten focused parity/regression tests; eight fresh native recovery/cancellation checks pass. The current quality-count fusion passes 14 focused tests and eight native recovery/cancellation checks. The assessment simplification and first-processing-only cache reuse pass scoped reviews; latest Silver selection candidate `37dc8c` passes 455 unit tests, 45 executed integrations and formatting, including seven recovery, one cancellation and five key-retirement tests (six disabled bodies excluded). Independent final functional QA passes on this latest candidate; fresh unchanged-profile healthy/runtime gates remain pending. The latest unprofiled healthy workload fails Bronze latency (40,085 ms); report p95 51,123 ms, backlog 464, storage and maintenance pass their individual bounds. A fresh complete instrumented diagnostic also fails Bronze latency. Unsuccessful watermark-cache experiments were reverted. Pending-only Bronze observation passes native parity/cost checks. AQE-off and empty-marker experiments are rejected; the existing two-thread profile and all acceptance limits remain unchanged. Current-source workload, lifecycle, continuous scenarios and final independent QA remain open. Runtime acceptance and production activation remain gated by the [streaming specification](docs/specs/continuous-hiring-analytics.md).
- `[ ] Phase 8 — Search Evaluation:` reproducible relevance datasets and measured keyword/vector/hybrid search quality; planned embedding-pipeline maintainability and ranking-alternative evaluation.
- `[ ] Phase 9 — Scale:` workload-backed capacity, latency, throughput, and storage tuning beyond the required Phase 7 acceptance workload; adopt incremental recomputation or layout changes only with measured benefit.
- `[~] Phase 10 — Observability & Resilience:` structured diagnostics and runtime telemetry exist; planned pure streaming recovery-policy refinement, bounded independent report reconciliation, and actionable alerts; add interview scheduling as a scoped feature with durable saga coordination when an external calendar is introduced. End-to-end failure, recovery, and operational evidence remain open.
- `[ ] Phase 11 — Production Hardening:` deployment controls, security review, deployed retention/writer/key evidence, and deletion-safe recovery drills. Existing production activation prerequisites apply before this milestone whenever production streaming is proposed.
- `[ ] Phase 12 — Discovery Intelligence & Search Quality (Post-MVP):` radius discovery, richer Atlas search/facets, and evidence-backed ranking extensions.

The roadmap also schedules a functional embedding pipeline and reproducible ranking-alternative evaluation in Phase 8, then pure streaming recovery-policy decisions in Phase 10 while retaining Phase 7 correctness gates. Conditional Decorator, Observer, Bridge, Filter, Composite, and ranking extensions remain tied to concrete requirements. See [development milestones](docs/development-milestones.md) for adoption conditions and acceptance expectations.

## Future: Big Data & Analytics
Operational (MongoDB, low-latency GraphQL) and analytical workloads are kept separate. The local batch [analytics design](docs/big-data-architecture.md) and [implementation specification](docs/specs/hiring-analytics-lakehouse.md) describe the current Kafka-to-Delta path, retained data limits, recovery, and data-quality boundaries. Structured Streaming is in progress; Search Evaluation, Scale, Production Hardening, and Discovery Intelligence & Search Quality remain later milestones. The [development milestones](docs/development-milestones.md) place DDIA-informed performance, recovery, maintenance, and audit refinements in their owning phases. The final discovery stage requires Atlas for Atlas Search and Vector Search; local MongoDB Community remains suitable for core workflows and transaction tests. This roadmap does not provision Atlas or include its cost.

## Engineering Focus

1. Production-style GraphQL API design with Sangria
2. Functional architecture with Cats Effect
3. Concurrent/streaming workflows with FS2
4. MongoDB modeling by access pattern + compound-index design
5. N+1 prevention & batching, cursor-based pagination
6. Domain modeling & state transitions
7. Vector & hybrid semantic search
8. Practical RAG/AI integration
