# Hiring Management Platform

## Retrieval and publication reliability checkpoint

The current remediation is tracked in [hiring retrieval and publication reliability](docs/specs/hiring-retrieval-publication-reliability.md). Discovery uses a shared four-permit process budget, four costly roots per GraphQL request and a 2000 ms MongoDB query deadline. Search branches return bounded retrieval hits; current authorized documents are hydrated once. Embedding workers renew leases, reschedule revision-conflicted work without charging provider failures and expose recovery health.

Migration `014_deleted_account_embeddings` removes vectors and metadata retained in previously deleted accounts before workers activate. Stop older writers first; failed cleanup remains resumable and a completed cleanup fails closed if retained fields reappear. See [account erasure and recovery](docs/specs/hiring-retrieval-publication-reliability.md#account-erasure-and-recovery-follow-up).

Operational storage cutovers require stopping incompatible application and analytics writers. New migrations preserve producer attribution in separate bounded records and establish strict workflow validators. They do not authorize a data reset. Local implementation and test evidence do not establish Atlas performance, relevance, real-provider acceptance or deployed phase completion.

The current [search and publication reliability slice](docs/specs/hiring-search-publication-reliability.md) validates embedding model provenance, projects application-admission reads, drains full workflow publication batches and isolates account-deletion recovery. Use `scripts/run-local-tests.sh recovery` for the recovery drill; it owns temporary Mongo/Kafka namespaces and a synthetic API process on the verified test stack. Ordinary tests reuse cached pinned images and tmpfs services; server-wide failpoints/restarts remain dedicated. Live Atlas runners additionally require `ATLAS_TEST_DISPOSABLE=true` and exact comma-separated `ATLAS_TEST_ALLOWED_HOSTS` matching the test URI, with a restricted test account.


Backend-only hiring management platform built with **Scala 3, Sangria GraphQL, Cats Effect, and MongoDB**.

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

Continuous Hiring Analytics is locally complete with an opt-in `analytics-streaming` Compose profile and a fail-closed resource-managed query runtime. The runtime requires an explicitly selected immutable, time-bounded Mongo activation grant; it stops when that grant expires. Production activation requires Phase 6 real-horizon retention, writer exclusion, guarded HMAC retirement, audit, and independent signoff. The isolated synthetic test environment can use independently reviewed accelerated physical retention and explicitly simulated calendar boundaries. See [the Phase 7 specification](docs/specs/continuous-hiring-analytics.md). Default application startup never provisions a grant; an authorized isolated proof run explicitly provisions one immutable grant after independent readiness validation.

The streaming profile reads `.local/config/analytics-streaming.conf`, selects its grant with `activation-grant-id`, and requires explicit starting offsets for every Kafka partition. Its trigger defaults to 10 seconds. Start it only with a valid selected grant and the applicable independently reviewed Phase 6 evidence. Production requires genuine deployed horizons; test activation must bind unused nonce-scoped resources. A durable lakehouse registration keeps ordinary batch ingestion disabled after stream shutdown. The checked-in [sanitized configuration example](analytics/analytics-streaming.conf.example) intentionally uses a single example partition and must be adjusted to the broker's actual partition set.

The local proof launcher supports `scenarios` for continuous functional acceptance, `diagnose` for optional bounded Spark cost measurements, and `run` for the historical healthy-load workload. Each requires a fresh staged nonce and independently reviewed activation evidence. Latency and burst drain-time qualification are deferred to Phase 15; a functional scenario pass is not an SLO pass. `stage-maintenance` stages a separate test fixture with 60-second progress retention; its `maintenance` execution checks natural checkpoint/journal pruning and continuation, without claiming production elapsed retention or healthy-load freshness. Successful maintenance must occur within five minutes under the accepted local load; sanitized observations include deferrals and monotonic maximum age. Each new proof uses a small non-overlapping task network and preserves existing proof data.

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

For project-only sbt JDK selection, create an ignored `.sbtopts` file in this root with `-java-home /path/to/jdk-17` (use the path installed on your machine). The installed sbt launcher reads it for commands such as `sbt compile` and `sbt test`; it does not change your shell's Java or other projects. Keep machine-specific paths out of Git. IntelliJ's JDK settings remain separate. The local-check script's Java prerequisite check still uses `JAVA_HOME`, so set that variable for the script as described below.

Use Java 17+ and sbt 1.11.1. Set `JAVA_HOME` to your installed JDK when the default Java is older, then run:

```bash
bash scripts/check-local.sh
```

The command validates local skills and runs the Docker-independent MUnit suite. Run `sbt 'IntegrationTest / test'` separately for real HTTP lifecycle and disposable MongoDB tests; Docker is required for the database tests. These commands are local checks, not deployment or performance certification.

For repeated local database/Kafka checks, use `scripts/run-local-tests.sh start`, then `scripts/run-local-tests.sh test` or `scripts/run-local-tests.sh analytics`. `status` inspects the owned stack and `stop` stops it. The isolated `compose.test.yaml` stack reuses pinned images and containers, binds separate loopback ports and stores service data in tmpfs. Mongo has a 2 GiB tmpfs and 3 GiB memory limit, including its native free-space reserve and 256 MiB WiredTiger cache; Kafka has a 768 MiB memory limit. These limits are not preallocated; JVM test processes and dedicated drills need additional memory. Its ignored manifest and credentials live under `.local/config/test-services/`; each run owns temporary databases, topics and groups under a locked registry. Endpoint identity must match before writes or cleanup. Ordinary test namespaces are removed on release; server restart/failpoint checks use dedicated containers. Direct sbt integration runs retain suite-owned Testcontainers when reusable mode is absent. Existing application databases and durable analytics proof volumes are outside this test stack. See [the implementation specification](docs/specs/hiring-event-and-test-reliability.md).

For a focused integration rerun with the full root unit suite, use `scripts/run-local-tests.sh test '*MongoJobDiscoveryIntegrationSpec' '*OperationalEventComposeIntegrationSpec'`. With no suite globs, `test` runs every root integration suite. Quote globs to prevent shell expansion; the wrapper accepts only suite-name characters and retains its locked namespace ownership. For analytics integration tests only, use `scripts/run-local-tests.sh analytics '*HiringAnalyticsStreamingRecoveryIntegrationSpec'`; analytics with no globs runs its full unit and integration suites.

Wrapper runs cache compiler output and reports under `.local/data/test-builds/root/target/` or `.local/data/test-builds/analytics/target/`, isolated from IDE and ordinary sbt `target/` output. Each build has its own lock outside that target, so root test/proof commands serialize while root and analytics can run together; an explicit sbt clean cannot remove the held lock. The caches persist across runs and service restarts; ordinary sbt keeps its normal target unless `hiring.test.buildRoot` is explicitly set.

## Test coverage

JaCoCo coverage is available for both SBT builds. Run `sbt jacoco` from the repository root for main-project unit coverage; run `sbt 'IntegrationTest / jacoco'` to run its integration tests with coverage and merge their results. Reports are written under `target/scala-<version>/jacoco/report/test/` and `.../report/it/`, with the merged report under `.../report/merged/`. Run the same commands from `analytics/` to generate analytics unit and integration coverage. Integration coverage requires the same Docker-backed services as the integration test suite. Coverage thresholds are not configured yet; reports provide measurement without imposing a baseline.

## Documentation

- [Current roadmap and acceptance gates](docs/development-milestones.md)
- [Detailed specifications for remaining phases](docs/development-milestones.md#remaining-delivery-order-and-acceptance), including behavior, code style, boundaries, alternatives and verification requirements
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

## Implemented capabilities and planned extensions

### Hiring
- Candidate, Recruiter, and Admin roles (RBAC)
- Job posting creation/management, filtering, search, cursor-based pagination
- Candidate application submission & tracking
- Recruiter application management
- Application lifecycle: `Created → Accepted / Declined / Rejected`, `Accepted → Interview`, `Interview → Hired / Rejected`; terminal states have no outgoing transitions.
- Application status history & recruiter feedback
- Live GraphQL subscriptions remain a future extension; the current schema exposes queries and mutations.
- Admin statistics & hiring analytics

### GraphQL
- Sangria API with queries and mutations; subscriptions are not implemented.
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
| AI Integration | Embedding provider behind service ports; optional search experiments |

## High-Level Architecture

```text
                GraphQL API (Sangria + http4s)
                            │
                            ▼
                 Application Layer (Services)
                            │
                       Service-owned Ports
                            │
           ┌────────────────┼────────────────┐
           ▼                ▼                 ▼
     Repository Ports   Search Port     Embedding Port
           │                │                 │
           ▼                ▼                 ▼
              Infrastructure: MongoDB · Vector Search · Embedding API
```

Dependencies point inward (`transport → service → domain`); the domain layer stays free of Sangria, MongoDB, http4s, Circe, JWT, and AI-SDK dependencies.

Current package layout: `api/` (GraphQL, HTTP, auth adapters) → `service/` (use-case protocols, outbound ports, and job/application/search use cases) → `domain/` (model, pagination, error, pure policy). `repository.mongo/` and `infrastructure/` contain adapters that implement service ports. Event and search contracts live with their service capabilities; pagination contracts live in the domain because domain page requests use the validated page size. `shared/` contains only neutral utilities.

## MVP Use Cases (13)

Structured & semantic job search, job details, application submission/tracking, candidate job recommendations, job management, recruiter application review & status changes, semantic candidate search, domain event publishing, hiring funnel analytics, time-to-hire analytics.

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
Lakehouse + Spark Batch Analytics (6)
   ↓
Structured Streaming (7)
   ↓
MongoDB Access + Vector Retrieval Optimization (8)
   ↓
Search Evaluation + Embedding Architecture (9)
   ↓
AI Discovery + Search Quality (10)
   ↓
Kafka Workflow Contracts + Saga + State (11)
   ↓
Operational Observability + Resilience (12)
   ↓
Analytics Architecture + Reconciliation + Scale Planning (13)
   ↓
Production Hardening (14)
   ↓
Deployed Analytics Optimization (15, final)
```

Status markers: `[x]` complete, `[~]` in progress, `[ ]` planned. Complete means the capability is implemented in the local application; it does not claim production rollout or external-service certification.

- `[x] Phase 1 — Foundation:` app skeleton, resource-managed runtime, health/readiness, Mongo connectivity, and basic GraphQL schema.
- `[x] Phase 2 — Domain + MongoDB:` core hiring entities, persistence, account roles, and job/application lifecycle use cases.
- `[x] Phase 3 — GraphQL + Performance:` hiring operations, authorization, bounded pagination, batching, and query/resource limits.
- `[x] Phase 4 — Vector / Hybrid Search:` semantic and hybrid job/candidate search with MongoDB Vector Search integration.
- `[x] Phase 5 — Event Architecture:` operational events, transactional outbox, Kafka publication, idempotent consumption, and quarantine.
- `[x] Phase 6 — Lakehouse + Spark Batch Analytics:` all HAL-01–HAL-14 pass final independent local QA, covering batch/Admin, account deletion, recovery and shortened physical retention with explicitly simulated calendar boundaries. Genuine deployed retention, writer exclusion, guarded key retirement and operational signoff remain production gates in the [lakehouse specification](docs/specs/hiring-analytics-lakehouse.md).
- `[x] Phase 7 — Structured Streaming:` full continuous functional scenarios on recorded source `8c01e` and separate grant-expiry, active-callback TERM/INT, seven abrupt-loss boundaries and accelerated pruning proofs have source-qualified QA PASS evidence. Current-source validation passes 470 analytics units, 45 executed integrations and formatting (six disabled integration bodies excluded); independent final Code, Security and QA closure verdicts PASS. The historical `37dc8c` baseline passed 455 analytics units and 45 integrations (six disabled bodies excluded); it is not current-checkout certification. Its Bronze p95 **40,084 ms FAIL** remains recorded. Latency and burst drain-time qualification move to Phase 15 after deployment. See the [streaming specification](docs/specs/continuous-hiring-analytics.md) for exact evidence and production prerequisites.
- `[ ] Phase 8 — MongoDB Access and Vector Retrieval Optimization:` [query/index and retrieval specification](docs/specs/mongodb-vector-retrieval-optimization.md), preserving authorization and measuring index costs.
- `[ ] Phase 9 — Search Evaluation and Embedding Architecture:` [evaluation and embedding specification](docs/specs/hiring-search-enhancements.md#search-evaluation-and-embedding-architecture). Local embedding preparation/recovery, typed evaluation, provisional fixture replay, judgment review export, curated retrieval capture and scoped paired ranking assessment are implemented. The conservative evaluation policy is approved; human relevance review, live Atlas comparisons and ranking adoption remain open.
- `[ ] Phase 10 — AI Discovery and Search Quality:` [discovery specification](docs/specs/hiring-discovery-search-quality.md) for radius search, Atlas search/facets and evaluated ranking; promoted from the former post-MVP Phase 12.
- `[ ] Phase 11 — Kafka Workflow Contracts, Saga and State:` [durable workflow specification](docs/specs/durable-hiring-workflows.md) for stream contracts, pure decisions, recovery and conditional interview scheduling.
- `[~] Phase 12 — Operational Observability and Resilience:` [observability and resilience specification](docs/specs/hiring-observability-resilience.md), extending existing telemetry with actionable diagnostics and recovery drills.
- `[ ] Phase 13 — Analytics Architecture, Reconciliation and Scale Planning:` [reconciliation and recovery specification](docs/specs/analytics-reconciliation-recovery.md); further Spark/Delta work follows Mongo/AI/workflow acceptance.
- `[ ] Phase 14 — Production Hardening:` [production readiness specification](docs/specs/production-readiness.md); genuine retention, writer/key and recovery prerequisites apply whenever production streaming is proposed.
- `[ ] Phase 15 — Deployed Analytics Optimization (final):` [deployed performance specification](docs/specs/deployed-analytics-performance.md), qualifying latency and burst drain time after authorized deployment.

Phases 1–7 retain their existing scope and acceptance gates. The revised order prioritizes MongoDB and AI features before further Spark tools; it does not declare full MVP complete. Each capability includes pure FP architecture refinement using immutable ADTs, existing lifecycle State programs, typed errors and resource-owned interpreters. See [development milestones](docs/development-milestones.md) for dependencies, pattern adoption conditions and acceptance criteria.

## Future: Big Data & Analytics
Operational (MongoDB, low-latency GraphQL) and analytical workloads are kept separate. The local batch [analytics design](docs/big-data-architecture.md) and [implementation specification](docs/specs/hiring-analytics-lakehouse.md) describe the current Kafka-to-Delta path, retained data limits, recovery, and data-quality boundaries. Structured Streaming is locally complete; MongoDB optimization, search evaluation, AI discovery and Kafka workflows come next, before further analytics refinement and production hardening. The [development milestones](docs/development-milestones.md) place DDIA-informed performance, recovery, maintenance, and audit refinements in their owning phases. The discovery stage requires Atlas for Atlas Search and Vector Search; local MongoDB Community remains suitable for core workflows and transaction tests. This roadmap does not provision Atlas or include its cost.

## Engineering Focus

1. Production-style GraphQL API design with Sangria
2. Functional architecture with Cats Effect
3. Concurrent/streaming workflows with FS2
4. MongoDB modeling by access pattern + compound-index design
5. N+1 prevention & batching, cursor-based pagination
6. Domain modeling & state transitions
7. Vector & hybrid semantic search
8. Practical RAG/AI integration

### MongoDB and retrieval evidence

Phase 8 now includes scoped application/history and nested reads, minimized authoritative search eligibility, ordinary index verification/recovery tests, and reproducible operational and authenticated structured HTTP measurement suites. The [Phase 8 specification](docs/specs/mongodb-vector-retrieval-optimization.md) records criterion-level results, the [first iteration latency plan](docs/specs/mongodb-vector-retrieval-optimization.md#first-iteration-latency-plan), and selected local write/storage overhead ceilings. Extended measurements distinguish warmed timings, completed writes, actual query-plan choices and scoped resource samples; generated evidence stays in ignored `.local/data/`. Actual Atlas branch execution and paired ANN/exact measurements require secure `ATLAS_TEST_URI`; missing access and skipped live tests are blocked gates. These local changes do not close paired optimization acceptance or later embedding architecture work.

### Local discovery and interview scheduling

Local geographic discovery accepts optional job coordinates, exposes `nearbyJobs` with distance and bound cursors, and computes exact structured `jobDiscoveryFacets` before pagination. Existing coordinate-free jobs remain valid. See [discovery contracts and evidence](docs/specs/hiring-discovery-search-quality.md). Human relevance labels await review and personalization remains deferred.

Interview scheduling is opt-in with `INTERVIEW_ENABLED=true` and separate configured Kafka command publisher, result publisher and fencer credentials. `scheduleInterview` persists a workflow before reserving the local durable fake calendar, committing the guarded hiring transition and notifying both participants. Inspect progress through `interviewWorkflow`; Admin repair requires expected revision and idempotency. Stop incompatible writers before applying new operational migrations. See [workflow contracts and local evidence](docs/specs/durable-hiring-workflows.md). Completion and deployment readiness remain subject to its validation gates.

The [embedding and workflow boundary specification](docs/specs/embedding-workflow-boundary-reliability.md) tracks finite provider vectors, independent cleanup subject progress, explicit worker diagnostics and reuse of the pure application lifecycle for interview commits. Its functional follow-ups add bounded subject-ID cleanup sweeps with per-record failure isolation, pure interview message/admission decisions, validated canonical candidate filters, injectable scheduling effects, byte-preserving cursor encoding and named workflow/ranking/facet values. Admin repair reconciles existing effects without reopening released reservations or extending deadlines. Public contracts and stored schemas remain unchanged; follow-up acceptance is tracked separately from historical checks.

For the local Compose broker, enable Kafka and use `KAFKA_INTERVIEW_ORCHESTRATOR_USERNAME=interview_command_publisher`, `KAFKA_INTERVIEW_WORKER_USERNAME=interview_result_publisher` and `KAFKA_INTERVIEW_FENCER_USERNAME=interview_fencer`. Supply their distinct `KAFKA_INTERVIEW_ORCHESTRATOR_PASSWORD`, `KAFKA_INTERVIEW_WORKER_PASSWORD` and `KAFKA_INTERVIEW_FENCER_PASSWORD` values through ignored local configuration; Compose uses those same password variables to provision its principals. The existing Kafka publisher/consumer configuration is still required. Use the configured local plaintext protocol only with loopback bootstrap addresses. Scheduling timestamps use UTC milliseconds; intervals that collapse at that precision are rejected. Executable scheduling operations are in `src/test/resources/graphql/interview-scheduling.graphql`.

### Workflow recovery maintenance cutover

Before activating transactional interview publication, stop all old interview publishers and revoke their topic-write credentials. The Compose fixture replaces the old `interview_orchestrator`/`interview_worker` login identities; do not retain them on an existing broker. Migration `010_interview_workflow_attempts` preserves prior execution slots and queues uncertain executions for receipt lookup. Migration `011_interview_publication_fencing` reopens legacy cleanup proof, including previously complete rows, without restoring deleted data. Startup applies and verifies both before starting workers. Cleanup requires broker-confirmed fencing, durable CAS progress and fresh physical-retention barriers. This repository change does not perform that maintenance on a deployed environment. See [recovery and validation criteria](docs/specs/hiring-workflow-recovery.md).
