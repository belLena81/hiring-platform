# Hiring Management Platform

The [candidate discovery and interview integrity specification](docs/specs/candidate-discovery-and-interview-integrity.md) tracks the current local corrections to bounded deletion attribution, Kafka quarantine identity and candidate residence verification, plus geographic and broker-outage acceptance tests. Its verification status is separate from Atlas, relevance and deployed acceptance.

## Retrieval and publication reliability checkpoint

The current remediation is tracked in [hiring retrieval and publication reliability](docs/specs/hiring-retrieval-publication-reliability.md). Discovery uses a shared four-permit process budget, four costly roots per GraphQL request and a 2000 ms MongoDB query deadline. Search branches return bounded retrieval hits; current authorized documents are hydrated once. Embedding workers renew leases, reschedule revision-conflicted work without charging provider failures and expose recovery health.

Migration `014_deleted_account_embeddings` removes vectors and metadata retained in previously deleted accounts before workers activate. Stop older writers first; failed cleanup remains resumable and a completed cleanup fails closed if retained fields reappear. See [account erasure and recovery](docs/specs/hiring-retrieval-publication-reliability.md#account-erasure-and-recovery-follow-up).

Operational storage cutovers require stopping incompatible application and analytics writers. New migrations preserve producer attribution in separate bounded records and establish strict workflow validators. They do not authorize a data reset. Local implementation and test evidence do not establish Atlas performance, relevance, real-provider acceptance or deployed phase completion.

The current [search and publication reliability slice](docs/specs/hiring-search-publication-reliability.md) validates embedding model provenance, projects application-admission reads, drains full workflow publication batches and isolates account-deletion recovery. Use `scripts/run-local-tests.sh recovery` for the recovery drill; it owns temporary Mongo/Kafka namespaces and a synthetic API process on the verified test stack. Ordinary tests reuse cached pinned images and tmpfs services; server-wide failpoints/restarts remain dedicated. Live Atlas runners additionally require `ATLAS_TEST_DISPOSABLE=true` and exact comma-separated `ATLAS_TEST_ALLOWED_HOSTS` matching the test URI, with a restricted test account.


Backend-only hiring management platform built with **Scala 3, Sangria GraphQL, Cats Effect, and MongoDB**.

A practical playground for functional Scala, GraphQL API design, MongoDB data modeling/query optimization, and AI-powered semantic search & candidate/job matching via MongoDB Vector Search.

## Agent-driven development

Start with the `product-manager` skill (`/product-manager`) to coordinate specialist work and independent reviews. See [agent workflow and usage](docs/agent-development.md) and [project rules](AGENTS.md). Local skills cover Product Manager, Software Architect, Scala Developer, Data Engineer, Big Data Engineer, QA Engineer, Security Engineer, and Code Reviewer.

The build uses Scala 3.9 LTS and Java 17+, with Cats Effect/FS2, Sangria/http4s Ember, Circe, MongoDB reactive driver, fs2-kafka, PureConfig with Iron refinements, cats-retry, Logback, and MUnit/Testcontainers core. It serves the current hiring API with a resource-managed MongoDB client.

## Trusted Admin initialization

Public Admin bootstrap has been removed. Before first startup, place `auth.admin-seed.enabled = true`, `auth.admin-seed.name`, and `auth.admin-seed.password` in explicitly loaded, ignored local HOCON configuration (or use `ADMIN_SEED_ENABLED`, `ADMIN_SEED_NAME`, `ADMIN_SEED_PASSWORD` substitutions). Seeding runs after Mongo setup/migration and before workers or HTTP. Disable seeding after initialization. Once the account registry is initialized the seed is a no-op: an existing Admin is preserved, including its password, and later renames or status changes never block startup; only an inconsistent registry blocks startup. Seeding returns no token; authenticate through `login`.

Login/signup idempotency receipts use domain-separated HMAC fingerprints keyed by `AUTH_RECEIPT_FP_SECRET` (falling back to `AUTH_JWT_HS256_SECRET`), and replay verifies credentials. Pre-MVP there is no conversion of older receipt shapes; receipts expire by TTL. Changing the receipt key invalidates outstanding authentication receipt matches: retry with a new idempotency key. See [security/runtime reliability](docs/specs/hiring-security-and-runtime-reliability.md) for acceptance evidence and verification limits.

## Local build

`sbt run` forks a separate JVM so Cats Effect `IOApp` owns the application lifecycle. The packaged configuration has no defaults for the HTTP bind address, port, MongoDB URI or JWT secret; supply them through the environment (`HTTP_HOST`, `HTTP_PORT`, `MONGODB_URI`, `AUTH_JWT_HS256_SECRET`) or an explicitly selected configuration file. `src/main/resources/local.conf` binds `127.0.0.1:8080`, enables schema exposure and is loaded only when selected by `config.resource`/`config.file`. Start MongoDB locally, then run the application:

```bash
docker compose up -d mongodb
HTTP_HOST=127.0.0.1 HTTP_PORT=8080 MONGODB_URI="mongodb://127.0.0.1:27017/?replicaSet=rs0&directConnection=true" AUTH_JWT_HS256_SECRET="<random secret of at least 32 bytes>" sbt run
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

Continuous Hiring Analytics has an opt-in `analytics-streaming` Compose profile and a fail-closed resource-managed query runtime. The runtime requires an explicitly selected immutable, time-bounded Mongo activation grant and stops when that grant expires. The runtime only reads grants: nothing in the application provisions or renews them. Production activation requires Phase 6 real-horizon retention, writer exclusion, guarded HMAC retirement, audit, and independent signoff. Local tests use shortened Kafka and Delta retention and explicitly simulated calendar boundaries. See [the Phase 7 specification](docs/specs/continuous-hiring-analytics.md) and the [verification specification](docs/specs/hiring-analytics-verification.md) for the criterion-to-test mapping.

The streaming profile reads `.local/config/analytics-streaming.conf`, selects its grant with `activation-grant-id`, and requires explicit starting offsets for every Kafka partition. Its trigger defaults to 10 seconds. Start it only with a valid selected grant and the applicable independently reviewed Phase 6 evidence. Production requires genuine deployed horizons. A durable lakehouse registration keeps ordinary batch ingestion disabled after stream shutdown. The checked-in [sanitized configuration example](analytics/analytics-streaming.conf.example) intentionally uses a single example partition and must be adjusted to the broker's actual partition set.

Phase 6 and Phase 7 local acceptance is by criterion-tagged tests, not by a standalone launcher. The proof launcher (`scenarios`, `diagnose`, `run`, `stage-maintenance`) and its nonce-scoped activation were removed in commit `3755455`; their run identifiers are historical only. Run the tests with:

```bash
bash scripts/check-local.sh                                                         # formatting, unit and integration suites (Docker, Java 17+, 85% merged coverage)
scripts/run-local-tests.sh analytics '*HiringAnalyticsStreamingBoundaryRecoveryIntegrationSpec'  # one or more analytics integration suites (glob)
scripts/run-local-tests.sh recovery                                                 # AccountDeletionRecoveryIntegrationSpec (HAL-04.4)
scripts/run-local-tests.sh retention                                                # KafkaTopicRetentionCheck: shortened Kafka topic retention
```

The `analytics` mode with suite globs runs integration suites only; unit suites run with `sbt test` in the root or in `analytics/`. In a default analytics run `AccountDeletionRecoveryIntegrationSpec` and opt-in cases such as the broker-outage and retired-publisher checks in `KafkaPublisherAccessIntegrationSpec` are reported as ignored or NOT RUN, not passed. Latency and burst drain-time qualification are deferred to Phase 15; a functional test pass is not an SLO pass.

### Local analytics erasure worker

Run the API at least once so MongoDB migrations and analytics control collections are initialized. Keep the API process running, then start the worker in another terminal:

```bash
docker compose up -d mongodb kafka
# In one terminal, start the API with `sbt run`.
docker compose --profile analytics-erasure up -d --build analytics-erasure-worker
docker compose logs -f analytics-erasure-worker
```

The worker shares `.local/data/analytics/` with batch runs. It requires the same HMAC secret and Kafka reader credentials as the batch, plus `KAFKA_FENCER_PASSWORD` in the ignored root `.env` for a principal scoped to the publisher transactional-ID prefix. `deleteMyAccount` commits the deletion and returns a durable receipt with `PENDING`; a verified token for the receipt owner can query `accountDeletionStatus(receiptId: "<receipt-id>")` for `PENDING`, `COMPLETE`, or `NOT_FOUND`, including while that account is tombstoned. The worker fences every publisher process recorded by the deletion transaction before it captures the Kafka barrier or purges outbox rows. It may then wait through Kafka retention and Delta's seven-day vacuum safety horizon before verifying snapshots, rebuilding the report, and atomically publishing the snapshot and completion ledger. A completed receipt remains queryable after the request marker expires. The current transactional publisher uses `hiring_publisher_v2` with `KAFKA_PUBLISHER_V2_PASSWORD`; the pre-transactional `publisher` identity has its topic, cluster-idempotence, and transactional-ID ACLs removed by ACL initialization. The regular local broker was cut over on 2026-09-25: its KRaft data was preserved in `.local/data/kafka`, its legacy PLAIN login was removed, and its stale ACL grants were revoked. The scoped regular-broker check verifies v2 publisher writes and fencing, legacy authentication rejection, and reader denials. Final deployed retention completion and guarded key retirement remain operational gates. See the [analytics specification](docs/specs/hiring-analytics-lakehouse.md).

Worker failures persist a fixed operational category and retry count; exhausted or invalid-state requests stay `PENDING` and require local repair. With the analytics HOCON and credentials configured, inspect them using `cd analytics && sbt 'runMain com.example.hiring.analytics.cli.AnalyticsErasureRepairMain inspect 50'`. After repairing the underlying cause, requeue one request with `sbt 'runMain com.example.hiring.analytics.cli.AnalyticsErasureRepairMain requeue <request-id> <observed-attempt-count>'`. The command refuses stale attempt counts or requests with a live lease and resumes the saved phase.

The accelerated Compose proof scripts and their overlay files were removed in commit `3755455`; the pre-production Kafka/Delta retention proof, external-writer exclusion, guarded key retirement and operational signoff remain required before production and are tracked in the analytics specs.

`GET /health` reports application liveness; `GET /ready` answers 503 only when MongoDB is unreachable or the Mongo-writes breaker is open; otherwise 200 with a `degraded` list naming stalled workers (including the embedding worker when vector search is enabled) and aged durable work, for internal probes only. `POST /graphql` accepts `{"query":"{ health { status } readiness { status } }"}`. MongoDB outages leave HTTP running and readiness reports `NOT_READY`. `GET /schema.graphql` exports the current schema and GraphQL introspection supports API documentation/testing clients, but only when `graphql.schema-exposure` is true (set `GRAPHQL_SCHEMA_EXPOSURE=true`, or `graphql.schema-exposure = true` in your own git-ignored `local.conf`; it is off in the packaged default and no local default is shipped, so both are refused with 404 and `FORBIDDEN` until you opt in). Anonymous callers share one query cost budget when the peer address is missing or the service sits behind a proxy not listed in `http.trusted-proxy-cidrs`; configure the real proxy networks for per-client budgets. See the [API reference](docs/api.md).

See the [API reference](docs/api.md) and the [current reset specification](docs/specs/pre-mvp-contract-reset.md) for the active contract and verification evidence.

See [diagnostic logging](docs/logging.md) for searchable markers, request tracing, masked defaults, and explicitly gated local payload capture.

For project-only sbt JDK selection, create an ignored `.sbtopts` file in this root with `-java-home /path/to/jdk-17` (use the path installed on your machine). The installed sbt launcher reads it for commands such as `sbt compile` and `sbt test`; it does not change your shell's Java or other projects. Keep machine-specific paths out of Git. IntelliJ's JDK settings remain separate. The local-check script's Java prerequisite check still uses `JAVA_HOME`, so set that variable for the script as described below.

Use Java 17+ and sbt 1.11.1. Set `JAVA_HOME` to your installed JDK when the default Java is older, then run:

```bash
bash scripts/check-local.sh
```

The command validates local skills, runs formatting checks, then runs the root and analytics unit and integration suites under JaCoCo. It therefore requires Docker (through `scripts/run-local-tests.sh`) and fails when merged unit + integration line coverage is below 85%. The unit suite alone is Docker-independent: `sbt test` runs it, and `sbt 'IntegrationTest / test'` runs the real HTTP lifecycle and disposable MongoDB tests. These commands are local checks, not deployment or performance certification.

For repeated local database/Kafka checks, use `scripts/run-local-tests.sh start`, then `scripts/run-local-tests.sh test` or `scripts/run-local-tests.sh analytics`. `status` inspects the owned stack and `stop` stops it. The isolated `compose.test.yaml` stack reuses pinned images and containers, binds separate loopback ports and stores service data in tmpfs. Mongo has a 2 GiB tmpfs and 3 GiB memory limit, including its native free-space reserve and 256 MiB WiredTiger cache; Kafka has a 768 MiB memory limit. These limits are not preallocated; JVM test processes and dedicated drills need additional memory. Its ignored manifest and credentials live under `.local/config/test-services/`; each run owns temporary databases, topics and groups under a locked registry. Endpoint identity must match before writes or cleanup. Ordinary test namespaces are removed on release; server restart/failpoint checks use dedicated containers. Direct sbt integration runs retain suite-owned Testcontainers when reusable mode is absent. Existing application databases and durable analytics proof volumes are outside this test stack. See [the implementation specification](docs/specs/hiring-event-and-test-reliability.md).

Compose files: `compose.yaml` starts the local MongoDB replica set, SASL Kafka and the opt-in analytics profiles; `compose.test.yaml` is the isolated test stack used by `scripts/run-local-tests.sh`; `compose.atlas-local.yaml` is an opt-in Atlas-compatible MongoDB for the Atlas-gated search specs (see [search evaluation](docs/search-evaluation.md#local-atlas-compatible-run)); `compose.cloud.yaml` is a prepared overlay that disables the local MongoDB/Kafka containers and points the analytics workers at managed services through `KAFKA_BOOTSTRAP_SERVERS`, `MONGODB_URI` and `ANALYTICS_LAKEHOUSE_ROOT` (`docker compose -f compose.yaml -f compose.cloud.yaml --profile analytics up analytics-batch`). It provisions nothing; managed endpoints, secrets and Kafka credentials must be supplied by the operator.

For a focused integration rerun with the full root unit suite, use `scripts/run-local-tests.sh test '*MongoJobDiscoveryIntegrationSpec' '*OperationalEventComposeIntegrationSpec'`. With no suite globs, `test` runs every root integration suite. Quote globs to prevent shell expansion; the wrapper accepts only suite-name characters and retains its locked namespace ownership. For analytics integration tests only, use `scripts/run-local-tests.sh analytics '*HiringAnalyticsStreamingRecoveryIntegrationSpec'`; analytics with no globs runs its full unit and integration suites.

Wrapper runs cache compiler output and reports under `.local/data/test-builds/root/target/` or `.local/data/test-builds/analytics/target/`, isolated from IDE and ordinary sbt `target/` output. Each build has its own lock outside that target, so root test/proof commands serialize while root and analytics can run together; an explicit sbt clean cannot remove the held lock. The caches persist across runs and service restarts; ordinary sbt keeps its normal target unless `hiring.test.buildRoot` is explicitly set.

## Test coverage

JaCoCo coverage is available for both SBT builds. Run `sbt jacoco` from the repository root for main-project unit coverage; run `sbt 'IntegrationTest / jacoco'` to run its integration tests with coverage and merge their results. Reports are written under `target/scala-<version>/jacoco/report/test/` and `.../report/it/`, with the merged report under `.../report/merged/`. Run the same commands from `analytics/` to generate analytics unit and integration coverage. Integration coverage requires the same Docker-backed services as the integration test suite. Both builds enforce an 85% line-coverage threshold on the merged unit + integration report (measured 2026-10-10: application 90.21%, analytics 87.93%): `sbt jacoco 'IntegrationTest / jacoco'` fails below it, and the full runs of `scripts/run-local-tests.sh test|analytics` (used by `scripts/check-local.sh`, which therefore needs Docker) run exactly that. Unit-only `sbt jacoco` reports without a threshold. Reports are written as HTML and `jacoco.xml`.

## Documentation

- [Current application architecture wiki](docs/wiki/README.md): use cases, domain entities, collections, workflows, design patterns, security and operations
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
- `[x] Phase 6 — Lakehouse + Spark Batch Analytics:` local evidence re-established by criterion-tagged tests on current source, with independent Code Reviewer, Security Engineer and final QA PASS (October 13, HEAD `1834236`). The evidence covers batch/Admin, account deletion, worker recovery, shortened Kafka/Delta retention with explicitly simulated calendar boundaries, key retirement and publisher fencing. The [verification specification](docs/specs/hiring-analytics-verification.md) maps each criterion to its suite. `scripts/check-local.sh` passes on that source (analytics 458 units, 102 integrations, 4 opt-in cases ignored; HAL-04.4 passes under `scripts/run-local-tests.sh recovery`). Opt-in cases not run: HAL-05-FENCE-06.3 (retired publisher) and HAL-05.4 (broker outage). The October 3 QA verdicts relied on proof programs removed in commit `3755455` and are historical. Genuine deployed retention, writer exclusion, guarded key retirement and operational signoff remain production gates in the [lakehouse specification](docs/specs/hiring-analytics-lakehouse.md).
- `[x] Phase 7 — Structured Streaming:` local evidence for HCS-01–HCS-13 is re-established by criterion-tagged tests in the analytics and root builds (see the [verification specification](docs/specs/hiring-analytics-verification.md) and the [current criterion table](docs/specs/continuous-hiring-analytics.md#current-evidence-verification-specification-2026-10-13)). Slice A6 is recorded: checkpoint ownership 8/8 (HCS-08.1–08.5, HCS-12.1–12.3) and the forked process-signal suite 3/3 (HCS-07.9, 07.10, 08.7). The HCS-08.3 topic-identity case is a recorded gap (C11), not a pass. Independent Code Reviewer, Security Engineer and final QA verdicts PASS on HEAD `1834236`, with `scripts/check-local.sh` passing. The historical proof-program results, including the `8c01e` runs and the Bronze p95 **40,084 ms FAIL**, are dated history in the streaming specification, not current evidence. Latency and burst drain-time qualification are deferred to Phase 15 after deployment. Production gates are unchanged.
- `[~] Phase 8 — MongoDB Access and Vector Retrieval Optimization:` [specification](docs/specs/mongodb-vector-retrieval-optimization.md). Plain-MongoDB access correctness, projections, pagination and operational measurements are recorded (MVR-01–03). Atlas-dependent MVR-04–10 are split into `.L` (Atlas Local) and `.C` (cloud) gates; commit `e2349f0` has Code and Security PASS, and the `.L` evidence run is pending. Remaining: Atlas Local evidence run (pending, $0): MVR-04.L–10.L, including the MVR-08.L rehearsal; Atlas cloud gates (deferred): MVR-04.C–10.C; any vector-setting change needs MVR-08.C limits (Recall@7 at least 0.95, ANN p95 at most baseline + 10%, write p95 regression at most 10%, index growth at most 25%); independent QA after the `.L` run; no human gate or real provider.
- `[~] Phase 9 — Search Evaluation and Embedding Architecture:` [specification](docs/specs/hiring-search-enhancements.md#search-evaluation-and-embedding-architecture). Embedding preparation/recovery, typed evaluation, fixture replay, review export, curated capture and paired assessment are implemented locally. Remaining: Atlas Local evidence run (pending, $0): HSE-04.L, HSE-06.L, SEB-02.L and SEB-04.L, plus final HSE QA; Atlas cloud gates (deferred): HSE-04.C, HSE-06.C, SEB-02.C, SEB-04.C, HSE-05.C (native `$rerank`) and HSE-07.C (Automated Embedding comparison); human gate: SEB-02.H (relevance labels, pending by user decision); real providers: genuine semantic embeddings for SEB-08. Ranking adoption stays deferred and unchanged.
- `[~] Phase 10 — AI Discovery and Search Quality:` [specification](docs/specs/hiring-discovery-search-quality.md). Radius search, input validation, exact structured facets and cursor/geo index cutover are recorded locally with plain-MongoDB evidence (HDQ-01–03, 05, 09–11). Lexical-query facets (HDQ-12) are implemented: `jobDiscoveryFacets` accepts an optional `query`, counts come from the authoritative eligible set, and `coverage` labels whether counts cover all matches or the top 1000 lexical matches. Security PASS and Code PASS for the code scope are recorded; `HDQ-12.L` is pending. Remaining: Atlas Local evidence run (pending, $0): HDQ-01.L, HDQ-04.L, HDQ-05.L, HDQ-06.L, HDQ-11.L and HDQ-12.L; Atlas cloud gates (deferred): the matching `.C` rows; ranker adoption (HDQ-07) and personalization (HDQ-08) stay deferred, with HDQ-08 also blocked by the Phase 9 human gate SEB-02.H. Independent QA PASS for the HDQ-12 local scope (October 13).
- `[~] Phase 11 — Kafka Workflow Contracts, Saga and State:` [specification](docs/specs/durable-hiring-workflows.md). The local closure checkpoint (October 13) records DHW-14–34 as Local PASS on the ledger and fake-provider path, except DHW-26, DHW-32 and the SES part of DHW-24 (deferred); Code Reviewer, Security Engineer and QA PASS are recorded. Interview cancellation and rescheduling are verified locally. An unknown calendar reservation on cancel stays fail-closed and ends in Admin repair (user decision). Atlas: none. Real providers and deployed gates: the calendar provider is not selected (DHW-25 provider contract run deferred); SES notifications (DHW-24 provider part, DHW-26, DHW-32) are deferred to the real-provider slice; production redundancy and deployed retention remain open.
- `[~] Phase 12 — Operational Observability and Resilience:` [specification](docs/specs/hiring-observability-resilience.md). Only the pre-existing telemetry runtime and structured logging exist; the draft spec's HOR-01–11 are all Not run (thresholds, routing and exporter decisions still unmade).
- `[ ] Phase 13 — Analytics Architecture, Reconciliation and Scale Planning:` [specification](docs/specs/analytics-reconciliation-recovery.md). Draft; ARR-01–11 Not run. Deliberately sequenced after Mongo/AI/workflow acceptance.
- `[ ] Phase 14 — Production Hardening:` [specification](docs/specs/production-readiness.md). Draft; PRD-01–12 Not run. Provider, topology, recovery objectives and owners are unresolved. Production streaming additionally needs genuine retention, writer exclusion and guarded key retirement.
- `[ ] Phase 15 — Deployed Analytics Optimization (final):` [specification](docs/specs/deployed-analytics-performance.md). Draft; DAP-01–11 Not run. Needs an authorized real deployment; also owns the Phase 7 Bronze p95 (40,084 ms FAIL) latency and burst-drain qualification.

### Status assessment (October 8, 2026)

Marker legend for Phases 8–11: `[~]` means the local implementation is done for its reviewed scope but whole-phase criteria stay open. The phase-level `[x]` is withheld until those remaining items close; this is a stricter bar than "local code complete".

| Group | Phases | What remains |
|---|---|---|
| Complete (local) | 1–5 | Roadmap items marked `[x]` above. Phases 6–7 are listed in the next row. |
| Complete (local), re-verified October 13 | 6–7 | Phase 6 and 7 criteria are re-established by tagged tests on current source with independent Code/Security/QA PASS ([verification specification](docs/specs/hiring-analytics-verification.md)). Open: the HCS-08.3 topic-identity gap (C11) and the opt-in HAL-05-FENCE-06.3 / HAL-05.4 cases. Production gates are unchanged: deployed retention, writer exclusion, key retirement, operational signoff. Phase 7 latency moves to Phase 15. |
| Small remains | 8, 9, 10, 11 | Atlas Local evidence runs (pending, $0), Atlas cloud gates (deferred), the Phase 9 human relevance gate (SEB-02.H), final HSE QA for Phase 9, and real providers or deployed gates. Phase 11 cancellation and rescheduling are Local PASS (Code, Security and QA PASS); SES notifications stay deferred. No further local feature work is required to reach these gates. |
| Not yet covered | 12–15 | Specs exist and are reviewed as drafts, but no acceptance criterion has run. Phase 12 is the next buildable phase; 13 depends on it, 14–15 depend on deployment decisions. |

Cross-cutting tracks (not roadmap phases): the [quality consolidation spec](docs/specs/hiring-platform-quality-consolidation.md) (RF-00–RF-09) is in progress, slice 0 only, with no criterion verified; the [embedding coverage report](docs/specs/embedding-coverage-report.md) is implemented and locally verified but awaits independent Code, Security and QA verdicts. Largest unblock: provision a disposable Atlas test cluster, which alone closes the open parts of Phases 8–10.

Phases 1–7 retain their existing scope and acceptance gates. The revised order prioritizes MongoDB and AI features before further Spark tools; it does not declare full MVP complete. Each capability includes pure FP architecture refinement using immutable ADTs, existing lifecycle State programs, typed errors and resource-owned interpreters. See [development milestones](docs/development-milestones.md) for dependencies, pattern adoption conditions and acceptance criteria.

## Future: Big Data & Analytics
Operational (MongoDB, low-latency GraphQL) and analytical workloads are kept separate. The local batch [analytics design](docs/big-data-architecture.md) and [implementation specification](docs/specs/hiring-analytics-lakehouse.md) describe the current Kafka-to-Delta path, retained data limits, recovery, and data-quality boundaries. Structured Streaming is in local re-verification (Phase 7 open); MongoDB optimization, search evaluation, AI discovery and Kafka workflows come next, before further analytics refinement and production hardening. The [development milestones](docs/development-milestones.md) place DDIA-informed performance, recovery, maintenance, and audit refinements in their owning phases. The discovery stage requires Atlas for Atlas Search and Vector Search; local MongoDB Community remains suitable for core workflows and transaction tests. This roadmap does not provision Atlas or include its cost.

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

Phase 8 now includes scoped application/history and nested reads, minimized authoritative search eligibility, ordinary index verification/recovery tests, and reproducible operational and authenticated structured HTTP measurement suites. The [Phase 8 specification](docs/specs/mongodb-vector-retrieval-optimization.md) records criterion-level results, the [first iteration latency plan](docs/specs/mongodb-vector-retrieval-optimization.md#first-iteration-latency-plan), and selected local write/storage overhead ceilings. Extended measurements distinguish warmed timings, completed writes, actual query-plan choices and scoped resource samples; generated evidence stays in ignored `.local/data/`. Atlas-dependent criteria are split into `.L` rows, run on Atlas Local (`compose.atlas-local.yaml`, loopback, $0; evidence pending on current HEAD), and `.C` rows, run on an authorized disposable cloud Atlas (deferred). Loopback results are labelled `local-container` and are not Atlas acceptance. Skipped live tests are not passes. These local changes do not close paired optimization acceptance or later embedding architecture work.

### Local discovery and interview scheduling

Local geographic discovery accepts optional job coordinates, exposes `nearbyJobs` with distance and bound cursors, and computes exact structured `jobDiscoveryFacets` before pagination. `jobDiscoveryFacets` also accepts an optional text `query` (HDQ-12, implemented locally; Atlas Local evidence pending). Its `coverage` field labels `ALL_MATCHES` (every match counted) or `TOP_MATCHES` (top 1000 lexical matches; not a total). Existing coordinate-free jobs remain valid. See [discovery contracts and evidence](docs/specs/hiring-discovery-search-quality.md). Human relevance labels await review and personalization remains deferred.

Interview scheduling is opt-in with `INTERVIEW_ENABLED=true` and separate configured Kafka command publisher, result publisher and fencer credentials. `scheduleInterview` persists a workflow before reserving the local durable fake calendar, committing the guarded hiring transition and notifying both participants. Inspect progress through `interviewWorkflow`; Admin repair requires expected revision and idempotency. Stop incompatible writers before applying new operational migrations. See [workflow contracts and local evidence](docs/specs/durable-hiring-workflows.md). Completion and deployment readiness remain subject to its validation gates.

The [embedding and workflow boundary specification](docs/specs/embedding-workflow-boundary-reliability.md) tracks finite provider vectors, independent cleanup subject progress, explicit worker diagnostics and reuse of the pure application lifecycle for interview commits. Its functional follow-ups add bounded subject-ID cleanup sweeps with per-record failure isolation, pure interview message/admission decisions, validated canonical candidate filters, injectable scheduling effects, byte-preserving cursor encoding and named workflow/ranking/facet values. Admin repair reconciles existing effects without reopening released reservations or extending deadlines. Public contracts and stored schemas remain unchanged; follow-up acceptance is tracked separately from historical checks.

For the local Compose broker, enable Kafka and use `KAFKA_INTERVIEW_ORCHESTRATOR_USERNAME=interview_command_publisher`, `KAFKA_INTERVIEW_WORKER_USERNAME=interview_result_publisher` and `KAFKA_INTERVIEW_FENCER_USERNAME=interview_fencer`. Supply their distinct `KAFKA_INTERVIEW_ORCHESTRATOR_PASSWORD`, `KAFKA_INTERVIEW_WORKER_PASSWORD` and `KAFKA_INTERVIEW_FENCER_PASSWORD` values through ignored local configuration; Compose uses those same password variables to provision its principals. The existing Kafka publisher/consumer configuration is still required. Use the configured local plaintext protocol only with loopback bootstrap addresses. Scheduling timestamps use UTC milliseconds; intervals that collapse at that precision are rejected. Executable scheduling operations are in `src/test/resources/graphql/interview-scheduling.graphql`. Cancellation and rescheduling use the mutations in `interview-cancellation.graphql` (see the [API reference](docs/api.md#interview-scheduling-cancellation-and-rescheduling)); cancelling an interview rejects the application through the existing `Interview → Rejected` transition.

### Workflow recovery maintenance cutover

Before activating transactional interview publication, stop all old interview publishers and revoke their topic-write credentials. The Compose fixture replaces the old `interview_orchestrator`/`interview_worker` login identities; do not retain them on an existing broker. Migration `010_interview_workflow_attempts` preserves prior execution slots and queues uncertain executions for receipt lookup. Migration `011_interview_publication_fencing` reopens legacy cleanup proof, including previously complete rows, without restoring deleted data. Startup applies and verifies both before starting workers. Cleanup requires broker-confirmed fencing, durable CAS progress and fresh physical-retention barriers. This repository change does not perform that maintenance on a deployed environment. See [recovery and validation criteria](docs/specs/hiring-workflow-recovery.md).
