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

### Local analytics erasure worker

Run the API at least once so MongoDB migrations and analytics control collections are initialized. Keep the API process running, then start the worker in another terminal:

```bash
docker compose up -d mongodb kafka
# In one terminal, start the API with `sbt run`.
docker compose --profile analytics-erasure up -d --build analytics-erasure-worker
docker compose logs -f analytics-erasure-worker
```

The worker shares `.local/data/analytics/` with batch runs. It requires the same HMAC secret and Kafka reader credentials as the batch, plus `KAFKA_FENCER_PASSWORD` in the ignored root `.env` for a principal scoped to the publisher transactional-ID prefix. `deleteMyAccount` commits the deletion and returns a durable receipt with `PENDING`; query `accountDeletionStatus(receiptId: "<receipt-id>")` to check for `COMPLETE`. The worker fences every publisher process recorded by the deletion transaction before it captures the Kafka barrier or purges outbox rows. It may then wait through Kafka retention and Delta's seven-day vacuum safety horizon before verifying snapshots, rebuilding the report, and atomically publishing the snapshot and completion ledger. A completed receipt remains queryable after the request marker expires. The current transactional publisher uses `hiring_publisher_v2` with `KAFKA_PUBLISHER_V2_PASSWORD`; the pre-transactional `publisher` identity has its topic, cluster-idempotence, and transactional-ID ACLs removed by ACL initialization. The regular local broker was cut over on 2026-09-25: its KRaft data was preserved in `.local/data/kafka`, its legacy PLAIN login was removed, and its stale ACL grants were revoked. The scoped regular-broker check verifies v2 publisher writes and fencing, legacy authentication rejection, and reader denials. Full worker-to-deletion composition and key retirement remain open. See the [analytics specification](docs/specs/hiring-analytics-lakehouse.md).

`GET /health` reports application liveness; `GET /ready` reports MongoDB connectivity. `POST /graphql` accepts `{"query":"{ health { status } readiness { status } }"}`. MongoDB outages leave HTTP running and readiness reports `NOT_READY`. `GET /schema.graphql` exports the current schema; GraphQL introspection supports API documentation/testing clients. See the [API reference](docs/api.md).

See the [API reference](docs/api.md) and the [current reset specification](docs/specs/pre-mvp-contract-reset.md) for the active contract and verification evidence.

See [diagnostic logging](docs/logging.md) for searchable markers, request tracing, masked defaults, and explicitly gated local payload capture.

For project-only sbt JDK selection, create an ignored `.sbtopts` file in this root with `-java-home /path/to/jdk-17` (this machine: `/usr/lib/jvm/java-17-openjdk-amd64`). The installed sbt launcher reads it for commands such as `sbt compile` and `sbt test`; it does not change your shell's Java or other projects. Keep machine-specific paths out of Git. IntelliJ's JDK settings remain separate. The local-check script's Java prerequisite check still uses `JAVA_HOME`, so set that variable for the script as described below.

Use Java 17+ and sbt 1.11.1. Set `JAVA_HOME` to your installed JDK when the default Java is older, then run:

```bash
bash scripts/check-local.sh
```

The command validates local skills and runs the Docker-independent MUnit suite. Run `sbt 'IntegrationTest / test'` separately for real HTTP lifecycle and disposable MongoDB tests; Docker is required for the database tests. These commands are local checks, not deployment or performance certification.

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

Current module layout: `api/` (GraphQL, HTTP, auth adapters) → `service/` (protocols and job/application/search use cases) → `repository/` (protocols and Mongo implementation) → `domain/` (model, error, pure policy) plus `shared/` utilities and DTOs.

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
- `[~] Phase 6 — Lakehouse + Spark Batch Analytics:` local Bronze/Silver/Gold processing and Admin reporting; erasure, publication, retention, recovery, and full source-to-projection evidence remain open in the [lakehouse specification](docs/specs/hiring-analytics-lakehouse.md).
- `[ ] Phase 7 — Structured Streaming:` continuous processing, checkpoint/restart behavior, late-event handling, and bounded state.
- `[ ] Phase 8 — Search Evaluation:` reproducible relevance datasets and measured keyword/vector/hybrid search quality.
- `[ ] Phase 9 — Scale:` workload-backed capacity, latency, throughput, and storage tuning.
- `[~] Phase 10 — Observability & Resilience:` structured diagnostics and runtime telemetry exist; end-to-end failure, recovery, and operational evidence remain open.
- `[ ] Phase 11 — Production Hardening:` deployment controls, security review, recovery procedures, and production readiness evidence.
- `[ ] Phase 12 — Discovery Intelligence & Search Quality (Post-MVP):` radius discovery, richer Atlas search/facets, and evidence-backed ranking extensions.

## Future: Big Data & Analytics
Operational (MongoDB, low-latency GraphQL) and analytical workloads are kept separate. The local batch [analytics design](docs/big-data-architecture.md) and [implementation specification](docs/specs/hiring-analytics-lakehouse.md) describe the current Kafka-to-Delta path, retained data limits, recovery, and data-quality boundaries. Structured Streaming, Search Evaluation, Scale, Production Hardening, and Discovery Intelligence & Search Quality remain later milestones. The final discovery stage requires Atlas for Atlas Search and Vector Search; local MongoDB Community remains suitable for core workflows and transaction tests. This roadmap does not provision Atlas or include its cost.

## Engineering Focus

1. Production-style GraphQL API design with Sangria
2. Functional architecture with Cats Effect
3. Concurrent/streaming workflows with FS2
4. MongoDB modeling by access pattern + compound-index design
5. N+1 prevention & batching, cursor-based pagination
6. Domain modeling & state transitions
7. Vector & hybrid semantic search
8. Practical RAG/AI integration
