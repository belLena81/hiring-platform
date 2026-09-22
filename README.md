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
```

Kafka publishes to `hiring.operational-events` with seven-day broker retention. MongoDB readiness and HTTP startup do not depend on Kafka availability; operational mutations write a transactional Mongo outbox first and the background publisher retries broker delivery.

### Local analytics batch

The analytics batch is an opt-in, one-shot Compose profile. It reads one explicit Kafka partition/offset range, writes local Delta data under the ignored `.local/data/analytics/` directory, and exits. It is not a streaming daemon and does not run with the default application stack.

```bash
docker compose up -d kafka
ANALYTICS_RUN_ID=local-001 \
  HIRING_ANALYTICS_HMAC_SECRET_BASE64="[REDACTED_SECRET]" \
  ANALYTICS_PARTITION=0 \
  ANALYTICS_START_OFFSET=0 \
  ANALYTICS_END_OFFSET_EXCLUSIVE=100 \
  docker compose --profile analytics run --rm analytics-batch
```

The profile uses Kafka's internal `kafka:9092` listener. Local host clients continue to use `127.0.0.1:9092`. Choose a range that exists in the local broker; the batch validates neither a live report publication nor account-erasure processing. See the [analytics specification](docs/specs/hiring-analytics-lakehouse.md) for the retention and release boundaries.

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

- [Architecture](ARCHITECTURE.md) and [use cases](docs/use-cases.md)
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
- Semantic job search (natural language) + hybrid filter/vector search
- Candidate ↔ Job recommendations, resume/profile semantic matching
- Similar job detection, AI-assisted job description analysis
- Explainable matching, RAG-based recruiter assistant over authorized hiring data
- Embeddings generated asynchronously alongside domain data

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
Lakehouse / Spark Analytics
   ↓
Structured Streaming
   ↓
Search Evaluation & Scale
```

**Phase 1 — Foundation:** app skeleton, Resource lifecycle, health query, Mongo connection, basic GraphQL schema.
**Phase 2 — Domain + MongoDB:** core domain models (User, Job, Application, ApplicationStatus, ApplicationEvent) and first use cases (job/application CRUD & lifecycle).

## Future: Big Data & Analytics
Operational (MongoDB, low-latency GraphQL) and analytical workloads are kept separate. The local batch [analytics design](docs/big-data-architecture.md) and [implementation specification](docs/specs/hiring-analytics-lakehouse.md) describe the current Kafka-to-Delta path, retained data limits, recovery, and data-quality boundaries. Structured Streaming, cloud deployment, and search evaluation remain later work.

## Engineering Focus

1. Production-style GraphQL API design with Sangria
2. Functional architecture with Cats Effect
3. Concurrent/streaming workflows with FS2
4. MongoDB modeling by access pattern + compound-index design
5. N+1 prevention & batching, cursor-based pagination
6. Domain modeling & state transitions
7. Vector & hybrid semantic search
8. Practical RAG/AI integration
