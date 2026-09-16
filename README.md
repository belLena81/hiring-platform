# Hiring Management Platform

Backend-first hiring management platform built with **Scala 3, Sangria GraphQL, Cats Effect, and MongoDB**.

A practical playground for functional Scala, GraphQL API design, MongoDB data modeling/query optimization, and AI-powered semantic search & candidate/job matching via MongoDB Vector Search.

## Agent-driven development

Start with `$product-manager` to coordinate specialist work and independent reviews. See [agent workflow and usage](docs/agent-development.md) and [project rules](AGENTS.md). Local skills cover Product Manager, Software Architect, Scala Developer, Data Engineer, Big Data Engineer, QA Engineer, Security Engineer, and Code Reviewer.

The stack described here is the target design. The current scaffold still uses Scala 2.13 and Doobie/PostgreSQL; migration is a separate implementation slice.

## Documentation

- [Architecture](ARCHITECTURE.md) and [development plan](INITIAL_DEVELOPMENT_PLAN.md)
- [Use cases](docs/use-cases.md) and [development milestones](docs/development-milestones.md)
- [MongoDB design](docs/mongodb-design.md) and [big data architecture](docs/big-data-architecture.md)
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

Dependencies point inward (`API → Application → Domain`); the domain layer stays free of Sangria, MongoDB, http4s, Circe, JWT, and AI-SDK dependencies.

Suggested module layout: `domain/` (model, error, service) → `application/` (port, service) → `infrastructure/` (mongo, ai, auth) → `api/graphql/` (schema, resolver, input, output, dataloader).

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
Operational (MongoDB, low-latency GraphQL) and analytical workloads are kept separate — historical domain events will feed an independent event-driven analytical pipeline (Kafka, Spark, Databricks / Delta Lake, and structured streaming) for large-scale hiring data analysis. The [big data design](docs/big-data-architecture.md) covers Bronze/Silver/Gold datasets, recovery, data quality, synthetic workloads, and reproducible search evaluation.

## Engineering Focus

1. Production-style GraphQL API design with Sangria
2. Functional architecture with Cats Effect
3. Concurrent/streaming workflows with FS2
4. MongoDB modeling by access pattern + compound-index design
5. N+1 prevention & batching, cursor-based pagination
6. Domain modeling & state transitions
7. Vector & hybrid semantic search
8. Practical RAG/AI integration
