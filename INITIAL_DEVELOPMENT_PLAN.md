# Hiring Management Platform — Development Plan

Target roadmap; see [README](README.md) for implemented status, [use cases](docs/use-cases.md) for workloads, and [development milestones](docs/development-milestones.md) for phase acceptance criteria.

Phase 1 contracts, bounded implementation slices, and local evidence are tracked in the [Foundation specification](docs/specs/phase-1-foundation.md). Its runtime is health-only; the domain and MongoDB hiring repositories remain Phase 2 work.

## 1. Project Goal

Build a backend-first hiring platform as a practical showcase of advanced:

- Scala 3
- Cats Effect
- FS2
- Sangria GraphQL
- MongoDB data modeling and performance optimization
- MongoDB Vector Search
- Kafka
- Apache Spark
- Databricks / Delta Lake
- distributed and event-driven architecture

The project is intentionally designed around measurable workloads rather than technology-driven features.

The development process follows:

```text
Use Case
   ↓
GraphQL Operation
   ↓
MongoDB Access Pattern
   ↓
Document Model + Index
   ↓
Domain Event
   ↓
Kafka
   ↓
Spark / Delta
   ↓
Business / Quality Metric
```

---

# 2. Core Domain

## Roles

### Candidate

Can:

- search jobs
- use semantic/hybrid search
- view jobs
- apply to jobs
- track applications
- receive job recommendations

### Recruiter

Can:

- create/manage jobs
- review applications
- change application status
- search/match candidates semantically

### Admin

Can:

- access system-wide analytics
- analyze hiring funnels
- analyze time-to-hire
- analyze skill demand and trends

Account lifecycle and access management are defined in Phase 4.5: the first account is an atomic singleton Admin bootstrap, later accounts sign up as Candidate or Recruiter, and every authenticated user can delete only their own account.

## User Account Management Capability

The account flow is intentionally separate from hiring-domain profile completion:

- the first account is created through a one-time Admin bootstrap; ordinary signup opens only after bootstrap completes
- signup requires only a unique name, role, and password
- email confirmation is out of scope for the initial flow
- Candidate and Recruiter profile fields are completed after signup through an authenticated self-service mutation
- login returns a short-lived signed access token; stored user data, not token role claims, determines authorization
- an Admin may query bounded non-secret user summaries but cannot delete another user
- account deletion is a transactional logical deletion that removes credentials and profile data, closes owned open jobs, preserves applications/history, and prevents future authentication

The detailed contracts, query shapes, MongoDB indexes, concurrency cases, and acceptance criteria are maintained in [Phase 4.5 User Account Management](docs/specs/user-account-management.md).

---

# 3. Core Application Lifecycle

```text
CREATED
   │
   ├──► ACCEPTED ──► INTERVIEW ──► HIRED
   │                       │
   │                       └──► REJECTED
   │
   ├──► DECLINED
   │
   └──► REJECTED
```

Every status change produces immutable historical information.

Invalid transitions are rejected by domain logic.

Job aggregate state changes use `cats.data.State` for pure transition logic. Services supply authorization, time, IDs, persistence, and event handoff around that pure state program; `State` must not hide effects.

---

# 4. MVP Use Cases

## Operational

### UC01 — Structured Job Search

Candidate searches open jobs by:

```text
status
location
skills
date
```

with cursor pagination.

Primary concerns:

- MongoDB compound indexes
- filter selectivity
- cursor pagination
- query-plan analysis

Target:

```text
p95 < 150 ms
```

---

### UC02 — Semantic / Hybrid Job Search

Candidate searches using natural language:

```text
"functional Scala backend role working
with distributed systems"
```

Pipeline:

```text
Query
  ↓
Embedding
  ↓
MongoDB Vector Search
  +
Metadata Filters
  ↓
Ranked Jobs
```

Metrics:

```text
Precision@K
Recall@K
MRR
NDCG@K
CTR
search → application conversion
```

Target:

```text
p95 < 500 ms
```

---

### UC03 — View Job

Candidate retrieves complete job information.

GraphQL relationships include recruiter information resolved through batching.

Metrics:

```text
p95 latency
views/job
search → view conversion
view → application conversion
```

Target:

```text
p95 < 100 ms
```

---

### UC04 — Submit Application

Candidate applies to an open job.

Rules:

- candidate authenticated
- job exists
- job is OPEN
- duplicate application forbidden
- initial status = CREATED

Database invariant:

```text
UNIQUE(candidateId, jobId)
```

Writes:

```text
Application
    +
ApplicationEvent
```

must be atomic.

Target:

```text
p95 < 200 ms
```

---

### UC05 — Track Applications

Candidate lists own applications by status using cursor pagination.

Access pattern:

```text
candidateId = current user
status = ?
ORDER BY createdAt DESC, _id DESC
```

Target:

```text
p95 < 150 ms
```

---

### UC06 — Job Recommendations

Recommend open jobs based on candidate profile embedding.

```text
Candidate Profile
       ↓
Embedding
       ↓
Vector Search
       ↓
Recommended Jobs
```

Metrics:

```text
Recall@K
NDCG@K
CTR
recommendation → application conversion
```

---

### UC07 — Manage Jobs

Recruiter can:

```text
createJob
updateJob
closeJob
```

Authorization is ownership-based.

Job content changes trigger asynchronous embedding regeneration.

Target mutation latency:

```text
p95 < 200 ms
```

---

### UC08 — Review Applications

Recruiter lists applications for an owned job:

```text
jobId
status
cursor
```

Candidate relationships are batch-loaded.

Primary concerns:

```text
MongoDB index efficiency
GraphQL N+1 prevention
DataLoader batch efficiency
```

Target:

```text
p95 < 150 ms
no N+1 queries
```

---

### UC09 — Change Application Status

Recruiter executes action-oriented mutations:

```text
acceptApplication
declineApplication
moveApplicationToInterview
rejectApplication
hireApplication
```

Transaction:

```text
validate transition
       ↓
update application
       +
append application event
```

Target:

```text
p95 < 200 ms
```

---

### UC10 — Candidate Matching

Recruiter finds candidates relevant to an owned job.

```text
Job Embedding
     ↓
Vector Search
     ↓
Candidate Profiles
     ↓
Ranked Candidates
```

Metrics:

```text
Recall@K
MRR
NDCG@K
result → interview conversion
```

---

# 5. Big Data Use Cases

Only workloads that naturally require historical/distributed processing are included.

## UC11 — Domain Event Pipeline

Operational events are published to Kafka.

Important events:

```text
JOB_CREATED
JOB_UPDATED
JOB_CLOSED
JOB_VIEWED

SEARCH_PERFORMED
SEARCH_RESULT_CLICKED

APPLICATION_CREATED
APPLICATION_STATUS_CHANGED
CANDIDATE_HIRED
```

Delivery model:

```text
at-least-once
      +
idempotent consumers
```

Metrics:

```text
throughput
consumer lag
duplicates
failed events
end-to-end latency
```

---

## UC12 — Hiring Funnel Analytics

Spark calculates:

```text
Applications
     ↓
Accepted
     ↓
Interview
     ↓
Hired
```

Metrics:

```text
application → interview rate
interview → hire rate
rejection rate
hire rate
```

Dimensions:

```text
job
recruiter
location
skill
time
```

---

## UC13 — Time-to-Hire Analytics

Spark reconstructs application timelines:

```text
CREATED
   │ 2 days
   ▼
ACCEPTED
   │ 4 days
   ▼
INTERVIEW
   │ 7 days
   ▼
HIRED
```

Calculate:

```text
timeToAccept
timeToInterview
timeToHire
interviewToHire
```

Statistics:

```text
median
P75
P90
P95
```

This workload demonstrates Spark window functions and event reconstruction.

---

## UC14 — Skill Demand Analytics

Analyze historical job data to identify:

```text
popular skills
fast-growing skills
skill combinations
skills by location
skills over time
```

Example:

```text
Scala

2025 Q1   8,300 jobs
2025 Q2   9,100
2025 Q3   9,800
2025 Q4  10,600
2026 Q1  11,400
```

This provides a natural large-scale aggregation workload.

---

## UC15 — Search Quality Evaluation

Evaluate:

```text
Keyword
   vs
Vector
   vs
Hybrid Search
```

against a versioned relevance dataset.

Spark calculates:

```text
Precision@K
Recall@K
MRR
NDCG@K
```

Experiments record:

```text
datasetVersion
embeddingModel
embeddingVersion
retrievalStrategy
searchParameters
metrics
timestamp
```

Search quality must be measured rather than assumed.

---

# 6. Architecture

```text
                         CLIENT
                           │
                           ▼
                  ┌─────────────────┐
                  │     Sangria     │
                  │     GraphQL     │
                  └────────┬────────┘
                           │
                           ▼
                  Application Services
                     Cats Effect
                           │
                ┌──────────┴──────────┐
                ▼                     ▼
            MongoDB                 Kafka
             OLTP                     │
                │                     ▼
                │             Spark Structured
                │                Streaming
                │                     │
                │                     ▼
                │                 Delta Lake
                │                     │
                │             ┌───────┼───────┐
                │             ▼       ▼       ▼
                │          Bronze   Silver   Gold
                │
                ▼
        MongoDB Vector Search
```

---

# 7. Clean Architecture

Use Ports and Adapters.

```text
GraphQL
   │
   ▼
Application
   │
   ▼
Domain
   ▲
   │
Infrastructure
```

Dependencies point inward.

Suggested structure:

```text
domain/
  model/
  error/
  service/

application/
  port/
  service/

infrastructure/
  mongo/
  kafka/
  ai/

api/
  graphql/
```

Infrastructure is exposed through ports:

```scala
trait JobRepository[F[_]]
trait ApplicationRepository[F[_]]
trait UserRepository[F[_]]

trait EmbeddingService[F[_]]
trait SemanticSearch[F[_]]

trait EventPublisher[F[_]]
```

Domain code must not depend on:

```text
MongoDB
Sangria
http4s
Kafka
Spark
external AI SDKs
```

---

# 8. Cats Effect / FS2

Cats Effect manages:

- effects
- resources
- concurrency
- cancellation
- error handling
- application lifecycle

`Resource` manages:

```text
MongoClient
Kafka producer/consumer
HTTP server
AI client
```

FS2 handles:

- asynchronous events
- embedding generation
- streaming workflows
- bounded parallelism
- backpressure

Example:

```text
Job Updated
    ↓
FS2 Queue
    ↓
parEvalMap(N)
    ↓
Embedding API
    ↓
MongoDB
```

---

# 9. MongoDB Collections

Initial collections:

```text
users
jobs
applications
application_events
```

Avoid introducing collections without a concrete access pattern.

---

## users

```text
_id
role
name
email

profile {
  skills
  experienceSummary
  resumeRef
}

embedding
embeddingMeta

createdAt
updatedAt
```

Patterns:

```text
Polymorphic
Embedded Document
Embedded Vector
```

---

## jobs

```text
_id
recruiterId

title
description
requirements[]
skills[]

location {
  country
  city
  remote
}

status

embedding
embeddingMeta

createdAt
updatedAt
```

Patterns:

```text
Reference
Embedded Document
Embedded Vector
```

---

## applications

```text
_id
candidateId
jobId
status
feedback
createdAt
updatedAt
```

Pattern:

```text
Reference
```

Applications remain independent because Candidate ↔ Job is a high-cardinality relationship.

---

## application_events

```text
_id
applicationId

previousStatus
newStatus
changedBy

reason
feedback

createdAt
```

Pattern:

```text
Unbounded Data Separation
```

History is not embedded into `applications`.

---

# 10. MongoDB Modeling Rules

### Embed when data is:

- bounded
- owned by one aggregate
- normally read with its parent
- independently meaningless

### Reference when data:

- has independent lifecycle
- is high-cardinality
- changes independently
- requires independent pagination

Default:

```text
minimal duplication
```

Do not use Extended Reference or Subset patterns initially.

Introduce controlled denormalization only when measurements show a real read bottleneck.

---

# 11. Index Strategy

Indexes are derived from complete access patterns.

Process:

```text
GraphQL Query
      ↓
Mongo Query Shape
      ↓
Filter + Sort
      ↓
Compound Index
      ↓
explain("executionStats")
```

Example:

```text
UC08:

jobId = ?
status = ?
ORDER BY createdAt DESC, _id DESC
```

Index:

```javascript
{
  jobId: 1,
  status: 1,
  createdAt: -1,
  _id: -1
}
```

Candidate applications:

```javascript
{
  candidateId: 1,
  status: 1,
  createdAt: -1,
  _id: -1
}
```

Recruiter jobs:

```javascript
{
  recruiterId: 1,
  createdAt: -1,
  _id: -1
}
```

Application history:

```javascript
{
  applicationId: 1,
  createdAt: -1,
  _id: -1
}
```

Domain invariant:

```javascript
UNIQUE {
  candidateId: 1,
  jobId: 1
}
```

Avoid speculative single-field indexes.

---

# 12. Pagination

Do not use deep:

```text
skip(N)
```

Use keyset pagination:

```text
ORDER BY

createdAt DESC
_id DESC
```

Cursor:

```text
(createdAt, _id)
```

Fetch:

```text
requestedLimit + 1
```

to calculate `hasNextPage`.

---

# 13. GraphQL N+1 Strategy

Nested GraphQL relationships must use request-scoped batching.

Instead of:

```text
100 applications
     ↓
100 candidate queries
```

use:

```text
candidateIds
     ↓
MongoDB

_id IN [...]
```

Batch loaders should provide:

- batching
- request-local caching
- deduplication

Storage should not be denormalized merely to compensate for inefficient GraphQL resolution.

---

# 14. Vector Search

Vectors remain with the entity they represent.

```text
Job
 └── embedding

Candidate
 └── embedding
```

Embedding metadata:

```text
model
version
sourceHash
updatedAt
```

`sourceHash` prevents unnecessary regeneration.

Structured fields such as:

```text
status
location
skills
role
```

remain normal MongoDB fields.

They are not replaced by embeddings.

---

# 15. Event Architecture

Canonical event:

```text
eventId
eventType
schemaVersion
occurredAt

aggregateId

payload
```

Application events use:

```text
Kafka partition key = applicationId
```

when lifecycle ordering matters.

Consumers must tolerate:

```text
duplicate events
retries
late events
consumer restarts
```

Kafka slices must follow an event-driven architecture pattern: operational services persist current state in MongoDB, record a durable event/outbox entry in the same reliable boundary where required, and publish to Kafka asynchronously. Kafka availability must not block OLTP writes. Consumers are at-least-once, idempotent, replayable, and version-aware.

Future cross-boundary workflows must use the Saga pattern when they coordinate MongoDB state with Kafka, embeddings, Vector Search, notifications, scheduling, external storage, or other systems that cannot share one transaction. Keep Phase 2 domain writes as MongoDB transactions; introduce Saga only in a later vertical slice with durable Saga state, idempotency keys, bounded retries, compensation or explicit non-reversible steps, and observable repair paths.

The strongest Saga demonstration for this app is a real hiring workflow with meaningful partial failures, such as application submission enrichment, interview scheduling, job closing with bulk downstream effects, outbox publication repair, or candidate/job profile reindexing.

---

# 16. Lakehouse

Use Databricks + Delta Lake.

```text
Kafka
  ↓
BRONZE
  ↓
SILVER
  ↓
GOLD
```

### Bronze

Raw immutable events.

Purpose:

```text
replay
audit
recovery
debugging
```

### Silver

Validated, deduplicated, enriched data.

### Gold

Business-oriented analytical datasets:

```text
hiring_funnel_metrics
time_to_hire_metrics
skill_demand_trends
search_quality_experiments
```

Gold must be rebuildable from lower layers.

---

# 17. Streaming Semantics

Spark Structured Streaming must explicitly demonstrate:

- event time
- processing time
- watermarks
- late events
- checkpointing
- deduplication
- restart recovery

Do not claim global exactly-once processing.

Prefer:

```text
at-least-once ingestion
       +
idempotent processing
```

with explicit guarantees at each boundary.

---

# 18. Data Quality

Pipeline:

```text
Raw
 │
 ▼
Validation
 ├──────────► Quarantine
 │
 ▼
Silver
```

Validate:

```text
schema
eventId
required IDs
timestamps
status values
status transitions
duplicates
```

Track:

```text
recordsReceived
recordsAccepted
recordsRejected
duplicateEvents
lateEvents
invalidSchema
```

---

# 19. Performance Engineering

Performance is part of the implementation, not a final optimization phase.

## MongoDB

Measure:

```text
executionTimeMillis
totalKeysExamined
totalDocsExamined
nReturned
```

using:

```text
explain("executionStats")
```

## GraphQL

Measure:

```text
p50
p95
p99

queries/request
DataLoader batch sizes
error rate
```

## Kafka

Measure:

```text
throughput
consumer lag
processing latency
failed events
```

## Spark

Inspect:

```text
physical plans
partitions
shuffle read/write
data skew
join strategies
predicate pushdown
partition pruning
spill
task duration
```

## Search

Measure:

```text
Precision@K
Recall@K
MRR
NDCG@K

search latency
CTR
conversion
```

---

# 20. Large-Scale Dataset

Provide deterministic synthetic-data generation.

Target scale for performance experiments:

```text
1M candidates
500K jobs
20M applications
100M+ events
```

Normal development uses much smaller datasets.

Large datasets exist to test:

- MongoDB indexing
- Spark partitioning
- distributed joins
- shuffle behavior
- streaming throughput
- Vector Search
- retrieval evaluation

---

# 21. Technology Stack

| Area | Technology |
|---|---|
| Language | Scala 3 |
| Functional Effects | Cats Effect 3 |
| Functional Streaming | FS2 |
| GraphQL | Sangria |
| HTTP | http4s |
| JSON | Circe |
| OLTP Database | MongoDB |
| Mongo Driver | MongoDB Scala Driver |
| Semantic Search | MongoDB Atlas Vector Search |
| Event Streaming | Apache Kafka |
| Distributed Processing | Apache Spark |
| Streaming Analytics | Spark Structured Streaming |
| Lakehouse | Databricks |
| Storage Format | Delta Lake / Parquet |
| Testing | MUnit + Cats Effect |
| Integration Testing | Testcontainers |
| Local Infrastructure | Docker Compose |
| Logging | SLF4J + Logback |

---

# 22. Development Order

Implement the phases and acceptance criteria in [Development Milestones](docs/development-milestones.md). Start with Foundation, then Domain + MongoDB. Each slice must be runnable and tested; do not implement the full architecture at once.
