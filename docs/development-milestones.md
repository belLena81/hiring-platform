# Development Milestones

Target design and acceptance criteria; see [core architecture](../ARCHITECTURE.md) and [development plan](../INITIAL_DEVELOPMENT_PLAN.md). These are not claims of implemented features.

# 22. Development Phases & Milestones

Development proceeds in vertical slices. Each phase must leave the system runnable and testable.

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
Spark Analytics
    ↓
Structured Streaming
    ↓
Search Evaluation
    ↓
Scale & Performance
    ↓
Production Hardening
```

---

## Phase 1 — Foundation

### Goal

Create the minimal production-shaped application skeleton.

### Implement

```text
Scala 3
Cats Effect
http4s
Sangria
MongoDB
Circe
Docker Compose
```

Establish:

- Clean Architecture module boundaries
- dependency injection through constructors
- Cats Effect `Resource` lifecycle
- configuration loading
- structured logging
- MongoDB connection management
- GraphQL endpoint
- GraphiQL
- health/readiness queries

### Milestone

```graphql
query {
  health {
    status
  }
}
```

runs against a fully resource-managed application.

### Done When

- application starts through `IOApp`
- MongoDB lifecycle is managed by `Resource`
- GraphQL endpoint works
- Docker environment starts locally
- unit/integration test infrastructure exists
- domain layer has no infrastructure dependencies

---

# Phase 2 — Domain Model + MongoDB

### Goal

Implement the transactional hiring domain before optimizing GraphQL.

### Implement

Core models:

```text
User
CandidateProfile
Job
Application
ApplicationStatus
ApplicationEvent
```

Ports:

```scala
UserRepository[F[_]]
JobRepository[F[_]]
ApplicationRepository[F[_]]
```

MongoDB adapters:

```text
MongoUserRepository
MongoJobRepository
MongoApplicationRepository
```

Implement use cases:

```text
UC03 — View Job
UC04 — Submit Application
UC05 — Track Applications
UC07 — Manage Jobs
UC08 — Review Applications
UC09 — Change Application Status
```

### Domain Rules

Implement explicit:

- ownership
- valid status transitions
- duplicate-application prevention
- closed-job validation
- rejection feedback
- decline reason

Use a unique database constraint:

```text
(candidateId, jobId)
```

for duplicate applications.

Use transactions for:

```text
Application state update
        +
ApplicationEvent append
```

### Milestone

A complete application can move:

```text
CREATED
   ↓
ACCEPTED
   ↓
INTERVIEW
   ↓
HIRED
```

while preserving consistent application history.

### Done When

- domain rules have unit tests
- repositories have Testcontainers integration tests
- duplicate applications are prevented by MongoDB
- invalid transitions are rejected
- application/history writes are atomic

---

# Phase 3 — GraphQL API + MongoDB Performance

### Goal

Expose the operational domain through a production-style GraphQL API and optimize MongoDB around real access patterns.

### Implement

```text
queries
mutations
input types
typed payloads
typed errors
RBAC
cursor pagination
nested resolvers
request-scoped batching
```

Implement:

```text
UC01 — Structured Job Search
```

Complete GraphQL exposure for Phase 2 use cases.

### GraphQL Performance

Prevent:

```text
1 applications query
+
N candidate queries
+
N job queries
```

through request-scoped batching.

### MongoDB

For every major GraphQL operation document:

```text
GraphQL Operation
       ↓
Mongo Query Shape
       ↓
Compound Index
       ↓
explain("executionStats")
```

Measure:

```text
executionTimeMillis
totalDocsExamined
totalKeysExamined
nReturned
```

### Milestone

Operational GraphQL API supports the complete basic hiring workflow with indexed cursor-based queries and no N+1 behavior.

### Done When

- authorization is enforced outside resolvers
- all list operations have bounded pagination
- no deep `skip(N)` pagination
- critical access patterns have documented indexes
- representative queries have `explain()` results
- GraphQL integration tests cover authorization and pagination

---

# Phase 4 — Vector & Hybrid Search

### Goal

Introduce AI functionality through measurable information-retrieval features.

### Implement

```text
EmbeddingService[F[_]]
SemanticSearch[F[_]]
```

Store embeddings for:

```text
jobs
candidate profiles
```

Store metadata:

```text
model
version
sourceHash
updatedAt
```

Implement:

```text
UC02 — Semantic / Hybrid Job Search
UC06 — Job Recommendations
UC10 — Candidate Matching
```

### Embedding Pipeline

```text
Entity Changed
      ↓
FS2 Queue
      ↓
bounded parEvalMap
      ↓
Embedding Service
      ↓
MongoDB
```

Embedding generation must not block normal mutations.

### Search Modes

Support:

```text
FILTER
VECTOR
HYBRID
```

as independently measurable strategies.

### Milestone

The same job-search request can be executed through lexical/structured and semantic retrieval strategies.

### Done When

- embedding generation is asynchronous
- stale embeddings can be detected through `sourceHash`
- vector queries support metadata filtering
- search model/version is observable
- integration tests cover vector-search behavior

---

# Phase 5 — Event-Driven Architecture

### Goal

Decouple operational workloads from analytics.

### Implement

Apache Kafka.

Define canonical event envelope:

```text
eventId
eventType
schemaVersion
occurredAt
aggregateId
payload
```

Publish:

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

### Reliability

Design explicitly for:

```text
at-least-once delivery
        +
idempotent consumers
```

Application lifecycle events use:

```text
partition key = applicationId
```

when ordering matters.

### Critical Architecture Requirement

Avoid an unsafe dual write:

```text
MongoDB write
     +
Kafka publish
```

where one can succeed while the other fails.

Introduce a transactional/event outbox strategy or equivalent reliable publication mechanism.

### Milestone

A successful domain operation eventually produces exactly one logical analytical event despite retries and duplicate delivery.

### Done When

- operational writes do not depend on Kafka availability
- events can be retried
- consumers are idempotent
- duplicate delivery is tested
- ordering assumptions are documented
- event schemas are versioned
- publication failures are observable

---

# Phase 6 — Databricks Lakehouse

### Goal

Create a durable analytical representation of historical platform activity.

### Architecture

```text
Kafka
  ↓
Bronze
  ↓
Silver
  ↓
Gold
```

### Bronze

Persist immutable raw events.

Responsibilities:

```text
ingestion
replay
audit
recovery
```

### Silver

Produce:

- validated
- deduplicated
- normalized
- enriched

domain events.

### Gold

Initial datasets:

```text
hiring_funnel_metrics
time_to_hire_metrics
skill_demand_trends
```

### Data Quality

Invalid records go to:

```text
quarantine
```

rather than silently disappearing.

### Milestone

A domain event can be traced:

```text
Mongo operation
      ↓
Kafka
      ↓
Bronze
      ↓
Silver
      ↓
Gold
```

### Done When

- Bronze events are immutable
- duplicate events do not duplicate Silver facts
- invalid events are quarantined
- Gold datasets are rebuildable
- pipeline lineage is understandable
- data-quality metrics are recorded

---

# Phase 7 — Spark Batch Analytics

### Goal

Use distributed computation for genuinely analytical workloads.

### Implement

```text
UC12 — Hiring Funnel
UC13 — Time-to-Hire
UC14 — Skill Demand
```

### Hiring Funnel

Calculate:

```text
Applications
     ↓
Accepted
     ↓
Interview
     ↓
Hired
```

by:

```text
time
job
recruiter
location
skill
```

### Time-to-Hire

Use Spark windows to reconstruct lifecycle transitions.

Calculate:

```text
timeToAccept
timeToInterview
timeToHire

median
P75
P90
P95
```

### Skill Trends

Calculate:

```text
job count / skill
growth / skill
skill combinations
skill / geography
skill / time
```

### Performance Engineering

Inspect:

```text
physical plans
partitions
shuffle
data skew
join strategy
predicate pushdown
partition pruning
spill
```

### Milestone

Gold analytical datasets can be rebuilt from historical events using reproducible Spark jobs.

### Done When

- Spark jobs have correctness tests
- partitioning strategy is documented
- important execution plans are analyzed
- skew/shuffle behavior is measured
- jobs are reproducible and idempotent

---

# Phase 8 — Spark Structured Streaming

### Goal

Demonstrate correct stateful stream-processing semantics rather than simply reading Kafka with Spark.

### Implement

```text
Kafka
   ↓
Spark Structured Streaming
   ↓
validation
   ↓
deduplication
   ↓
watermarking
   ↓
aggregation
   ↓
Delta
```

Explicitly handle:

```text
event time
processing time
late events
duplicate events
watermarks
checkpoints
restart recovery
```

### Metrics

Track:

```text
input rows/sec
processed rows/sec
consumer lag
batch duration
late events
rejected events
```

### Milestone

Near-real-time hiring metrics survive duplicate delivery, late events, and processor restarts without corrupting analytical results.

### Done When

- checkpoint recovery is tested
- duplicate-event behavior is tested
- late-event policy is documented
- watermark behavior is tested
- streaming state is observable

---

# Phase 9 — Search Quality Evaluation

### Goal

Turn semantic search from a feature into a measurable retrieval system.

Implement:

```text
UC15 — Search Quality Evaluation
```

### Dataset

Create a versioned relevance dataset:

```text
query
relevantJobIds
datasetVersion
```

Run:

```text
Keyword Search
      vs
Vector Search
      vs
Hybrid Search
```

### Spark Evaluation

Calculate:

```text
Precision@K
Recall@K
MRR
NDCG@K
```

Store experiments:

```text
experimentId
datasetVersion

embeddingModel
embeddingVersion

retrievalStrategy
parameters

metrics
createdAt
```

### Milestone

A change to embedding model or retrieval strategy can be evaluated reproducibly against previous versions.

### Done When

- evaluation dataset is versioned
- experiments are reproducible
- retrieval parameters are persisted
- vector/hybrid search has a measured baseline
- quality and latency can be compared together

---

# Phase 10 — Scale & Performance Engineering

### Goal

Validate architectural decisions under meaningful data volumes.

### Synthetic Data

Create deterministic generation for approximately:

```text
1M candidates
500K jobs
20M applications
100M+ events
```

Scale gradually rather than starting at maximum size.

### MongoDB Experiments

Compare:

```text
no index
single-field index
compound index
different compound ordering
```

Measure:

```text
p50
p95
p99
docs examined
keys examined
throughput
```

### GraphQL Experiments

Measure:

```text
nested query latency
batch sizes
DB queries/request
concurrent requests
pagination depth
```

### Kafka Experiments

Measure effects of:

```text
partition count
producer batching
consumer concurrency
event size
```

### Spark Experiments

Compare:

```text
partition strategies
join strategies
broadcast joins
shuffle joins
different file sizes
partition pruning
```

### Milestone

The repository contains benchmark evidence supporting important indexing, partitioning, and concurrency decisions.

### Done When

performance decisions can be explained as:

```text
Problem
   ↓
Measurement
   ↓
Hypothesis
   ↓
Change
   ↓
Measurement
   ↓
Conclusion
```

rather than as unsupported architectural preferences.

---

# Phase 11 — Observability & Resilience

### Goal

Make the distributed system diagnosable.

### Implement

Structured logging with correlation identifiers:

```text
requestId
userId
traceId
eventId
applicationId
jobId
```

Add metrics for:

```text
GraphQL
MongoDB
Kafka
embedding service
Spark pipelines
```

Introduce tracing across critical backend paths where practical.

### Resilience

Test:

```text
MongoDB unavailable

Kafka unavailable

embedding provider unavailable

duplicate events

consumer restart

Spark checkpoint recovery

slow downstream service
```

### Milestone

A failed application or event can be followed from API request through persistence and downstream processing.

### Done When

- important failures are observable
- retries are bounded
- external calls have timeouts
- graceful shutdown works
- resources are released correctly
- failure scenarios have integration tests

---

# Phase 12 — Security & Production Hardening

### Goal

Harden the complete platform without changing its fundamental architecture.

### Security

Implement/review:

```text
authentication
RBAC
ownership checks
GraphQL complexity limits
GraphQL depth limits
pagination limits
input validation
secret management
audit events
```

Authorization belongs in application/domain services, not exclusively in GraphQL resolvers.

### Performance Protection

Bound:

```text
GraphQL query depth
GraphQL complexity
page sizes
FS2 concurrency
MongoDB connection pools
Kafka producer/consumer settings
external API concurrency
```

### Testing

Complete:

```text
unit tests
repository integration tests
GraphQL integration tests
authorization tests
transaction tests
event reliability tests
stream recovery tests
data-quality tests
load tests
```

### Milestone

The project can be demonstrated end-to-end as one coherent architecture rather than as isolated technology examples.

---

# 23. Final Milestone

The final system should demonstrate the complete lifecycle:

```text
Candidate
   │
   │ "Scala distributed systems"
   ▼
Hybrid Search
   │
   ▼
MongoDB Vector Search
   │
   ▼
Job Viewed
   │
   ▼
Application Submitted
   │
   ▼
Recruiter Review
   │
   ▼
Interview
   │
   ▼
Hired
   │
   ▼
Domain Events
   │
   ▼
Kafka
   │
   ▼
Spark / Databricks
   │
   ├── Hiring Funnel
   ├── Time-to-Hire
   └── Skill Trends
```

At the same time:

```text
Search Events
     │
     ▼
Evaluation Dataset
     │
     ▼
Spark
     │
     ▼
Keyword vs Vector vs Hybrid
     │
     ▼
Precision / Recall / MRR / NDCG
```

---

# 24. What the Finished Project Demonstrates

The project should provide evidence of knowledge rather than merely list technologies.

### Scala / Functional Engineering

```text
Cats Effect
Resource safety
effect composition
structured concurrency
FS2
backpressure
typed errors
functional domain modeling
```

### API Engineering

```text
GraphQL schema design
Sangria
cursor pagination
N+1 prevention
batching
RBAC
query complexity management
```

### MongoDB Engineering

```text
access-pattern-driven modeling
embedding vs referencing
compound indexes
unique constraints
transactions
cursor pagination
query-plan analysis
Vector Search
```

### Distributed Systems

```text
event-driven architecture
Kafka partitioning
ordering
at-least-once delivery
idempotency
failure recovery
event schema evolution
```

### Data Engineering

```text
Spark
Structured Streaming
Delta Lake
Medallion architecture
window functions
distributed joins
partitioning
data quality
late data
watermarks
```

### Search / AI Engineering

```text
embeddings
vector retrieval
hybrid retrieval
candidate/job matching
retrieval evaluation
Precision@K
Recall@K
MRR
NDCG
```

### Performance Engineering

```text
SLO-driven development
MongoDB explain plans
GraphQL profiling
Kafka throughput/lag
Spark physical plans
shuffle/skew analysis
load testing
reproducible benchmarks
```

The central engineering principle remains:

> **Every major schema, index, pipeline, concurrency, partitioning, and infrastructure decision should be traceable to a use case and supported by measurable evidence.**
