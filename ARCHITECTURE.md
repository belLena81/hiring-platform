# Architecture

This document describes the active architecture. See [README](README.md) and the [current reset specification](docs/specs/pre-mvp-contract-reset.md) for its contract and verification evidence.

The runtime uses one sbt project with constructor-injected application, API, and infrastructure packages. It serves the hiring API through a resource-managed MongoDB client.

Supporting design: [use cases](docs/use-cases.md).

## 1. Goals

The Hiring Management Platform is designed around four primary engineering goals:

- Clean separation between domain, application, API, and infrastructure code
- Functional effect management using Cats Effect
- MongoDB data modeling optimized for GraphQL access patterns
- Saga-based coordination for future cross-boundary workflows with retries, compensation, and recovery
- High read performance with minimal unnecessary data duplication

The architecture should remain simple enough for an MVP while allowing individual infrastructure components to be replaced without changing business logic.

---

# 2. High-Level Architecture

The project follows **Clean Architecture / Ports and Adapters**.

```text
                    ┌──────────────────────┐
                    │     GraphQL API      │
                    │ Sangria + http4s     │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ Application Layer    │
                    │ Services / Use Cases │
                    └──────────┬───────────┘
                               │
                         Domain Ports
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
       ┌────────────┐    ┌────────────┐   ┌────────────┐
       │ Repository │    │   Search   │   │ Embedding  │
       │   Ports    │    │    Port    │   │    Port    │
       └─────┬──────┘    └──────┬─────┘   └─────┬──────┘
             │                  │               │
             ▼                  ▼               ▼
       ┌────────────────────────────────────────────┐
       │              Infrastructure                │
       │                                            │
       │ MongoDB   Vector Search   LLM/Embedding API│
       └────────────────────────────────────────────┘
```

Dependencies always point inward.

```text
Transport ───────► Service ───────► Domain

Repository adapters ─────────────► Repository protocols
```

The domain must not depend on:

- Sangria
- MongoDB
- http4s
- Circe
- JWT libraries
- external AI SDKs

---

# 3. Suggested Modules

```text
src/main/scala/
│
├── transport/
│   ├── graphql/
│   ├── http/
│   └── auth/
│
├── service/
│   ├── protocol/
│   ├── job/
│   ├── application/
│   ├── search/
│   └── auth/
│
├── repository/
│   ├── protocol/
│   └── mongo/
│
├── domain/
│   ├── model/
│   ├── error/
│   └── policy/
│
├── shared/
│   ├── pagination/
│   ├── search/
│   └── crypto/
│
├── infrastructure/
│   ├── embedding/
│   └── logging/
│
├── config/
├── runtime/
└── Main.scala
```

Transport owns protocol adapters such as GraphQL, HTTP, and JWT parsing. It talks to service protocols and shared DTOs, not Mongo repositories. Service implementations own hiring use cases, authorization, and effect sequencing. Repository protocols describe storage/search boundaries, and `repository.mongo` contains the current MongoDB implementation. Domain policies remain pure and infrastructure-free.

---

# 4. Domain Model

Domain objects must not represent MongoDB documents or GraphQL types directly.

Example:

```scala
final case class Job(
  id: JobId,
  recruiterId: UserId,
  title: String,
  description: String,
  requirements: List[String],
  skills: Set[String],
  location: Location,
  status: JobStatus,
  createdAt: Instant,
  updatedAt: Instant
)
```

Use domain-specific IDs:

```scala
opaque type Id[Tag] = UUID
type JobId = Id[JobTag]
type UserId = Id[UserTag]
type ApplicationId = Id[ApplicationTag]
type ApplicationEventId = Id[ApplicationEventTag]
```

Prefer enums and ADTs over raw strings:

```scala
enum ApplicationStatus:
  case Created
  case Accepted
  case Interview
  case Hired
  case Rejected
  case Declined
```

Business transitions belong in the domain/application layer rather than GraphQL resolvers or MongoDB repositories.

Job aggregate state changes must be modeled as pure `cats.data.State` programs over the `Job` aggregate. This applies to create/publish/update/close style transitions and any later job lifecycle expansion. `State` is for deterministic in-memory transition logic only; application services remain responsible for authorization, time/ID inputs, repository effects, atomic persistence, and event handoff.

---

# 5. Ports

Infrastructure dependencies are represented through interfaces.

```scala
trait JobRepository[F[_]]:
  def find(id: JobId): F[Option[Job]]

  def search(
    filter: JobFilter,
    cursor: Option[JobCursor],
    limit: Int
  ): F[List[Job]]

  def create(job: Job): F[Unit]

  def update(job: Job): F[Unit]
```

Application repository:

```scala
trait ApplicationRepository[F[_]]:
  def find(id: ApplicationId): F[Option[Application]]

  def findByCandidate(
    candidateId: UserId,
    cursor: Option[ApplicationCursor],
    limit: Int
  ): F[List[Application]]

  def findByJob(
    jobId: JobId,
    cursor: Option[ApplicationCursor],
    limit: Int
  ): F[List[Application]]
```

AI infrastructure is also hidden behind ports:

```scala
trait EmbeddingService[F[_]]:
  def embed(text: String): F[Vector[Float]]

trait SemanticSearch[F[_]]:
  def searchJobs(
    embedding: Vector[Float],
    filter: JobFilter,
    limit: Int
  ): F[List[JobMatch]]
```

Application services depend only on these interfaces.

---

# 6. Effect Architecture

Cats Effect controls all side effects.

```text
                    IOApp
                      │
                      ▼
                   Resource
                      │
       ┌──────────────┼──────────────┐
       ▼              ▼              ▼
 Mongo Client     HTTP Server    AI Client
       │
       ▼
 Repositories
       │
       ▼
 Services
       │
       ▼
 GraphQL
```

Resources should be constructed once during startup.

```scala
def appResource: Resource[IO, Server] =
  for
    mongo      <- mongoResource
    repos      <- repositories(mongo)
    ai         <- aiResource
    services   <- services(repos, ai)
    graphQL    <- graphQLResource(services)
    server     <- httpServer(graphQL)
  yield server
```

Do not create Mongo or HTTP clients inside resolvers.

Concurrency is introduced through Cats Effect only when a concrete workflow needs it:

- `Ref` is allowed for process-local coordination such as request-scoped counters, lifecycle gates, or in-memory test fixtures; it is not a substitute for MongoDB consistency.
- Bounded `Queue` is allowed for explicit asynchronous handoff such as embedding work or outbox dispatch; unbounded queues are forbidden.
- Fibers must be owned by `Resource`, request scope, or another structured supervisor, with cancellation and finalization tests for long-lived work.
- `parTraverse`, `parEvalMap`, and similar concurrency must have explicit bounds and must preserve authorization and backpressure.

Detached fire-and-forget fibers, unbounded queues, and process-local `Ref` state for durable business invariants are not allowed.

---

# 7. MongoDB Modeling Strategy

MongoDB documents are designed around **access patterns**, not around SQL normalization.

Primary collections:

```text
users
jobs
applications
application_events
```

AI embeddings should initially live with their owning searchable entity rather than creating a separate embedding collection.

The default rule is:

> Embed bounded data that belongs exclusively to the document. Reference independent entities with their own lifecycle.

---

# 8. Users Collection

```javascript
{
  _id: UUID,

  role: "CANDIDATE",

  name: "Alice Smith",
  email: "alice@example.com",

  profile: {
    skills: ["Scala", "Cats Effect", "MongoDB"],
    experienceSummary: "...",
    resumeRef: "..."
  },

  embedding: [...],

  createdAt: ISODate(...),
  updatedAt: ISODate(...)
}
```

Recruiters and candidates share one collection.

Avoid:

```text
candidates
recruiters
admins
```

unless their persistence requirements diverge significantly.

Benefits:

- one identity collection
- one email uniqueness constraint
- simpler authentication
- simpler RBAC lookup

Indexes:

```javascript
{ email: 1 } UNIQUE

{ role: 1 }
```

Do not create indexes for fields merely because they exist.

---

# 9. Jobs Collection

```javascript
{
  _id: UUID,

  recruiterId: UUID,

  title: "Senior Scala Developer",

  description: "...",

  requirements: [
    "5+ years JVM",
    "Functional programming"
  ],

  skills: [
    "Scala",
    "Cats Effect",
    "MongoDB"
  ],

  location: {
    country: "Ukraine",
    city: "Kyiv",
    remote: true
  },

  status: "OPEN",

  createdAt: ISODate(...),
  updatedAt: ISODate(...),

  embedding: [...]
}
```

Do **not** embed the recruiter document.

Store:

```text
recruiterId
```

and resolve the recruiter separately.

Reason:

A recruiter can own many jobs and recruiter information can change independently.

Embedding recruiter data would create:

```text
User update
    ↓
N job updates
```

which creates unnecessary write amplification.

---

# 10. Job Indexes

Expected GraphQL query:

```graphql
jobs(
  status: OPEN
  location: "Kyiv"
  first: 20
  after: "..."
)
```

Primary index:

```javascript
{
  status: 1,
  "location.city": 1,
  createdAt: -1,
  _id: -1
}
```

Recruiter jobs:

```graphql
recruiter {
  jobs(...)
}
```

Index:

```javascript
{
  recruiterId: 1,
  createdAt: -1,
  _id: -1
}
```

Avoid building separate indexes such as:

```javascript
{ title: 1 }
{ location: 1 }
{ status: 1 }
{ createdAt: 1 }
{ skills: 1 }
```

without a demonstrated query requirement.

Compound indexes should correspond to actual access patterns.

---

# 11. Applications Collection

Applications represent the high-cardinality relationship between candidates and jobs.

Do not embed applications inside either user or job documents.

```javascript
{
  _id: UUID,

  candidateId: UUID,
  jobId: UUID,

  status: "INTERVIEW",

  feedback: null,

  createdAt: ISODate(...),
  updatedAt: ISODate(...)
}
```

This avoids unbounded arrays:

```text
Job
 └── applications[]
       ├── ...
       ├── ...
       └── potentially thousands
```

and:

```text
Candidate
 └── applications[]
```

Applications therefore remain an independent collection.

---

# 12. Application Indexes

## Domain invariant

A candidate may apply to a job only once.

Enforce it at database level:

```javascript
{
  candidateId: 1,
  jobId: 1
}
UNIQUE
```

This simultaneously supports candidate/job existence checks.

---

## Candidate applications

GraphQL:

```graphql
candidate {
  applications(status: INTERVIEW)
}
```

Index:

```javascript
{
  candidateId: 1,
  status: 1,
  createdAt: -1,
  _id: -1
}
```

---

## Recruiter reviewing a job

GraphQL:

```graphql
job {
  applications(status: CREATED)
}
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

These indexes also support cursor pagination.

---

# 13. Application History

Status history is append-heavy and potentially unbounded.

Therefore avoid:

```javascript
{
  application: {
    history: [
      ...potentially unbounded...
    ]
  }
}
```

Use:

```text
application_events
```

instead.

Document:

```javascript
{
  _id: UUID,

  applicationId: UUID,

  type: "STATUS_CHANGED",

  previousStatus: "ACCEPTED",
  newStatus: "INTERVIEW",

  changedBy: UUID,

  reason: null,
  feedback: null,

  createdAt: ISODate(...)
}
```

Index:

```javascript
{
  applicationId: 1,
  createdAt: -1
}
```

This provides an append-only audit trail without continuously growing the application document.

---

# 14. Application State Transactions

Changing application status requires two writes:

```text
applications
      +
application_events
```

They should represent one logical operation.

```text
Mongo Transaction

BEGIN
   │
   ├── validate current state
   │
   ├── update application
   │
   └── insert application_event
   │
COMMIT
```

Domain validation occurs before persistence.

The database transaction protects consistency between current state and history.

---

# 15. Saga Pattern for Cross-Boundary Workflows

The Saga pattern is mandatory for future workflows that span multiple durable systems or external side effects that cannot participate in one MongoDB transaction.

Do not use Saga for the Phase 2 core hiring write path when MongoDB can protect the invariant directly. Submitting an application and changing an application status remain local transactional operations:

```text
MongoDB transaction

applications
     +
application_events
```

Use Saga only after the workflow crosses a boundary such as:

```text
MongoDB
Kafka
embedding provider
Vector Search index
email or notification provider
calendar or interview scheduling service
external storage
```

The preferred shape is an orchestrated Saga owned by the application layer:

```text
GraphQL mutation
      │
      ▼
Application service
      │
      ├── local transaction / outbox write
      │
      ▼
Saga state
      │
      ├── execute next step
      ├── retry transient failures
      ├── record completed steps
      ├── compensate reversible steps
      └── mark failed workflows for operator repair
```

Saga state must be durable, idempotent, observable, and recoverable after process restart. It must not live only in `Ref`, an in-memory queue, or a detached fiber.

Each Saga step must define:

- forward command
- idempotency key
- retry policy with bounded attempts or backoff
- compensating action, or an explicit statement that the step is not safely reversible
- terminal failure state and operator-visible repair path
- safe audit fields such as `sagaId`, `step`, `applicationId`, `jobId`, `eventId`, and timestamps

The most useful Saga showcases for this application are:

- application submission enrichment: submit application, then parse resume, generate embeddings, update Vector Search metadata, and notify recruiter
- interview scheduling: move an application to `INTERVIEW`, reserve an interview slot, send candidate/recruiter notifications, and compensate calendar reservations if notification or persistence fails
- job closing with bulk effects: close a job, prevent new applications immediately, then asynchronously decline or archive remaining active applications according to explicit product rules
- event publication repair: publish outbox events to Kafka with retry, deduplication, and dead-letter/quarantine handling while keeping MongoDB as operational truth
- candidate or job profile reindexing: update profile/job content first, then regenerate embeddings and search index entries without blocking the user-facing mutation

Saga is not a substitute for:

- MongoDB transactions inside one consistency boundary
- unique constraints for duplicate applications
- domain transition validation
- transactional outbox for reliable event handoff
- idempotent Kafka consumers

Future implementation should add Saga in the smallest vertical slice that includes one real external boundary, durable state, retry and compensation tests, and observability. Do not add a generic Saga framework before there is a concrete workflow that needs it.

---

# 16. Duplication Strategy

The default is **minimal duplication**, not zero duplication.

Use references for:

```text
Job ─────────► recruiterId

Application ─► candidateId
Application ─► jobId

Event ───────► applicationId
Event ───────► changedBy
```

Embed:

```text
Job
 ├── location
 ├── requirements
 └── skills

User
 └── profile
```

because these are bounded value objects owned by their parent.

---

# 17. Controlled Denormalization

Duplication is acceptable when it removes a measured performance bottleneck.

For example, an application listing may eventually contain:

```javascript
{
  candidateId: UUID,

  candidateSnapshot: {
    name: "...",
    headline: "..."
  }
}
```

This should **not** be introduced initially.

Start normalized:

```text
Application
     │
     ▼
 candidateId
```

Measure.

Then denormalize only if batching and indexes are insufficient.

Rule:

> Never duplicate data because MongoDB allows it. Duplicate only when a specific read path justifies the consistency cost.

---

# 18. GraphQL N+1 Strategy

Consider:

```graphql
query {
  applications {
    candidate {
      name
    }

    job {
      title

      recruiter {
        name
      }
    }
  }
}
```

Naive execution:

```text
1 applications query

N candidate queries
N job queries
N recruiter queries
```

Avoid this through request-scoped batching.

```text
Applications
     │
     ├──────── candidateIds ──────┐
     │                            ▼
     │                      users.find({
     │                        _id: {$in: [...]}
     │                      })
     │
     └──────── jobIds ────────────┐
                                  ▼
                             jobs.find({
                               _id: {$in: [...]}
                             })
```

Batch loaders should be scoped to the GraphQL request to provide both batching and request-local caching.

---

# 19. Cursor Pagination

Avoid deep pagination using:

```javascript
skip(N)
```

Use keyset pagination.

Ordering:

```text
createdAt DESC
_id DESC
```

Cursor contains:

```text
createdAt
_id
```

Query conceptually becomes:

```javascript
{
  status: "OPEN",

  $or: [
    { createdAt: { $lt: cursor.createdAt } },

    {
      createdAt: cursor.createdAt,
      _id: { $lt: cursor.id }
    }
  ]
}
```

with:

```javascript
.sort({
  createdAt: -1,
  _id: -1
})
.limit(21)
```

Fetch `limit + 1` records to determine `hasNextPage`.

---

# 20. Vector Search Data Model

Jobs contain their searchable embedding:

```javascript
{
  _id: UUID,

  title: "...",
  description: "...",
  requirements: [...],
  skills: [...],

  embedding: [...]
}
```

The embedding represents canonical searchable text:

```text
title
+
description
+
requirements
+
skills
```

Example:

```text
Senior Scala Developer

Backend engineering role building distributed JVM services.

Requirements:
Scala 3
Cats Effect
MongoDB

Skills:
Scala, Cats Effect, GraphQL, MongoDB
```

Keeping the vector on the job avoids an additional lookup after vector retrieval.

---

# 21. Candidate Embeddings

Candidate profiles can similarly contain:

```javascript
{
  _id: UUID,

  role: "CANDIDATE",

  profile: {
    skills: [...],
    experienceSummary: "...",
    resumeRef: "..."
  },

  embedding: [...]
}
```

This supports both:

```text
Candidate
    ↓
recommended jobs
```

and:

```text
Job
    ↓
recommended candidates
```

without introducing a dedicated vector-document collection during the MVP.

---

# 22. Embedding Metadata

Store enough metadata to determine whether an embedding is stale.

```javascript
{
  embedding: [...],

  embeddingMeta: {
    model: "...",
    version: 1,
    sourceHash: "...",
    updatedAt: ISODate(...)
  }
}
```

`sourceHash` is calculated from the canonical searchable content.

If:

```text
newSourceHash == storedSourceHash
```

the embedding does not need regeneration.

This prevents unnecessary embedding API calls.

---

# 23. Embedding Pipeline

Do not generate embeddings synchronously inside normal mutations unless immediately required.

```text
updateJob
    │
    ▼
MongoDB
    │
    ▼
EmbeddingRequested
    │
    ▼
FS2 Queue / Stream
    │
    ▼
parEvalMap(N)
    │
    ▼
EmbeddingService
    │
    ▼
MongoDB
```

This keeps mutation latency independent from embedding-provider latency.

Use bounded parallelism:

```scala
stream
  .parEvalMap(8)(generateEmbedding)
```

Concurrency must be configurable.

---

# 24. Hybrid Search

Semantic search should not replace structured filtering.

Example:

```graphql
searchJobs(
  query: "functional Scala distributed systems"
  location: "Remote"
  status: OPEN
  first: 20
)
```

Execution:

```text
Structured constraints
        +
Vector similarity
        │
        ▼
MongoDB Search
        │
        ▼
Ranked results
```

Metadata such as:

```text
status
location
recruiterId
```

should remain structured fields rather than being encoded only into embeddings.
