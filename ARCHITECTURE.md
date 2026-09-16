# Architecture

This document describes the target architecture. See [README](README.md) for the current scaffold and [development plan](INITIAL_DEVELOPMENT_PLAN.md) for delivery sequencing.

The Foundation runtime uses one sbt project with constructor-injected application, API, and infrastructure packages. It serves health/readiness with a resource-managed MongoDB client. Retired SQL/user-query scaffolds and their dependencies have been removed; hiring domain implementation remains Phase 2. See [Foundation contracts and evidence](docs/specs/phase-1-foundation.md).

Supporting design: [MongoDB modeling and indexes](docs/mongodb-design.md), [big data architecture](docs/big-data-architecture.md), and [use cases](docs/use-cases.md).

## 1. Goals

The Hiring Management Platform is designed around four primary engineering goals:

- Clean separation between domain, application, API, and infrastructure code
- Functional effect management using Cats Effect
- MongoDB data modeling optimized for GraphQL access patterns
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
API ───────► Application ───────► Domain

Infrastructure ─────────────────► Domain Ports
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
├── domain/
│   ├── model/
│   │   ├── User.scala
│   │   ├── Job.scala
│   │   ├── Application.scala
│   │   └── ApplicationStatus.scala
│   │
│   ├── error/
│   └── service/
│
├── application/
│   ├── port/
│   │   ├── JobRepository.scala
│   │   ├── ApplicationRepository.scala
│   │   ├── UserRepository.scala
│   │   ├── SemanticSearch.scala
│   │   └── EmbeddingService.scala
│   │
│   └── service/
│       ├── JobService.scala
│       ├── ApplicationService.scala
│       └── MatchingService.scala
│
├── infrastructure/
│   ├── mongo/
│   │   ├── model/
│   │   ├── repository/
│   │   ├── codec/
│   │   └── index/
│   │
│   ├── ai/
│   └── auth/
│
├── api/
│   └── graphql/
│       ├── schema/
│       ├── resolver/
│       ├── input/
│       ├── output/
│       └── dataloader/
│
└── Main.scala
```

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
opaque type JobId = UUID
opaque type UserId = UUID
opaque type ApplicationId = UUID
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

# 15. Duplication Strategy

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

# 16. Controlled Denormalization

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

# 17. GraphQL N+1 Strategy

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

# 18. Cursor Pagination

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

# 19. Vector Search Data Model

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

# 20. Candidate Embeddings

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

# 21. Embedding Metadata

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

# 22. Embedding Pipeline

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

# 23. Hybrid Search

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
