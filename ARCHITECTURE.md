# Architecture

See the [current application architecture wiki](docs/wiki/README.md) for a source-derived use-case map, domain and collection inventories, workflow patterns, security and runtime boundaries.

The [discovery and interview integrity correction](docs/specs/candidate-discovery-and-interview-integrity.md) retains the existing pure decisions and durable interpreters. Deletion removes selected workflows' children before their last attribution records; transport quarantine uses a bounded deterministic digest of record coordinates, payload digest and rejection reason. Residence integrity is a narrow Mongo startup proof over the existing document shape.

## Retrieval and durable publication boundaries

The [reliability specification](docs/specs/hiring-retrieval-publication-reliability.md) owns this implementation checkpoint. Pure search eligibility criteria render separately into ordinary MongoDB and Atlas Search filters. Ranked retrieval hits contain identity, score and embedding metadata; authoritative hydration supplies current public data. Geo/facet queries begin with the trusted active actor and return a typed authority failure when that actor is absent.

The existing lifecycle StateT and interview state/command ADTs remain the decision model. Kafka adapters own Resource lifetimes, partition concurrency and sequential durable progress per partition. A publisher claims immediately before sending, renews its claim and charges attempts only at authorization. Null payloads quarantine durably before progress; bounded clock skew defers without acknowledgment.

Producer attribution lives in `producer_registrations`, with bounded cleanup pages and broker-confirmed fencing checkpoints. Active registrations have no TTL. Normal generation retirement requires stopping that generation and fencing its canonical transactional ID before marking registrations fenced. Closing a producer is insufficient evidence.

Embedding values must prove the configured model identity before document persistence or query retrieval; an absent provider model retains the configured fallback. Application submission uses an immutable ID/status/revision snapshot with the existing transactional version/status write guard. Workflow publication shares one Resource-owned loop: successful full bounded passes yield and continue immediately, while idle/deferred passes sleep for validated `kafka.interview.publication-poll-interval-ms` (100–60000 ms, default 1000). Individual producer transactions remain serialized. [Search/publication reliability](docs/specs/hiring-search-publication-reliability.md) separates measured local adoption from external acceptance.

Operational event payload ADTs contain only the analytical facts required by current consumers; their strict wire validation retains the seven-field envelope. Outbox lists validate before insertion, receipt deduplication uses Mongo's unique insert, and publication renews at the start of each bounded four-claim wave. Kafka owns transaction serialization and fencing. Operational facts use event time and permit out-of-order publication; interview revisions and inbox receipts govern workflow ordering. Cleanup integrity is established by migration 015 and exact native-validator verification, with explicit bounded maintenance audits and an active-state partial index. Temporary test namespaces use a guarded reusable local stack; durable proof resources stay separate. [Event and test reliability](docs/specs/hiring-event-and-test-reliability.md) tracks the local checks and external acceptance gates.


This document describes the active architecture and explicitly planned refinements. Collection snippets and diagrams are abbreviated design illustrations, not complete BSON validators or executable API fixtures. The [current GraphQL SDL](src/test/resources/graphql/hiring.graphql), [Mongo codecs](src/main/scala/com/example/graphQL/cats/repository/mongo/MongoHiringPersistenceCodecs.scala), and [index definitions](src/main/scala/com/example/graphQL/cats/repository/mongo/MongoHiringIndexSetup.scala) define exact implemented shapes. See [README](README.md) and the [current reset specification](docs/specs/pre-mvp-contract-reset.md) for its contract and verification evidence.

The hiring API is one sbt build with constructor-injected application, API and infrastructure packages. A separate `analytics/` sbt build owns Spark/Delta batch, erasure and streaming runtimes. MongoDB remains operational truth; API transactions do not wait for analytics. See the [analytics boundary](docs/big-data-architecture.md) and [roadmap](docs/development-milestones.md) for current acceptance and planned work.

Supporting design: [use cases](docs/use-cases.md).

## 1. Goals

The Hiring Management Platform is designed around the following engineering goals:

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
                         Service-owned Ports
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
       │ MongoDB   Vector Search   Embedding API│
       └────────────────────────────────────────────┘
```

Dependencies always point inward.

```text
API ──────────────► Service ───────► Domain
                         ▲
                         │ ports
Repository / infrastructure adapters
```

The domain must not depend on:

- Sangria
- MongoDB
- http4s
- Circe
- JWT libraries
- external AI SDKs

---

# 3. Current Package Boundaries

```text
src/main/scala/com/example/graphQL/cats/
│
├── api/
│   ├── graphql/
│   ├── http/
│   └── auth/
│
├── service/
│   ├── protocol/
│   ├── port/
│   ├── job/
│   ├── application/
│   ├── events/
│   ├── search/
│   └── auth/
│
├── repository/
│   └── mongo/
│
├── domain/
│   ├── pagination/
│   ├── model/
│   ├── error/
│   └── policy/
│
├── shared/
│   ├── crypto/
│   ├── HiringHttpPaths.scala
│   └── Parsing.scala
│
├── infrastructure/
│   ├── embedding/
│   ├── kafka/
│   └── logging/
│
├── config/
├── runtime/
└── Main.scala
```

The API owns GraphQL, HTTP, and authentication adapters. It talks to service use-case protocols and service-owned request/result contracts. Service implementations own hiring use cases, authorization, effect sequencing, and the `service.port` interfaces used for persistence and external capabilities. Mongo and other infrastructure adapters implement those inward-facing ports; `repository.mongo` contains the MongoDB adapters. Domain policies remain pure and infrastructure-free. `shared` is reserved for neutral utilities with no dependency on domain or service contracts.

---

# 4. Domain Model

Domain objects must not represent MongoDB documents or GraphQL types directly.

Abbreviated example (the current `Job` also carries optional `closedAt` and `embedding`):

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

Job transitions over an existing aggregate (publish/update/close and later lifecycle expansions) must be modeled as pure `LifecycleProgram` (`StateT` over `Either[DomainError, *]`) programs over the `Job` aggregate. Initial-job validation remains a direct typed check because creation has no existing aggregate state to thread. Application lifecycle transitions use the same program type. These programs are for deterministic in-memory transition logic only; application services remain responsible for authorization, time/ID inputs, repository effects, atomic persistence, and event handoff.

Use functional forms of patterns where they clarify an existing boundary; do not introduce generic pattern frameworks. Adapter is the established ports-and-adapters boundary. Resource-based Factory composition owns clients, workers, and other long-lived resources. Facade belongs at a capability-specific application boundary, immutable Command data represents requested work, and lifecycle State programs keep aggregate decisions pure. These approaches are existing architecture choices, not a mandate to add layers.

Refinements follow the [development milestones](docs/development-milestones.md). Job and candidate embedding use a functional Template Method/Pipeline: `EmbeddingPreparation` shares pure preparation (including one source-hash calculation) and the worker supplies an effectful persistence callback closing over the original versioned entity. Adapters retain version-guarded writes and workers retain retries, document limits, and resource ownership. Current metadata is checked before the size limit so replay preserves its existing no-work behavior. Search ranking stays a deterministic pure application function, with Mongo execution in adapters; evaluate current alternatives before adding an abstraction for another concrete algorithm. This embedding cleanup is maintainability work and provides no search-quality or performance result. The [search specification](docs/specs/hiring-search-enhancements.md#search-evaluation-and-embedding-architecture) owns source-qualified local evidence and remaining gates.

Search evaluation separates immutable numerical reports, pure paired assessment and JSON rendering. Bounded capture owns adapter resources and measures supported queries; the test-scoped curated Atlas adapter reuses production service authorization, retrieval and authoritative eligibility in a disposable nonce database. Ranking origin and embedding provenance are separate: observed Atlas execution with synthetic vectors remains tooling evidence. Scoped recommendations require reviewed labels, an agreed policy and exact-run-bound privacy/eligibility/billing evidence; missing evidence defers and observed failures reject. Assessments cannot change runtime ranking configuration.

In Phase 13, streaming recovery/revision decisions may be extracted as pure functions over immutable observations, while the coordinator executes effects. Existing Phase 7 authorization, journal ordering, deletion checks, publication fencing, watermark commits, and acknowledgement rules remain acceptance requirements. Any extraction must preserve them and be checked against restart, deletion, and publication races.

Phase 7 local functional closure passes final independent Code, Security and QA review on source `8c01e`, with 470 analytics unit tests, 45 executed integrations and formatting passing. Local root 431-test evidence retains unchanged-source applicability.

The October 5 user decision schedules analytics latency and burst drain-time qualification and optimization in the final phase after deployment to the real environment. Local Phase 7 closure retains correctness, authorization, recovery, retention, bounded backlog/storage and maintenance requirements; the recorded 40,084-ms Bronze p95 failure remains a deferred SLO result. Genuine deployed retention and operational activation prerequisites still apply before production streaming. See the [final deployed optimization milestone](docs/development-milestones.md#deployed-analytics-optimization-phase-15-final-phase).

Phase 8 first establishes MongoDB/vector access baselines, Phase 10 completes evaluated AI discovery, and Phase 11 applies durable Saga and pure State decisions to operational workflows. Further Spark refinement follows in Phase 13. See the planned Kafka contracts below; their proposed topics are not currently provisioned.

The [remaining capability specifications](docs/development-milestones.md#remaining-delivery-order-and-acceptance) define concrete behavior and boundaries for Phases 8–15. They retain the application's service-owned `IO`/`EitherT` ports, analytics' `F[_]` ports, and resource-owned adapters. Proposed contracts and provider/deployment alternatives are labelled separately from current source facts; an unresolved choice blocks its dependent implementation. Specification readiness does not replace implementation, privacy, recovery or deployed acceptance evidence.

Other pattern use remains conditional: Decorator for shared instrumentation across multiple provider adapters (preserving error and cancellation behavior without duplicate retries); Observer as managed streams for new local notifications while restart-recoverable work keeps durable handoffs; Bridge only for integrations with two independently varying dimensions; typed Filter criteria as discovery requirements grow; recursive Composite only for accepted nested Boolean criteria; and additional ranking strategies only when evaluation demonstrates benefit. See [development milestones](docs/development-milestones.md) for phase placement and acceptance conditions.

---

# 5. Ports

Infrastructure dependencies are represented through interfaces in `service.port`. The active application specializes those ports to Cats Effect `IO`: use cases use `UseCaseIO[A] = EitherT[IO, UseCaseError, A]`, while operational ports return `RepositoryIO[A] = EitherT[IO, RepositoryError, A]`. This is a deliberate single-runtime choice; typed ports and fake adapters provide service-test seams, while a second effect runtime would require a broader contract change.

```scala
trait JobRepository:
  def find(id: JobId): RepositoryIO[Option[Job]]

trait JobUseCases:
  def viewJob(actor: ActorContext, jobId: JobId): UseCaseIO[Job]
```

AI infrastructure is also hidden behind ports:

```scala
trait EmbeddingService:
  def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]]
```

Application services depend on these interfaces. `RepositoryError` belongs to this service boundary; adapters translate driver failures into it.

---

# 6. Effect Architecture

Cats Effect controls all side effects.

The main Hiring runtime intentionally selects `IO` at service and repository boundaries. `UseCaseIO` and `RepositoryIO` keep expected failures typed in `EitherT`; neither alias makes the application effect-polymorphic. See [local engineering quality](docs/engineering-quality.md) for the rationale and revisit condition.

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

  role: "Candidate",

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
{ emailCanonical: 1 } UNIQUE SPARSE
{ nameCanonical: 1 } UNIQUE
{ role: 1, accountStatus: 1, createdAt: -1, _id: -1 }
{ adminSingletonKey: 1 } UNIQUE WHERE role = "Admin"
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

  status: "Open",

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

Current GraphQL query (candidate visibility is enforced by the service):

```graphql
query {
  jobs(city: "Kyiv", first: 20) {
    edges { node { id title } }
    pageInfo { hasNextPage endCursor }
  }
}
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
query {
  myJobs(first: 20) { edges { node { id title } } }
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

  status: "Interview",

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
query {
  myApplications(first: 20, status: INTERVIEW) {
    edges { node { id status } }
  }
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
query ReviewApplications($jobId: JobID!) {
  jobApplications(jobId: $jobId, first: 20, status: CREATED) {
    edges { node { id status } }
  }
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

  previousStatus: "Accepted",
  newStatus: "Interview",

  actorId: "<user-uuid>",

  reason: null,
  feedback: null,

  occurredAt: ISODate(...)
}
```

Index:

```javascript
{
  applicationId: 1,
  occurredAt: -1,
  _id: -1
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

The Saga pattern is mandatory for multi-step business workflows that coordinate durable changes or external side effects across transaction boundaries and need explicit recovery or compensation. A single outbox publication or derived projection update can use its existing durable idempotent worker without a separate saga engine.

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

Cross-boundary workflows and supporting durable workers for this application include:

- application submission enrichment: submit application, then parse resume, generate embeddings, update Vector Search metadata, and notify recruiter
- interview scheduling: reserve a slot, commit the guarded move to `INTERVIEW`, then notify candidate and recruiter; release the reservation if the status change fails and retry or reconcile uncertain notification delivery
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


### Planned Kafka workflow contracts

Phase 11 must produce a reviewed topic/stream catalog before changing routing. Current implementation uses `hiring.operational-events`, a seven-field unversioned envelope, transactional outbox publication and idempotent consumption. The following is a target logical split, not a declaration of existing topics or new infrastructure. Keep a stream together when ownership, ordering, retention and access rules match; split only when these requirements differ. Resolve exact topic names, partitions, replication/minimum ISR, size limits and retention from the bounded workload and recovery/privacy requirements before implementation.

| Stream | Owner and key | Delivery, retention and access contract |
|---|---|---|
| Operational facts (`hiring.operational-events`, existing) | Domain outbox producer; audit the existing aggregate partition key before any change | Immutable facts, finite delete retention, consumer group per independent capability; preserve analytics/deletion contracts |
| Workflow commands (proposed capability-specific topic) | Application saga orchestrator; workflow ID | One command owner/worker group; persisted deadline and step idempotency key; least-privilege producer/consumer ACLs |
| Workflow results (proposed) | Owning worker; same workflow ID | Correlate command/step/revision; orchestrator consumes durably; reject stale or unrelated replies |
| Discovery telemetry (split only if justified) | Authorized telemetry producer; documented session/query key | Best-effort product telemetry must not delay domain commands; bounded retention and minimized payload; never a source of hiring truth |
| Retry/quarantine (capability-specific when required) | Owning consumer/operator | Bounded attempts, restricted access, sanitized failure codes, explicit replay authorization and expiry; no automatic replay of deleted subjects |

Preserve the current active envelope until a scoped change updates all producers, consumers, analytics parsers and fixtures. Proposed workflow messages require typed command/event ID, correlation/workflow ID, causation ID, step ID, expected workflow revision and UTC occurrence/deadline values. IDs/revisions are distinct from any future schema version. Before full MVP use one active contract, with no dual readers/writers or speculative compatibility layer. An existing incompatible local lakehouse stays preserved and fails closed unless its exact reset is authorized.

Ordering exists only within a partition, and the same key on separate topics does not create cross-topic order. Preserve outbox aggregate order, process each ordered key sequentially with bounded parallelism across independent keys, and advance offsets only through the contiguous durable completion frontier. Partition-count/key changes require explicit routing/replay analysis. Delayed results and retries are checked against durable workflow revisions and allowed transitions, not arrival order. Retry topics may reorder records; block later commands for that workflow or validate them against the durable state before applying them.

For a consumed message, atomically record its inbox deduplication identity, guarded next workflow state and outgoing outbox commands in MongoDB, then acknowledge Kafka. A crash before acknowledgement causes a safe duplicate; never acknowledge first. Retain deduplication records through the permitted replay horizon or refuse older replay. Persist deadlines, attempts and repair status; fence stale worker completions through expected revisions/claim tokens. External operations use stable step idempotency keys and reconciliation for uncertain outcomes; Kafka transactions cannot atomically commit MongoDB state or a calendar/email side effect. Use `read_committed` for transactional records. The [Kafka design documentation](https://kafka.apache.org/41/design/design/) describes partition ordering and the limits of exactly-once processing across external systems.

State is a pure ADT transition with immutable Command outputs; Saga is the durable orchestration and recovery policy around that transition. Keep local aggregate lifecycle State programs separate from saga progress. Represent compensating, retryable, awaiting-result, completed and operator-repair outcomes explicitly where the workflow requires them. Reversible reservations can be compensated; sent notifications, deletion and elapsed retention need forward recovery/reconciliation. Do not invent business status transitions to fit a generic workflow engine.

Kafka Streams is optional for a demonstrated stream join/window/state-store workload. Compare it with the existing FS2/fs2-kafka consumers, including extra state-store/changelog restoration, repartitioning, operational cost and deletion obligations. A compacted derived-state topic cannot replace durable saga state or an immutable event history; compaction/tombstones alone do not prove timed erasure. Every new stream, retry copy, state store and backup must be included in retention, deletion barriers and replay-denial analysis before activation. Keep raw resumes, vectors, credentials and private search filters out of workflow messages and diagnostics.

---

# 16. Duplication Strategy

The default is **minimal duplication**, not zero duplication.

Use references for:

```text
Job ─────────► recruiterId

Application ─► candidateId
Application ─► jobId

Event ───────► applicationId
Event ───────► actorId
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

Conceptual relationship traversal (use `myApplications` or `jobApplications` connections in the current API):

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
  status: "Open",

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

  role: "Candidate",

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
    sourceHash: "...",
    updatedAt: ISODate(...)
  }
}
```

`sourceHash` is calculated from the canonical searchable content.

If:

```text
newSourceHash == storedSourceHash && configuredModel == storedModel
```

the embedding does not need regeneration.

The current embedding metadata contains `model`, `sourceHash` and `updatedAt`; it has no separate embedding-version field. The Mongo user/job `version` is an internal concurrency revision used to guard writes, not an embedding schema version. This freshness check avoids unnecessary embedding API calls.

---

# 23. Embedding Pipeline

Do not generate embeddings synchronously inside normal mutations unless immediately required.

```text
updateJob
    │
    ▼
MongoDB transaction: entity update + durable embedding_work
    │
    ▼
Bounded wake-up Queue + periodic durable-work polling
    │
    ▼
Claim / lease / retry through FS2 Stream
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
query {
  semanticJobSearch(query: "functional Scala distributed systems", first: 20) {
    results { job { id title } score }
  }
}
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

## Candidate matching search

Recruiter candidate matches are authorized against an owned open job before embedding-provider or search work. The existing job-vector branch remains the baseline. An optional recruiter query adds a query-vector branch and a lexical branch, fused with deterministic application-side reciprocal-rank fusion by default. Required skills, candidate role, active-account status, and consent-aware private predicates are applied before each branch limit.

Candidate residence and availability stay out of embedding text. Only opted-in candidate profiles are constrained by supplied residence/availability filters; missing consent is treated as opted out. Candidate profile API fields are visible only to the authenticated profile owner, and recruiter match results use a reduced candidate shape. Atlas index readiness must be verified before query use; experimental Mongo fusion and reranking require explicit opt-in and version support.

Raw candidate-filter requests pass through one pure accumulating validator before embedding or retrieval. `ValidatedCandidateMatchFilters` supplies canonical skills/residence and typed availability to both Mongo predicates and authoritative eligibility; profile comparisons use the same Locale.ROOT canonicalization. Original skill-count and blank-input limits are checked before deduplication, and the service preserves its established outward error precedence. GraphQL and stored field shapes are unchanged.

### Bounded hiring read authorization

`HiringReadScope` is created only after persisted-actor validation. Mongo application/history and relationship queries gate the actor and authorized parent in the same selection. Relation-aware fetcher keys remain scoped to each request. Search eligibility uses required scoped projection ports and a pure `SearchEligibilityPolicy`: services sequence prechecks/retrieval, adapters enforce the final actor/query-entity gate and bounded selection, and policy validates current source/metadata/filter state before response truncation. Driver errors carrying transient transaction labels remain with the transaction retry owner inside active outbox sessions, with final sanitized errors at the repository boundary. The [MongoDB/vector specification](docs/specs/mongodb-vector-retrieval-optimization.md) tracks local, live Atlas and performance acceptance separately.

### Local interview coordination

Interview hiring commits derive aggregate and history values from the same pure `ApplicationLifecycle` program used by ordinary status mutations; the Mongo adapter retains atomic persistence and authorization. Workers receive an explicit diagnostics sink. Cleanup isolates failures between subjects while preserving sequential fencing, purge and retention steps within each subject; a failed subject stays retryable. Embedding provider validation rejects non-finite vector values before either document persistence or query execution. The [boundary reliability specification](docs/specs/embedding-workflow-boundary-reliability.md) owns validation evidence for these refinements.

Cleanup selection uses bounded subject-ID sweeps and independent row decoding. An immutable opaque continuation is threaded through a sequential resource-owned stream; only the Mongo adapter interprets its BSON ordering coordinates. Durable cleanup progress remains protected by existing state/revision CAS and fencing, while page failures retain scheduling position. Pure `InterviewMessagePolicy` functions classify command, result and expired observations in the application layer, preserving acknowledgment/reconciliation rules without importing service ports into the domain. Nearby cursor canonical encoding belongs to its codec; rank contributions and facet dimensions use named immutable values.

`InterviewExecutionPolicy` stages pure admission, lease and budget decisions over fresh stored observations; the Mongo adapter retains conditional lease decoding, transactional budget reads, subject fences and guarded writes. Expired execution queues reconciliation without consuming another slot. Domain transitions and repair return a named `InterviewWorkflowDecision` with ordered intents. Scheduling receives reusable clock/identity effects while Mongo independently checks current deadline time. Admin repair reconciles existing effects; it neither reopens a released reservation nor extends its immutable deadline.

`InterviewSchedulingService` owns actor authorization and request persistence; capability-specific pure workflow transitions produce outgoing intents. `MongoInterviewWorkflowRepository` commits revision, inbox receipt and outgoing intent atomically and owns guarded hiring commits. Resource-owned workers interpret effects through persistent fake calendar and notification adapters. Separate command/result topics use workflow IDs as keys; scheduling details stay in MongoDB and operational event envelopes keep their seven fields. Provider receipts and reconciliation coordinate effects outside hiring transactions. Pure workflow policies select applicability, repair and bounded execution/publication outcomes; typed advancement causes own receipt identity. Expired execution reconciles with stable provider keys. Initialized immutable transactional producer generations register in the Mongo subject-fence transaction before sending; replacement requires fresh authorization. Subject deletion traverses all attributable generations through bounded registry pages and a separate broker fencer confirms their retirement before Mongo purge and both physical-retention barriers. Cleanup advances typed states by expected-state/revision CAS alongside existing analytics erasure before public completion. See [current specification](docs/specs/durable-hiring-workflows.md) for evidence and remaining gates.
