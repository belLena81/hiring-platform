# Architecture

The [security/runtime reliability slice](docs/specs/hiring-security-and-runtime-reliability.md) moves Admin provisioning to startup seed, protects authentication receipt fingerprints and replay, bounds provider responses and recovery backoff, and preserves pure policies and existing transaction boundaries.

See the [current application architecture wiki](docs/wiki/README.md) for a source-derived use-case map, domain and collection inventories, workflow patterns, security and runtime boundaries.

The [discovery and interview integrity correction](docs/specs/candidate-discovery-and-interview-integrity.md) retains the existing pure decisions and durable interpreters. Deletion removes selected workflows' children before their last attribution records; transport quarantine uses a bounded deterministic digest of record coordinates, payload digest and rejection reason. Residence integrity is a narrow Mongo startup proof over the existing document shape.

## Retrieval and durable publication boundaries

The [reliability specification](docs/specs/hiring-retrieval-publication-reliability.md) owns this implementation checkpoint. Pure search eligibility criteria render separately into ordinary MongoDB and Atlas Search filters. Ranked retrieval hits contain identity, score and embedding metadata; authoritative hydration supplies current public data. Geo/facet queries begin with the trusted active actor and return a typed authority failure when that actor is absent.

The existing lifecycle StateT and interview state/command ADTs remain the decision model. Kafka adapters own Resource lifetimes, partition concurrency and sequential durable progress per partition. A publisher claims immediately before sending and renews every claim for the whole send; the claim charges one attempt and `releaseUncharged` refunds it when the failure was not the event's own (see Kafka flow contracts below). Null payloads quarantine durably before progress; bounded clock skew defers without acknowledgment.

Producer attribution lives in `producer_registrations`, one row per subject, transactional id and producer generation, with bounded cleanup pages and broker-confirmed fencing checkpoints. Active registrations have no TTL. Retirement (`MongoProducerGenerationMaintenance.retire`) reads a cut-off from the database clock, fences the transactional id on the broker, then marks Fenced only the Active rows whose database-stamped `registeredAt` precedes the cut-off, so a successor generation that started after the fence keeps its rows; `fencedAt` and `expiresAt` are also database-stamped, so no application or tool clock takes part in registration, fencing or retention. Closing a producer is insufficient evidence.

Embedding values must prove the configured model identity before document persistence or query retrieval; an absent provider model retains the configured fallback. Application submission uses an immutable ID/status/revision snapshot with a transactional status guard that advances `jobs.admissionFence` (not the content revision). Workflow publication shares one Resource-owned loop: successful full bounded passes yield and continue immediately, while idle/deferred passes sleep for validated `kafka.interview.publication-poll-interval-ms` (100–60000 ms, default 1000). Individual producer transactions remain serialized. [Search/publication reliability](docs/specs/hiring-search-publication-reliability.md) separates measured local adoption from external acceptance.

Operational event payload ADTs contain only the analytical facts required by current consumers. The producer side of the wire contract is strict (the encoder emits exactly the seven envelope fields and the known payload keys, `OperationalEventWireCodec`), the reader side is tolerant (unknown additive keys are ignored; missing or mistyped known keys and repeated keys are malformed), and record headers carry the schema version. Outbox lists validate before insertion, receipt deduplication uses Mongo's unique insert, and publication renews every lease of a bounded four-claim wave before and during its single transactional send. Kafka owns transaction serialization and fencing. Operational facts use event time and permit out-of-order publication; interview revisions and inbox receipts govern workflow ordering. Cleanup integrity is established by migration 015 and exact native-validator verification, with explicit bounded maintenance audits and an active-state partial index. Temporary test namespaces use a guarded reusable local stack; durable proof resources stay separate. [Event and test reliability](docs/specs/hiring-event-and-test-reliability.md) tracks the local checks and external acceptance gates.

Duplicate delivery containment ([specification](docs/specs/duplicate-delivery-containment.md)): delivery is at least once, and a lease is not exclusivity, so two workers can briefly hold the same work. The store's unique key decides a duplicate. The operational consumer inserts a consumer receipt (`consumer_receipts`, `_id = consumerGroup:eventId`) before its offset can be committed (offsets commit in batches, never per record; a record that is not durable stops its partition before any later offset is committed); a duplicate key means the event is already handled. A record stays readable for at most `retention.ms + segment.ms` after it was produced, with or without later appends: the broker rolls and deletes an expired active segment (verified on `apache/kafka:3.9.2`). A duplicate key is a correct answer only while the receipt outlives that span, so before it consumes, the consumer reads both values from the broker (`KafkaTopicRetentionReader`, DESCRIBE_CONFIGS on the operational topic) and refuses to start unless the receipt TTL is at least `retention.ms + segment.ms + 1 hour` (`ConsumerReceiptWindow`; `-1` retention is refused). A configuration-only declaration was rejected because it would not reflect the topic the broker actually runs. The shipped values are 7-day retention, a 1-day segment roll and a 9-day receipt TTL, so a record is readable for at most 8 days. The bound is approximate, not exact: Kafka rolls a segment after `segment.ms` plus `segment.jitter.ms` (default 0), runs its deletion check every few minutes, and uses producer `CreateTime`, so a producer clock ahead of the broker delays deletion; the one-hour margin absorbs these. The check runs before any consumer exists on every restart, and again every 5 minutes while the consumer runs. A refusal at either point stops the consumer and records no further beat; the next start gate refuses again until the topic is corrected, and a corrected topic is picked up on a later restart without a process restart. A duplicate delivery extends the receipt's `expiresAt` to its own delivery time plus the TTL and never shortens it, so an outbox re-send of an `InFlight` row is still deduplicated. The receipt is the whole consumer effect today. An effect keyed by the event id is written in the same transaction as its receipt; that rule and its revisit trigger are on `ConsumerReceiptRepository`.

## Kafka flow contracts

The [Kafka flow hardening specification](docs/specs/kafka-flow-hardening.md) records the decisions below (2026-11-04); the source files named hold the rules.

- **Publication wave** (`OperationalEventKafkaRuntime`): the publisher claims up to four due events (the claim yields at most one event per partition key, so ordering within an aggregate is kept) and sends them in one Kafka transaction. Reason: a transaction costs two coordinator round trips, and per-record transactions bought nothing beyond idempotence because no consumer offsets are committed with them. Every claim's lease is renewed before the send and heartbeated during it. Outcomes: success marks each claim Published independently (one failure does not skip the others); a lease lost before the send sends nothing and releases the other claims uncharged; a lease lost during the send cancels it, leaves the lost claim to expire and releases the others uncharged; a fenced producer releases every claim uncharged and the generation ends; a failure in a wave of several claims that is not a broker-availability failure releases all of them uncharged and they retry one per wave, so one oversize or rejected event cannot exhaust the attempts of its neighbours. Attempts are charged at claim time and refunded by `releaseUncharged` (`OperationalEventOutboxRepository`). A crash between the Kafka commit and `markPublished` resends identical bytes, key and headers, which the consumer receipt absorbs.
- **Batched offset commits** (`KafkaPartitionProcessing`, `KafkaRuntimeSupport`): processed offsets of a partition are committed per batch (`kafka.consumer.commit-batch-size`, `commit-batch-interval-millis`, same keys under `kafka.interview`), never per record. Reason: a synchronous commit per record was the dominant RPC cost of every consumer. Correctness is unchanged because the receipt or quarantine is durable before the offset is emitted and an undurable record stops its partition first. The concurrency permit wraps processing only. A commit lost to a rebalance (`CommitFailedException`, `RebalanceInProgressException`) is logged and counted (`hiring.kafka.consumer.commits.dropped`) but does not restart the consumer; the redelivered records are deduplicated by receipts. Any other commit failure ends the stream.
- **Wire contract and headers** (`OperationalEventWireCodec`, `OperationalEventHeaders`, `InterviewMessageHeaders`, `JsonDuplicateKeys`): the producer is strict and the reader is tolerant, so an additive optional field can be introduced without a flag day; see [schema evolution](docs/schema-evolution.md#operational-event-evolution-policy). Records carry `content-type`, `hiring-schema-version` (1), `hiring-event-type` and `hiring-event-id` headers (interview records: content type, version and `hiring-step`); headers are optional metadata, the body is authoritative, a present header must agree with the body, an unsupported version is quarantined as `UnsupportedVersion` before the body is read, malformed headers (duplicate key, value over 256 bytes, non-UTF-8 contract value) quarantine as `MalformedEnvelope`, and headers are never stored or fingerprinted. No producer writes `traceparent` today; it is ignored on read.
- **Producer identity** (`ProducerIdentity`, `ProducerTransactionalId.parse` is the single parser): the transactional id is `<role prefix><worker id>` (`kafka.publisher.worker-id` / `KAFKA_WORKER_ID`, `kafka.interview.worker-id` / `INTERVIEW_WORKER_ID`, pattern `[a-z0-9][a-z0-9-]{0,63}`) and a fresh generation id (UUID) is drawn on every producer initialization. Reason: a restart with the same worker id now fences its predecessor through Kafka's epoch bump without operator action, while the generation id keeps per-lifetime attribution for erasure. One worker id per running instance, taken from a per-instance identity (for example a pod ordinal), never from a variable shared by replicas: two instances with one worker id fence each other, visible as repeated `hiring.kafka.producer.send.failures` with `error.type` `ProducerFencedException` or `InvalidProducerEpochException` within minutes. A single fence is expected: erasure and retirement fence the running publisher's transactional id, which restarts one generation. A Fenced *generation* never publishes again; the transactional id does.
- **SCRAM-SHA-512** is the client mechanism (`kafka.sasl-mechanism`, default SCRAM; PLAIN only for loopback brokers). The local broker layout and the production notes are in [security and operations](docs/wiki/security-and-operations.md#kafka-authentication-scram-sha-512).
- **Client tuning** (`KafkaClientSettings`): acks=all, idempotence, lz4 compression, `linger.ms` 5, `max.poll.records` and `max.poll.interval.ms` from configuration, `read_committed`; the producer's `delivery.timeout.ms` and `transaction.timeout.ms` are both `max(10 s, lease - 5 s)` of the outbox lease (`kafka.publisher.lease-seconds`, 15 to 905 s) or the interview claim window (`kafka.interview.claim-seconds`, also 15 to 905 s), the transaction timeout is additionally capped at the broker default `transaction.max.timeout.ms` of 900 s, and `request.timeout.ms` is `min(10 s, delivery / 2)`, so a wave's send plus commit always finishes inside the lease that protects it. Interview consumers apply the same header rules as the operational consumer (version, content type, `hiring-step` against the body's step, duplicate or oversize or non-UTF-8 headers refused) and refuse a body with a repeated JSON key; refusals go to the interview quarantine path with a closed reason. kafka-clients 4.3.1 and fs2-kafka 3.9.1 run against the `apache/kafka:3.9.2` Compose broker.
- **Topic contract**: see [big-data architecture](docs/big-data-architecture.md#kafka-topic-contract).
- **Client metrics** (`KafkaClientMetrics`): consumer lag from the client's own `records-lag` metric, producer send duration and failures by a closed error class, rebalances and dropped commits; see [local observability](docs/local-observability.md).
- **Receipt consumer**: the in-application operational consumer still only records the receipt; it exists to prove the deduplication and receipt-window rules. The first real consumer effect must be written in the same transaction as its receipt; the revisit trigger is on `ConsumerReceiptRepository` (`OperationalEventRepositories.scala`).

This document describes the active architecture and explicitly planned refinements. Collection snippets and diagrams are abbreviated design illustrations, not complete BSON validators or executable API fixtures. The [current GraphQL SDL](src/test/resources/graphql/hiring.graphql), [Mongo codecs](src/main/scala/com/example/graphQL/cats/repository/mongo/MongoHiringPersistenceCodecs.scala), and [index definitions](src/main/scala/com/example/graphQL/cats/repository/mongo/MongoHiringIndexSetup.scala) define exact implemented shapes. See [README](README.md), the [API reference](docs/api.md) and [schema evolution](docs/schema-evolution.md) for its contract and verification evidence.

The hiring API is one sbt build with constructor-injected application, API and infrastructure packages. A separate `analytics/` sbt build owns Spark/Delta batch, erasure and streaming runtimes. MongoDB remains operational truth; API transactions do not wait for analytics. See the [analytics boundary](docs/big-data-architecture.md) and [roadmap](docs/development-milestones.md) for current acceptance and planned work.

Supporting design: [use cases](docs/use-cases.md).

## 1. Goals

The Hiring Management Platform is designed around the following engineering goals:

- Clean separation between domain, application, API, and infrastructure code
- Functional effect management using Cats Effect
- MongoDB data modeling optimized for GraphQL access patterns
- Saga-based coordination for cross-boundary workflows with retries, compensation, and recovery; the pure Saga engine currently runs the interview scheduling workflow only
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
│   ├── crypto/          (Hmac, HmacJwt, SourceHash)
│   ├── CauseChain.scala
│   ├── HiringHttpPaths.scala
│   └── Parsing.scala
│
├── infrastructure/
│   ├── auth/            (Argon2 hasher, JWT issuer, HMAC receipt fingerprints)
│   ├── embedding/
│   ├── kafka/
│   ├── logging/
│   ├── search/
│   └── telemetry/
│
├── config/
├── runtime/
└── Main.scala
```

The API owns GraphQL, HTTP, and authentication adapters. It talks to service use-case protocols and service-owned request/result contracts. Service implementations own hiring use cases, authorization, effect sequencing, and the `service.port` interfaces used for persistence and external capabilities. Mongo and other infrastructure adapters implement those inward-facing ports; `repository.mongo` contains the MongoDB adapters. Domain policies remain pure and infrastructure-free. `shared` is reserved for neutral utilities with no dependency on domain or service contracts.

---

# 4. Domain Model

Domain objects must not represent MongoDB documents or GraphQL types directly.

Abbreviated example (the current `Job` also carries an optional `embedding`; its `state` is a `JobState`, `Draft`, `Open` or `Closed(at)`, so a closed job always has its closing instant, and `status` and `closedAt` are derived from it):

```scala
final case class Job(
  id: JobId,
  recruiterId: UserId,
  title: String,
  description: String,
  requirements: List[String],
  skills: Set[String],
  location: Location,
  state: JobState,
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

Phase 7 current local evidence is criterion-tagged test evidence on HEAD `1834236` (2026-10-13), with independent Code Reviewer, Security Engineer and final QA PASS; the HCS-08.3 topic-identity case (C11) is an accepted MVP limitation (2026-11-03), not a pass. See the [streaming specification](docs/specs/continuous-hiring-analytics.md#current-evidence-verification-specification-2026-10-13). The earlier `8c01e` closure (470 analytics unit tests, 45 executed integrations) relied on proof programs removed in commit `3755455` and is historical.

The October 5 user decision schedules analytics latency and burst drain-time qualification and optimization in the final phase after deployment to the real environment. Local Phase 7 closure retains correctness, authorization, recovery, retention, bounded backlog/storage and maintenance requirements; the recorded 40,084-ms Bronze p95 failure remains a deferred SLO result. Genuine deployed retention and operational activation prerequisites still apply before production streaming. See the [final deployed optimization milestone](docs/development-milestones.md#deployed-analytics-optimization-phase-15-final-phase).

Phase 8 first establishes MongoDB/vector access baselines, Phase 10 completes evaluated AI discovery, and Phase 11 applies durable Saga and pure State decisions to the interview workflows (the generic engine drives scheduling only; cancel/reschedule, subject cleanup and analytics erasure keep their own pure policies, see section 15). Further Spark refinement follows in Phase 13. See the planned Kafka contracts below; their proposed topics are not currently provisioned.

The [remaining capability specifications](docs/development-milestones.md#remaining-delivery-order-and-acceptance) define concrete behavior and boundaries for Phases 8–15. They keep resource-owned adapters and analytics' `F[_]` ports; the application's ports and services are written over `F[_]` ([effect-polymorphic services](docs/specs/effect-polymorphic-services.md)), and a generic Saga engine exists in `domain/saga` and drives interview scheduling only ([hiring saga orchestration](docs/specs/hiring-saga-orchestration.md), done locally 2026-10-15; the lifecycle port failed its go/no-go gate), both user-approved 2026-10-14. Proposed contracts and provider/deployment alternatives are labelled separately from current source facts; an unresolved choice blocks its dependent implementation. Specification readiness does not replace implementation, privacy, recovery or deployed acceptance evidence.

Other pattern use remains conditional: Decorator for shared instrumentation across multiple provider adapters (preserving error and cancellation behavior without duplicate retries); Observer as managed streams for new local notifications while restart-recoverable work keeps durable handoffs; Bridge only for integrations with two independently varying dimensions; typed Filter criteria as discovery requirements grow; recursive Composite only for accepted nested Boolean criteria; and additional ranking strategies only when evaluation demonstrates benefit. See [development milestones](docs/development-milestones.md) for phase placement and acceptance conditions.

---

# 5. Ports

Infrastructure dependencies are represented through interfaces in `service.port`. Ports are written over `F[_]`: use cases return `UseCaseF[F, A] = EitherT[F, UseCaseError, A]`, while operational ports return `RepositoryF[F, A] = EitherT[F, RepositoryError, A]`. Services take the weakest Cats Effect capability they need; Mongo, Kafka and HTTP adapters, `runtime/` wiring and the Sangria interop fix `IO`. Typed ports, fake adapters and `TestControl` provide the service-test seams.

```scala
trait JobRepository[F[_]]:
  def find(id: JobId): RepositoryF[F, Option[Job]]

trait JobUseCases[F[_]]:
  def viewJob(actor: ActorContext, jobId: JobId): UseCaseF[F, Job]
```

AI infrastructure is also hidden behind ports:

```scala
trait EmbeddingService[F[_]]:
  def embed(input: EmbeddingInput): F[Either[EmbeddingError, EmbeddingVector]]
```

The Voyage adapter (`infrastructure/embedding/VoyageEmbeddingService.scala`) implements `EmbeddingService[IO]` and also `MeteredEmbeddingService`. `embedMetered` returns a `MeteredEmbedding`: the same vector that `embed` returns, plus the provider-reported `usage.total_tokens`. The token count is metadata. It is absent, never zero, when the field is missing, malformed or negative, and it never fails an embedding. The worker-facing port is unchanged, so the embedding workers and the search services do not read the count. Only the owner-started search evaluation runner calls `embedMetered`, through `VoyageEmbeddingService.meteredResource`, which applies the same request, response-size limit and single-vector validation as `embed`. Reason: the count is billing evidence for the evaluation, and adding it to `EmbeddingService.embed` would change every caller of the port. Each `meteredResource` opens its own Ember client, as `resource` does.

Application services depend on these interfaces. `RepositoryError` belongs to this service boundary; adapters translate driver failures into it.

---

# 6. Effect Architecture

Cats Effect controls all side effects.

Services are effect-polymorphic; `IO` is selected only in `Main`, `runtime/` wiring, the `api/` boundary (Sangria interop, HTTP, admission), configuration loading, and the adapters that deliberately fix it (Mongo, Kafka, auth, embedding, logging and telemetry). `UseCaseF` and `RepositoryF` keep expected failures typed in `EitherT`. See [local engineering quality](docs/engineering-quality.md) for the rule.

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

The implemented composition follows this shape. `Main.run` reads `AppConfig` once (the masking flag is read first so invalid configuration still logs safely). `MongoHiringRuntime.resource(config, diagnostics, embeddingService)` then runs Mongo setup and Admin seeding, creates one Argon2 hasher, and builds the services. Vector search is a closed `VectorSearchConfig` ADT, `Disabled` or `Enabled(voyage, embedding, indexes, ...)`, that selects either the disabled capability or the embedding pipeline and semantic search. Setup, the Admin seed, and the Kafka and interview runtime resources are acquired before `HiringApiRoutes` builds the single HTTP middleware stack, which applies the active-request cap to every route except `/health` and `/ready`. The maintenance entry points `EmbeddingWorkRepair`, `ProducerGenerationRetirementMain` and `MongoWorkflowIntegrityAudit` load configuration on their own through `AppConfig.load`.

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
  version: Long(0),                 // content revision, incremented by profile changes and deletion only (not by embedding writes or fences); not an API or embedding version

  role: "Candidate",                // stored Scala enum name: "Candidate" | "Recruiter" | "Admin"
  accountStatus: "Active",          // "Active" | "Deleted"

  name: "Alice Smith",
  nameCanonical: "alice smith",     // NFKC, trimmed, lower-cased (AccountName.canonical); unique
  email: "alice@example.com",       // absent on Deleted accounts
  emailCanonical: "alice@example.com", // sparse unique; absent on Deleted accounts

  profile: {                        // Candidate shape; a Recruiter profile carries organizationName and jobTitle instead
    kind: "Candidate",
    skills: ["Cats Effect", "MongoDB", "Scala"],       // stored sorted
    skillsCanonical: ["cats effect", "mongodb", "scala"], // canonical sidecar used by search filters
    experienceSummary: "...",
    resumeRef: "...",
    currentResidence: {             // optional; canonical sidecars for filters
      country: "Ukraine",
      countryCanonical: "ukraine",
      city: "Kyiv",
      cityCanonical: "kyiv"
    },
    availabilityStatus: "AVAILABLE_NOW",
    recruiterSearchOptIn: true
  },

  embedding: BinData(9, ...),       // optional candidate vector: BSON binary vector, float32 (migration 028); removed on account deletion
  embeddingMeta: {                  // present with embedding
    model: "...",
    sourceHash: "...",
    updatedAt: ISODate(...)
  },

  createdAt: ISODate(...),
  updatedAt: ISODate(...)           // written on insert (= createdAt), then at each update of the account
}
```

A Deleted account keeps a tombstone (`accountStatus`, `deletedAt`, a tombstone `name` and `nameCanonical`); its email, password hash, profile and embedding fields are removed. In the domain the pair is one value, `AccountState.Deleted(at)`; the codec rejects a stored `Deleted` without `deletedAt`.

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
{ emailCanonical: 1 } UNIQUE SPARSE                          // users_emailCanonical_unique
{ nameCanonical: 1 } UNIQUE                                  // users_nameCanonical_unique
{ accountStatus: 1, createdAt: -1, _id: -1 }                 // users_accountStatus_created_id
{ role: 1, accountStatus: 1, createdAt: -1, _id: -1 }        // users_role_accountStatus_created_id
{ adminSingletonKey: 1 } UNIQUE WHERE role = "Admin"         // users_adminSingleton_unique
{ "embeddingMeta.model": 1, role: 1 }                        // users_embedding_meta_filters
```

Do not create indexes for fields merely because they exist.

---

# 9. Jobs Collection

```javascript
{
  _id: UUID,
  version: Long(0),                 // content revision, incremented by job edits, status changes and closure only (not by submissions or embedding writes)
  admissionFence: Long(0),          // optional (absent = 0), internal conflict fence; $inc by application submission and interview scheduling

  recruiterId: UUID,

  title: "Senior Scala Developer",

  description: "...",

  requirements: [
    "5+ years JVM",
    "Functional programming"
  ],

  skills: [                         // display values, stored sorted
    "Cats Effect",
    "MongoDB",
    "Scala"
  ],
  skillsCanonical: [                // one per skill, same order: trim + lower-case (SearchTerm.canonical); every job skill filter compares these
    "cats effect",
    "mongodb",
    "scala"
  ],

  location: {
    country: "Ukraine",
    city: "Kyiv",                   // optional, display value
    cityCanonical: "kyiv",          // present exactly when city is; every job city filter compares this
    remote: true,
    point: {                        // optional GeoJSON Point, [longitude, latitude]
      type: "Point",
      coordinates: [30.52, 50.45]
    }
  },

  status: "Open",                   // stored Scala enum name: "Draft" | "Open" | "Closed"

  createdAt: ISODate(...),
  updatedAt: ISODate(...),
  closedAt: ISODate(...),           // optional

  embedding: BinData(9, ...),       // optional job vector: BSON binary vector, float32 (migration 028)
  embeddingMeta: {                  // present with embedding
    model: "...",
    sourceHash: "...",
    updatedAt: ISODate(...)
  }
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
  jobs(filter: { city: "Kyiv" }, first: 20) {
    searchId
    edges { node { id title } }
    pageInfo { hasNextPage endCursor }
  }
}
```

The city filter is case-insensitive: it compares the canonical form (trimmed, lower-cased), and the same rule applies to the `skills` filter (`$all` over `skillsCanonical`).

Primary index:

```javascript
{
  status: 1,
  "location.cityCanonical": 1,
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

Complete job index set (`MongoHiringIndexSetup`):

```javascript
{ recruiterId: 1, status: 1, createdAt: -1, _id: -1 }                     // jobs_recruiter_status_created_id
{ recruiterId: 1, createdAt: -1, _id: -1 }                                // jobs_recruiter_created_id
{ createdAt: -1, _id: -1 }                                                // jobs_created_id
{ status: 1, createdAt: -1, _id: -1 }                                     // jobs_open_created_id
{ status: 1, "location.cityCanonical": 1, createdAt: -1, _id: -1 }        // jobs_open_city_canonical_created_id
{ "location.point": "2dsphere" }                                          // jobs_location_point_2dsphere
{ "embeddingMeta.model": 1, status: 1, "location.cityCanonical": 1, recruiterId: 1 } // jobs_embedding_meta_canonical_filters
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

Complete application index set (`MongoHiringIndexSetup`):

```javascript
{ candidateId: 1, jobId: 1 } UNIQUE                 // applications_candidate_job_unique
{ candidateId: 1, status: 1, createdAt: -1, _id: -1 } // applications_candidate_status_created_id
{ candidateId: 1, createdAt: -1, _id: -1 }          // applications_candidate_created_id
{ jobId: 1, status: 1, createdAt: -1, _id: -1 }     // applications_job_status_created_id
{ jobId: 1, createdAt: -1, _id: -1 }                // applications_job_created_id
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
  actorRole: "Recruiter",   // stored Scala enum name; required (ledger step 033 and the validator). Not exposed by GraphQL as an identifier.

  // feedback and reason are present only when supplied.
  // previousStatus is absent on the initial CREATED event.

  occurredAt: ISODate(...)
}
```

Index (`application_events_application_created_id`):

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

Future implementation should add Saga in the smallest vertical slice that includes one real external boundary, durable state, retry and compensation tests, and observability. The earlier rule against a generic Saga framework is superseded by the user-approved [hiring saga orchestration](docs/specs/hiring-saga-orchestration.md) (2026-10-14; done locally 2026-10-15). Its pure engine (`domain/saga`, standard library only) currently drives only the interview scheduling workflow. Interview cancel/reschedule was evaluated and not ported; interview subject cleanup and analytics erasure were not ported by decision and keep their existing pure policies. The analytics build does not use the engine.


### Planned Kafka workflow contracts

Phase 11 must produce a reviewed topic/stream catalog before changing routing. Current implementation uses `hiring.operational-events`, a seven-field JSON envelope whose schema version travels in the `hiring-schema-version` record header (version 1; absent means 1), transactional outbox publication and idempotent consumption. The following is a target logical split, not a declaration of existing topics or new infrastructure. Keep a stream together when ownership, ordering, retention and access rules match; split only when these requirements differ. Resolve exact topic names, partitions, replication/minimum ISR, size limits and retention from the bounded workload and recovery/privacy requirements before implementation.

| Stream | Owner and key | Delivery, retention and access contract |
|---|---|---|
| Operational facts (`hiring.operational-events`, existing) | Domain outbox producer; audit the existing aggregate partition key before any change | Immutable facts, finite delete retention, consumer group per independent capability; preserve analytics/deletion contracts |
| Workflow commands (proposed capability-specific topic) | Application workflow orchestrator (today the interview workflows: scheduling through the saga engine, cancel/reschedule through `InterviewLifecyclePolicy`); workflow ID | One command owner/worker group; persisted deadline and step idempotency key; least-privilege producer/consumer ACLs |
| Workflow results (proposed) | Owning worker; same workflow ID | Correlate command/step/revision; orchestrator consumes durably; reject stale or unrelated replies |
| Discovery telemetry (split only if justified) | Authorized telemetry producer; documented session/query key | Best-effort product telemetry must not delay domain commands; bounded retention and minimized payload; never a source of hiring truth |
| Retry/quarantine (capability-specific when required) | Owning consumer/operator | Bounded attempts, restricted access, sanitized failure codes, explicit replay authorization and expiry; no automatic replay of deleted subjects |

Preserve the current active envelope until a scoped change updates all producers, consumers, analytics parsers and fixtures. Proposed workflow messages require typed command/event ID, correlation/workflow ID, causation ID, step ID, expected workflow revision and UTC occurrence/deadline values. IDs/revisions are distinct from any future schema version. Dual readers/writers and compatibility code are added only by a scoped change that follows [schema evolution](docs/schema-evolution.md); none is speculative. An existing incompatible local lakehouse stays preserved and fails closed unless its exact reset is authorized.

Ordering exists only within a partition, and the same key on separate topics does not create cross-topic order. Operational facts use the envelope `aggregateId` (job, application or search ID) as the Kafka record key, so one aggregate stays in one partition; the per-user subject leases of the outbox claim serialize publication per user and do not give aggregate order. The outbox claim itself enforces aggregate order: it does not return an event while an earlier `Retryable` or `InFlight` event with the same `partitionKey` exists, ordered by `(occurredAt, _id)`, so a retrying or lease-expired event is delivered before its successors while other aggregates keep publishing. A `Failed` event (retries exhausted) does not block its aggregate: later events continue, the gap stays visible as a `Failed` outbox row with its sanitized `lastError` and the publisher failure log for operator repair. Events written in one transaction (for example `APPLICATION_STATUS_CHANGED` and `CANDIDATE_HIRED`) have no defined relative order, and consumers must not depend on one; no sequence field exists. Preserve outbox aggregate order, process each ordered key sequentially with bounded parallelism across independent keys, and advance offsets only through the contiguous durable completion frontier. Partition-count/key changes require explicit routing/replay analysis. Delayed results and retries are checked against durable workflow revisions and allowed transitions, not arrival order. Retry topics may reorder records; block later commands for that workflow or validate them against the durable state before applying them.

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

Batch loaders should be scoped to the GraphQL request to provide both batching and request-local caching. The implemented fetchers (`HiringGraphQLDsl`) cap each repository call at 100 keys (`PageSize.Max`) and split larger sets into batches; their caches live only for one request, so no data is shared across users or requests.

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

  embedding: BinData(9, ...)        // float32 BSON binary vector
}
```

Embeddings are stored as BSON binary vectors (binData subtype 9, float32) rather than float64 arrays: the element type in the domain is `Float`, so the stored form is exact and about a third of the size (0.31 measured for 1000 × 1024-dimension vectors). The Atlas vector index keeps `path: embedding`, `numDimensions` and `similarity`; job vector and lexical indexes carry the canonical filter paths (`location.cityCanonical`, `skillsCanonical`) under the configured names (defaults `jobs_embedding_vector_canonical_v1`, `jobs_text_search_canonical_v1`).

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
  embedding: BinData(9, ...),

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

Within one claim, a vector the provider already returned is held in memory, keyed by model and source hash. A transient entity-write failure, or a revision conflict whose re-read still needs an embedding with the same source hash, persists the held vector without a second provider call; a changed source hash calls the provider again. The held vector never outlives its claim and never serves another entity. A crash or lease loss after the provider answered repeats the call, because Voyage offers no idempotency key (assumption, not verified against provider documentation). A claim that takes over an expired `Processing` lease logs `EMBEDDING_LEASE_RECLAIMED` (WARN, attempt count only), so the count of these events bounds that residual. See [duplicate delivery containment](docs/specs/duplicate-delivery-containment.md).

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

Skill-evidence ranking is implemented as a pure function in `domain/search/SkillEvidenceRanking.scala`, beside `SearchFusionStrategy`. It depends only on `SearchTerm` and cats data types, performs no I/O, and has unit tests in `SkillEvidenceRankingSpec`. It is not adopted and has no production caller: `SemanticSearchService`, the repositories and the GraphQL layer do not call it. The fusion described above is therefore unchanged, and recommendations and recruiter matches keep today's order. Adoption is decided separately under the HDQ-08 gates in the [discovery and search quality specification](docs/specs/hiring-discovery-search-quality.md#candidate-ranker-and-personalization-adoption-path-hdq-07-hdq-08). Adoption would place the ranker after the authoritative recheck and before the result limit. Its internal `fusedValue` maps to no API field.

Raw candidate-filter requests pass through one pure accumulating validator before embedding or retrieval. `ValidatedCandidateMatchFilters` supplies canonical skills/residence and typed availability to both Mongo predicates and authoritative eligibility; profile comparisons use the same Locale.ROOT canonicalization. Original skill-count and blank-input limits are checked before deduplication, and the service preserves its established outward error precedence. GraphQL and stored field shapes are unchanged.

### Bounded hiring read authorization

`HiringReadScope` is created only after persisted-actor validation (`ActorAuthorization.readScope`), over the actor projection (`ActorRecord`, read by `users.findActor`). An `AuthenticatedActor` carries the projection read for the request, so later authorization in the same request makes no further user read; search revalidation after a provider or vector call uses `currentReadScope`, which always re-reads. `AuthorizedJobAccess` and `AuthorizedApplicationAccess` are the one resolve/load-ownership/`canManage` chains for jobs and applications, over the ownership projection (`JobOwner`, `jobs.findOwner`). `ActorAuthorization.requireAdmin` is the shared check for a live, Active, singleton Admin and is used by analytics reporting, embedding coverage and the interview repair/notification-repair operations. Mongo application/history and relationship queries gate the actor and authorized parent in the same selection. Relation-aware fetcher keys remain scoped to each request. Search eligibility uses required scoped projection ports and a pure `SearchEligibilityPolicy`: services sequence prechecks/retrieval, adapters enforce the final actor/query-entity gate and bounded selection, and policy validates current source/metadata/filter state before response truncation. Driver errors carrying transient transaction labels remain with the transaction retry owner inside active outbox sessions, with final sanitized errors at the repository boundary. The [MongoDB/vector specification](docs/specs/mongodb-vector-retrieval-optimization.md) tracks local, live Atlas and performance acceptance separately.

### Local interview coordination

Interview hiring commits derive aggregate and history values from the same pure `ApplicationLifecycle` program used by ordinary status mutations; the Mongo adapter retains atomic persistence and authorization. Workers receive an explicit diagnostics sink. Cleanup isolates failures between subjects while preserving sequential fencing, purge and retention steps within each subject; a failed subject stays retryable. Embedding provider validation rejects non-finite vector values before either document persistence or query execution. The [boundary reliability specification](docs/specs/embedding-workflow-boundary-reliability.md) owns validation evidence for these refinements.

Application status changes have one persistence path, `ApplicationStatusWrite`: a `$set` of status and `updatedAt` guarded by the previous status and the stored owner facts, the append-only history row and the outbox rows in one transaction. The API status actions, the interview hiring commit and the interview cancellation all use it, so the stored status is always the latest history row. Interview request idempotency is a `mutation_receipts` row (`Idempotent.executeFor`, operation `interviewWorkflow`) sharing the transaction of the guarded write, and the hiring commit is evidenced by its deterministic history row; the interview collections keep no receipt documents of either kind. The outbox claim leases a bounded batch with one claim token and one subject-lease bulk write per page; `LeaseHeartbeat.guard` is the shared renewal loop of the interview publisher, the embedding pipeline and the Kafka publisher, and `MongoLeaseQueue.claimReady` plus `LeaseRequest.validate` serve the embedding and search-session work queues. Details, accepted deviations and the ledger steps 027 to 030 are in [the MongoDB design](docs/mongodb-design.md#status-write-work-rows-and-the-batch-claim).

Cleanup selection uses bounded subject-ID sweeps and independent row decoding. An immutable opaque continuation is threaded through a sequential resource-owned stream; only the Mongo adapter interprets its BSON ordering coordinates. Durable cleanup progress remains protected by existing state/revision CAS and fencing, while page failures retain scheduling position. Pure `InterviewMessagePolicy` functions classify command, result and expired observations in the application layer, preserving acknowledgment/reconciliation rules without importing service ports into the domain. Nearby cursor canonical encoding belongs to its codec; rank contributions and facet dimensions use named immutable values.

`InterviewExecutionPolicy` stages pure admission, lease and budget decisions over fresh stored observations; the Mongo adapter retains conditional lease decoding, transactional budget reads, subject fences and guarded writes. Expired execution queues reconciliation without consuming another slot. Domain transitions and repair return a named `InterviewWorkflowDecision` with ordered intents. Scheduling receives reusable clock/identity effects while Mongo independently checks current deadline time. Admin repair reconciles existing effects; it neither reopens a released reservation nor extends its immutable deadline.

`InterviewSchedulingService` owns actor authorization and request persistence; capability-specific pure workflow transitions produce outgoing intents. `MongoInterviewWorkflowRepository` is the entry point over the Mongo interview components: `MongoInterviewWorkflowStore` (shared collections and write fences), `MongoInterviewCommandQueue` (command claims, execution budgets, publication and settlement), `MongoInterviewLifecycleStore` (cancellation, rescheduling and proposal transitions), `MongoInterviewCalendarLedger` and `MongoInterviewNotificationLedger`; `MongoInterviewWorkflowCodec` owns the stored document shapes. It commits revision, inbox receipt and outgoing intent atomically and owns guarded hiring commits. Resource-owned workers interpret effects through the ledger calendar adapter and the notification provider named by `kafka.interview.notifications.provider`. The default is the platform ledger, which records one receipt per idempotency key and sends nothing. The prepared SES adapter is at least once (a duplicate email is possible, a silent loss is not) and is not activatable in this build: with the interview runtime enabled, selecting it fails application startup, because the interview runtime is composed into `MongoHiringRuntime` and no SES client exists yet ([SES operations note](docs/ses-notification-operations.md)). Separate command/result topics use workflow IDs as keys; scheduling details stay in MongoDB and operational event envelopes keep their seven fields. Provider receipts and reconciliation coordinate effects outside hiring transactions. Pure workflow policies select applicability, repair and bounded execution/publication outcomes; typed advancement causes own receipt identity. Expired execution reconciles with stable provider keys. Each initialized producer generation (stable transactional id per worker id, fresh generation id per initialization) registers in `producer_registrations` with kind Interview inside `MongoInterviewCommandQueue.authorizePublication` before sending; a replacement keeps the transactional id, draws a new generation id and is authorized afresh. Subject deletion traverses all attributable generations through bounded registry pages and a separate broker fencer confirms their retirement before Mongo purge and both physical-retention barriers. Cleanup advances typed states by expected-state/revision CAS alongside existing analytics erasure before public completion. See [current specification](docs/specs/durable-hiring-workflows.md) for evidence and remaining gates.
