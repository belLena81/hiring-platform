# Domain language and bounded contexts

The shared vocabulary for specs, tests, code, GraphQL names and agent handoffs. It names concepts and boundaries; field-level detail stays in the [domain wiki](wiki/domain-and-use-cases.md), [use cases](use-cases.md) and source. Behavior rules stay in [AGENTS.md](../AGENTS.md). When this page and source disagree, source describes implemented behavior: report the mismatch and fix the page.

Add a term here in the same change that introduces it, and only when it is new or ambiguous. Do not list every field. Use these words, not synonyms, in specs, acceptance criteria, test names and identifiers.

## Glossary

| Term | Meaning | Not to be confused with | Code / API name |
|---|---|---|---|
| Candidate | Role that searches visible open jobs, applies, and sees only their own applications | Admin/Recruiter accounts | `UserRole.Candidate` |
| Recruiter | Role that owns jobs and handles applications to those jobs | Any user who can see a job; only the owner (or Admin) may manage it | `UserRole.Recruiter` |
| Admin | Singleton operator provisioned only by explicit seed/config | A generic superuser role created at runtime | `UserRole.Admin` |
| Job | A posting owned by one Recruiter; Draft, Open or Closed | An Application | `Job`, `JobStatus` |
| Publish / Close | Job lifecycle actions: Draft→Open, Draft/Open→Closed. A closed job rejects new applications | Delete | `publishJob`, `closeJob` |
| Application | One Candidate's request for one Job; at most one per pair | A resume or profile | `Application` |
| Submit | Create an Application in `Created`, with initial history | Apply-then-accept in one step | `submitApplication` |
| Accept / Move to interview / Hire / Reject / Decline | Action-specific status transitions. Reject needs feedback; Decline needs a reason. All status actions require the owning Recruiter or the singleton Admin; there is no candidate withdrawal action | A generic status update (none exists) | `ApplicationLifecycle`, matching mutations |
| ApplicationStatus | Closed set: Created, Accepted, Declined, Interview, Hired, Rejected. Declined, Hired and Rejected are terminal | Job status | `ApplicationStatus` |
| ApplicationEvent | One append-only history record: previous/new status, actor, UTC time, feedback/reason. Written atomically with the status | An operational event on Kafka | `ApplicationEvent` |
| Operational event | Fact published from the transactional outbox for consumers: at-least-once, idempotent consumers, one unversioned seven-field envelope | ApplicationEvent history | outbox / Kafka publication |
| Interview workflow | Durable, conditional scheduling around Accepted→Interview; it adds no statuses | The status `Interview` | `domain/workflow` |
| Embedding | Derived search data on a Job or Candidate; never operational truth | A user-visible field | embedding work |
| Consent | Candidate's recruiter-search opt-in; governs candidate matching visibility | Authentication | `CandidateProfile` |
| Late fact | Analytics event that arrives after its UTC day is closed; retained with an expiry and excluded from published reports until replayed | A quarantined (invalid or conflicting) event | `AnalyticsLateFactStage` |
| Candidate watermark | Event-time bound a streaming batch proposes; becomes the report watermark only after the batch's terminal commit | Kafka offset or Spark checkpoint position | `candidateWatermark` |
| Erasure marker | Durable record that a deleted account's subject must be excluded from every analytics write and report | The account tombstone in operational MongoDB | active deletion marker |
| Durable boundary | One of seven persisted points in a streaming batch (Prepared … TerminalCommitted) from which recovery resumes | A Spark micro-batch commit | `StreamingDurableBoundary` |
| Stream owner | Exclusive Mongo-held ownership of one lakehouse by one running stream | The per-callback lakehouse mutex | `MongoAnalyticsStreamingRegistry` |
| Activation grant | Immutable, expiring authorization that permits a streaming process to start | A user role or JWT | `StreamingActivationGate` |
| Lexical-query facet | Bucket counts (skills, countries, cities, remote) for a job text query, computed from the authoritative jobs collection restricted to a bounded set of lexically matched IDs (cap 1000). Payload field `coverage`: `ALL_MATCHES` (counts cover every matching Open job) or `TOP_MATCHES` (more than the cap matched; counts cover only the top-N by lexical score, never a total). Search failure is a typed sanitized `FACETS_UNAVAILABLE` (`SEARCH_UNAVAILABLE`). Never read from the search index | Structured facet (no text query); search-index facet collector | `jobDiscoveryFacets(query:)`, `HDQ-12` |
| Human label gate | Pending sign-off by a human domain reviewer of fabricated relevance judgments against an exact corpus digest. Criterion suffix `.H`; blocks ranking adoption whatever the `.L`/`.C` outcomes | Automated or agent rubric verification | `SEB-02.H` |
| Atlas-compatible local gate | Search acceptance evidence from the $0 Atlas Local container (`compose.atlas-local.yaml`, mongot, loopback). Criterion suffix `.L`. Proves branch behavior and runs a rehearsal; never Atlas acceptance, never adopts a tuning change | Atlas cloud gate | `MVR-xx.L`, `DisposableAtlas.deployment = local-container` |
| Atlas cloud gate | Search acceptance evidence from an authorized disposable cloud Atlas deployment: native `$rerank`, cloud-scale recall/latency, index memory/storage, telemetry. Criterion suffix `.C`; the only gate that can adopt a vector tuning change | Atlas-compatible local gate | `MVR-xx.C` |
| Circuit breaker | Dependency guard around one outbound adapter (closed, open, half-open). While open it rejects calls without touching the dependency; it counts only dependency-unavailability failures, never business outcomes. Process-local; applied to Voyage, Atlas discovery, the Kafka publisher and Mongo writes only. Only the Mongo-writes breaker affects readiness. See [dependency circuit breakers](specs/dependency-circuit-breakers.md) | Retry or timeout; readiness probe | `CircuitBreaker` |
| Saga | Durable, revisioned multi-step workflow across a boundary transaction cannot cover (calendar, notification, retention). Advances only through a pure decision plus an atomic store write. Implemented for interview scheduling only; cancel/reschedule, subject cleanup and account erasure are not Saga definitions. See [hiring saga orchestration](specs/hiring-saga-orchestration.md) | A single-transaction status change; an outbox publication | `SagaDefinition`, `SagaEngine` (scheduling definition: `InterviewSchedulingSaga`) |
| Saga step | One named unit of a Saga with an idempotency key, a retry budget and a declared undo (Compensation or Irreversible) | An application status | `SagaStep` |
| Compensation | A semantic undo of a completed Saga step (for example releasing a calendar reservation). Idempotent, bounded retry; failure leads to Repair | Database rollback | `StepUndo.Compensate` |
| Irreversible step | A Saga step that cannot be undone (for example a delivered notification in the scheduling definition); recovery is forward-only and its reason is declared. Account tombstone and elapsed retention are not Saga steps; they keep their existing forward-only policies | A step without compensation by omission | `StepUndo.Irreversible` |
| Repair | Saga status `RepairRequired`: automatic progress stopped and an Admin action resumes by lookup, never by repeating an effect with a new key | Retry | `SagaStatus.RepairRequired` |
| Subject fence | Per-user document in `outbox_subject_fences` written by account deletion and by guarded writes in the same transaction, so a write racing a deletion conflicts instead of committing for a deleted actor. Two writers use the same document: application submit/status change (`MongoApplicationWriteFence`: reads the active actor, then `$inc fencingVersion`; leaves `users.version` alone) and the interview workflow (`MongoInterviewWorkflowStore.fenceParticipants`, which also bumps `users.version`; its `writeApplicationStatus` runs under that fence). The two are kept separate on purpose: the interview path fences several participants and tolerates the extra user write, the application path is per actor and hot | The account tombstone; a Saga compensation | `MongoApplicationWriteFence`, `fenceSubjects`, [ARR-02](specs/architecture-review-remediation.md) |
| Query cost budget | Per-actor (or per-client-IP when anonymous) allowance of GraphQL complexity per time window, charged before execution | Request-count rate limit; the per-query complexity cap | ARR-14 |
| Schema exposure | Configuration that enables introspection and the SDL download; off in the production default | Authorization of data fields | `graphql.schema-exposure`, ARR-15 |
| Persisted-query allowlist | Optional mode (`graphql.persisted-queries.mode = allowlist`, default `off`) in which `/graphql` runs only full documents whose SHA-256 is listed in a startup-read manifest; others get HTTP 400 `PERSISTED_QUERY_NOT_FOUND`. Introspection-only documents are governed by schema exposure. Full documents only, no hash-only registration. See [API reference](api.md#persisted-query-allowlist) | Document cache; query cost budget; schema exposure | `graphql.persisted-queries`, `PersistedQueryAllowlist` |
| Error code | Closed GraphQL enum `ErrorCode` on `UserError.code`, derived from the failure catalog; top-level errors carry the same value as the string `extensions.code` | Free-form message text; HTTP status | `ErrorCode`, `GraphQLFailureCatalog` |
| Bounded top-N list | Search result list of at most N items from one ranking pass, with no cursor and no `pageInfo`, because the service cannot continue it | Paginated connection | `RankedJobResults`, `RankedCandidateResults` |
| Effect-polymorphic port | Service or repository port written over `F[_]` with the weakest Cats Effect capability it needs; `IO` is fixed only at Main, runtime wiring, configuration loading, IO adapters (Mongo, Kafka, auth, embedding) and the Sangria interop. See [effect-polymorphic services](specs/effect-polymorphic-services.md) | A generic framework, free algebra or transformer stack | `RepositoryF`, `UseCaseF` |
| Search recording failure | The operational fact (`SEARCH_RECORDING_FAILED`) that a search ran but its search session could not be recorded: search id, search kind and a bounded error code or exception class only. Published best-effort; it never changes the search response | Any query text, result id or message | `OperationalEventPayload.SearchRecordingFailed`, [AIR-04.3](specs/ai-search-resilience.md) |
| Search filter snapshot | The criteria a search session was run with, kept as a closed type (`JobCriteria` with city, sorted skills and optional created-after; `Unfiltered`; `ForJob`) instead of free-form JSON. Stored with the session; stored text decodes only as one of the three written shapes, otherwise the stored document is invalid. The API builds it from the request | The query text or the job-discovery request input | `SearchFilterSnapshot`, `SearchFilterWireCodec`, [ARR-09](specs/architecture-review-remediation.md) |
| Degraded search | A ranked search answer produced by the lexical fallback because the query embedding provider (`ProviderUnavailable`) or `$vectorSearch` (`VectorSearchUnavailable`) was unavailable; it carries the reason and its hits have mode `LEXICAL` and no embedding model. A missing or stale embedding is never degraded; it stays its own typed failure. If the fallback is also unavailable the result is `VectorSearchUnavailable`. Applies to `semanticJobSearch` and `candidateMatches` only, not `recommendedJobs` or facets | A partial result or an empty page; a retry | `DegradedSearch`, `SearchResults`, [AIR-05](specs/ai-search-resilience.md) |
| Lexical fallback | Keyword-only retrieval (`$search`, no vector, no provider call) used for a degraded search. It keeps every live eligibility check of the vector path (open job, filters, Candidate role, Active account, profile, consent-gated private filters, authorization) and drops only embedding freshness, then re-checks hits against current data. It shares search admission, `maxTimeMS` and the breaker with vector search and runs only after role, ownership and open-job checks | Hybrid search (lexical plus vector, normal mode); the `jobDiscoveryFacets` lexical match | `LexicalSearchQuery`, `SearchEligibilityPolicy.jobLexical`, `candidateLexical`, [AIR-06](specs/ai-search-resilience.md) |
| Operational event wire codec | The infrastructure component that turns an operational event into its seven-field JSON envelope and payload bytes and back, and enforces the envelope size budget where bytes exist (outbox write, Kafka decode). The service layer owns the typed payload and the pure semantic rules (`OperationalEventContract`) and never touches JSON; the stored and published bytes are fixed by golden tests | A versioned event schema; the Mongo storage codec | `OperationalEventWireCodec`, `OperationalEventContract`, [ARR-09](specs/architecture-review-remediation.md) |

## Bounded contexts

| Context | Owns | Relationship |
|---|---|---|
| Hiring (operational) | Users, Jobs, Applications, history, workflow, outbox; MongoDB is the source of truth | Upstream of the others. Transactions never wait on them |
| Search and embedding | Retrieval, ranking, embedding work, search telemetry | Derives from Hiring data. Provider outage must not block hiring writes. Hydrates current public data for hits |
| Analytics | Spark/Delta projections and reports | Consumes operational events only and never gates transactions. It writes result snapshots back to MongoDB; operational decisions never read them |

Cross-context interaction goes through published events or explicit ports with their own types (an anti-corruption layer), not shared domain classes. A change that crosses a context line needs the Software Architect.

## Aggregates and invariants

| Aggregate | Invariant | Enforced by |
|---|---|---|
| Job | Valid Draft/Open/Closed transitions; closed means closed | Pure `JobLifecycle` program; persisted with a revision guard |
| Application (with its history) | Only permitted transitions; status and history change atomically; one Application per Candidate/Job; no submission to a closed Job | Pure `ApplicationLifecycle`; store-level unique index and transaction |
| Interview workflow | Own revisioned decision; adds no application statuses | Pure workflow decision in `domain/workflow`; revision guard |
| User | Singleton Admin; role-specific profile; deleted accounts keep no profile | Domain constructors; store-level uniqueness |

Keep aggregates small. Reference other aggregates by typed ID, embed only bounded owned values, and change one aggregate per transaction unless an invariant spans two. Decisions are pure functions that return a new state or a typed error; services only sequence effects and authorize.

## Naming rules

- Types and values use glossary words. Domain events and history records are past-tense facts; commands and mutations are imperative actions.
- Value objects are typed IDs, enums and ADTs, not strings or booleans.
- Delivery phases, scaffolding and process labels never appear in names.
