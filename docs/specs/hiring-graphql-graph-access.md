# Hiring GraphQL Graph Access

## Identity and scope

- Task / roadmap phase: GraphQL capability enhancements on the current API (supports Phase 3 GraphQL/performance goals and Phase 13 observability; no new roadmap phase).
- Status: draft
- Coordinator: Product Manager. Proposed owners: Software Architect (contract and subscription transport decision), Scala Developer (schema, resolvers, routes), Data Engineer (projection and indexes), Security Engineer and Code Reviewer (independent verdicts), QA Engineer (final validation).
- User outcome: use GraphQL features that change code size, client flexibility or runtime cost: graph-shaped nested access, query-driven database projection, push updates, persisted operations, shared interfaces, schema-declared policy and field-level telemetry.
- Problem / actors / behavior:
  - Candidate, Recruiter and Admin clients cannot load a job, its applications, each candidate and the status history in one request: `applicationHistory`, `jobApplications` and `myApplications` are root queries only, and `Application` links only to `candidate` and `job`.
  - List queries read whole documents even when a client selects only `title`.
  - Clients must poll `interviewWorkflow(workflowId)` and application lists to see changes.
  - Every client sends full query text on each request; the document cache only saves parsing.
  - Connection and result types repeat the same structure; complexity weights are hand-written lambdas per field.
- Authorized scope: the items in "Requirements" below (high: R1-R4; medium: R5-R9). Spec only; no implementation is authorized by this document.
- Non-goals: GraphQL federation or stitching, batch mutations over several applications, frontend assets or a Node toolchain, changing role model (Candidate, Recruiter, singleton Admin), changing application status transitions, real email or paid infrastructure, replacing service-layer authorization with directives.
- Dependencies: [pre-MVP contract reset](pre-mvp-contract-reset.md) (one active API shape), [durable hiring workflows](durable-hiring-workflows.md) (interview workflow revisions and operational events), [hiring observability resilience](hiring-observability-resilience.md) (tracer and diagnostics), [engineering quality](../engineering-quality.md), [schema evolution](../schema-evolution.md).

## Source context and decisions

### Verified implementation (read 2026-10-14; not re-run)

| Source | Fact |
|---|---|
| `src/test/resources/graphql/hiring.graphql` | Active SDL. `type Query` has `job`, `jobs`, `myJobs`, `myApplications`, `jobApplications`, `applicationHistory`, `interviewWorkflow`, search roots. No `type Subscription`, no custom directives, one interface (`UserError`), unions for every mutation result. `Application { candidate, job }`; `Job { recruiter }`. |
| `api/graphql/HiringGraphQLFetchers.scala` | Request-scoped `Fetcher`s: `usersFetcher`, `jobsFetcher`, `emailVisibilityFetcher`, backed by `RequestContext.relatedUsers`, `relatedJobs`, `visibleEmailUsers`. |
| `api/graphql/HiringGraphQLSchemaAssembly.scala` | `MaxQueryDepth = 16`, `MaxQueryComplexity = 1000`, hand-written `connectionComplexity`, `expensivePageComplexity`, `facetComplexity`, `expensiveRootBudget` reducer, `InterviewActionComplexity`. |
| `api/graphql/GraphQLDocumentCache.scala` | Caffeine cache of parsed `Document` keyed by query text, 256 entries, 60 s. |
| `api/http/GraphQLHttpRoutes.scala` | Single GraphQL HTTP route; uses the document cache; `HiringApiRoutes` also serves `/health` and `/ready`. |
| `api/graphql/RequestContext.scala` | Per-request context: `authenticatedActor`, `traceField(name, action)`, `requestScoped`, rate limits. |
| `service/events/*`, `infrastructure/kafka/OperationalEventKafkaRuntime.scala` | Operational events (seven-field envelope) are published through the Mongo outbox to Kafka. Not yet exposed to GraphQL clients. |
| `repository/mongo/MongoJobRepository.scala` | Existing job reads; projection support to be inspected by the Data Engineer before R2. |

### Assumptions

| ID | Assumption | Impact if wrong | Validation |
|---|---|---|---|
| A1 | The installed Sangria version supports `Projector`, `Middleware`, custom directives and subscription streams. | R2, R6, R8 or R3 change shape or need a dependency. | Architect spike reading `build.sbt` and the library source before design. |
| A2 | An fs2-friendly bridge to Sangria subscription streams can be written without a new dependency. | R3 may need a dependency (provenance and cost review required). | Architect spike. |
| A3 | Internal event delivery for subscriptions can start process-local (see Open questions). | Multi-instance deployments miss events. | Decision Q1. |

### Open questions

| ID | Question | Blocks |
|---|---|---|
| Q1 | Subscription event source: process-local pub/sub fed by the post-commit path, Kafka consumer, or Mongo change streams? | R3 only. |
| Q2 | WebSocket subprotocol (`graphql-transport-ws`) versus server-sent events for http4s. | R3 only. |
| Q3 | Persisted operations: allow-list only (reject unknown hashes) or automatic registration (APQ) with the allow-list as a config switch? Recommended default: build-time allow-list, reject unknown. | R4 only. |
| Q4 | Is `@defer`/`@stream` supported by the Sangria version in use? If not, R7 is limited to a documented non-implementation decision. | R7 only. |
| Q5 | Relay `Node` global IDs: keep typed IDs (`JobID`, `ApplicationID`) and add `Node` only as an interface, or introduce global opaque IDs? Recommended: interface only, no new ID format. | R5 only. |

Independent work continues while a question is open; only the named requirement is blocked.

### Decisions / change notes

- 2026-10-14: draft written from user request "embrace high and medium points". Federation (point 10) and batch mutations (point 11) excluded as speculative or in conflict with single-transaction invariants.
- Pre-MVP policy applies: one active schema shape. Update SDL, fixtures, examples and tests directly; no dual shapes or compatibility fields.
- Authorization stays in services. Nested fields, subscriptions and projection must not create a second authorization path.

## Behavior and contracts

### Cross-cutting rules

1. Every new nested field and subscription resolves through the same service methods and authorization predicates as the equivalent root query. A forbidden resource appears as the same sanitized outcome as via the root query (null or typed error as that root does today).
2. Nested connection fields use the existing `first`/`after` bounded pagination, cursor codec and complexity weights. No unbounded list.
3. No data is fetched before authorization, and projection never widens what a caller may read.
4. Pure/effect split: field-selection analysis and policy decisions are pure; Mongo, Kafka and WebSocket I/O stay in adapters (see [engineering quality](../engineering-quality.md)).
5. Errors stay sanitized and typed; no new business exceptions.

### R1 Nested graph fields (high)

Add to the SDL:

- `Job.applications(first: Int!, after: String, status: ApplicationStatus): ApplicationConnection!` (owner recruiter or Admin only).
- `Application.history(first: Int!, after: String): ApplicationEventConnection!` (same visibility as `applicationHistory`).
- `Application.interview: InterviewWorkflow` (null when none; visibility as `interviewWorkflow`).
- `User.applications(first: Int!, after: String, status: ApplicationStatus): ApplicationConnection!` (the viewer themself only, equal to `myApplications`).

Behavior: a recruiter's request for `myJobs { edges { node { title applications(first: 20) { edges { node { status candidate { id } history(first: 5) { edges { node { newStatus } } } } } } } } }` is answered with batched reads per level. Number of Mongo round trips per nesting level is bounded and independent of the number of parents (no N+1).

Root queries `jobApplications`, `myApplications`, `applicationHistory` remain unless a decision removes them. If kept, they and the nested fields share one resolver implementation.

### R2 Query-driven projection (high)

Behavior: the selected fields of a list or single read are mapped to a pure `FieldSelection` value and passed to the repository port, which returns only the needed fields (Mongo projection). Fields needed for authorization, ordering, cursors and nested batching are always included. Unselected large fields (`description`, `requirements`, embedding-related fields) are not read.

Constraints: domain models do not depend on Sangria; mapping from AST to `FieldSelection` is API-layer; the repository port speaks only in domain terms (a set of named projection fields, not a Mongo `Bson`). Embedded or derived fields not selected must not be defaulted to misleading values: partial domain objects use a dedicated read-model type or `Option`.

Performance: record before/after query plans, bytes read and latency for a fixed seeded workload (see Performance).

### R3 Subscriptions (high)

Add `type Subscription`:

- `applicationStatusChanged(applicationId: ApplicationID!): ApplicationStatusChange!` for the application's candidate, owning recruiter or Admin.
- `newApplication(jobId: JobID!): Application!` for the owning recruiter or Admin.
- `interviewWorkflowUpdated(workflowId: UUID!): InterviewWorkflow!` for participants allowed by `interviewWorkflow`.

Rules:
- The actor comes from authenticated connection context, never from an argument. Ownership is checked on subscribe and rechecked for each delivered event, so a role change, job reassignment, account deletion or closed session stops delivery.
- Events are emitted only after the Mongo transaction commits (from the existing post-commit path or outbox). A rolled-back write emits nothing. Delivery is at-most-once to the live subscriber; clients recover state by querying (documented). No client-visible guarantee of ordering across different aggregates.
- Bounds: maximum subscriptions per connection and per actor, bounded per-subscriber buffer with a documented overflow policy (drop and signal), idle and maximum connection lifetime, message size limit, and rate limiting analogous to `AuthRateLimiter`.
- Connection resources are owned by `Resource`; cancellation releases the subscription deterministically.
- Payloads never include data the subscriber could not read by query (for example candidate email, per `emailVisibilityFetcher`).
- Complexity: a subscription counts as an expensive operation for depth/complexity validation.

### R4 Persisted operations (high)

Behavior: clients may send `{ "extensions": { "persistedQuery": { "sha256Hash": "..." } } }` without the query text. The server resolves the hash from an allow-list built from the operation fixtures in `src/test/resources/graphql/` (Q3 default). Unknown hash returns a sanitized error without logging the document. When enforcement is on, requests with raw query text are rejected; introspection stays available per existing policy.

Constraints: hashing and lookup are pure; the allow-list loads at startup via typed configuration (see [engineering quality](../engineering-quality.md)); the existing `GraphQLDocumentCache` is reused for parsed documents; request size limits remain in force; hash collision or mismatch (hash disagrees with supplied text) is rejected.

### R5 Shared interfaces (medium)

- Add `interface Connection { pageInfo: PageInfo! }` and `interface Node { id: ID! }` (Q5), implemented by the existing connection and entity types. Existing fields are unchanged.
- Optionally an `ApplicationOutcome` interface or a shared `MutationError` interface already represented by `UserError`; do not rename result unions.
- Goal: less duplicated schema and resolver definition code (shared field list helpers), and shared client fragments. Success is measured by removed duplicated definitions without changing the wire shape of existing fields.

### R6 Schema directives (medium)

- Declare `@auth(role: [UserRole!])` and `@cost(value: Float!)` (final names at design time) and apply them to fields.
- A Sangria middleware enforces `@auth` as a defence-in-depth check before the service call; services remain the authoritative enforcement point (project rule: deny by default in services). A mismatch between directive and service result is a test failure, not a second policy.
- `@cost` replaces the hand-written complexity lambdas where the weight is a constant; non-constant formulas (for example `connectionComplexity`) remain code but reference named constants.
- Directive metadata is available through introspection-visible SDL for documentation.

### R7 Incremental delivery `@defer`/`@stream` (medium, conditional)

Depends on Q4. If the Sangria version supports incremental delivery, enable it for `semanticJobSearch` and `recommendedJobs` slow fields only, over the HTTP route, with the same authorization and complexity rules. If unsupported, record the decision here and instead keep the fast `jobs` list separate from slower ranked search (no code change). No dependency swap for this requirement alone.

### R8 Field-level telemetry middleware (medium)

A Sangria `Middleware` records per-field timing and outcome using the existing otel4s `Tracer` and `RequestContext.traceField`. It records only operation name, field path and duration/outcome class; never arguments, variable values, tokens or document text. Slow-field attributes feed complexity-weight tuning. Overhead is bounded: sampling or limiting the span count per request is required for large lists.

### R9 Facets on result connections (medium)

Optionally expose `facets: JobDiscoveryFacets` on `JobConnection`/`RankedJobResults` so one round trip returns results and counts with the same filter. Cost uses `facetComplexity`. The existing `jobDiscoveryFacets` root stays unless a decision removes it. Marked optional; may be dropped without blocking R1-R8.

### Compatibility and migration

Pre-MVP: one active shape; SDL changes are additive and updated in the same change as fixtures, examples and tests. No data migration is expected (R1-R9 do not change stored documents). Index changes for R1/R2 (for example application lookup by job and status, history by application) follow [schema evolution](../schema-evolution.md) and Mongo migration rules; the Data Engineer decides after query-plan evidence.

## Domain language and examples

- Context, aggregate and invariant touched: hiring read model (Job, Application, ApplicationEvent) and Interview Workflow; no domain invariant changes. Authorization invariants (owner-only, candidate sees own) must hold on every new access path.
- Glossary terms used: Candidate, Recruiter, Admin, Job, Application, ApplicationStatus, ApplicationEvent, Interview Workflow. Terms to add to `docs/domain-language.md` in the same change: "Persisted operation", "Subscription", "Field selection".

| Rule | Example tag | Scenario (Given / When / Then) |
|---|---|---|
| Nested read matches root read | AC-01.1 | Given Recruiter Rita owns Job J1 with Applications A1 (Created) and A2 (Interview), when Rita queries `job(id: J1) { applications(first: 10) { edges { node { id status } } } }`, then she gets A1 and A2, equal to `jobApplications(jobId: J1, first: 10)` |
| Other recruiter forbidden | AC-01.2 | Given Recruiter Rob does not own J1, when Rob queries `job(id: J1) { applications(first: 10) { ... } }`, then he receives the same sanitized forbidden/empty outcome as `jobApplications(jobId: J1)` and no application data |
| Candidate cannot see others' history | AC-01.3 | Given Candidate Ana owns A1 and Candidate Ben does not, when Ben requests `application { history }` for A1, then he gets no events |
| No N+1 | AC-01.4 | Given 20 jobs each with 10 applications, when a recruiter loads `myJobs { applications { candidate { id } } }`, then candidate loading performs one batched user read and applications one batched read per level, not one per parent |
| Bounded nesting | AC-01.5 | Given `first: 100` at three nested levels, when the query is validated, then complexity exceeds 1000 and the request is rejected before execution |
| Projection reads only selected fields | AC-02.1 | Given 10 000 jobs, when a client requests `jobs(first: 20) { edges { node { id title } } }`, then the Mongo find includes projection of `id`, `title` and cursor/order/authorization fields only and omits `description` and `requirements` |
| Projection never widens access | AC-02.2 | Given a Candidate cannot see a closed job, when they select any fields, then the same visibility predicate filters the documents |
| Selection of nested field adds needed fields | AC-02.3 | Given the client selects `recruiter { id }` on `Job`, then the projection includes the recruiter reference needed by the fetcher |
| Status change is pushed | AC-03.1 | Given Ana subscribes to `applicationStatusChanged(A1)`, when Rita moves A1 to Interview and the transaction commits, then Ana receives `newStatus = Interview` once |
| Rolled-back write emits nothing | AC-03.2 | Given a subscription on A1, when the status write rolls back after a concurrent conflict, then no event is delivered |
| Subscribe is authorized | AC-03.3 | Given Ben does not own A1, when Ben subscribes to A1, then subscription is refused with a sanitized error and no stream is opened |
| Delivery rechecks authority | AC-03.4 | Given Rita subscribed to `newApplication(J1)` and J1 is then reassigned to Rob, when an application arrives, then Rita receives nothing |
| Deleted account stops delivery | AC-03.5 | Given Ana's account deletion completes while subscribed, then her stream terminates |
| Bounded resources | AC-03.6 | Given a per-actor limit of N subscriptions (configured), when Ana opens N+1, then the last is refused; a slow subscriber past the buffer limit receives an overflow signal and is closed |
| Persisted hash executes | AC-04.1 | Given hash H of `MyJobsWithApplications` in the allow-list, when a client sends only H and variables, then the operation runs as the stored document |
| Unknown hash rejected | AC-04.2 | Given hash X is not in the allow-list, when sent, then a sanitized error returns and no query text is logged |
| Enforcement blocks raw text | AC-04.3 | Given enforcement is on, when a client sends raw query text, then it is rejected; with enforcement off it executes under existing limits |
| Mismatched hash and text rejected | AC-04.4 | Given hash H and different query text, then the request is rejected |
| Interface adoption keeps wire shape | AC-05.1 | Given the SDL before and after R5, when existing fixture operations run, then results are byte-identical apart from added interface fields |
| `@auth` mirrors service rule | AC-06.1 | Given `@auth(role: [RECRUITER])` on `myJobs`, when a Candidate calls it, then middleware and service both deny, and a test fails if they disagree |
| `@cost` weight applied | AC-06.2 | Given `@cost(100)` on an expensive root, then validation uses 100 plus children, equal to the former lambda |
| Telemetry has no content | AC-08.1 | Given a request with a password variable, when spans are exported, then no span attribute contains variable values or document text |
| Slow field visible | AC-08.2 | Given a resolver delayed by 200 ms in a test, then the field span reports at least 200 ms with its path |
| Facets use same filter | AC-09.1 | Given `jobs(city: "Berlin") { facets { skills { value count } } }`, then counts reflect only Berlin jobs visible to the viewer |

Open questions found while writing examples are recorded above (Q1-Q5).

## Acceptance and evidence

Evidence columns are filled only after observation.

| ID | Given / When / Then | Implementation and test paths | Verification command or method | Actual outcome |
|---|---|---|---|---|
| AC-01 | Nested fields (AC-01.1-01.5) return the same authorized data as root queries and use batched reads | Not implemented | `sbt test` (schema and access spec); integration test with query counting | Not run |
| AC-02 | Projection (AC-02.1-02.3) limits fields read and never changes visibility | Not implemented | Repository integration test with Mongo `explain` and command monitoring; before/after workload | Not run |
| AC-03 | Subscriptions (AC-03.1-03.6) deliver after commit only to authorized, bounded subscribers | Not implemented | Integration test with replica-set Mongo and WebSocket client; concurrency test | Not run |
| AC-04 | Persisted operations (AC-04.1-04.4) execute by hash, allow-list enforced | Not implemented | `sbt test` route tests | Not run |
| AC-05 | Interfaces (AC-05.1) keep wire shape | Not implemented | SDL diff check and fixture operation run | Not run |
| AC-06 | Directives (AC-06.1-06.2) agree with services and replace constant complexity lambdas | Not implemented | `sbt test`; agreement matrix test across roles | Not run |
| AC-07 | `@defer`/`@stream` enabled or documented as unsupported (Q4) | Not implemented | Spike result and, if enabled, route test | Not run |
| AC-08 | Telemetry (AC-08.1-08.2) records timings without content | Not implemented | Unit test with in-memory tracer | Not run |
| AC-09 | Facets on connections (AC-09.1), if kept | Not implemented | `sbt test` plus Atlas Local integration if lexical facets involved | Not run |
| AC-10 | Security gates: forbidden-access tests for direct ID, nested relationship, batch-loader and subscription paths for Candidate, Recruiter (owner and non-owner), Admin | Not implemented | `sbt test` and integration tests | Not run |
| AC-11 | Docs and fixtures updated: SDL file, operation fixtures, README/ARCHITECTURE GraphQL sections, domain language terms | Not implemented | Review plus schema/fixture checks | Not run |
| AC-12 | `sbt test` and configured formatting/linting pass; integration tests run for persistence and subscription changes; missing infrastructure reported as unverified | Not implemented | `scripts/check-local.sh`, `sbt test`, `sbt it:test` (confirm command from build) | Not run |

## Performance and cost

- Workload to fix before R2: a seeded local dataset (for example 10 000 jobs, 50 000 applications, 20 000 users), single-actor and 16-concurrent-client runs, local Docker Compose Mongo replica set. Record SDL operations used, environment, p50/p95/p99 latency, throughput, errors, bytes read and CPU. These values are unknown until measured; no figure here is a result.
- Baseline: current root queries and whole-document reads. Simpler alternative for R2: a fixed per-list projection per root without field-aware logic; compare and justify the added complexity only if measured gain is material.
- R3 cost: open connections and buffers consume memory. Record per-connection memory and connection limits; no paid infrastructure.
- R4: measure saved request bytes and parse time against the existing document cache.
- R8: measure overhead with telemetry on versus off; ceiling to be set after baseline.
- No claim of improvement is valid without the before/after record.

## Implementation handoff

Proposed order (each slice independently verifiable; one author at a time on overlapping files such as `HiringGraphQLSchemaAssembly.scala`, `hiring.graphql`):

1. Architect spike: A1, A2, Q1-Q5 decisions, update this spec. Owner: Software Architect. Write paths: this spec only.
2. R1 nested fields (AC-01, AC-10 part). Owner: Scala Developer. Paths: `api/graphql/*`, `service/read/*` if needed, tests, SDL, fixtures.
3. R5 interfaces and R6 directives (AC-05, AC-06). Owner: Scala Developer. Same GraphQL paths; serialize after slice 2.
4. R2 projection (AC-02). Owners: Data Engineer (ports, indexes, plans) then Scala Developer (field selection, resolvers); `repository/mongo/*`, `service/port/*`. Record workload first.
5. R4 persisted operations (AC-04). Owner: Scala Developer. Paths: `api/http/*`, `api/graphql/GraphQLDocumentCache.scala`, config reader.
6. R8 telemetry (AC-08). Owner: Scala Developer. Paths: `api/graphql/*`, `service/Diagnostics`.
7. R3 subscriptions (AC-03, AC-10 part). Owners: Scala Developer; Security Engineer reviews early (design) and at the end. Depends on Q1/Q2.
8. R7, R9 conditional (AC-07, AC-09).
9. Docs and fixtures (AC-11) with each slice; final consolidation by Technical Writer.

Routing: Security Engineer verdict required (authorization, new access paths, long-lived connections, config). Code Reviewer verdict required (shared GraphQL and persistence changes). QA final validation on the final state. Delegation of reviewers must keep them read-only.

Risks: nested fields create new authorization paths (mitigated by shared resolvers and AC-10); projection can produce partial objects that leak into logic (mitigated by dedicated read-model types); subscriptions are resource-exhaustion prone (bounds in R3); persisted allow-list can drift from fixtures (build-time generation and test).

Recovery: all slices are code-only; revert by removing SDL fields and resolvers. No stored-data change expected.

## Checkpoint and review

- Completed criteria and changed files: none; this document is the only change.
- Latest commands/results: none run for this spec; source facts above come from reading files on 2026-10-14.
- Blockers and next concrete action: Architect spike for A1/A2 and Q1-Q5; then mark the spec ready and start slice 2.
- Code Reviewer verdict and scope: pending
- Security Engineer verdict and scope: pending
- Final independent QA verdict: pending
