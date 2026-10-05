# Durable Hiring Workflows

## Identity and scope

- Roadmap owner: Phase 11, Kafka workflow contracts, Saga and State.
- Status: **draft**. Durable-work invariants and local failure scenarios are specified; stream sizing, a concrete cross-boundary workflow, and external integration choices remain gated below.
- Coordinator: Product Manager. Implementation owners: Scala Developer and Data Engineer; Architect and Big Data Engineer own boundary/catalog review; Security Engineer and QA provide independent verdicts.
- Outcome: accepted hiring work survives process failure and duplicate delivery, with explicit progress and authorized repair when it cannot complete automatically.
- Authorized present task: specification only. Future implementation proceeds in the ordered slices below after their dependencies are resolved.
- Non-goals: a generic Saga engine, event sourcing, new application statuses, changing the existing interview mutation, paid providers, cloud provisioning, or replacing MongoDB transactions with Kafka transactions.
- Dependencies: [search and embedding architecture](hiring-search-enhancements.md), current operational invariants, [architecture](../../ARCHITECTURE.md#planned-kafka-workflow-contracts), and [roadmap](../development-milestones.md).
- Existing analytics deletion/activation gates apply immediately; this future phase cannot defer them or certify them by designing another workflow.

## Verified source baseline

| Source | Implemented behavior and limit |
|---|---|
| [OperationalEvent.scala](../../src/main/scala/com/example/graphQL/cats/service/events/OperationalEvent.scala) | `OperationalEventEnvelope` has exactly seven fields: `eventId`, `eventType`, `occurredAt`, `aggregateType`, `aggregateId`, `actorId`, `payload`. Its key is `aggregateId`; strict decoding rejects a different field set. |
| [OperationalEventKafkaRuntime.scala](../../src/main/scala/com/example/graphQL/cats/infrastructure/kafka/OperationalEventKafkaRuntime.scala) | Resource-owned publisher and consumer; publication groups claimed work by key, sequentially publishes within each group, and consumer uses `read_committed`. Existing receipt/quarantine handling is not a workflow orchestrator. |
| [MongoOperationalEventRepositories.scala](../../src/main/scala/com/example/graphQL/cats/repository/mongo/MongoOperationalEventRepositories.scala) | Mongo outbox and publication/receipt controls form the existing durable event boundary. Reuse their invariants rather than adding an uncoordinated second publisher. |
| [EmbeddingPipeline.scala](../../src/main/scala/com/example/graphQL/cats/service/search/EmbeddingPipeline.scala), [MongoEmbeddingWorkRepository.scala](../../src/main/scala/com/example/graphQL/cats/repository/mongo/MongoEmbeddingWorkRepository.scala) | Existing asynchronous embedding work is the first recovery baseline; this spec does not imply it already has the proposed workflow state machine. |
| [LifecycleProgram.scala](../../src/main/scala/com/example/graphQL/cats/domain/policy/LifecycleProgram.scala) | Pure local aggregate transition program. `StateT` over domain `Either` does not persist workflow progress. |
| [ApplicationService.scala](../../src/main/scala/com/example/graphQL/cats/service/application/ApplicationService.scala) | Application status use cases retain service authorization and local transactional status/history behavior. Calendar scheduling is a future separate capability. |

[Lifecycle state programs](hiring-lifecycle-state-programs.md), [mutation reliability](mutation-reliability.md), and [runtime safety](hiring-runtime-safety.md) provide prior requirements/evidence. Their results do not establish any acceptance criterion introduced here.

## Stream and ownership contract

Before routing changes, produce a reviewed catalog with one entry per actual stream, including producer principal, consumer groups, key, ordering scope, partition count, replication/minimum ISR, maximum record size, retention/cleanup policy, ACLs, retry/quarantine location, replay horizon and deletion handling.

| Logical stream | Required ownership and behavior | Status |
|---|---|---|
| `hiring.operational-events` | Committed operational facts from the existing outbox; preserve event identity and `aggregateId` key; analytics consumes only supported facts. | Existing; preserve current envelope and seven-day configured broker retention until an explicitly scoped contract change. |
| Capability workflow commands | Application orchestrator produces; one command owner executes; key is workflow identity; stable command/step identity and deadline. | Proposed; physical topic names and provisioning unresolved. |
| Capability workflow results | Owning worker produces; orchestrator consumes; correlate workflow, command, step and expected workflow revision. | Proposed; no ordering implied across command/result topics. |
| Discovery telemetry | Authorized producer, minimized payload, documented session/query key; split only for distinct ownership, retention or ACL needs. | Conditional; cannot become a source of hiring truth. |
| Retry/quarantine copies | Explicit owner, durable reason class, bounded payload/retention, replay authorization and subject-deletion inventory. | Inventory existing storage before introducing more copies. |

Use the current stream layout when ownership, ordering, retention and access match. A new stream requires a concrete reason. Kafka partition order does not establish ordering between topics; partition count/key changes require replay and routing analysis.

Proposed workflow messages carry typed command/event ID, workflow/correlation ID, causation ID, step ID, expected workflow revision, and UTC occurrence/deadline values. These are logical minimum requirements, **not an approved wire schema**. Resolve payloads with the selected workflow and update every producer, consumer and fixture together. Do not insert these fields into the current operational envelope implicitly.

## Behavior and contracts

### Actors and public boundaries

- Candidate retains access only to their own hiring data; recruiters initiate work only for owned jobs/applications; singleton Admin has authorized repair visibility. Background workers use narrowly scoped service identities.
- Actor identity comes from trusted authentication. Workflow records retain the minimal initiating authority/audit reference, and sensitive steps recheck current resource ownership, account state and permitted application transition at their guarded write.
- Ordinary application mutations return according to their current local transaction. Kafka/provider unavailability does not undo a committed hiring write; failure to commit its required Mongo outbox does fail the transaction.
- No new public GraphQL contract is approved here. A future scheduling API needs explicit input/payload/error definitions, authorization tests, SDL and executable operation fixtures before its implementation slice.
- Operator inspection and repair use a scoped service boundary, with expected revision, bounded selection and safe audit. Admin authorization is required; a caller-supplied workflow ID is never authority. Decide CLI versus API delivery before exposing this boundary.

### Pure workflow state and typed outcomes

A selected workflow defines a closed state ADT and pure decision function:

```scala
// Illustrative application-independent policy shape, not an approved wire schema.
def decide(
    state: WorkflowState,
    input: WorkflowInput,
    now: Instant
): Either[WorkflowError, (WorkflowState, List[WorkflowCommand])]
```

Use capability-specific states for waiting on a result, retry due, compensating, completed, and repair required. Keep progress separate from `ApplicationStatus`. Transition policies cannot read the clock, invoke providers, allocate IDs, log, or inspect Mongo/Kafka types. Commands describe effects; the interpreter executes them.

Expected results distinguish applied, duplicate, stale revision, denied/deleted subject, retry scheduled, and repair required. Validation, unavailable dependencies and conflicts remain typed at service/repository ports. A stale reply never moves state backwards or issues another external command. Unrecognized/inconsistent messages are recorded as bounded quarantine outcomes before acknowledgment, or remain unacknowledged if durable recording fails.

### Transaction and acknowledgment frontier

1. Validate record size/shape and authorized producer routing before interpretation; never execute embedded instructions from message text.
2. Load the workflow and authoritative deletion/authorization guards; invoke the pure decision using explicit time/identity inputs.
3. In one Mongo transaction, insert the inbox deduplication identity, compare-and-set workflow revision, and insert outgoing outbox commands. Unique constraints protect deduplication and command identity.
4. Commit durable state before acknowledging Kafka. A failed transaction must not leave a completed inbox marker. A crash after commit and before acknowledgment produces a harmless replay.
5. Process each ordered key sequentially and bound concurrency across keys. Commit only the contiguous durable completion frontier per partition; a later completed record cannot advance past an unresolved earlier record.
6. Persist terminal rejection/quarantine when it is safe to consume a poison record. Never skip an earlier unrecorded failure merely to improve lag.

Mongo failure leaves the Kafka record available for replay. Kafka transaction success alone cannot certify Mongo or calendar/email completion. Inbox retention covers the maximum permitted replay horizon; older replay is denied or requires a separate reviewed recovery procedure.

### Claims, timeouts, retries and cancellation

- Persist deadlines, attempts, next due time, revision and claim fencing identity. A timeout is durable work discovered after restart, not just a sleeping fiber.
- Only the interpreter owns workflow retries. Provider adapters perform one logical attempt and classify outcomes; account for any unavoidable SDK internal retry in the same total budget.
- Configure bounded attempts, capped backoff, bounded in-flight calls and claim durations. A deterministic retry policy takes attempt/time/jitter input; independent invalid settings accumulate at startup.
- A completion must match current step, revision and claim token. An expired worker cannot write success over a newer owner. External fencing depends on provider capabilities; reconciliation is required when local fencing cannot prevent an already sent external request.
- Cancellation stops new claims and awaits owned fibers/resources. Do not mark unfinished work complete; another worker may recover after claim expiry. Cancellation during uncertain external completion enters reconciliation rather than blind retry.
- Attempt exhaustion and compensation failure produce durable repair state with reason, last safe observation and permitted operator actions. Repair itself is idempotent, guarded and audited.

### Recovery and deletion

For every step document its durable precondition, idempotency key, timeout, retry budget, success observation, compensating action or reason it is irreversible, and repair action. Reuse the same key after a network timeout. If the provider cannot prove whether an effect happened, query its state or retain an explicit uncertain outcome; do not assert duplicate-free delivery.

Deletion prevents new subject work and replay from recreating derived data. Inventory commands/results, inbox, outbox, quarantines, provider state, logs and backups in retention/deletion analysis. Preserve sufficient minimized deduplication/fencing evidence to deny replay without retaining prohibited payloads. Reversible reservations can be released; sent notifications, account tombstones and elapsed retention need forward recovery. Never resurrect deleted content to compensate.

## Workflow catalog and conditional scheduling slice

| Capability | Required approach | Readiness limit |
|---|---|---|
| Embedding/reindexing | First prove existing durable worker recovery, guarded source/revision writes, deletion safety and convergence to current content. Add explicit Saga progress only when a real second durable boundary requires it. | Reuse Phase 9 preparation/persistence ownership; do not build generic machinery for a single projection update. |
| Account deletion | Retain existing receipt and erasure phases; continue forward recovery, publisher fencing and retention barriers. | Existing lakehouse gates remain authoritative; no replacement Saga or reset is authorized. |
| Interview scheduling | Reserve slot, commit guarded hiring status/history, then notify. Durable compensation/reconciliation spans calendar and notification boundaries. | Local fake contract and scoped provider choice required before new scheduling implementation. |
| Application enrichment or bulk effects on job close | Capture as future candidates only. | Product rules for resume processing, affected statuses and notifications are absent; do not invent transitions or automatically decline applications. |

For interview scheduling, an authorized recruiter requests a reservation for an owned application. Validate interval/time-zone interpretation, participants, idempotency and currently permitted transition before reserving. Persist intent before calling the calendar. Only after a confirmed reservation, atomically commit the guarded move to `Interview`, history and notification intent. Recheck authorization/status inside that transaction.

If the guarded status write fails or the deadline expires before it can commit, release the reservation idempotently. If release fails, preserve compensation/repair work. If the transaction outcome is uncertain, inspect its durable receipt/state before deciding to release. A confirmed hiring commit must not accidentally lose its reservation through speculative compensation.

Candidate/recruiter notifications occur after commit. Partial delivery tracks each recipient independently; sent messages cannot be recalled. Unknown delivery requires provider lookup/idempotency or visible repair. Restart and replay cannot generate another reservation for the same accepted step. Existing `moveApplicationToInterview` remains a status/history operation with its existing semantics; it does not silently acquire calendar/provider dependencies.

Scheduling-specific choices still required: request fields and response lifecycle, time-zone/availability rules, cancellation/rescheduling behavior, provider guarantees, recipient data minimization, and whether scheduling completion requires notification delivery. These block scheduling implementation, not pure recovery policy or existing-worker tests.

## Code style and boundaries

Apply [engineering quality](../engineering-quality.md) and [schema evolution](../schema-evolution.md).

- Domain owns immutable state/transition/error ADTs. Use typed IDs and exhaustive matches; `Option` models legitimate absence. Avoid null/sentinel outcomes, business exceptions, unchecked `.get`, and mutable global progress.
- Application services retain `UseCaseIO[A] = EitherT[IO, UseCaseError, A]`; repositories retain `RepositoryIO[A] = EitherT[IO, RepositoryError, A]`. Preserve aliases through helpers/transaction callbacks; unwrap only at owning adapter/runtime boundaries.
- Adapters own Mongo codecs/sessions, Kafka serializers/offsets and provider protocol translation. Driver exceptions are classified once; retryable business outcomes are not thrown and immediately recovered.
- `Resource` owns consumers, producers, clients, workers and timers; managed FS2 streams provide backpressure. No detached fibers or unbounded queues. `Ref` can coordinate a live process but cannot become the durable workflow store.
- Prefer capability names such as interview reservation and embedding recovery over phase names. Introduce shared machinery only after concrete repeated behavior warrants it.
- No new schema version field, dual readers/writers or legacy contract backfills before MVP. Operational Mongo collection/index changes still need versioned repeatable migration, restart/concurrent-start verification and recovery procedures.
- For operational migration, define uniqueness and due-work query indexes, bounded initialization, verification before worker enablement, mixed-binary restrictions, and forward recovery. An application rollback is not a database rollback.

## Open decisions and alternatives

| Decision | Alternatives and default direction | Evidence / owner / blocked work |
|---|---|---|
| First concrete Saga | Extend existing reindexing only if multiple durable boundaries need coordination; otherwise use scoped fake-calendar scheduling. | Architect + PM demonstrate the external boundary and business outcome; blocks Saga implementation selection. |
| Topic layout/sizing | Preserve shared operational facts; separate commands/results only for ownership/retention/ordering differences. | Big Data + Security review volume, replay/deletion horizon, principal rights; blocks provisioning/routing. |
| Consumer engine | Existing FS2/fs2-kafka by default; Kafka Streams only for measured keyed joins/windows/state-store benefit. | Architect records restore/changelog/repartition/deletion and cost comparison; blocks new library adoption only. |
| Calendar/notification provider | Deterministic fake first; real provider compared on idempotency, lookup, cancellation, privacy, SLA and request cost. | PM + Security scoped authorization and capability evidence; blocks real integration and duplicate-free claims. |
| Repair surface | Restricted local CLI or authenticated Admin API. | PM/operator access workflow and Security audit review; blocks exposed repair contract. |
| Retry/replay limits | Bounded configured policy, not arbitrary infinite retry; retain inbox through allowed replay. | QA failure workload and operations recovery objectives; blocks production tuning/retention approval. |

No provider, production budget, partition count or latency threshold is chosen by this document.

## Acceptance and evidence

Future production/test implementation paths are assigned with each ready slice. The linked baseline is source context only; every new criterion below is **Not implemented / Not run**.

| ID | Given / When / Then | Planned verification | Actual outcome |
|---|---|---|---|
| DHW-01 | Given current facts, when catalog/routing is reviewed, then every stream has owner/key/retention/ACL/replay/deletion rules and the seven-field envelope remains explicit. | Catalog walkthrough + active contract fixtures. | Not run |
| DHW-02 | Given any selected state/input/time, when deciding, then deterministic commands obey valid business transitions with no effects. | Pure transition table/property tests. | Not run |
| DHW-03 | Given duplicate delivery, when workers race, then one inbox/state/outbox transaction applies and no acknowledged work is lost. | Replica-set race and crash-before/after-commit tests. | Not run |
| DHW-04 | Given an earlier unresolved partition record, when later work finishes, then committed offsets do not pass the durable frontier. | Kafka integration with controlled interleavings. | Not run |
| DHW-05 | Given expired claims or stale/out-of-order replies, when processing resumes, then current state and command identity remain guarded. | Fake-clock concurrent-worker and replay tests. | Not run |
| DHW-06 | Given a provider succeeds before process failure, when restarted, then reconciliation converges without blindly repeating an uncertain effect. | Fake provider crash-window and real provider contract checks when authorized. | Not run |
| DHW-07 | Given cancellation or overdue durable deadlines, when restarted, then work resumes or enters bounded repair without leaked workers. | Cats Effect resource/cancellation and restart tests. | Not run |
| DHW-08 | Given exhausted retry or failed compensation, when operators inspect/repair, then progress and allowed guarded action are visible and audited. | Failure drill and forbidden/repeated repair tests. | Not run |
| DHW-09 | Given deleted subjects and replay/retention boundaries, when work is replayed, then no deleted data or effects are recreated. | Deletion-race, old-replay-denial and storage inventory tests. | Not run |
| DHW-10 | Given a confirmed interview reservation and rejected status commit, when compensation runs, then reservation releases; uncertain commit is reconciled first. | Conditional scheduling transaction/provider tests. | Not run |
| DHW-11 | Given committed interview status and partial/uncertain notifications, when retried, then per-recipient reconciliation or repair is visible without invented delivery guarantees. | Conditional notification duplicate/restart tests. | Not run |
| DHW-12 | Given provider/broker outage, when normal hiring transactions execute, then local durable acceptance remains independent and later delivery recovers. | Mongo + Kafka outage integration; existing embedding recovery tests extended. | Not run |
| DHW-13 | Given operational storage evolution, when concurrent startup/restart occurs, then verified indexes/state become ready without corrupting live revisions. | Disposable migration and rollback/recovery rehearsal. | Not run |

## Performance, implementation handoff and checkpoint

Use small deterministic synthetic workflows. Record seed/count, message bytes, key skew, partitions, concurrent workers, replay fraction, provider latency/failure distribution, queue age, p50/p95/p99 completion time, retries, throughput and errors. Compare reuse of the existing worker with added orchestration. Capture Mongo query plans/index/write cost, broker traffic/storage and provider call counts. Production freshness/recovery targets and cost ceiling remain unresolved; no benchmark result is implied.

1. Architect/Big Data/Data Engineer finalize the current catalog and concrete workflow boundary (DHW-01); Security reviews ACL/deletion scope.
2. Scala Developer extracts only necessary pure capability policies and extends existing embedding recovery evidence (DHW-02, DHW-05–07, DHW-12).
3. Data Engineer/Scala Developer implement guarded durable records and migration in agreed exclusive paths (DHW-03–05, DHW-09, DHW-13); this depends on the selected workflow contract.
4. Scala Developer integrates the interpreter, reconciliation and authorized repair (DHW-06–09). Test with fake providers and disposable Kafka/Mongo before external integration.
5. Conditional scheduling slice resolves its open public/provider contract, then implements reservation/status/notification recovery (DHW-10–11). Update SDL, operations, use cases and catalog together.
6. Independent Code Reviewer, Security Engineer and final QA evaluate final source and failure evidence. Run root `sbt test`, configured format checks and relevant `IntegrationTest / test`; use separate provider/deployed gates when applicable.

Checkpoint: specification drafted; no future implementation criterion completed. Test commands above are planned, not executed evidence. No provider or infrastructure was provisioned. Next action: independent specification review and resolve only decisions required by the first implementation slice. Code Reviewer: pending. Security Engineer: pending. Final QA: pending. Documentation review does not approve future production code or activation.
