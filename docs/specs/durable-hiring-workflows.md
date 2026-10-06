# Durable Hiring Workflows

## Identity and scope

- Roadmap owner: Phase 11, Kafka workflow contracts, Saga and State.
- Status: **in progress**. The local interview scheduling/fake-provider decisions are supplied by the implementation handoff; real providers and deployed retention remain outside this local implementation.
- Coordinator: Product Manager. Implementation owners: Scala Developer and Data Engineer; Architect and Big Data Engineer own boundary/catalog review; Security Engineer and QA provide independent verdicts.
- Outcome: accepted hiring work survives process failure and duplicate delivery, with explicit progress and authorized repair when it cannot complete automatically.
- Authorized present task: local implementation of durable interview scheduling with fake calendar/notification adapters and correction of the existing undurable-consumer acknowledgment gap.
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
- The local GraphQL contract is `scheduleInterview(input)`, `interviewWorkflow(workflowId)` and `repairInterviewWorkflow(input)`. Inputs require typed application/workflow IDs, UTC instants, idempotency keys and an expected revision for Admin repair. The active SDL and representative operations live in `src/test/resources/graphql/`.
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

Local scheduling uses future UTC intervals, `[start,end)` participant availability, persistent fake provider receipts and completion only after both participant notifications. Cancellation/rescheduling and real providers are deferred.

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

### Local scheduling decisions (2026-10-06)

- Add `scheduleInterview(applicationId, startsAt, endsAt, idempotencyKey)`, require a future UTC interval and an `Accepted` application, and derive both participants from authoritative records. Status/history change remains `Accepted -> Interview` under the existing lifecycle policy.
- Use durable capability-specific workflow state, inbox/outbox coordination and fake calendar/notification providers. Commands/results use separate `hiring.interview-commands` and `hiring.interview-results` topics keyed by workflow ID; preserve the existing seven-field operational envelope. Local defaults and failure behavior follow the implementation handoff. No real provider or new hiring status is authorized.
- Candidate/recruiter workflow inspection follows existing ownership rules; Admin repair uses expected revision/idempotency. Account deletion must fence publication and purge attributable workflow/provider data before deletion is complete.
- Checkpoint: local implementation, execution evidence and independent Code/Security/QA gates are complete below. Live deployment, real-provider guarantees and elapsed deployed retention horizons remain pending.

| Decision | Alternatives and default direction | Evidence / owner / blocked work |
|---|---|---|
| First concrete Saga | Scoped durable fake-calendar interview scheduling selected; existing reindexing remains independent. | Implemented and locally verified reservation, guarded hiring commit, notification and compensation boundaries. |
| Topic layout/sizing | Shared operational facts unchanged; separate interview commands/results with the approved local limits and rights. | Actual SASL worker/orchestrator routing and accelerated retention passed; deployed sizing remains pending. |
| Consumer engine | Existing FS2/fs2-kafka by default; Kafka Streams only for measured keyed joins/windows/state-store benefit. | Architect records restore/changelog/repartition/deletion and cost comparison; blocks new library adoption only. |
| Calendar/notification provider | Deterministic fake first; real provider compared on idempotency, lookup, cancellation, privacy, SLA and request cost. | PM + Security scoped authorization and capability evidence; blocks real integration and duplicate-free claims. |
| Repair surface | Authenticated Admin API selected with revision/idempotency and audit. | Authorized/forbidden access and reconciliation/repair integrations passed; no local CLI is required. |
| Retry/replay limits | Bounded configured policy, not arbitrary infinite retry; retain inbox through allowed replay. | QA failure workload and operations recovery objectives; blocks production tuning/retention approval. |

The authorized local fake providers, stream defaults and retry budgets are defined above. Production providers, redundancy, budget and latency acceptance remain unresolved.

## Acceptance and evidence

The local scheduling implementation and executable acceptance fixtures are present in this checkout. The matrix below records execution evidence separately from source implementation; pending executions cannot establish a passing gate.

| ID | Given / When / Then | Planned verification | Actual outcome |
|---|---|---|---|
| DHW-01 | Given current facts, when catalog/routing is reviewed, then every stream has owner/key/retention/ACL/replay/deletion rules and the seven-field envelope remains explicit. | Catalog walkthrough + active contract fixtures. | Local contract fixtures and actual SASL topic operation pass; catalog and ACL configuration are implemented. Production redundancy and deployed retention remain pending. |
| DHW-02 | Given any selected state/input/time, when deciding, then deterministic commands obey valid business transitions with no effects. | Pure transition table/property tests. | Pure scheduling and transaction-classification focused suite: 10 passed. Final full-suite gate remains separate. |
| DHW-03 | Given duplicate delivery, when workers race, then one inbox/state/outbox transaction applies and no acknowledged work is lost. | Replica-set race and crash-before/after-commit tests. | Mongo revision/inbox transaction cases pass; actual two-worker drill completes 32 workflows with exactly 32 reservations and 64 recipient receipts despite repeated requests. |
| DHW-04 | Given an earlier unresolved partition record, when later work finishes, then committed offsets do not pass the durable frontier. | Kafka integration with controlled interleavings. | Actual Kafka consumer restart replays failed record before the following record; null record quarantine reaches the next valid record. Both tests passed. |
| DHW-05 | Given expired claims or stale/out-of-order replies, when processing resumes, then current state and command identity remain guarded. | Fake-clock concurrent-worker and replay tests. | Real Mongo stale publication/execution tokens, guarded revision/inbox cases and pure transition tests pass; actual restart waits for fenced lease recovery. |
| DHW-06 | Given a provider succeeds before process failure, when restarted, then reconciliation converges without blindly repeating an uncertain effect. | Fake provider crash-window and real provider contract checks when authorized. | Actual resource cancellation after durable calendar success and after durable notification success both converge on restart. Real providers remain pending. |
| DHW-07 | Given cancellation or overdue durable deadlines, when restarted, then work resumes or enters bounded repair without leaked workers. | Cats Effect resource/cancellation and restart tests. | Real elapsed-deadline reservation reconciliation releases the slot without a hiring commit; worker resource restart proof passes. |
| DHW-08 | Given exhausted retry or failed compensation, when operators inspect/repair, then progress and allowed guarded action are visible and audited. | Failure drill and forbidden/repeated repair tests. | Five failed release attempts preserve the reservation in visible repair. Five failed notification attempts preserve the hiring commit; authorized Admin repair resets the attempt generation and completes. |
| DHW-09 | Given deleted subjects and replay/retention boundaries, when work is replayed, then no deleted data or effects are recreated. | Deletion-race, old-replay-denial and storage inventory tests. | Actual Mongo account deletion, provider fencing, purge and canonical command/result replay pass with no resurrection. Four cleanup cases pass. Separately, accelerated physical earliest-offset advancement passed for both isolated Kafka topics; no deployed retention claim. |
| DHW-10 | Given a confirmed interview reservation and rejected status commit, when compensation runs, then reservation releases; uncertain commit is reconciled first. | Conditional scheduling transaction/provider tests. | Guarded commit/receipt cases and real deadline compensation pass; uncertain driver outcomes have a separate unavailable classification and stable receipt lookup. |
| DHW-11 | Given committed interview status and partial/uncertain notifications, when retried, then per-recipient reconciliation or repair is visible without invented delivery guarantees. | Conditional notification duplicate/restart tests. | Partial notification exhaustion/Admin repair passes; actual postcommit notification-success crash/restart retains the reservation and finishes both receipts. |
| DHW-12 | Given provider/broker outage, when normal hiring transactions execute, then local durable acceptance remains independent and later delivery recovers. | Mongo + Kafka outage integration; existing embedding recovery tests extended. | Scheduling service acceptance passes without Kafka/provider resources; actual worker outage/restart and provider-unavailable recovery pass. A dedicated broker-process-outage acceptance test is not claimed. |
| DHW-13 | Given operational storage evolution, when concurrent startup/restart occurs, then verified indexes/state become ready without corrupting live revisions. | Disposable migration and rollback/recovery rehearsal. | Real Mongo repeatable setup and concurrent inbox index cutover pass; source preserves unrelated records and fails closed on unknown definitions. Stop incompatible writers before production cutover. |

## Performance, implementation handoff and checkpoint

Use small deterministic synthetic workflows. Record seed/count, message bytes, key skew, partitions, concurrent workers, replay fraction, provider latency/failure distribution, queue age, p50/p95/p99 completion time, retries, throughput and errors. Compare reuse of the existing worker with added orchestration. Capture Mongo query plans/index/write cost, broker traffic/storage and provider call counts. Production freshness/recovery targets and cost ceiling remain unresolved; no benchmark result is implied.

1. Architect/Big Data/Data Engineer finalize the current catalog and concrete workflow boundary (DHW-01); Security reviews ACL/deletion scope.
2. Scala Developer extracts only necessary pure capability policies and extends existing embedding recovery evidence (DHW-02, DHW-05–07, DHW-12).
3. Data Engineer/Scala Developer implement guarded durable records and migration in agreed exclusive paths (DHW-03–05, DHW-09, DHW-13); this depends on the selected workflow contract.
4. Scala Developer integrates the interpreter, reconciliation and authorized repair (DHW-06–09). Test with fake providers and disposable Kafka/Mongo before external integration.
5. Conditional scheduling slice resolves its open public/provider contract, then implements reservation/status/notification recovery (DHW-10–11). Update SDL, operations, use cases and catalog together.
6. Independent Code Reviewer, Security Engineer and final QA evaluate final source and failure evidence. Run root `sbt test`, configured format checks and relevant `IntegrationTest / test`; use separate provider/deployed gates when applicable.

Checkpoint: local implementation is underway; executions and independent verdicts are recorded below as they become available. The historical documentation checkpoint does not certify the new source or production activation.

## Local implementation checkpoint (2026-10-06)

The authorized local implementation is complete and its execution evidence is recorded below. Partial source existed before this task; its presence was not counted as runtime evidence. Geographic discovery, workflow persistence/interpretation, scheduling API/runtime, Kafka transport and deletion cleanup have passed the final local executions. Independent Code Reviewer, Security Engineer and final independent QA: PASS for the authorized local scope.

- Runtime is opt-in through `kafka.interview.enabled`, with separate orchestrator/worker SASL identities and consumer groups. Commands and results use one active typed metadata envelope; participants and intervals remain in MongoDB. Existing operational envelopes are unchanged.
- Defaults: five attempts; exponential retry from one second to 30 seconds; ten-second provider timeout; 60-second fenced claims; five-minute precommit window bounded by interview start. Topics have one partition, replication/minimum ISR one, delete cleanup, seven-day retention and 64 KiB maximum records. Replay is bounded to seven days; completed evidence is retained for eight days. Unfinished work has no TTL.
- Deletion marks the existing permanent subject fence in the account transaction and enqueues attributable interview cleanup. The cleanup worker waits for claims, provider calls and Kafka sends to drain, purges workflow/provider data, captures both topic end-offset barriers and waits for actual earliest-offset advancement. Public deletion completion requires both existing analytics erasure and interview cleanup. No lakehouse reset or existing-data deletion is authorized by this implementation.
- Operational migrations `006_job_geo_points`, `007_interview_workflow_storage` and `008_interview_subject_cleanup` and `009_interview_inbox_identity` preserve existing local data. Stop incompatible writers before cutover; startup verifies required index definitions before workers run.
- Validation: final Java 17 formatting and 526 units passed; 85 integration cases executed successfully. Four Atlas cases were skipped and two older operational Compose checks were disabled. Actual worker restarts, deadline/compensation/notification/deletion replay drills, 32-workflow/two-worker measurement and accelerated physical retention passed. Independent Code Reviewer, Security Engineer and final independent QA: PASS.
- Resolved findings: MongoDB rejected the absent-field partial-index expression, so participant availability uses an ordinary compound index. The first workload exposed transient database failures being treated as confirmed business conflicts; unavailable outcomes now reconcile and publication authorization failures retry promptly. Fixtures now exercise genuinely stale revisions and persisted reservations. UTC millisecond normalization prevents collapsed intervals and aligns mutation, inspection and replay.
- Pending external gates: Atlas execution, human relevance approval, production redundancy, real providers, cancellation/rescheduling and deployed retention.

Scheduling acceptance rechecks the authoritative application and job in its MongoDB transaction. It advances the existing application `updatedAt` monotonically to acquire a real document write lock; it adds no application fields or statuses. Admin repair records its actor and atomically supersedes earlier commands before issuing reconciliation work. Canonical message identity, polarity, step, revision, causation, occurrence and deadline are checked before interpretation; expired applicable work enters visible repair and unrelated/stale records receive bounded durable diagnostics.

Interview transactions classify exhausted transient MongoDB write conflicts as unavailable outcomes requiring reconciliation. They remain distinct from a confirmed guard rejection; existing transaction callers retain their configured classification. Transient publication authorization failures return the fenced claim to due work with bounded backoff rather than waiting for its entire lease. Timeout/claim validation and deletion drain calculations use widened arithmetic so extreme configuration values cannot wrap into a shorter barrier wait.

## Local scheduling measurements and executions

The disposable MongoDB and isolated local SASL Kafka proof executed 32 workflows with two worker resources. Workers were cancelled after a durable calendar reservation and again after a durable notification receipt following the hiring commit, then restarted. All 32 workflows completed with 32 reservations, 64 recipient receipts and zero workflows requiring repair. The separate Kafka tests passed both failed-record replay before the next offset and null-record quarantine before a valid record.

| Measurement | Recorded result |
|---|---|
| Completion p50 / p95 / p99 | 150,257 / 154,872 / 154,872 milliseconds |
| Calendar reserve / notification delivery / notification lookup requests | 33 / 66 / 1 |
| Broker publication / command consumption / result consumption requests | 264 / 161 / 133 |
| MongoDB commands | 24,270, including request persistence and evidence reads |
| Sampled pending workflow backlog | 32 to 0; 369 samples with request-age and phase counts |
| MongoDB storage growth | 413,696 bytes |
| MongoDB index growth | 1,122,304 bytes |
| MongoDB container CPU / memory | 9.43 CPU seconds; 127.25 to 139.53 MB |

Completion latency includes deliberate crash recovery and the 60-second fenced leases; it is not a normal-operation latency estimate. Resource measurements cover the disposable MongoDB container, including probe processes, rather than total JVM/broker usage. The zero error count describes completion outcomes; intentional failures and retries are captured in request counts and recovery tests. No SLO or production cost claim follows from these measurements.

Ignored local evidence: `.local/data/interview-scheduling-workload.json`, `.local/logs/interview-scheduling-final-proof.log` and `.local/logs/interview-scheduling-recovery-final.log`. The final focused run passed 10 pure/classification tests and 10 real MongoDB repository/recovery tests. The final Java 17 unit suite passed all 526 tests with configured formatting checks. The complete integration command exited successfully with 91 framework cases: 85 actually executed, four explicitly skipped Atlas cases and two disabled older operational Compose checks. Final-state logs are `.local/logs/hiring-capabilities-final-unit.log` and `.local/logs/hiring-capabilities-final-integration.log`; the latter includes the repeated 32-workflow proof and both actual Kafka acknowledgment tests. Independent Code Reviewer, Security Engineer and final independent QA verdicts are PASS for the authorized local implementation.

Accelerated physical Kafka retention was repeated after the final integration suite on both isolated topics: command earliest offset reached barrier 402, and result earliest offset reached barrier 389. The final script exited successfully; evidence is `.local/logs/interview-physical-retention-final.log`. Regular local Kafka/MongoDB data was preserved. This proves physical cleanup on the isolated accelerated configuration, not seven elapsed production days or an end-to-end deployed erasure acceptance run.

### Review remediation checkpoint (2026-10-06)

- Hiring history and the transactional operational outbox now attribute the guarded `Accepted -> Interview` transition to `workflow.initiatedBy`, preserving Admin attribution when Admin schedules.
- `recordResult` compare-and-set now also requires command state `Executing`; Admin repair clears owner, fencing token and lease when superseding commands. The result write intentionally does not require the old workflow phase because the guarded status commit advances phase before its execution result is recorded.
- Worker and Mongo adapter share pure `InterviewWorkflow.commandIsApplicable` phase/revision policy. Outstanding notification work from an earlier revision remains eligible while notifications are pending; future revisions and stale commands in other phases are rejected. Transport/domain result conversion is exhaustive and preserves the active persisted/message shapes.
- Regression coverage includes Admin actor fields in both history/outbox, a provider-success/live-claim result arriving after repair, applicability revision boundaries, and durable completion failure diagnostics. Final Java 17 `scalafmtAll scalafmtSbt scalafmtCheckAll scalafmtSbtCheck test` passed formatting and 533/533 units; focused `MongoInterviewWorkflowRepositoryIntegrationSpec` passed 8/8 and `InterviewWorkflowRecoveryIntegrationSpec` passed 4/4. Independent Code Reviewer, Security Engineer and QA verdicts are PASS. The full Kafka/runtime suite, deployed runtime and real providers were not rerun as part of this narrow remediation.
