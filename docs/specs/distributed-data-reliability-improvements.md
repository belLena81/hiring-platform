# Distributed Data Reliability Improvements

Status: **MVP scope complete (2026-11-01)**: items 5, 6, 1, 3 and DDR-04.1 to 04.3 are implemented and passed their review and QA gates (commits 181f5bb, 9114a87, 462aac9, a3645e0, 1363f3a and the DDR-03 test commit). Owner decision (2026-11-01, MVP condition 1): items 5, 6, 1, 3 and DDR-04.1 to DDR-04.3 land before the MVP (local) claim; items 2, 7, 8 and 9 and DDR-04.4's re-measurement are explicitly deferred as post-MVP work (no API or analytics shape change in the MVP scope). Created from a read-through of the
codebase against *Designing Data-Intensive Applications* (Kleppmann, 1st ed., 2017). Chapter references are to that book.

Items 5, 6, 1, 3 and DDR-04.1 to 04.3 were implemented under the pre-MVP contract policy, so they add no migration,
backfill or dual-read/write code. The policy applies until the owner explicitly asks to start versioning and migration scripts for schema changes (owner decision 2026-11-08; the owner calls that point the first cloud release; [schema evolution](../schema-evolution.md)). The post-MVP items (2, 7, 8, 9 and DDR-04.4's re-measurement) keep the versioned Mongo migration ledger where stored data changes. Their API and analytics shape changes are implemented directly under the policy, with no deprecation window or compatibility code.

## How to read this plan

- Items are in **severity order** (1 = most important). Severity weighs the chance of a wrong or lost business outcome,
  not effort.
- Each item has a **Verification status**:
  - **Confirmed**: the behavior was read in source on the date below.
  - **Unverified**: inferred from specs, file headers or milestones; the first task is to check it.
  - **Evidence gap**: the design exists, but measured evidence is pending by recorded decision.
- Acceptance IDs (`DDR-nn.m`) are stable. Per `AGENTS.md`, the ID appears in the test name once a test exists.
- Source facts were read on 2026-10-27 at branch `refactor`, HEAD `4f90e90`, with unrelated uncommitted changes in
  `analytics/`. Recheck source before acting; this document cannot certify current behavior.

## Non-goals

- No sharding, multi-region replication or new infrastructure in this plan. Item 9 is analysis only.
- No change to the Candidate / Recruiter / singleton Admin role model.
- No new dependency unless an item states a concrete capability gap.
- No weakening of an existing review, security or QA gate to save effort.

## Summary table

| # | Improvement | DDIA | Severity | Verification status |
|---|---|---|---|---|
| 1 | Decide lease expiry on the database clock | Ch. 8 | Medium | Confirmed |
| 2 | Fake providers must reproduce real failure modes | Ch. 8, 9 | Medium | Confirmed (fakes only) |
| 3 | Fail-closed test at the deduplication retention horizon | Ch. 11, 12 | Medium | Unverified |
| 4 | Allowed-lateness policy and the failed Bronze latency measurement | Ch. 11 | Medium | Evidence gap |
| 5 | Verify durability settings (write concern, read concern) | Ch. 5, 7 | Medium-low (could rise) | Unverified |
| 6 | Verify per-aggregate ordering in the outbox relay | Ch. 11 | Medium-low | Unverified |
| 7 | Invariant-to-fence checklist for cross-document rules | Ch. 7 | Low | Confirmed pattern, no checklist |
| 8 | Measured evidence for derived search data | Ch. 5, 12 | Low (acceptance gate) | Evidence gap |
| 9 | Partitioning analysis before real data volume | Ch. 6 | Low (deferred) | Analysis only |

---

## 1. Decide lease expiry on the database clock

**Severity: Medium.** **Status: Confirmed.** **DDIA:** Ch. 8, unreliable clocks and process pauses.

### Problem

Lease expiry compares instants that application nodes compute from their own clocks.

- `repository/mongo/MongoRepositorySupport.scala:245-282`: `MongoLeaseQueue` defines a lease as held while
  `leaseUntil > now` and reclaimable once `leaseUntil <= now`. `now` and `leaseUntil` are parameters written as literals
  (`now.toDate`).
- Callers derive them from the local clock: `service/application/InterviewPublication.scala:42,140-145`,
  `infrastructure/kafka/OperationalEventKafkaRuntime.scala:219`, `service/events/SearchSessionHandoff.scala:83`.
- A search of `src/main` for `$$NOW`, `$$CLUSTER_TIME` and `currentDate` found no match.
- The shared lease code serves embedding work, search-session work, the event outbox and interview commands.

### Risk

A worker whose clock runs behind sees another worker's live lease as expired and reclaims the work. An NTP step forward
makes one node reclaim leases early. `LeaseHeartbeat` renewal does not help, because the problem is two nodes
disagreeing about the time, not a slow worker.

### Existing mitigation (why this is Medium, not High)

- Writes after a claim filter on `leaseToken` (`MongoSearchSessionWorkRepository.scala:203-206`), so a displaced worker's
  late Mongo write is rejected. Skew causes duplicate work, not corrupted state.
- Outbox consumers are idempotent through inbox receipts.
- `InterviewProducerFencer` and the transactional ID cover zombie Kafka publishers. This was not read in detail; do not
  treat it as proof the gap is closed.

### Plan

1. Record the baseline: for each claimable-work query, capture `explain("executionStats")` plans and the existing index
   used (`AGENTS.md` requires plans for performance-relevant changes).
2. Replace instant parameters with durations. A claim or renewal passes `leaseDuration`; Mongo computes
   `leaseUntil = $$NOW + duration` through an update pipeline in `findOneAndUpdate`.
3. Express the expiry predicate with `$expr: { $lte: ["$leaseUntil", "$$NOW"] }`. `$$NOW` is not usable in a plain
   filter document. Re-run the plans from step 1. If `$expr` prevents index use on the claimable queries, stop and
   record the finding before choosing an alternative (for example, a bounded-skew margin as a documented fallback).
4. Change the ports in `service/port/` (`OperationalEventRepositories`, `EmbeddingSearchRepositories`,
   `SearchSessionWorkRepository`, `InterviewWorkflowRepository`) so callers no longer supply lease instants. Keep
   `availableAt` and retry backoff as caller values; they are scheduling inputs, not ownership decisions.
5. Update `LeaseRequest.validate` (`MongoLeaseRequest.scala`): drop `WindowNotAfterNow`, add a bounded duration check.
6. Rework the lease integration specs that pass explicit instants (for example `OutboxBatchClaimIntegrationSpec`).

### Acceptance

| ID | Criterion |
|---|---|
| DDR-01.1 | A claimed lease's expiry equals the database time at claim plus the requested duration, for all four queues. |
| DDR-01.2 | A worker whose local clock is skewed by plus or minus 10 minutes cannot reclaim a live lease, and a live holder is not displaced. |
| DDR-01.3 | An expired lease is reclaimable exactly once under concurrent claimers. |
| DDR-01.4 | Renewal after expiry reports loss, and the stale worker's writes are rejected (token fence unchanged). |
| DDR-01.5 | Recorded query plans before and after show the claimable-work indexes still serve the queries, or the finding is documented. |

### Findings and evidence (2026-11-01)

Implemented at HEAD 462aac9 plus uncommitted changes; independent Code Reviewer, Security Engineer and QA verdicts are tracked by the coordinator.

- **Gate result.** `$expr: {$lte: ["$leaseUntil", "$$NOW"]}` does not prevent index use. The planner folds `$$NOW` to a date constant and derives the bound `[MinKey, now]` on `leaseUntil`; every claim query keeps its index and examines the same keys. The `$expr` remains a residual `FETCH` filter, which re-fetches the expired candidates. No index changed, so there is no migration.
- **Plans** (MongoDB 8.0.32 test replica set, 4000 rows per collection, `LeaseQueueClaimPlanIntegrationSpec`; the caller-clock figure is the replaced predicate, kept in the spec as baseline):

| Query | Pinned index | Keys (caller, DB clock) | Docs (caller, DB clock) | Returned |
|---|---|---|---|---|
| embedding claim | `embedding_work_available_lease` | 2002, 2002 | 400, 600 | 1 |
| search-session claim | `search_session_work_claim` | 2002, 2002 | 400, 600 | 1 |
| outbox page (limit 64) | `event_outbox_claim` | 2001, 2001 | 64, 264 | 64 |
| outbox page after cursor | `event_outbox_claim` | 2001, 2001 | 400, 600 | 0 |
| interview claim | `interview_workflow_commands_due`, `_lease` | 401, 401 | 400, 201 | 1 |
| backlog embedding count (cap 10000) | `embedding_work_available_lease` | 2002 | 600 | 400 |
| backlog embedding age reads | `embedding_work_available_lease` | 1, 9 | 1, 1 | 1 |
| backlog outbox count | `event_outbox_claim` | 2001 | 600 | 400 |
| backlog outbox age reads | `event_outbox_claim` | 1, 9 | 1, 1 | 1 |

- **Decisions.** Lease expiry and every ownership predicate use the database clock (`$$NOW`); `availableAt`, retry backoff and `updatedAt` stay caller values; the `leaseToken` fence is unchanged. Renewal after expiry is a lost lease even before a takeover. An upsert cannot carry `$expr` (server error 224), so outbox subject leases use two ordered bulk entries per subject. The interview effect fence (`fenceExecution`) uses the database-clock held predicate and no longer reads the clock. Out of scope and confirmed not to be queue ownership decisions: the analytics worker heartbeat readiness (`MongoAnalyticsRepositories` `workerReady`) and the `EmbeddingCoverage` report classification (read-only reporting).
- **Tests.** `sbt test`: 1724 passed, 0 failed. Lease and queue integration set run by the author (20 spec globs: lease, outbox, interview, embedding work, search-session, backlog, producer registration, Kafka compose; it did not include `MongoEmbeddingCoverageIntegrationSpec`'s regression because that run predated the fixture fix below): 330 total, 325 passed, 5 skipped, 0 failed. QA's independent run of the wider set: 328 total, 322 passed, 1 failed, 5 skipped; the one failure was `MongoEmbeddingCoverageIntegrationSpec` "ECR-02 ECR-03 the cross-tab counts every searchable entity once", a fixture defect (it pinned an already-expired lease before later claims, so a later claim reclaimed it). Fixed by pinning the report-time expiries after all claims; rerun of `MongoEmbeddingCoverageIntegrationSpec` and `MongoEmbeddingWorkIntegrationSpec`: 19 total, 18 passed, 1 skipped, 0 failed, and `sbt test` 1724 passed. DDR-01.1 to 01.4 are in `LeaseExpiryDatabaseClockIntegrationSpec` (search-session, outbox, interview, including the effect fence refused with a stale caller clock and the subject-lease duplicate key at the lease write) and `MongoEmbeddingWorkIntegrationSpec`; DDR-01.5 in `LeaseQueueClaimPlanIntegrationSpec`.

### Owners and gates

Data Engineer (query shape, plans), Scala Developer (ports, callers, tests). Independent Code Reviewer, Security
Engineer (lease and fencing semantics) and QA. Run `sbt test` and the integration suite on a replica set.

---

## 2. Fake providers must reproduce real failure modes

**Severity: Medium.** **Status: Confirmed for the fakes; real providers deferred.** **DDIA:** Ch. 8 partial failure, Ch. 9.

### Problem

The interview saga (`domain/workflow/InterviewSchedulingSaga.scala`) models `OutcomeUnknown` and lookup events, but
evidence comes only from fake providers. `docs/development-milestones.md` (Phase 11) records DHW-26, DHW-32 and the SES
part of DHW-24 as deferred to the real-provider slice.

### Risk

A fake that always answers cleanly cannot show that the lookup-before-retry path survives "succeeded but the response
was lost", duplicate delivery after retry, or a reply that arrives after a newer revision.

### Plan

1. Read the existing fake calendar and notification providers and list which failure modes they can already inject.
2. Add controllable fault injection to the fakes: timeout after success, timeout before effect, duplicate reply,
   delayed (out-of-order) reply, lookup returning found/absent/unavailable.
3. Add one integration scenario per saga step and phase, driven through `SagaOrchestrator.settle`, asserting the final
   phase and that no step with `StepUndo.Irreversible` is compensated.
4. Cover the fence: a delayed reply for an old revision is refused through `StaleRevision` and changes nothing.

### Acceptance

| ID | Criterion |
|---|---|
| DDR-02.1 | After "reservation succeeded, response lost", the workflow looks the reservation up and does not reserve twice. |
| DDR-02.2 | After "status commit succeeded, response lost", the receipt lookup finds it and the workflow does not compensate. |
| DDR-02.3 | A duplicate notification reply advances `notified` once. |
| DDR-02.4 | A reply carrying a stale revision returns `StaleRevision` and leaves state unchanged. |
| DDR-02.5 | Exhausted retries on every step end in `RepairRequired` with a sanitized reason, never an unbounded retry. |

### Owners and gates

Scala Developer. Independent Code Reviewer and QA. Security Engineer only if provider interfaces change.

---

## 3. Fail-closed test at the deduplication retention horizon

**Severity: Medium.** **Status: Unverified.** **DDIA:** Ch. 11 exactly-once through idempotence, Ch. 12 end-to-end
operation identifiers.

### Problem

Inbox deduplication records are kept for a bounded time: `completedDedupRetentionSeconds` has a minimum of 691,200
seconds, about 8 days (`config/AppConfigModels.scala:484`, default at `:505`). The architecture text says older replays
are refused, not reprocessed.

### Risk

A consumer paused or replayed beyond the horizon could reapply a message if the "refuse older replay" rule is missing
at the boundary, or if it depends on a clock comparison (see item 1).

### Plan

1. Read the receive path (`service/application/InterviewMessageReceiver.scala`, the `ReplayExpired` advance cause in
   `domain/workflow/InterviewLifecycle.scala`) and confirm which check enforces the horizon and which clock it uses.
2. Add boundary tests: message just inside the horizon is deduplicated; message just outside is refused
   (`ReplayExpired`), not applied.
3. If the horizon uses a caller clock, apply the item 1 approach or document why skew cannot matter here.

### Acceptance

| ID | Criterion |
|---|---|
| DDR-03.1 | A duplicate delivered within the retention window is acknowledged without side effects. |
| DDR-03.2 | A message older than the window is refused, recorded and never executed: a message that does not match an applicable open command is quarantined (`:expired`) with no state change; an applicable command on an open workflow marks it `RepairRequired` (visible repair). |
| DDR-03.3 | Deleted-subject replays stay denied (per `AGENTS.md`: no automatic replay of deleted subjects). |

### Findings and decisions (2026-11-01)

Read on HEAD `462aac9`. The horizon is an explicit age check, `InterviewMessagePolicy.replayDisposition` (`service/application/InterviewMessagePolicy.scala:36-44`), applied by `InterviewMessageReceiver.withinReplayWindow` before any deduplication: the replay window is pinned to 7 days (`replayRetentionSeconds`, `StrictEqual[604800L]`), future messages beyond the skew tolerance are refused. TTL deletion of completed evidence (`completedDedupRetentionSeconds`, at least 8 days) is only a backstop and deletes late, never early; if evidence is gone, admission quarantines and never executes. Deleted subjects are purged and fenced; redelivered messages find no workflow and are acknowledged without effect (DHW-30). The check compares the producer's `occurredAt` with the consumer clock, so the item 1 database-clock approach does not apply; skew can only shift the window by the NTP skew, covered by the 1-day retention margin. Decisions: DDR-03.2 is reworded to the existing, deliberate and tested behaviour (an applicable expired command produces a visible `RepairRequired`, not silent refusal); the plan's file pointer for `ReplayExpired` is `domain/workflow/InterviewWorkflowPolicy.scala` and `InterviewRepairReason.scala`. The DDR-03.x tagged tests listed under Evidence below were added after item 1 landed; Security Engineer PASS and QA PASS (2026-11-01).

### Evidence (2026-11-01, after item 1, commit `a3645e0`)

- DDR-03.1: `InterviewWorkflowWorkerSpec` "DDR-03.1 a duplicate command delivered exactly at the replay window is acknowledged without side effects" and "DDR-03.1 a result delivered exactly at the replay window is applied once as timely".
- DDR-03.2: `InterviewWorkflowWorkerSpec` "DDR-03.2 a message one nanosecond past the replay window on a completed workflow is quarantined as expired" and "DDR-03.2 an expired message whose workflow and command are gone is quarantined and never executed"; the `RepairRequired` branch is the existing "an expired matching command requires repair even when its stored result exists"; `InterviewWorkflowConfigSpec` "DDR-03.2 the smallest completed-evidence retention outlives the replay window by at least one day".
- DDR-03.3: unit "DDR-03.3 a command and a result for a purged subject are acknowledged and recreate nothing"; integration "DHW-30 a result and a command redelivered after the deletion are settled without any effect" in `InterviewLifecycleAccountDeletionIntegrationSpec` (script run: 15 passed, 0 failed).
- Totals: `sbt scalafmtCheckAll test` 1730 passed, 0 failed (tests only; no `src/main` change). Status of DDR-03: criteria evidenced; awaiting independent review and QA.

### Owners and gates

Scala Developer, Data Engineer for retention and TTL indexes. Security Engineer (deletion and replay denial). QA.

---

## 4. Allowed-lateness policy and the failed Bronze latency measurement

**Severity: Medium.** **Status: Evidence gap by recorded decision.** **DDIA:** Ch. 11 reasoning about time, windows and
stragglers.

### Problem

`AnalyticsLateFactReplay` exists, but the lateness policy has not been measured under load. The milestones record a
Bronze p95 of **40,084 ms: FAIL**, kept as a failed measurement, with latency and burst drain-time qualification
deferred to Phase 15 until a real deployment exists.

### Plan

1. Write down the allowed-lateness bound as a configured value and state what happens to a fact later than that bound
   (replay path, quarantine, or dropped with a metric).
2. Add boundary tests with Spark watermark behavior: fact inside the bound, at the bound, past the bound, after the
   affected report was published.
3. Do **not** relabel the 40,084 ms result. When a deployment is authorized, re-measure with the workload, dataset size,
   concurrency, p50/p95/p99, error and resource data that `AGENTS.md` requires.

### Acceptance

| ID | Criterion |
|---|---|
| DDR-04.1 | The lateness bound is a typed configuration value with a documented default. |
| DDR-04.2 | A fact past the bound follows exactly one documented path and is counted in a metric. |
| DDR-04.3 | A late fact after publication triggers the documented correction or replay, with the report generation recorded. |
| DDR-04.4 | The Bronze latency result remains recorded as FAIL until a new measured run replaces it. |

### Decisions (2026-11-01)

Gap analysis on HEAD `c43ac94`: the bound is the fixed 24 h `WatermarkLag` (effective tolerance 24 to 48 h depending on the clock position within the UTC day); CLOSED facts go only to `lateFacts`, FUTURE facts only to quarantine (blocking that batch's publication); the per-batch counts are measured but never emitted; replay is explicit and records `generation`, `revision` and `runId`. Owner decisions:

- DDR-04.1: `analytics.streaming.allowed-lateness` (default 24 hours, `ANALYTICS_STREAMING_ALLOWED_LATENESS`), validated and part of the streaming activation identity (local grants and checkpoints are re-created). Future skew and the 30-day late-fact retention stay fixed policy values.
- DDR-04.2: counts are emitted as one sanitized structured log line per ingest, `STREAMING_EVENT_TIME_ADMISSION closedDay=N future=N allowedLatenessMillis=M batch=<id> generation=<g> revision=<r>` (revision added 2026-11-01: replay publishes a newer revision of the pinned generation, so generation alone cannot identify the report a late fact missed); no journal columns, no metrics dependency, no analytics table or Mongo snapshot shape change.
- DDR-04.3: the policy is explicit operator replay by Kafka coordinate into a new generation; the log line records which published generation a late fact missed. Automatic replay is a post-MVP option.

### Owners and gates

Big Data Engineer. Independent Code Reviewer and QA. Large benchmarks need explicit scope and budget.

---

## 5. Verify durability settings (write concern, read concern)

**Severity: Medium-low, could rise to High if a setting is weak.** **Status: Unverified.** **DDIA:** Ch. 5 replication,
Ch. 7 isolation.

### Facts seen

- `config/HttpMongoConfigReader.scala:27` rejects a URI whose write concern is not acknowledged.
- `ReadPreference.primary()` is pinned in the places read (`MongoHiringSetup.scala:40`,
  `MongoEmbeddingCoverageRepository.scala:43`), and `ReadConcern.SNAPSHOT` is used for coverage reads (`:54`).
- Not confirmed: whether acknowledged means `w=majority` for transactions and critical writes in every environment.

### Risk

A write acknowledged by the primary alone can be rolled back after a failover, losing an applied status change or an
outbox row the rest of the system already trusts.

### Plan

1. List every place a client, database or collection is created, and the effective read preference, read concern and
   write concern, including transaction options and compose and cloud URIs.
2. Make the intended policy explicit in typed configuration: majority write concern for transactions and outbox writes;
   primary reads for read-your-writes paths.
3. Add a startup or integration check that fails when the effective concern is weaker than the policy.
4. Document any read path that may later use secondaries, with the Ch. 5 anomalies it would expose (read-your-writes,
   monotonic reads).

### Acceptance

| ID | Criterion |
|---|---|
| DDR-05.1 | A table of effective concerns per repository operation is recorded in `docs/mongodb-design.md`. |
| DDR-05.2 | Transactions and outbox or status writes use majority write concern, enforced by configuration validation. |
| DDR-05.3 | A URI with a weaker write concern is rejected at startup with a sanitized error. |

### Decisions (2026-11-01)

Verification on HEAD `c43ac94`: **Confirmed, not enforced.** The application sets no write concern, read concern or read preference (only the Admin embedding-coverage read pins primary/snapshot); `MongoDatabaseProbe` applies the URI and nothing else, `MongoTransactionRunner` starts transactions without `TransactionOptions`, and `HttpMongoConfigReader` rejects only `w=0`. Analytics validates only that the URI parses; its lock, streaming registry/gate, HMAC authorization and late-fact journal pin majority with journal, while the erasure queue, barrier and progress, deletion markers, report publisher and key-retirement scan inherit the URI default. The effective value is the server default (majority on the shipped single-member replica set, unasserted). Owner decision: reject weak URIs **and** override. Startup rejects `w` below majority, `journal=false`, a non-primary read preference and an unsupported read-concern level with the existing redacted error, in both the application and analytics; the client builders (`MongoDatabaseProbe.effectiveSettings`, analytics `SparkRuntime.mongoClient`) and transactions set majority write concern with journal, majority read concern and primary read preference explicitly. `docs/mongodb-design.md` gains the effective-concern table (DDR-05.1).

### Owners and gates

Data Engineer. Security Engineer (configuration, secrets in URIs). Code Reviewer and QA.

---

## 6. Verify per-aggregate ordering in the outbox relay

**Severity: Medium-low.** **Status: Unverified.** **DDIA:** Ch. 11 log-based messaging, partition ordering.

### Facts seen

`ARCHITECTURE.md` §15 states that ordering exists only within a Kafka partition, that outbox aggregate order is
preserved, that each ordered key is processed sequentially, and that offsets advance only through the contiguous
completion frontier. `MongoOperationalEventRepositories.scala` claims batches with a held-subject check
(`heldSubjects`, line 360). Not read in full: retry and failure paths.

### Plan

1. Read the claim, publish, failure and retry paths and confirm no later event of an aggregate can publish before an
   earlier one that is retrying or quarantined.
2. Add an integration test: event A of aggregate X fails transiently, event B of X is due; B must not publish first.
3. Confirm the Kafka message key is the aggregate ID and that a partition-count change is gated by the replay analysis
   the architecture already requires.

### Acceptance

| ID | Criterion |
|---|---|
| DDR-06.1 | Under a transient failure of an earlier event, a later event of the same aggregate is not published first. |
| DDR-06.2 | Events of different aggregates continue to publish while one aggregate is blocked. |
| DDR-06.3 | The Kafka key for operational events is documented and tested as the aggregate partition key. |

### Decisions (2026-11-01)

Verification on HEAD `c43ac94`: **Confirmed failing; severity raised to Medium.** The Kafka key is the aggregate ID, and the producer is idempotent and transactional, so the producer cannot reorder a partition. The relay can: publication is serialized only by per-user subject leases, `releaseForRetry` releases the leases of a transiently failed event, and the claim has no "earlier unpublished event of the same aggregate" predicate, so a later event of one aggregate can be published before an earlier one that is retrying or whose lease expired (DDR-06.1 fails). Today's consumers are order-independent (analytics earliest-wins and deduplication; the application consumer records receipts), so the practical impact is low, but `ARCHITECTURE.md` promises aggregate order. Owner decisions:

- The claim does not return an event while an earlier non-terminal (`Retryable` or `InFlight`) event with the same partition key exists, ordered by `(occurredAt, _id)`; other aggregates keep publishing (DDR-06.2).
- A `Failed` event (retries exhausted) does not block its aggregate: later events continue, the gap is documented, and the failed event stays visible and logged for operator repair.
- Events written in one transaction (for example `APPLICATION_STATUS_CHANGED` and `CANDIDATE_HIRED`) have no defined relative order; this is documented, and no sequence field is added.

### Owners and gates

Big Data Engineer (Kafka), Scala Developer. Code Reviewer and QA.

---

## 7. Invariant-to-fence checklist for cross-document rules

**Severity: Low.** **Status: Pattern confirmed, no checklist.** **DDIA:** Ch. 7 write skew and phantoms.

### Facts seen

`MongoApplicationWriteFence` and `MongoSubjectFence` deliberately create write conflicts so a concurrent change cannot
slip past a cross-document check; `users.version` content revisions are kept separate from the fence
(`MongoSubjectFence.scala:14`, `MongoJobAdmissionFenceMigrations.scala`).

### Risk

A new rule that spans two documents can be added without a fence, because nothing lists which invariants need one.

### Plan

1. Inventory the cross-document invariants (duplicate application, closed-job admission, deleted-account writes,
   interview slot ownership, singleton Admin).
2. For each, record in `docs/mongodb-design.md`: the invariant, the documents involved, the mechanism (unique index,
   fence document, revision compare-and-set), and the test that proves it under a race.
3. Add the checklist to the specification template so new specs must fill it for cross-document rules.

### Acceptance

| ID | Criterion |
|---|---|
| DDR-07.1 | Every inventoried invariant names its mechanism and a named race test. |
| DDR-07.2 | Any invariant without a race test gets one, or is recorded with a reason. |

### Owners and gates

Data Engineer, Technical Writer for documentation. Code Reviewer.

---

## 8. Measured evidence for derived search data

**Severity: Low as a defect; it is an acceptance gate.** **Status: Evidence gap by recorded decision.** **DDIA:** Ch. 5
replication lag as a model for index lag, Ch. 12 derived data and auditing.

### Facts seen

Embeddings and the vector index follow Mongo asynchronously. `EmbeddingCoverageService` and `EmbeddingWorkRepair`
measure and repair the lag. `docs/development-milestones.md` records the Atlas and cloud rows as pending or deferred
(Phases 8 to 10) and the human relevance labels (SEB-02.H) as a pending human gate. No vector-setting change is
adopted.

### Plan

1. Keep the evidence boundary explicit: local results do not stand in for Atlas results.
2. When Atlas access and budget are authorized, run the recorded cloud rows (MVR-08.C and the `.C` rows) with the
   thresholds already written: Recall@7 at least 0.95 per filter bucket, ANN p95 at most baseline plus 10%, write p95
   regression at most 10%, index growth at most 25%, zero unexpected errors.
3. Add a lag measure to the observability stack: age of the oldest unembedded work item, alongside the existing backlog
   gauges.

### Acceptance

| ID | Criterion |
|---|---|
| DDR-08.1 | Embedding lag (oldest pending age) is exported as a metric and alertable. |
| DDR-08.2 | Atlas rows are recorded PASS or FAIL against the stated thresholds, never inferred from local runs. |

### Owners and gates

Big Data Engineer, Data Engineer. Product Manager for the budget decision.

---

## 9. Partitioning analysis before real data volume

**Severity: Low, deferred.** **Status: Analysis only.** **DDIA:** Ch. 6 key-range vs hash partitioning, hot spots,
secondary indexes, rebalancing.

### Facts seen

Production redundancy and deployed retention are external and deferred (milestones, `AGENTS.md`). No sharding design was
seen in the documents read.

### Plan (documentation only)

1. Propose candidate shard keys for `applications`, `application_events`, `jobs` and the outbox.
2. For each, list which current access paths (candidate application list, recruiter jobs, keyset cursors, vector and
   text search) remain single-shard and which become scatter-gather.
3. Identify hot-key risks (a popular job, one very active recruiter) and the effect on the cursor ordering.
4. Record the conclusion in `docs/mongodb-design.md`; no code or infrastructure change.

### Acceptance

| ID | Criterion |
|---|---|
| DDR-09.1 | A recorded shard-key analysis lists access-path impact per collection. |
| DDR-09.2 | Any outcome that requires a cursor or index change is logged as a new scoped spec, not implemented here. |

### Owners and gates

Software Architect, Data Engineer. No implementation gate.

---

## Delivery order and dependencies

1. Verification-only tasks first because they are cheap and may change severities: items 5 and 6, and the reading step
   of item 3.
2. Item 1 (lease clock). Item 3 depends on its outcome if the horizon check uses a caller clock.
3. Item 2 (fault-injecting fakes). Independent of item 1; can run in parallel with it under separate write paths.
4. Items 4 and 7 in either order.
5. Items 8 and 9 follow authorization of the deployment and Atlas scope.

## Required gates per item (`AGENTS.md`)

- `sbt test` for every completed task; integration tests for any DB or resolver change, on a replica set.
- Independent Code Reviewer verdict, plus Security Engineer where authorization, leases, fencing, retention or
  configuration change, and a separate QA verdict on the final state.
- Missing or blocked integration infrastructure is **unverified**, not a pass.

## Known unknowns

- Whether the Mongo server clock should be the authority for every queue, or whether any queue has a reason to keep a
  caller clock (item 1, step 3 decides this from the recorded plans).
- The effective write and read concerns in compose and cloud configurations (item 5).
- Kafka-side fencing completeness under lease loss (item 1 mitigation, not read in detail).
- Whether any deployment-scale latency target applies before Phase 15 (item 4).

## Evidence log

| Date | Item | Result |
|---|---|---|
| 2026-10-27 | 1 | Confirmed by reading `MongoRepositorySupport.scala` and its callers; no database-clock use found. |
| 2026-10-27 | 2 to 9 | Not yet verified beyond the facts quoted above. |
| 2026-11-01 | 4 (DDR-04.1 to DDR-04.3) | Implemented; Code Reviewer PASS and Security Engineer PASS (2026-11-01); final QA pending. Final state: analytics `sbt test` 608 total, 606 passed, 2 skipped, 0 failed; formatting checks pass; integration `HiringAnalyticsStreamingAllowedLatenessIntegrationSpec` 3 of 3 (inside/at/past, future-only, replay after publication), `HiringAnalyticsLateFactReplayRecoveryIntegrationSpec` 7 of 7, `HiringAnalyticsStreamingIngestionIntegrationSpec` 4 of 4 on a lightly loaded rerun (two earlier runs at load average 5 to 8 each had one test exceed its fixed 5-minute bound; the 4 of 4 passes before and after those runs show load sensitivity, not a regression). `analytics.streaming.allowed-lateness` (default 24 hours, 1 hour to 7 days, in the activation identity) replaces the fixed `WatermarkLag`; `StreamingEventTimeAdmissionLog` emits `STREAMING_EVENT_TIME_ADMISSION` (with `generation` and `revision` from the batch's publication reservation) per ingest, with a unit test that captures the emitted log event; explicit replay is the documented correction policy. Tests: `AnalyticsStreamingSettingsSpec` and `AnalyticsEventTimeSpec` (DDR-04.1 and bound cases), `StreamingEventTimeAdmissionLogSpec`, `EventTimeAdmissionExpressionSpec` (inside, at, past), `HiringAnalyticsStreamingAllowedLatenessIntegrationSpec` (DDR-04.2 path exclusivity, DDR-04.3 replay into a newer revision of the pinned generation; the generation number changes only through erasure fencing, so the corrected report is identified by `revision` and `runId`). DDR-04.4 unchanged: Bronze p95 40,084 ms stays FAIL. |
