# Analytics data model refactoring

## Identity and scope

- Task: refactor the Spark/Delta analytics data model against current Spark 4.2 / Delta 4.4 guidance and the reviewed books (High Performance Spark 2e, Data Engineering with Scala and Spark, Real-Time Analytics with Apache Spark, Big Data in Practice, Building Medallion Architectures, Mastering Data Engineering and Analytics). Analytics context; UC12, UC13.
- Status: in progress
- Coordinator: Product Manager (main session). Authors: Big Data Engineer (analytics build), Scala Developer / Data Engineer (application report contract, S3 only).
- User outcome: an analytics model that enforces its own invariants, does each expensive step once per batch, reports time to hire without the 30-day truncation bias, publishes per-stage hiring durations, and runs on Spark 4.2.0 / Delta 4.4.1.
- Authorized scope: slices S1–S6 below. Each slice is committed on its own after its checks and review gates pass (user request, 2026-10-26).
- The user authorized a reset of the local lakehouse under `.local/data/analytics/`. That covers Delta tables and streaming checkpoints. Other lakehouse roots, such as `/tmp/hiring-analytics-phase6-cutover`, are out of scope.
- Non-goals:
  - partitioning or liquid clustering;
  - change data feed;
  - enabling deletion vectors;
  - replacing the streaming batch journal with `txnAppId`/`txnVersion`;
  - deployed performance claims;
  - legacy, dual-read or backfill code (pre-MVP contract policy).

## Source context and decisions

Review findings F1–F12 are summarized in the slice table below. The verified source pointers are:

| Finding | Source pointer |
|---|---|
| Table shapes | `analytics/.../adapter/spark/AnalyticsTableSchemas.scala` |
| Event parsing, validation and Silver projection | `OperationalEventTransforms.scala` |
| Gold transforms | `HiringGoldTransforms.scala` |
| Gold writes | `AnalyticsGoldStage.scala` |
| Tokenization (an RDD round-trip, called 3–5 times per batch) | `AnalyticsSubjectPrivacy.scala` |
| Silver and quarantine stage | `AnalyticsBatchSilverStage.scala` |
| Bronze ingestion | `AnalyticsBatchIngestionStage.scala` |
| Delta table properties | `AnalyticsDeltaRetention.scala` |
| Streaming admission UDF | `SparkStreamingBatchStages.scala:615` |
| Bronze description that contradicts the code | `docs/wiki/mongodb-and-analytics.md:94` |

Decisions (user, 2026-10-26):

| ID | Decision |
|---|---|
| D1 | Application stage times retention (named "lifecycle snapshot" when decided): 30 days after a terminal status (Hired, Rejected, Declined); an open application expires 180 days after its last lifecycle event. |
| D2 | Hiring stage durations are published end to end: Gold, then the Mongo report snapshot, then an additive admin GraphQL field. The existing `timeToHire` stays and is derived from the snapshot. |
| D3 | Target versions are Spark 4.2.0 and Delta 4.4.1 (Scala 2.13 artifacts used from Scala 3). |
| D4 | The local lakehouse under `.local/data/analytics/` is reset once the model changes. Code keeps failing closed on mismatched tables. |
| D5 | Compaction runs automatically from the streaming maintenance tick on a configured interval, under the existing lakehouse mutex. |
| D6 | Tokenization moves to a deterministic Scala UDF, evaluated once per row per batch. |

Assumption: Delta 4.4.1 honors `delta.dataSkippingStatsColumns` for path-based OSS tables. The OSS Delta optimizations page documents only `delta.dataSkippingNumIndexedCols`; the stats-columns property is documented for Databricks, so OSS support is unverified. S4 verifies it from the Delta log and fails closed if it does not hold. Fallback if it does not: order each subject-attributed table's columns so every allowed stats column precedes the first token or identifier column, and set `delta.dataSkippingNumIndexedCols` to that count. The OSS page counts nested fields as columns and says long string/binary columns are costly to index, so the count is computed from the flattened schema.

### Verified external facts (checked 2026-10-26)

| Fact | Source | Effect on this spec |
|---|---|---|
| Delta 4.4.1 (released 2026-10-05) targets Spark 4.2.0, 4.1.0 and 4.0.1; unsuffixed artifacts default to Spark 4.2.0. | Delta GitHub releases v4.4.0/v4.4.1 | D3 holds. The build pins `delta-spark_4.2_2.13`. The docs.delta.io compatibility table lags at 4.3.x. |
| `delta-iceberg` (UniForm) is not compatible with Spark 4.2. | Delta v4.4.0 release notes | UniForm stays out of scope; add no Iceberg dependency. |
| CHECK constraints are added with `ADD CONSTRAINT`, validate existing rows, appear as `delta.constraints.*` properties, and raise `InvariantViolationException` on violation. Adding one upgrades the writer protocol. | docs.delta.io constraints | S1 declares constraints at table creation. |
| NOT NULL cannot apply to array or map elements; a nested NOT NULL needs a non-null parent. | docs.delta.io constraints | S1 nullability rules below. |
| `OPTIMIZE` is idempotent, uses snapshot isolation, and leaves replaced files on storage until VACUUM. Auto compaction and optimized write exist since Delta 3.1.0, are synchronous with the writer, and optimized write is off by default. | docs.delta.io optimizations | S4 keeps explicit `OPTIMIZE` under the lakehouse mutex. |
| Changing stats properties does not recompute stats for existing files. | Databricks data-skipping docs (OSS equivalent unverified) | The D4 reset is what guarantees that no pre-change file carries token stats. |

Book guidance applied (Building Medallion Architectures, Mastering Data Engineering and Analytics):

- Rejected data is flagged and kept in a sibling quarantine table in Silver, not deleted. Our quarantine does this.
- Silver holds current-state, cleaned records. Slowly changing history (SCD2) and surrogate keys belong in Gold or later. `silver/application_stage_times` is current state at the `applicationId` grain, with no SCD2 and no surrogate key.
- Bronze is normally raw and immutable. Ours deliberately stores no raw envelope, for erasure and privacy. S6 records this deviation.
- Delta log and `RESTORE` support recovery and audit, not history queries. Stage times are therefore stored as columns, not recovered from table versions.
- Change data feed writes row data into `_change_data` files. That is an additional place for subject tokens to survive, and the non-goal on CDF stays for that reason.

## Behavior and contracts

### S5 — Spark 4.2.0 / Delta 4.4.1

Upgrade the analytics build and container image. Behavior does not change. Streaming recovery must still pass with the stricter checkpoint-metadata check in Spark 4.2.

### S1 — Table catalog

- **One spec per Delta table.** `DeltaTableSpec` holds: ordered typed fields with real nullability, named CHECK constraints, table properties, stats columns (filled in S4), and a subject-attributed flag. It is the single place these are declared.
- **Target validation is strict.** Create-or-validate compares names, types, nullability and the set of `delta.constraints.*`. Any mismatch raises `DeltaSchemaMismatch`.
- **Source-frame validation stays loose.** It compares names and types only, because Spark-derived nullability is not authoritative. Delta enforces NOT NULL and CHECK at write time.
- **Nullability scope.** NOT NULL applies to top-level columns. Delta cannot enforce NOT NULL on array elements or nested struct fields, so those stay nullable; stored nested nullability is still compared structurally. No lambda `forall` CHECK is used, because Delta stores constraints as re-rendered tokens and corrupts lambda arrows.
- **Constraints at creation.** The table is created with its columns and properties, and each CHECK is added by `ALTER TABLE ... ADD CONSTRAINT` right after creation, while the table is empty. A crash between the two commits leaves an empty table without constraints; create-or-validate completes exactly that case (columns equal, stored constraints a subset of the declared ones, no data files). Any other mismatch, including a missing constraint on a table that holds data, fails closed with no repair.
- **Stats columns.** `statsColumns` is reserved for S4. The field exists in `DeltaTableSpec` and is not applied yet.
- **Gold writes keep the declared schema.** They use `overwriteSchema` false, and the target is created or validated from its spec first.
- **Gold days are dates.** Gold `day` becomes `DateType`. The report boundary converts it to the UTC start-of-day `Instant`, so the Mongo and GraphQL contracts are unchanged by S1.
- **Typed decoding.** Gold report extraction uses typed row decoders. A null or out-of-type cell fails as `InvalidGoldSchema`.

### S2 — Parse, validate and tokenize once

- **One parsed frame.** The persisted parsed frame carries `isValidEvent`, `eventFingerprint`, `subjectToken` and `subjectTokens`, each computed once.
- **Reuse.** Bronze, quarantine, conflict detection, Silver and counts reuse those columns.
- **Parsing.** JSON is parsed once into a VARIANT and reused wherever that is semantically equivalent. Acceptance behavior is unchanged.
- **Erasure-marker exclusion uses equality joins.** Tokens are exploded and a `left_semi` join finds the marked keys; a `left_anti` join then removes those rows by Kafka coordinate (event ID as fallback).
- **Tokenization is a deterministic UDF.** It builds one HMAC per task. Key material never appears in plans, logs or Delta.

### S3 — Application stage times and hiring stage durations

Split into S3a (analytics build) and S3b (application build). The Mongo report document shape below is the contract between them. Architect review (2026-10-28) shaped this section. The working name "application lifecycle snapshot" became "application stage times": "snapshot" collides with the Mongo report snapshot, and `ApplicationLifecycle` is the operational transition policy.

**`silver/application_stage_times` (S3a)**

| Aspect | Rule |
|---|---|
| Grain | One row per `applicationId`. The merge source is pre-aggregated per application, because a Delta MERGE fails on duplicate source matches. |
| Columns | `applicationId`, `subjectToken` (the candidate token, pinned per application), `subjectTokens` (union of candidate and actor tokens), `createdAt`, `acceptedAt`, `declinedAt`, `interviewAt`, `hiredAt`, `rejectedAt`, `lastEventAt`, `terminalAt`, `expiresAt` |
| Source | `APPLICATION_CREATED` sets `createdAt`. `APPLICATION_STATUS_CHANGED` sets the stage named by `newStatus`. `CANDIDATE_HIRED` is excluded, as in the funnel. |
| Merge semantics | Each stage keeps its earliest time (`least`, ignoring nulls). `lastEventAt` takes the latest (`greatest`). `subjectTokens` takes the union. `terminalAt` and `expiresAt` are never merged on their own; they are recomputed from the merged row. Matched rows that would not change are not rewritten. |
| Terminal and expiry (D1) | `terminalAt = least(declinedAt, hiredAt, rejectedAt)`. `expiresAt = terminalAt + terminal retention` (default 30 d); with no terminal stage, `expiresAt = lastEventAt + open retention` (default 180 d). Both retentions are typed configuration next to `silverDays`. |
| Out-of-order delivery | The status transition matrix has no cycles, so an apparent regression can only be out-of-order delivery. Earliest-wins plus recomputation absorbs it. An earlier duplicate terminal event moves `terminalAt`, and therefore `expiresAt`, earlier; that is accepted. There is no CHECK limiting terminal stages to one, because a producer defect would then block the whole batch. |
| Write paths | It is fed from the same frame as Silver, at the two places Silver is merged: `AnalyticsBatchSilverStage.mergeSilver` (batch and streaming) and late-fact replay `mergeSelectedFacts`, under the same freshness guard. Late facts that are still held are not merged. |
| Ordering | Stage times are merged before Silver. A crash between the two commits leaves Silver as a subset of the stage times, and the idempotent replay repairs it. |
| Recovery | Silver holds 30 days, so this table is the only record of older applications and cannot be rebuilt from Silver. Pre-MVP this is accepted under D4 (reset) and fail-closed. Only when the table does not exist (first initialization) and Silver exists, it is backfilled from Silver with the same merge, leaving out every application that involves an active deletion marker. An existing table, even an empty one, is never backfilled, and no erasure purge backfills it, because rebuilding from the Silver events that survive a purge would recreate partial rows. After MVP, losing it permanently loses durations older than 30 days; that is a recorded risk. |
| Classification | Subject-attributed. Catalog registration covers the inventory, erasure purge and evidence, expiry, vacuum rewrite and key retirement. These hard-coded lists must also include it: `AnalyticsKeyContinuityStage` (token validation and stored-token checks), `AnalyticsGoldStage.clear` and rebuild, `HiringAnalyticsBatch` and `AnalyticsPublication`. Because erasure deletes any row whose `subjectTokens` contains an erased token, an erased recruiter also removes the applications that recruiter acted on, as in Silver. |
| Privacy and cost | It holds per-application stage times until 30 days after a terminal status, or 180 days after the last lifecycle event of an open application (defaults; each configurable from 1 to 3650 days), against 30 days for Silver. Age from creation is therefore not bounded by a fixed figure. This needs Security Engineer signoff. The vacuum rewrite covers the whole table; at local scale this is bounded and not measured. |

**Gold outputs (S3a)**

| Output | Rule |
|---|---|
| `gold/time_to_hire` | Computed from stage-time rows with `expiresAt > asOf`. Shape, `excludedCount` and suppression are unchanged. It shares the duration computation with the new table. |
| New `gold/hiring_stage_durations` | One row per metric: `metric`, P50/P75/P90/P95 hours, and `eligibleCount` as distinct candidate subjects. A metric is omitted when `eligibleCount < 10`. Only rows where both stages are present and the duration is non-negative count. An application that skipped a stage (e.g. Created → Rejected) contributes nothing to metrics involving that stage. |
| Rebuild gate | Each Gold family follows its own source: time to hire and hiring stage durations rebuild whenever the stage-times table exists, even if Silver is absent, and are cleared when the stage-times table is absent; funnel and skills Gold follow Silver the same way. |

Metrics, named after `ApplicationStatus` values (a "stage" is the time an application first entered a status):

- `TIME_TO_ACCEPT` (created → accepted)
- `TIME_TO_INTERVIEW` (created → interview)
- `TIME_TO_HIRE` (created → hired)
- `ACCEPTED_TO_INTERVIEW` (accepted → interview)
- `INTERVIEW_TO_HIRE` (interview → hired)

**Report contract (S3a writes, S3b reads)**

- The Mongo report document gains a required `stageDurations` array: `[{ metric: "TIME_TO_HIRE", p50Hours: double, p75Hours: double, p90Hours: double, p95Hours: double, eligibleCount: long }]`. An empty array is valid.
- Verified 2026-10-28: there is no strict validator on `analytics_report_snapshots` (`MongoHiringValidators` covers the event outbox, users and jobs), so no operational Mongo migration is needed. Its index entries are unchanged.
- The shared fixture `test-support/.../analytics-report-snapshot-contract.json` is updated once and used by both builds.
- Deploy order (pre-MVP, single active shape): a published document without `stageDurations` is unreadable and fails closed with the sanitized repository error `UNAVAILABLE` (the same as any other malformed snapshot field) until the next analytics publish. `ANALYTICS_UNAVAILABLE` stays reserved for "no snapshot published".
- Admin GraphQL `AnalyticsReport` gains `stageDurations: [AnalyticsHiringStageDuration!]!`, next to `timeToHire`.
  - `AnalyticsHiringStageDuration { metric: AnalyticsHiringStageMetric!, p50Hours: Float!, p75Hours: Float!, p90Hours: Float!, p95Hours: Float!, eligibleCount: Long! }`.
  - `enum AnalyticsHiringStageMetric` lists the five metrics.
  - It is admin-only through `AnalyticsReportingService.report` and is not period-filtered, like `timeToHire`.
  - The SDL golden file, operation fixtures and `docs/api.md` are updated.

### S4 — Privacy-safe file skipping and compaction

- **Stats columns.** Subject-attributed tables replace `delta.dataSkippingNumIndexedCols=0` with `delta.dataSkippingStatsColumns`, which lists only non-subject columns. Kafka coordinates, `occurredAt`, `ingestedAt` and `expiresAt` are allowed; token columns and raw identifiers are not. Raw application identifiers (`applicationId`, the same value as `aggregateId`) are forbidden too, so the stage-times allow-list is `lastEventAt`, `terminalAt` and `expiresAt`.
- **Fail closed.** Initialization refuses a table whose properties would gather stats on a token column. A stats-columns property that the Delta log shows is ignored counts as a failure, and the `delta.dataSkippingNumIndexedCols` fallback in the assumption above then applies.
- **No stale stats.** Stats properties apply to future writes only. Tables are created with the final properties after the D4 reset, so no file written under the old property survives.
- **Compaction.** The streaming maintenance tick runs `OPTIMIZE` per table once its configured interval has elapsed, under the lakehouse mutex. It runs before vacuum eligibility, never while an erasure purge for that table is in progress, and is disabled when the interval is zero. `OPTIMIZE` is idempotent and uses snapshot isolation, so concurrent readers are safe. The files it replaces still hold subject tokens until VACUUM, so the erasure data-file horizon is measured from the later of the purge and the last `OPTIMIZE` that replaced files holding the erased subject's rows; AC-04.4 tests this.
- **Manual OPTIMIZE (decision 2026-10-31).** The scheduled tick never compacts while an erasure request is Pending or Processing. An operator-run `OPTIMIZE` after a purge is still seen by the erasure worker and extends the data-file horizon (it never shortens it), but the completion budget in `ErasureCompletionRule` does not model it, so completion can exceed the budget. No application gate is added because the action is out-of-band; the runbook rule is: do not run a manual `OPTIMIZE` on a subject-attributed table while an erasure request is open.
- **Alternatives rejected.** Auto compaction and optimized write run synchronously inside the writer, outside the mutex and the erasure ordering. Z-ORDER and liquid clustering are non-goals.

### W — Analytics worker startup resilience

An analytics process whose MongoDB is unreachable must not crash-loop. Observed 2026-10-26: an orphaned erasure worker from an ad-hoc Compose project restarted 182 times. Its `mongodb` host no longer resolved, each start built a Spark session before failing on Mongo server selection, the driver logged a stack trace every second, and `restart: unless-stopped` restarted it indefinitely.

- Mongo is checked (DNS resolution and server selection) before the Spark session is created, for every analytics command.
- Long-running workers (erasure worker, streaming) wait for Mongo in-process with capped exponential backoff. Each attempt logs one sanitized WARN line and no stack trace. They continue normally once Mongo is reachable, and exit non-zero with one sanitized ERROR line after a configured maximum wait.
- One-shot commands fail fast with one sanitized line.
- Compose analytics services use a bounded restart policy (`on-failure:N`), not `unless-stopped`.
- Mongo driver cluster-monitor logging is WARN or higher in the analytics image.
- The README explains how to remove leftover ad-hoc Compose projects.

### T — Analytics test runtime

The analytics unit suite takes about 58 minutes on the local workstation (S3a run, 2026-10-28), mostly fixed Spark/Delta overhead on tiny data. The slowest suites:

| Suite | Tests | Wall time |
|---|---|---|
| `AnalyticsTransformsSpec` | 66 | 21.5 min, a full batch per test |
| `AnalyticsDeltaCatalogSpec` | 17 | 8.2 min |
| `DeltaStreamingBatchJournalSpec` | 9 | 7.0 min |
| `ApplicationStageTimesSpec` | 19 | 5.0 min |
| `HiringAnalyticsWorkloadSpec` | 1 | 2.9 min, a 100,000-record benchmark |

Production behavior does not change; only tests and build test settings do.

- Suites that build a lakehouse create the empty, constrained tables once per suite and copy that directory per test, instead of recreating 11 tables and their constraint commits.
- `HiringAnalyticsWorkloadSpec` becomes opt-in through an environment variable, like the timing spec.
- One shared, tuned test Spark session replaces ad hoc builders where the suite allows: shuffle partitions 1 to 2, UI off, and Delta snapshot and checkpoint settings chosen for tiny tables. Production-semantic settings (the vacuum retention key, UTC) stay identical.
- Suites run in a few forked JVM groups (`testGrouping`) instead of one serial JVM. Suites that share global Spark or Delta state stay in one group.
- Target: the unit suite runs in under 20 minutes on this workstation, with the same test count and no weakened assertion. It is measured before and after on the same machine.

**Implementation (2026-10-29).** Test code and `analytics/build.sbt` test settings only; `src/main` is unchanged.

- `AnalyticsTestSpark` builds every standard unit-test session: `local[2]`, UI and console progress off, loopback driver address (removes the host-network-address `RemoteClassLoaderError` flake), 2 shuffle partitions, UTC, `retentionWindowIgnore.enabled=false`, Delta extension and catalog, and `spark.databricks.delta.snapshotPartitions=2` (Delta default 50: every snapshot reconstruction was a 50-partition job). 27 builders migrated. Left alone: `AnalyticsBatchResourceSpec`, `SparkDriverExecutionSpec`, `SparkShutdownDrainSpec` (`local[1]`, they test cancellation and drains) and `HiringAnalyticsWorkloadSpec` (`local[4]`). Not changed: adaptive execution, stats collection, checkpoint interval and log retention, because tests assert plans, statistics, checkpoints and log cleanup; Delta 4.4.1 applies `snapshotPartitions` to state reconstruction only.
- `AnalyticsLakehouseTemplate` builds an initialised lakehouse once per suite by running the production initialisation (`validateHmacConfiguration` then `configureRawTables`) and copies the directory (attributes kept) per test. Delta logs use relative paths, so a copy is an independent table. Used by `AnalyticsTransformsSpec` (13 batch tests) and `ApplicationStageTimesSpec` (10 tests, stage-times table only). Tests that assert creation, repair, absence of tables, the key registry or seed tables before the first batch keep fresh directories: the 12 key-continuity, marker-before-registry, offset-range and `AEC-09.x` tests, plus the stale-guard, backfill, initialisation and Gold-check tests. The constraint commits during creation were not touched.
- `HiringAnalyticsWorkloadSpec` runs only with `HIRING_WORKLOAD_RECORDS=100000` (the size its counts are written for): `cd analytics && HIRING_WORKLOAD_RECORDS=100000 sbt "testOnly *HiringAnalyticsWorkloadSpec"`. Otherwise it is skipped by `assume`, so the test count is unchanged.
- `Test / testGrouping` makes every suite its own group (its own forked JVM), longest known suite first, with `Tags.limit(ForkedTestGroup, 4)` and `Test / parallelExecution := true` (sbt runs forked groups concurrently only then; `testForkedParallel` stays off, so nothing runs concurrently inside a JVM). Suites that touch global Spark or Delta state (`SparkShutdownDrainSpec`, `SparkDriverExecutionSpec`, `AnalyticsBatchResourceSpec`) therefore never share a JVM with anything. `HiringAnalyticsRetentionClock` is used by integration specs only; `IntegrationTest` grouping is unchanged. JaCoCo: all JVMs append to one `jacoco.exec` (2.9 MB after the run).

Per-suite wall time (seconds; "after" is the full parallel run of 2026-10-29 on a host that another session was loading at load average 10 to 13, so it is an upper bound; "alone" is the same code run one JVM at a time):

| Suite | Before (S3a, 2026-10-28) | After, alone | After, 4 JVMs under load |
|---|---|---|---|
| `AnalyticsTransformsSpec` (66) | 1289 | 845 | 1493 |
| `AnalyticsDeltaCatalogSpec` (17) | 489 | 131 | 275 |
| `DeltaStreamingBatchJournalSpec` (9) | 418 | 123 | 283 |
| `ApplicationStageTimesSpec` (21 after S3a) | 298 | 217 | 389 |
| `HiringAnalyticsLateFactReplayStagesSpec` (5) | 203 | 112 | 138 |
| `HiringAnalyticsWorkloadSpec` (1) | 176 | skipped | skipped |
| `DeltaVacuumEligibilitySpec` (9) | 89 | 169 | 260 |
| `HiringAnalyticsLateFactsSpec` (5) | 83 | 48 | 66 |
| `StreamingOpenSilverSelectionSpec` (3) | 74 (invalid: the suite failed early in that run); about 830 alone on the old configuration | 895 | 1267, timed out |

`StreamingOpenSilverSelectionSpec` is a trial-based benchmark (12 timed runs). The Code Reviewer measured it alone at load about 4 on 16 cores: 833.5 s with the pre-T spec file and 895.5 s with T, a difference within noise. T did not slow it down. The 74 s baseline came from the S3a run in which this suite failed early on the `array_union` defect. The timed sections take about 140 s, so most of its roughly 14 minutes is untimed setup. Its 12-minute munit timeout is below its real solo runtime, which explains the timeout under 4-way load. It is the critical path for AC-08 and needs a faster fixture or an explicit benchmark gate (follow-up). `DeltaVacuumEligibilitySpec` is also slower than its baseline, again under load.

### S6 — Documentation and admission expression

- **Bronze description.** The wiki's Bronze description matches the code, and states that Bronze keeps no raw envelope, so it departs from the raw-and-immutable Bronze convention in the Medallion literature, for erasure and privacy reasons.
- **Admission expression.** Decided 2026-10-31: the Scala UDF is replaced by the built-in expression `EventTimeAdmissionExpression.classify` (epoch-microsecond `Long` arithmetic, UTC days by `pmod`, no session time zone), proven equal to `AnalyticsEventTimePolicy.admit` by `EventTimeAdmissionExpressionSpec`. A null event time stays `FUTURE`. The policy object remains the source of truth for the constants and the other callers; the spec is the guard against the two drifting.
- **Canonical docs.** `docs/big-data-architecture.md`, the wiki storage page and `docs/domain-language.md` reflect S1–S4.

## Domain language and examples

- Context: Analytics. Aggregates touched: Application lifecycle (read-only projection). Invariants: K=10 suppression, erasure exclusion, retention expiry.
- New glossary terms:
  - **Application stage times:** the pseudonymized per-application earliest time the application entered each status.
  - **Hiring stage duration:** a suppressed percentile distribution of time between two lifecycle stages.

| Rule | Tag | Scenario |
|---|---|---|
| Long hires count | AC-03.1 | Given application A created on day 0 and hired on day 45, with Silver retention of 30 days, when Gold rebuilds, then A contributes 1080 hours to `TIME_TO_HIRE` and to `timeToHire`. |
| Stage-times merge is idempotent | AC-03.2 | Given Interview on day 3 already merged, when the same event is replayed and an earlier duplicate Interview on day 2 arrives, then `interviewAt` is day 2 and only one row exists for A. |
| Terminal expiry | AC-03.3 | Given A rejected on day 10, then `expiresAt` is day 40. Given B open with its last event on day 5, then `expiresAt` is day 185. |
| Erasure covers stage times | AC-03.4 | Given an erasure marker for A's candidate, when the purge runs, then no stage-times row carries that subject token. Given a marker for the recruiter who accepted A, A's row is also removed. |
| Crash between commits | AC-03.6 | Given stage times merged and the process stopped before the Silver merge, when the batch replays, then Silver and stage times agree and no row is duplicated. |
| Skipped stage | AC-03.7 | Given A goes Created → Rejected, then A contributes to no duration metric and appears in `excludedCount` for time to hire. |
| Backfill | AC-03.8 | Given Silver holds A's events and the stage-times table does not exist, when `configureRawTables` runs at start, then A's row is backfilled from Silver before Gold rebuilds, unless an active deletion marker names a subject of A. Given the table exists but is empty, nothing is backfilled. |
| Key retirement | AC-03.9 | Given a stage-times row tokenized with an HMAC key, then key continuity and retirement treat that key as in use until the row expires or is erased. |
| Suppression | AC-03.5 | Given 9 eligible subjects for `INTERVIEW_TO_HIRE`, then that metric is absent from Gold, the Mongo snapshot and GraphQL. Given 10, it is present. |
| Constraint enforced | AC-01.2 | Given a Silver row with `eventType = 'BOGUS'`, when it is written, then Delta rejects it with an invariant violation. |
| Strict target | AC-01.1 | Given an existing Silver table whose `eventId` is nullable, when the batch starts, then it fails with `DeltaSchemaMismatch`. |
| Tokenize once | AC-02.2 | Given a batch of N parsed records, then the HMAC UDF is evaluated at most once per record per action. A counter test proves it. |
| Compaction does not shorten erasure | AC-04.4 | Given an erasure purge for A, then an `OPTIMIZE` that replaces a file once holding A's rows, when vacuum eligibility is computed, then the horizon runs from the `OPTIMIZE`. No eligible file is removed earlier. |
| No token stats | AC-04.1 | Given any subject-attributed table, then `_delta_log` add-action stats contain no `subjectToken` or `subjectTokens` keys. |

## Acceptance and evidence

| ID | Criterion | Evidence (filled when observed) |
|---|---|---|
| AC-05 | Analytics builds and every unit and integration suite passes on Spark 4.2.0 / Delta 4.4.1, and the container image starts. | Partly observed 2026-10-26. `scalafmtCheckAll scalafmtSbtCheck` pass. `scripts/run-local-tests.sh analytics`: 500/500 unit tests pass; 108 of 109 integration tests pass (4 ignored) with 1 failure, `MongoActiveDeletionMarkerIntegrationSpec`, a pre-existing hard-coded `expiresAt` of 2026-10-25 that expired (no Spark involvement); after changing it to 2099-01-01 that spec passes (12/12 in the follow-up run). The user built the image (`docker build -f analytics/Dockerfile -t hiring-analytics:spark42-s5 .`, 2026-10-26; dependency resolution and `stageContainerRuntime` succeeded). A runtime start against Mongo/Kafka is not verified yet; final QA covers it. Review fixes: the Delta retention-window key is set to false in every test session that can run VACUUM, `DeltaVacuumEligibility.canSkip` fails closed when it is true, `SparkRuntime` has a unit test for it, the workload report reads the Delta version at runtime, and the marker test date moved to 2099 (unit 501/501, `AnalyticsKeyRetirementIntegrationSpec` 6/6). Container start observed 2026-11-01 (user-run; agent `docker run` is denied by project permissions): image `hiring-analytics:local` built from HEAD `b525de7`; `docker compose --profile analytics-erasure up -d analytics-erasure-worker` against the compose Mongo (rs0) and SASL Kafka gave `running=true restarts=0`, Kafka SASL login, Mongo check passed, Spark 4.2.0 session started, and `analytics erasure worker starting: worstCaseCompletion=15 days erasureCompletionBudget=21 days`. The first attempt on a fresh Mongo exited with `analytics configuration is invalid` (5 bounded restarts, then stopped) until the API had run once to create the control collections, as the README requires. Findings: the CLI hides which configuration problem failed (fixed internal strings are not logged; follow-up), and README's claim that `.local/data/kafka` holds preserved KRaft data was false on this host (Docker created the directory as root; `chown 1000:1000` fixed it). |
| AC-01 | One catalog defines every table. Targets are validated strictly (AC-01.1). Constraints are enforced (AC-01.2). Gold has no `overwriteSchema`, Gold `day` is a `DateType`, and extraction is typed (AC-01.3). | Observed 2026-10-27. `AnalyticsDeltaCatalogSpec` AC-01.1 to AC-01.3 (13 tests): strict target (nullable `eventId`, missing, extra and changed constraint), loose source, Delta rejects `eventType = 'BOGUS'`, null `eventId`, negative offset, negative Gold count and unordered percentiles, Gold `DATE` with UTC start-of-day extraction, drifted Gold table not overwritten, null cell gives `InvalidGoldSchema`. Unit suite 524 tests; after fixing fixtures that wrote tables directly, the affected specs pass. Integration: BatchKafkaRange, StreamingIngestion, MongoReportPublisher, LateFactReplayRecovery, ErasureRetention 28/28; KeyRetirement 6/6. Streaming recovery specs were disturbed by the host Wi-Fi address changing under a running Spark driver (see checkpoint). |
| AC-02 | Output is identical on existing fixtures (AC-02.1). Tokenization runs once (AC-02.2). Plans contain no nested-loop join for marker exclusion and no repeated JSON parse (AC-02.3). | Observed 2026-10-28. AC-02.1: the existing behavior suites pass unchanged (OperationalEventWireContractSpec, AnalyticsTransformsSpec, quarantine and streaming specs; unit 536/536) plus `OperationalEventParsedFrameSpec` (validity, SHA-256 fingerprint and HMAC tokens equal independent expectations; analyzed and inline selections equal). AC-02.2: counting tokenizer, 2 tokenizations per record for one action, none on later actions over the persisted frame, one tokenizer per task; Silver adds no tokenizer to a tokenized frame. AC-02.3: no BroadcastNestedLoopJoin or CartesianProduct with broadcast on and off; markers exclude actor, candidate and candidate-search-result positions. Raw-JSON minimum on rawValue: 2 get_json_object (`$.occurredAt`, `$.payload`), 1 json_object_keys (envelope key set), 1 try_parse_json; 8 get_json_object and 4 json_object_keys in total (was 16 and 11, evaluated about 4 times per batch); consumer plans hold none. Integration (7 specs): 32/32, 1 ignored. |
| AC-03 | S3a: AC-03.1 through AC-03.9 and the Mongo document carries `stageDurations` (shared fixture). S3b: the application reads it, GraphQL exposes `stageDurations`, and the SDL golden file, operation fixtures and `docs/api.md` are updated. | S3b done 2026-10-28: `AnalyticsReportSnapshotContractSpec`, `AnalyticsReportSnapshotDocumentSpec` (AC-03 round trip, empty array, 9 rejections), `AnalyticsReportingServiceSpec`, `HiringGraphQLAccessSpec` (AC-03 Admin, empty, unauthenticated), SDL golden file. S3a verified 2026-10-29: `sbt scalafmtCheckAll scalafmtSbtCheck test IntegrationTest/compile` passes, unit 568 passed, 1 skipped, 0 failed (569). AC-03.1 to AC-03.9 are covered by `ApplicationStageTimesSpec` (22 tests, including AC-03.6 through the real `mergeSilver` with a failing Silver writer, AC-03.8 through `configureRawTables`, marker exclusion in the backfill, and the below-K decode guard), `AnalyticsKeyContinuitySpec`, `AnalyticsRuntimeConfigSpec` and `HiringAnalyticsLateFactReplayStagesSpec`. Integration: `MongoAnalyticsReportPublisherIntegrationSpec` 9/9; `AnalyticsErasureRetentionIntegrationSpec` 6/6, `HiringAnalyticsLateFactReplayRecoveryIntegrationSpec` 7/7, `AnalyticsKeyRetirementIntegrationSpec` 6/6, `HiringAnalyticsBatchKafkaRangeIntegrationSpec` 3/3 and `HiringAnalyticsStreamingIngestionIntegrationSpec` 4/4 on the rerun. The first run of those five failed 13 tests, all `RemoteClassLoaderError` after the host address changed from 192.168.1.104 to 192.168.1.105 under the Spark driver (known flake; no failure of any other kind). |
| AC-04 | No token stats (AC-04.1). The merge scans fewer files, with a local before/after count (AC-04.2). The maintenance tick compacts and records file counts before and after (AC-04.3). Compaction does not shorten the erasure horizon (AC-04.4). | Observed 2026-10-30/31 (verification pass). AC-04.1: `DeltaFileSkippingSpec` reads every add action's `stats` in `_delta_log` for all 5 subject-attributed tables, after 3 appends each: stats keys are exactly the declared allow-list, no `subjectToken(s)`, `aggregateId`, `jobId` or `applicationId` key and no `test_` token text; this proves Delta 4.4.1 OSS honors `delta.dataSkippingStatsColumns` on path tables (the same spec also refuses a table with the positional property or a token column). `DeltaTableCompactionSpec` re-checks the log after OPTIMIZE (still has `minValues`, no token). AC-04.2 (local, Bronze, 8 files of 100 rows, 5-row source, files-read metric from the Spark status store): unbounded condition reads 8 files with stats on and 8 with stats off; coordinate-bounded condition reads 1 file with stats on and 8 with stats off; the production writer reads at most 2 of 8 and stays idempotent (800 rows after duplicates, 805 after new offsets applied twice); an executed MERGE over topics `it's`, `its`, `back\\slash`, `it\\'s`, `plain` on 2 partitions inserts each new coordinate once. AC-04.3: `DeltaTableCompactionSpec` 6 files to 1 (Bronze) and 4 to 1 (Silver) with a `DELTA_COMPACTION ... filesBefore=6 filesAfter=1` log line; IT tick: Silver 8 to 1, Bronze 4 to 1, contents unchanged, not repeated before the interval, not run while an erasure request is Pending, and a failing OPTIMIZE (read-only table directory) logs `DELTA_COMPACTION_FAILED` and the tick continues. AC-04.4: `DeltaTableCompactionSpec` (OPTIMIZE after a purge moves the horizon to the OPTIMIZE; a committed OPTIMIZE whose completion was not recorded is still found) and `ErasureCompletionRuleSpec`, `AnalyticsErasureWorkerSpec`. Unit specs: S4 and modified specs 136/136 before the review fixes; after them the affected specs (`DeltaFileSkippingSpec`, `DeltaTableCompactionSpec`, `AnalyticsTableInitializationSpec`, `AnalyticsDeltaPrivacyPropertiesSpec`, `ErasureCompletionRuleSpec`, `AnalyticsErasureWorkerSpec`, `AnalyticsStreamingSettingsSpec`, `AnalyticsRuntimeConfigSpec`, `ApplicationStageTimesSpec`) 104 passed, 2 failed (a test written for the old shape and the new Column-literal assertion, both fixed), then `DeltaFileSkippingSpec` + `ApplicationStageTimesSpec` 29/29. Integration (after the review fixes): Maintenance 7/7 (rerun alone after the last test change; the first combined run failed only the new failure-isolation test, whose fault injection was a read-only Delta log that broke an earlier tick step, so it now uses a read-only table directory), StorageBounds 3/3, AnalyticsErasureRetention 6/6, BatchKafkaRange 3/3, LateFactReplayRecovery 7/7. See the S4 bullet under Checkpoint and review for what is unverified. |
| AC-07 | Given the erasure worker starts while `mongodb` does not resolve, then no Spark session is created, it logs one WARN line per backoff attempt, and it exits non-zero after the configured maximum wait. When Mongo becomes reachable within the wait, it proceeds normally. One-shot commands fail fast. Compose restarts are bounded. | Observed 2026-10-26. `MongoStartupResilienceSpec` AC-07.1 to AC-07.10 pass (10/10; virtual-time backoff 1, 2, 4 s capped, give-up after the maximum wait with 3 warnings and one error, one-shot fail-fast, `managedSparkMongo` never acquires Spark, sanitized text, DNS versus server-selection, invalid URI not retried, real driver ping bounded against a closed port, defaults and `ANALYTICS_MONGO_STARTUP_*` overrides and validation). `scalafmtCheckAll scalafmtSbtCheck` pass; full `sbt test` 510 tests, 509 passed on the first run, the one failure (a log-text assertion in AC-07.1) was fixed and the affected specs rerun green (57/57). Compose uses `restart: on-failure:5` for `analytics-erasure-worker` (the only analytics service with a restart policy); `log4j2.properties` sets `org.mongodb.driver.cluster` to WARN. Review fixes (2026-10-26): the three `src/it` callers of `sparkMongo` pass `IO.unit`; DNS runs on the blocking pool; the README wording is exact (backoff sleep sums to `max-wait`, each attempt adds up to twice `check-timeout`). `log4j2.properties` keeps plain-text console logging explicitly, because with a log4j2 configuration on the classpath Spark skips its own defaults and `spark.log.structuredLogging.enabled` could select the JSON layout (I could not read its default from the jar; the integration run logged plain-text lines in the `yy/MM/dd` format). After the fixes: `scalafmtCheckAll scalafmtSbtCheck test IntegrationTest/compile` all pass (511/511 unit, clean IT compile); `scripts/run-local-tests.sh analytics` on the Ingestion, StorageBounds, Recovery and ErasureRetention integration specs: 23/23 pass. Compose and the container image were not run (project hook). |
| AC-08 | The analytics unit suite keeps its test count and assertions. The workload benchmark is opt-in. The suite runs in under 20 minutes locally, measured before and after on the same machine. | Partly met (2026-10-29). Test count unchanged (569 in the run: the workload spec is skipped through `assume`); no assertion changed. Coverage data is still produced by the parallel JVMs. Not met: wall time. Sequential before 58 min; with the changes, sequential 53 min (host loaded), and parallel with 4 JVMs 33.5 min with 2 failures that are load timeouts in `StreamingOpenSilverSelectionSpec` (fail: 721 s timeout, then no session). The critical path is `AnalyticsTransformsSpec` at 845 s alone. The under-20-minute target needs a quiet host (another session was running CPU-heavy tests, load average 10 to 13) and probably splitting `AnalyticsTransformsSpec`; unproven. Per-suite tables are in section T. Owner decision (2026-11-01): accepted as met for this refactoring at 20 min 23 s (final QA, full `sbt test` 595 tests, host load peaking about 10); no further timing work now. Revisit only if the unit-suite wall time rises to an uncomfortable level again. |
| AC-06 | Docs match the code, and the admission UDF is either replaced with an equivalence property test or its retention is justified. | Admission part observed 2026-10-31: `EventTimeAdmissionExpressionSpec` 4/4 (generated plus boundary event times across 6 observed instants and 8 watermarks each, more than 5000 comparisons: exactly at watermark and day end, at the 5-minute skew limit, one microsecond either side, nanosecond-precision observed instant and watermark, null time, epoch, years 0002 and 9999; time-zone independence). A mutation (`<=` to `<` on the day-end test) fails the spec. Physical plan: before `*(1) Project [UDF(t) AS a]`, after one `*(1) Project [struct(status, CASE WHEN unix_micros(t) > ... THEN FUTURE WHEN ... THEN CLOSED ELSE OPEN END, effectiveTime, CASE ... timestamp_micros(least(...)) END)]`. The UDF did not block whole-stage codegen; the gain is that Catalyst sees the expression (no Row/Timestamp conversion per row, no opaque function). No pushdown difference: the admission is derived on in-memory frames. Docs part (2026-10-31): `docs/big-data-architecture.md`, `docs/wiki/mongodb-and-analytics.md` and `docs/domain-language.md` updated for S1 to S4 and the admission expression, each claim checked against `analytics/src/main`; the wiki states Bronze keeps no raw envelope, departing from the raw-and-immutable Medallion convention for erasure and privacy; seven spec/code mismatches found and resolved (S2 marker joins, S3 retention and rebuild-gate rows, manual OPTIMIZE decision under S4, `optimize-interval` comment). |

## Implementation handoff

- Order: S5 → W → S1 → S2 → S3 → T → S4 → S6. One owner per slice, and each slice is committed only after its review gates pass.
- Write paths:
  - S1, S2, S4 and S5: `analytics/` and analytics tests.
  - S3b (first, contract owner): `test-support/` shared report fixture, `src/main`, `src/test` and `src/it` for the report read model and GraphQL, `src/test/resources/graphql/`, operation fixtures, `docs/api.md`. No Mongo migration.
  - S3a: `analytics/` and `docs/domain-language.md`. It consumes the fixture S3b defines.
  - Docs belong to the slice that changes the behavior.
- Gates:
  - Code Reviewer for every slice.
  - Security Engineer for S2 (HMAC and marker exclusion), S3 (new subject-attributed table, erasure and API) and S4 (log stats, OPTIMIZE alongside erasure).
  - Final independent QA on the final state.

## Checkpoint and review

- Committed: S5 (`a6369d6`), W.
- S5: done. Code Reviewer PASS (2026-10-26); findings M1 (test sessions now pin the vacuum key, and a test asserts the `SparkRuntime` setting), L1, L2 and L3 (`DeltaVacuumEligibility` fails closed when the key is true) fixed; `sbt test` 501/501 and `AnalyticsKeyRetirementIntegrationSpec` 6/6 after the fixes. Delta 4.4 defaults `spark.databricks.delta.vacuum.retentionWindowIgnore.enabled` to true; `SparkRuntime` pins it to false to keep the explicit VACUUM retention check.
- W: done (2026-10-26). Security Engineer PASS; Code Reviewer FAIL (integration sources did not compile), fixed, then PASS; unit 511/511 and the four affected integration specs 23/23. `MongoStartupCheck` (adapter/mongo) runs DNS plus a bounded server-selection ping before the Spark session in `SparkRuntime.managedSparkMongo`; the erasure worker and streaming use `Mode.AwaitReachable` (cats-retry capped exponential backoff, `limitRetriesByCumulativeDelay` on `analytics.mongo.startup.max-wait`, one sanitized WARN per attempt, then `MongoUnreachable` with one ERROR line from the CLI), batch, late-fact replay, repair and audit use `Mode.FailFast`. Settings: `analytics.mongo.startup.{check-timeout,initial-backoff,max-backoff,max-wait}` (defaults 5 s, 1 s, 60 s, 10 min; `ANALYTICS_MONGO_STARTUP_*`). `max-wait` bounds total backoff sleeping, so the wall-clock wait can exceed it by up to one `check-timeout`.- Verification policy (2026-10-26): the full analytics suite takes about 3 h 13 min. Each slice runs formatting, unit tests and the integration specs it touches. The full suite runs once on the final state for QA.
- S1: committed `79e80a0` and follow-up `c28aaa8`, Code Reviewer PASS twice. The local lakehouse checkpoints under `.local/data/analytics/` were reset by the user (D4, 2026-10-27). `HiringAnalyticsStreamingRecoveryIntegrationSpec` has no clean run yet: the host network address changed under a running driver. Final QA reruns it.
- S2: Code Reviewer and Security Engineer PASS; committed as 82ec805. Follow-up (2026-10-28, uncommitted): review findings F1 to F5, I1, I2, L1 and L2 addressed (streaming fingerprint reuse, AC-02.1 validity and marker edge-case tests, tokenizer call under the lock, optimized-plan count, property-gated timing spec, one architecture sentence). Original S2 notes: Parsed frame (`OperationalEventTransforms.analyzed`) holds `isValidEvent`, `eventFingerprint`, `subjectToken(s)` computed once; deterministic UDF `SubjectTokenFunction` (tokenizer built lazily per task); marker exclusion is explode + left_semi + left_anti on the Kafka coordinate (event ID fallback). Local timing evidence only (20000 records, fixed fixture, local[2], one machine, `OperationalEventIngestionTimingSpec`): stage wall time before vs after parse 2.5 s vs 5.6 s (now includes validity and tokens), Bronze 42.4 vs 36.8 s, quarantine 24.4 vs 22.7 s, Silver 22.4 vs 28.4 s, total 91.7 vs 93.5 s; Delta MERGE dominates these stages, so end-to-end time is unchanged. Compute-only, four consumers without Delta writes: 15.0 s recomputed per consumer vs 3.8 s computed once including materialization.
- S3a: committed 2026-10-28 at the user's request, before its own verification finished. Code Reviewer PASS and Security Engineer PASS (both static). Verification (2026-10-29) found the committed code failing 37 unit tests; fixed in `321c2eb`: (1) `ApplicationStageTimes.merge` no longer casts `subjectTokens` and wraps the union and the source aggregate in `array_compact` (the cast failed under Spark 4.2); (2) `withoutMarkedApplications` restores the Silver column order after its USING left_anti join, which had moved `applicationId` first and failed `validateSilverSchema` (`InvalidSilverSchema`) in every backfill. Follow-up items done: the backfill runs only on first initialization with marker exclusion, the Gold CHECK `eligible_count_minimum`, the `decodeStageDuration` guard, the 3650-day retention cap, the budget line and the freshness-read comment were already in the commit; this follow-up added tests for the decode guard and marker exclusion through `configureRawTables`, and replaced the repeated-backfill history-count assertion with a zero-rows-rewritten assertion. Results: see the AC-03 cell.
- T: committed `92d9fbf`, Code Reviewer PASS.
- T follow-up (2026-10-30):
  - `AnalyticsTransformsSpec` is split into `AnalyticsEventTransformsSpec` (32 tests), `AnalyticsSubjectErasureSpec` (16), `AnalyticsBatchPublicationSpec` (10) and `AnalyticsHmacKeyContinuityTransformsSpec` (8), with shared `AnalyticsBatchSpecFixtures`. All 66 tests are kept.
  - `StreamingOpenSilverSelectionSpec` builds its 7,500-row seeded Silver once and copies it per trial. Its timed assertions are unchanged.
  - The author agent's run was killed by a host reboot, so the coordinator verified it: `scalafmtCheck Test/scalafmtCheck scalafmtSbtCheck test` passed with 569 tests, 0 failed (2 skipped are the opt-in benchmarks), in 30 min 16 s.
  - The load average rose from 3 to 15.6 during the run because another session started heavy work, so this is not a quiet-host measurement.
  - The new critical path is `AnalyticsSubjectErasureSpec` at 1138 s, then `AnalyticsBatchPublicationSpec` at 663 s and `StreamingOpenSilverSelectionSpec` at 639 s.
  - AC-08 stays partly met: under 20 minutes is not demonstrated on this shared host.
  - The integration format check fails on another session's uncommitted `src/it/.../hiringstory/` files. They are not part of this plan.
- S6: closed 2026-10-31. Admission UDF replaced by `EventTimeAdmissionExpression` with an equivalence property test; canonical docs synced to S1 to S4; Code Reviewer PASS (ran `EventTimeAdmissionExpressionSpec` 4/4; Lows: wiki link text fixed, plan `println` in the spec kept). Remaining for this refactoring: a clean `HiringAnalyticsStreamingRecoveryIntegrationSpec` run and final independent QA (full analytics suite, container start, AC-05, AC-08).
- S4: closed 2026-10-31. Code Reviewer PASS and Security Engineer PASS on the fixes (static re-reviews); final two Lows closed and rerun as below. Full analytics suite and streaming recovery are left to final QA.
- S4 verification (2026-10-30/31, analytics/): the committed `ddaa5df` S4 code was run for the first time. `sbt scalafmtCheckAll scalafmtSbtCheck` first failed on 3 files (`AnalyticsDeltaRetention`, `HiringStageDurationPrecisionSpec`, `StoryAnalyticsReportSuite`), reformatted, now passes. The S4 and touched unit specs passed 136/136 on the committed code (9 min 23 s, load average 1 to 3; local AC-04.2 count: 8 files unbounded, 1 file bounded with stats on, 8 with stats off). Review findings fixed: Code Reviewer M1 and Security F2 (`MergeKeyBounds.bounded` builds the condition with the Column API, no SQL string escaping; executed MERGE test over topics with quote and backslash, several partitions, idempotent; `boundedBy` test for the 257-range fallback, L3), L1 (`lastCompactionAt` is one metadata read per table: wall-clock `analytics.lastOptimizeCompletedAt` taken after the commit, plus `analytics.lastOptimizeCompletedVersion`, the table version the scheduled OPTIMIZE produced, so history is read only for later versions and any later manual OPTIMIZE is seen (no recorded or unparsable version reads the full history); a run marked `analytics.optimizePendingAt`, that is crashed between the OPTIMIZE commit and its record, falls back to the full history, so the horizon is never shorter), L2 (`DELTA_STATS_PROPERTY_REPAIRED table=...` WARN, comment corrected), Security F1 (`applicationId` removed from the stage-times allow-list and added to `DeltaStatsPolicy.ForbiddenColumns`; `ApplicationStageTimesSpec` AC-03.3 updated; **S6: `docs/big-data-architecture.md` still lists applicationId in the stage-times statistics, sync it**), F3 (compaction wrapped in `handleErrorWith` with a sanitized WARN of the error class; integration test with a read-only table directory). Note for AC-04.4: the horizon extension is defensive, because compaction cannot run while an erasure request is Pending or Processing. Unverified: the host clock and sleep made some integration wall times unreliable (reported durations of hours); no full `sbt test` was run. Closure of the final two Lows (2026-10-31): the version-bounded history read (test: scheduled compaction, 15 appends, bare manual OPTIMIZE, 15 appends, the manual time is returned) and the L2 WARN test (`AnalyticsDeltaPrivacyPropertiesSpec`: a drifted `delta.dataSkippingNumIndexedCols` logs one `DELTA_STATS_PROPERTY_REPAIRED table=bronze_operational_events` line without the value, none on a clean table or a second run). Final runs: `scalafmtCheckAll scalafmtSbtCheck` pass; `DeltaTableCompactionSpec`, `AnalyticsErasureWorkerSpec`, `ErasureCompletionRuleSpec`, `AnalyticsDeltaPrivacyPropertiesSpec` 30/30; `HiringAnalyticsStreamingStorageMaintenanceIntegrationSpec` 7/7. Host-address-change `RemoteClassLoaderError` did not occur.
- Next action (2026-10-30): the T follow-up (split of `AnalyticsTransformsSpec` into `AnalyticsEventTransformsSpec`, `AnalyticsBatchPublicationSpec`, `AnalyticsSubjectErasureSpec` and `AnalyticsHmacKeyContinuityTransformsSpec` over `AnalyticsBatchSpecFixtures`, a seeded Silver template in `StreamingOpenSilverSelectionSpec`, updated weights in `build.sbt`) is in the working tree, uncommitted, with `Test/compile` green; run the split suites, obtain the Code Reviewer verdict, commit, then S4. The remainder is also listed in [hiring platform refinements](hiring-platform-refinements.md#a-analytics-data-model-refactoring-remainder-mvp-condition-1).
- S3b: done 2026-10-28 (read model, GraphQL, SDL, shared fixture, `docs/api.md`). Code Reviewer PASS and Security Engineer PASS. `sbt test` 1680/0; focused reruns by the reviewers 123/0 and 26/0. Decision (coordinator): no K check on read. K=10 stays owned by the analytics publisher, as for `timeToHire`; the endpoint is singleton-Admin only, and Security rated read-side K defense-in-depth Low and optional. Deploy S3a with or before S3b: until S3a publishes `stageDurations`, the application treats the current report as unavailable. Low follow-ups: an array-size cap before decoding, a Candidate-role and non-Admin GraphQL test, and a repository-path test to `ANALYTICS_UNAVAILABLE`.
- S1: implemented (2026-10-27), review gates pending. `AnalyticsDeltaCatalog` declares all 11 Delta tables; Silver, Bronze and late facts are NOT NULL on keys, tokens and timestamps; constraints are derived from the domain enums. Lambda `forall` constraints are not used because Delta does not round-trip them. Existing lakehouses fail closed; reset per D4.
- S6 admission expression (2026-10-31): replaced the UDF in `SparkStreamingBatchStages.withEventTimeAdmission` with `EventTimeAdmissionExpression`; `application.conf` `optimize-interval` comment states the validation. Ran `scalafmtCheckAll scalafmtSbtCheck` pass; `EventTimeAdmissionExpressionSpec`, `AnalyticsEventTimeSpec`, `StreamingAdmissionQualitySpec`, `StreamingAssessmentQualitySpec`, `WatermarkCandidateSelectionSpec` 25/25. `HiringAnalyticsStreamingIngestionIntegrationSpec` 4/4 (1365 s).
- Final QA streaming fixes (2026-11-01, analytics/): (1) `HiringAnalyticsStreamingProcessSignalIntegrationSpec` HCS-07.9/.10 child JVM died on `IllegalAccessException ... sun.util.calendar.ZoneInfo` because `spawn` passed only one `--add-opens`; it now reads the opens from `spark-jvm.args`, and the failure message carries the first error lines plus the log tail. (2) `HiringAnalyticsStreamingRecoveryIntegrationSpec` HCS-02.4 was cost, not livelock: the shared recovery runtime used the default 50 Delta snapshot partitions (50-task stages per snapshot); it now sets `spark.databricks.delta.snapshotPartitions=2` as the unit sessions do; spec 11/11 in 1104 s (HCS-02.4 previously timed out at 901 s, the spec took 56 min). (3) `build.sbt` pins `spark.driver.host`/`bindAddress` 127.0.0.1 and `SPARK_LOCAL_IP` for Test and IntegrationTest JVMs and the spawned children, because the runtime sessions built by `SparkRuntime` (not `AnalyticsTestSpark`) did not. Signal spec 3/3 and Recovery spec 11/11 pass.
