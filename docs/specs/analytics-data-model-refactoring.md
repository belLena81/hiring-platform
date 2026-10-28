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
- **Erasure-marker exclusion is an equality anti-join.** Explode tokens, then `left_anti` on the token.
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
| Recovery | Silver holds 30 days, so this table is the only record of older applications and cannot be rebuilt from Silver. Pre-MVP this is accepted under D4 (reset) and fail-closed. If the table is missing or empty while Silver exists, it is backfilled from Silver with the same merge. After MVP, losing it permanently loses durations older than 30 days; that is a recorded risk. |
| Classification | Subject-attributed. Catalog registration covers the inventory, erasure purge and evidence, expiry, vacuum rewrite and key retirement. These hard-coded lists must also include it: `AnalyticsKeyContinuityStage` (token validation and stored-token checks), `AnalyticsGoldStage.clear` and rebuild, `HiringAnalyticsBatch` and `AnalyticsPublication`. Because erasure deletes any row whose `subjectTokens` contains an erased token, an erased recruiter also removes the applications that recruiter acted on, as in Silver. |
| Privacy and cost | It holds per-application stage times for up to 210 days, against 30 for Silver. This needs Security Engineer signoff. The vacuum rewrite covers a table at most 210 days deep; at local scale this is bounded and not measured. |

**Gold outputs (S3a)**

| Output | Rule |
|---|---|
| `gold/time_to_hire` | Computed from stage-time rows with `expiresAt > asOf`. Shape, `excludedCount` and suppression are unchanged. It shares the duration computation with the new table. |
| New `gold/hiring_stage_durations` | One row per metric: `metric`, P50/P75/P90/P95 hours, and `eligibleCount` as distinct candidate subjects. A metric is omitted when `eligibleCount < 10`. Only rows where both stages are present and the duration is non-negative count. An application that skipped a stage (e.g. Created → Rejected) contributes nothing to metrics involving that stage. |
| Rebuild gate | Gold durations rebuild whenever the stage-times table exists, even if Silver is absent. They are cleared only when both are absent. |

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
- Deploy order (pre-MVP, single active shape): a published document without `stageDurations` fails closed as reports unavailable until the next analytics publish.
- Admin GraphQL `AnalyticsReport` gains `stageDurations: [AnalyticsHiringStageDuration!]!`, next to `timeToHire`.
  - `AnalyticsHiringStageDuration { metric: AnalyticsHiringStageMetric!, p50Hours: Float!, p75Hours: Float!, p90Hours: Float!, p95Hours: Float!, eligibleCount: Long! }`.
  - `enum AnalyticsHiringStageMetric` lists the five metrics.
  - It is admin-only through `AnalyticsReportingService.report` and is not period-filtered, like `timeToHire`.
  - The SDL golden file, operation fixtures and `docs/api.md` are updated.

### S4 — Privacy-safe file skipping and compaction

- **Stats columns.** Subject-attributed tables replace `delta.dataSkippingNumIndexedCols=0` with `delta.dataSkippingStatsColumns`, which lists only non-subject columns. Kafka coordinates, `occurredAt`, `ingestedAt` and `expiresAt` are allowed; token columns and raw identifiers are not.
- **Fail closed.** Initialization refuses a table whose properties would gather stats on a token column. A stats-columns property that the Delta log shows is ignored counts as a failure, and the `delta.dataSkippingNumIndexedCols` fallback in the assumption above then applies.
- **No stale stats.** Stats properties apply to future writes only. Tables are created with the final properties after the D4 reset, so no file written under the old property survives.
- **Compaction.** The streaming maintenance tick runs `OPTIMIZE` per table once its configured interval has elapsed, under the lakehouse mutex. It runs before vacuum eligibility, never while an erasure purge for that table is in progress, and is disabled when the interval is zero. `OPTIMIZE` is idempotent and uses snapshot isolation, so concurrent readers are safe. The files it replaces still hold subject tokens until VACUUM, so the erasure data-file horizon is measured from the later of the purge and the last `OPTIMIZE` that replaced files holding the erased subject's rows; AC-04.4 tests this.
- **Alternatives rejected.** Auto compaction and optimized write run synchronously inside the writer, outside the mutex and the erasure ordering. Z-ORDER and liquid clustering are non-goals.

### W — Analytics worker startup resilience

An analytics process whose MongoDB is unreachable must not crash-loop. Observed 2026-10-26: an orphaned erasure worker from an ad-hoc Compose project restarted 182 times. Its `mongodb` host no longer resolved, each start built a Spark session before failing on Mongo server selection, the driver logged a stack trace every second, and `restart: unless-stopped` restarted it indefinitely.

- Mongo is checked (DNS resolution and server selection) before the Spark session is created, for every analytics command.
- Long-running workers (erasure worker, streaming) wait for Mongo in-process with capped exponential backoff. Each attempt logs one sanitized WARN line and no stack trace. They continue normally once Mongo is reachable, and exit non-zero with one sanitized ERROR line after a configured maximum wait.
- One-shot commands fail fast with one sanitized line.
- Compose analytics services use a bounded restart policy (`on-failure:N`), not `unless-stopped`.
- Mongo driver cluster-monitor logging is WARN or higher in the analytics image.
- The README explains how to remove leftover ad-hoc Compose projects.

### S6 — Documentation and admission expression

- **Bronze description.** The wiki's Bronze description matches the code, and states that Bronze keeps no raw envelope, so it departs from the raw-and-immutable Bronze convention in the Medallion literature, for erasure and privacy reasons.
- **Admission expression.** Streaming event-time admission either replaces the Scala UDF with built-in expressions, proven equivalent to `AnalyticsEventTimePolicy` by a property test, or keeps the UDF with the rationale recorded here.
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
| Backfill | AC-03.8 | Given Silver holds A's events and the stage-times table is absent, when the batch starts, then A's row is backfilled from Silver before Gold rebuilds. |
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
| AC-05 | Analytics builds and every unit and integration suite passes on Spark 4.2.0 / Delta 4.4.1, and the container image starts. | Partly observed 2026-10-26. `scalafmtCheckAll scalafmtSbtCheck` pass. `scripts/run-local-tests.sh analytics`: 500/500 unit tests pass; 108 of 109 integration tests pass (4 ignored) with 1 failure, `MongoActiveDeletionMarkerIntegrationSpec`, a pre-existing hard-coded `expiresAt` of 2026-10-25 that expired (no Spark involvement); after changing it to 2099-01-01 that spec passes (12/12 in the follow-up run). The user built the image (`docker build -f analytics/Dockerfile -t hiring-analytics:spark42-s5 .`, 2026-10-26; dependency resolution and `stageContainerRuntime` succeeded). A runtime start against Mongo/Kafka is not verified yet; final QA covers it. Review fixes: the Delta retention-window key is set to false in every test session that can run VACUUM, `DeltaVacuumEligibility.canSkip` fails closed when it is true, `SparkRuntime` has a unit test for it, the workload report reads the Delta version at runtime, and the marker test date moved to 2099 (unit 501/501, `AnalyticsKeyRetirementIntegrationSpec` 6/6). |
| AC-01 | One catalog defines every table. Targets are validated strictly (AC-01.1). Constraints are enforced (AC-01.2). Gold has no `overwriteSchema`, Gold `day` is a `DateType`, and extraction is typed (AC-01.3). | Observed 2026-10-27. `AnalyticsDeltaCatalogSpec` AC-01.1 to AC-01.3 (13 tests): strict target (nullable `eventId`, missing, extra and changed constraint), loose source, Delta rejects `eventType = 'BOGUS'`, null `eventId`, negative offset, negative Gold count and unordered percentiles, Gold `DATE` with UTC start-of-day extraction, drifted Gold table not overwritten, null cell gives `InvalidGoldSchema`. Unit suite 524 tests; after fixing fixtures that wrote tables directly, the affected specs pass. Integration: BatchKafkaRange, StreamingIngestion, MongoReportPublisher, LateFactReplayRecovery, ErasureRetention 28/28; KeyRetirement 6/6. Streaming recovery specs were disturbed by the host Wi-Fi address changing under a running Spark driver (see checkpoint). |
| AC-02 | Output is identical on existing fixtures (AC-02.1). Tokenization runs once (AC-02.2). Plans contain no nested-loop join for marker exclusion and no repeated JSON parse (AC-02.3). | Observed 2026-10-28. AC-02.1: the existing behavior suites pass unchanged (OperationalEventWireContractSpec, AnalyticsTransformsSpec, quarantine and streaming specs; unit 536/536) plus `OperationalEventParsedFrameSpec` (validity, SHA-256 fingerprint and HMAC tokens equal independent expectations; analyzed and inline selections equal). AC-02.2: counting tokenizer, 2 tokenizations per record for one action, none on later actions over the persisted frame, one tokenizer per task; Silver adds no tokenizer to a tokenized frame. AC-02.3: no BroadcastNestedLoopJoin or CartesianProduct with broadcast on and off; markers exclude actor, candidate and candidate-search-result positions. Raw-JSON minimum on rawValue: 2 get_json_object (`$.occurredAt`, `$.payload`), 1 json_object_keys (envelope key set), 1 try_parse_json; 8 get_json_object and 4 json_object_keys in total (was 16 and 11, evaluated about 4 times per batch); consumer plans hold none. Integration (7 specs): 32/32, 1 ignored. |
| AC-03 | S3a: AC-03.1 through AC-03.9 and the Mongo document carries `stageDurations` (shared fixture). S3b: the application reads it, GraphQL exposes `stageDurations`, and the SDL golden file, operation fixtures and `docs/api.md` are updated. | Not run |
| AC-04 | No token stats (AC-04.1). The merge scans fewer files, with a local before/after count (AC-04.2). The maintenance tick compacts and records file counts before and after (AC-04.3). Compaction does not shorten the erasure horizon (AC-04.4). | Not run |
| AC-07 | Given the erasure worker starts while `mongodb` does not resolve, then no Spark session is created, it logs one WARN line per backoff attempt, and it exits non-zero after the configured maximum wait. When Mongo becomes reachable within the wait, it proceeds normally. One-shot commands fail fast. Compose restarts are bounded. | Observed 2026-10-26. `MongoStartupResilienceSpec` AC-07.1 to AC-07.10 pass (10/10; virtual-time backoff 1, 2, 4 s capped, give-up after the maximum wait with 3 warnings and one error, one-shot fail-fast, `managedSparkMongo` never acquires Spark, sanitized text, DNS versus server-selection, invalid URI not retried, real driver ping bounded against a closed port, defaults and `ANALYTICS_MONGO_STARTUP_*` overrides and validation). `scalafmtCheckAll scalafmtSbtCheck` pass; full `sbt test` 510 tests, 509 passed on the first run, the one failure (a log-text assertion in AC-07.1) was fixed and the affected specs rerun green (57/57). Compose uses `restart: on-failure:5` for `analytics-erasure-worker` (the only analytics service with a restart policy); `log4j2.properties` sets `org.mongodb.driver.cluster` to WARN. Review fixes (2026-10-26): the three `src/it` callers of `sparkMongo` pass `IO.unit`; DNS runs on the blocking pool; the README wording is exact (backoff sleep sums to `max-wait`, each attempt adds up to twice `check-timeout`). `log4j2.properties` keeps plain-text console logging explicitly, because with a log4j2 configuration on the classpath Spark skips its own defaults and `spark.log.structuredLogging.enabled` could select the JSON layout (I could not read its default from the jar; the integration run logged plain-text lines in the `yy/MM/dd` format). After the fixes: `scalafmtCheckAll scalafmtSbtCheck test IntegrationTest/compile` all pass (511/511 unit, clean IT compile); `scripts/run-local-tests.sh analytics` on the Ingestion, StorageBounds, Recovery and ErasureRetention integration specs: 23/23 pass. Compose and the container image were not run (project hook). |
| AC-06 | Docs match the code, and the admission UDF is either replaced with an equivalence property test or its retention is justified. | Not run |

## Implementation handoff

- Order: S5 → W → S1 → S2 → S3 → S4 → S6. One owner per slice, and each slice is committed only after its review gates pass.
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
- Next action: S3.
- S1: implemented (2026-10-27), review gates pending. `AnalyticsDeltaCatalog` declares all 11 Delta tables; Silver, Bronze and late facts are NOT NULL on keys, tokens and timestamps; constraints are derived from the domain enums. Lambda `forall` constraints are not used because Delta does not round-trip them. Existing lakehouses fail closed; reset per D4.
