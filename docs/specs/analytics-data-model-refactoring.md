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
| D1 | Application lifecycle snapshot retention: 30 days after a terminal status (Hired, Rejected, Declined); an open application expires 180 days after its last lifecycle event. |
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
- Silver holds current-state, cleaned records. Slowly changing history (SCD2) and surrogate keys belong in Gold or later. `silver/application_lifecycle` is current state at the `applicationId` grain, with no SCD2 and no surrogate key.
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
- **Nullability scope.** NOT NULL applies to top-level and struct columns. Delta cannot enforce NOT NULL on array elements, so `subjectTokens` uses a CHECK such as `forall(subjectTokens, t -> t IS NOT NULL)` where the invariant matters.
- **Constraints at creation.** CHECK constraints are declared when a table is created, because `ADD CONSTRAINT` validates existing rows and upgrades the writer protocol. A mismatching existing table still fails closed, with no `ALTER`-based repair.
- **Gold writes keep the declared schema.** They use `overwriteSchema` false, and the target is created or validated from its spec first.
- **Gold days are dates.** Gold `day` becomes `DateType`. The report boundary converts it to the UTC start-of-day `Instant`, so the Mongo and GraphQL contracts are unchanged by S1.
- **Typed decoding.** Gold report extraction uses typed row decoders. A null or out-of-type cell fails as `InvalidGoldSchema`.

### S2 — Parse, validate and tokenize once

- **One parsed frame.** The persisted parsed frame carries `isValidEvent`, `eventFingerprint`, `subjectToken` and `subjectTokens`, each computed once.
- **Reuse.** Bronze, quarantine, conflict detection, Silver and counts reuse those columns.
- **Parsing.** JSON is parsed once into a VARIANT and reused wherever that is semantically equivalent. Acceptance behavior is unchanged.
- **Erasure-marker exclusion is an equality anti-join.** Explode tokens, then `left_anti` on the token.
- **Tokenization is a deterministic UDF.** It builds one HMAC per task. Key material never appears in plans, logs or Delta.

### S3 — Application lifecycle snapshot and hiring stage durations

**`silver/application_lifecycle`**

| Aspect | Rule |
|---|---|
| Grain | One row per application |
| Columns | `applicationId`, `subjectToken`, `subjectTokens`, then the earliest `createdAt`, `acceptedAt`, `declinedAt`, `interviewAt`, `hiredAt`, `rejectedAt`, then `lastEventAt`, `terminalAt`, `expiresAt` |
| Merge | Each batch, streaming batch and late-fact replay merges into it from admitted Silver lifecycle events |
| Merge semantics | Per-stage earliest timestamp; idempotent under replay and reorder |
| Expiry (D1) | `expiresAt = terminalAt + 30d`, otherwise `lastEventAt + 180d` |
| Classification | Subject-attributed. It participates in erasure purge, expiry retention, storage inventory and HMAC key retirement exactly like Silver. |

**Gold outputs**

| Output | Rule |
|---|---|
| `gold/time_to_hire` | Computed from unexpired snapshot rows. Its shape and suppression rules are unchanged. |
| New `gold/hiring_stage_durations` | One row per metric, with P50/P75/P90/P95 hours and `eligibleCount` as distinct subjects. A metric is suppressed when `eligibleCount < 10`. Negative durations are excluded. |

Metrics in `gold/hiring_stage_durations`:

- `TIME_TO_ACCEPT` (created → accepted)
- `TIME_TO_INTERVIEW` (created → interview)
- `TIME_TO_HIRE` (created → hired)
- `ACCEPTED_TO_INTERVIEW` (accepted → interview)
- `INTERVIEW_TO_HIRE` (interview → hired)

**Report contract**

- The Mongo report snapshot gains `stageDurations`, an array of `{metric, p50Hours, p75Hours, p90Hours, p95Hours, eligibleCount}`.
- If a strict collection validator covers report snapshots, change it with a versioned operational Mongo migration.
- Admin GraphQL `AnalyticsReport` gains `stageDurations: [AnalyticsHiringStageDuration!]!` with enum `AnalyticsHiringStageMetric`. This is additive; existing fields are unchanged.

### S4 — Privacy-safe file skipping and compaction

- **Stats columns.** Subject-attributed tables replace `delta.dataSkippingNumIndexedCols=0` with `delta.dataSkippingStatsColumns`, which lists only non-subject columns. Kafka coordinates, `occurredAt`, `ingestedAt` and `expiresAt` are allowed; token columns and raw identifiers are not.
- **Fail closed.** Initialization refuses a table whose properties would gather stats on a token column. A stats-columns property that the Delta log shows is ignored counts as a failure, and the `delta.dataSkippingNumIndexedCols` fallback in the assumption above then applies.
- **No stale stats.** Stats properties apply to future writes only. Tables are created with the final properties after the D4 reset, so no file written under the old property survives.
- **Compaction.** The streaming maintenance tick runs `OPTIMIZE` per table once its configured interval has elapsed, under the lakehouse mutex. It runs before vacuum eligibility, never while an erasure purge for that table is in progress, and is disabled when the interval is zero. `OPTIMIZE` is idempotent and uses snapshot isolation, so concurrent readers are safe. The files it replaces still hold subject tokens until VACUUM, so the erasure data-file horizon is measured from the later of the purge and the last `OPTIMIZE` that replaced files holding the erased subject's rows; AC-04.4 tests this.
- **Alternatives rejected.** Auto compaction and optimized write run synchronously inside the writer, outside the mutex and the erasure ordering. Z-ORDER and liquid clustering are non-goals.

### S6 — Documentation and admission expression

- **Bronze description.** The wiki's Bronze description matches the code, and states that Bronze keeps no raw envelope, so it departs from the raw-and-immutable Bronze convention in the Medallion literature, for erasure and privacy reasons.
- **Admission expression.** Streaming event-time admission either replaces the Scala UDF with built-in expressions, proven equivalent to `AnalyticsEventTimePolicy` by a property test, or keeps the UDF with the rationale recorded here.
- **Canonical docs.** `docs/big-data-architecture.md`, the wiki storage page and `docs/domain-language.md` reflect S1–S4.

## Domain language and examples

- Context: Analytics. Aggregates touched: Application lifecycle (read-only projection). Invariants: K=10 suppression, erasure exclusion, retention expiry.
- New glossary terms:
  - **Application lifecycle snapshot:** the pseudonymized per-application earliest stage times.
  - **Hiring stage duration:** a suppressed percentile distribution of time between two lifecycle stages.

| Rule | Tag | Scenario |
|---|---|---|
| Long hires count | AC-03.1 | Given application A created on day 0 and hired on day 45, with Silver retention of 30 days, when Gold rebuilds, then A contributes 1080 hours to `TIME_TO_HIRE` and to `timeToHire`. |
| Snapshot merge is idempotent | AC-03.2 | Given Interview on day 3 already merged, when the same event is replayed and an earlier duplicate Interview on day 2 arrives, then `interviewAt` is day 2 and only one row exists for A. |
| Terminal expiry | AC-03.3 | Given A rejected on day 10, then `expiresAt` is day 40. Given B open with its last event on day 5, then `expiresAt` is day 185. |
| Erasure covers snapshot | AC-03.4 | Given an erasure marker for A's candidate, when the purge runs, then no snapshot row carries that subject token. |
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
| AC-01 | One catalog defines every table. Targets are validated strictly (AC-01.1). Constraints are enforced (AC-01.2). Gold has no `overwriteSchema`, Gold `day` is a `DateType`, and extraction is typed (AC-01.3). | Not run |
| AC-02 | Output is identical on existing fixtures (AC-02.1). Tokenization runs once (AC-02.2). Plans contain no nested-loop join for marker exclusion and no repeated JSON parse (AC-02.3). | Not run |
| AC-03 | AC-03.1 through AC-03.5. The report snapshot and GraphQL expose `stageDurations`, and the GraphQL schema and operation fixtures are updated. | Not run |
| AC-04 | No token stats (AC-04.1). The merge scans fewer files, with a local before/after count (AC-04.2). The maintenance tick compacts and records file counts before and after (AC-04.3). Compaction does not shorten the erasure horizon (AC-04.4). | Not run |
| AC-06 | Docs match the code, and the admission UDF is either replaced with an equivalence property test or its retention is justified. | Not run |

## Implementation handoff

- Order: S5 → S1 → S2 → S3 → S4 → S6. One owner per slice, and each slice is committed only after its review gates pass.
- Write paths:
  - S1, S2, S4 and S5: `analytics/` and analytics tests.
  - S3: `analytics/`, plus `src/main` and `src/test` for the report snapshot, GraphQL types and any Mongo migration.
  - Docs belong to the slice that changes the behavior.
- Gates:
  - Code Reviewer for every slice.
  - Security Engineer for S2 (HMAC and marker exclusion), S3 (new subject-attributed table, erasure and API) and S4 (log stats, OPTIMIZE alongside erasure).
  - Final independent QA on the final state.

## Checkpoint and review

- Completed: none committed yet.
- S5: done. Code Reviewer PASS (2026-10-26); findings M1 (test sessions now pin the vacuum key, and a test asserts the `SparkRuntime` setting), L1, L2 and L3 (`DeltaVacuumEligibility` fails closed when the key is true) fixed; `sbt test` 501/501 and `AnalyticsKeyRetirementIntegrationSpec` 6/6 after the fixes. Delta 4.4 defaults `spark.databricks.delta.vacuum.retentionWindowIgnore.enabled` to true; `SparkRuntime` pins it to false to keep the explicit VACUUM retention check.
- Verification policy (2026-10-26): the full analytics suite takes about 3 h 13 min. Each slice runs formatting, unit tests and the integration specs it touches. The full suite runs once on the final state for QA.
- Next action: commit S5 after review, then S1.
