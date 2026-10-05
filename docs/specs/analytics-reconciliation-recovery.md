# Analytics Reconciliation and Recovery

## Identity, outcome, and scope

- Roadmap: Phase 13 in [development milestones](../development-milestones.md).
- Status: **draft**. Recovery-policy extraction and synthetic reconciliation are specified; retained-input access and recurring verification remain conditional on the decisions below.
- Coordinator: Product Manager. Implementation: Big Data Engineer with Scala Developer; Architect owns boundaries, Security Engineer owns privacy review, independent Code Reviewer and QA own final verdicts.
- Outcome: operators can explain recovery decisions and independently verify a bounded published hiring report without changing publication or weakening deletion protection.
- Authorized work in this document: specification only. Future implementation proceeds in the ordered slices below.
- Non-goals: local latency optimization, a new processing engine, an always-on audit service, a new audit database, new public reports, reconstruction of deleted data, or production activation.
- Existing [lakehouse](hiring-analytics-lakehouse.md), [continuous analytics](continuous-hiring-analytics.md), and [erasure safety](analytics-erasure-safety.md) criteria retain ownership. This capability adds an independent oracle; it does not rename or close HAL, HCS, or AES gates.
- Dependencies: Phase 7 functional recovery contracts, [operational observability](hiring-observability-resilience.md), and the current [analytics architecture](../big-data-architecture.md). Latency and burst-drain qualification belong to [deployed performance](deployed-analytics-performance.md).

## Verified source baseline

The following paths were inspected while preparing this specification; they establish implemented seams, not a new runtime PASS:

| Source | Implemented fact and proposed use |
|---|---|
| [StreamingBatchCoordinator.scala](../../analytics/src/main/scala/com/example/hiring/analytics/service/streaming/StreamingBatchCoordinator.scala) | Immutable preparation, decision revision, journal state, terminal outcomes and staged effects already exist. Extract decision logic without moving durable effects into policy functions. |
| [AnalyticsReport.scala](../../analytics/src/main/scala/com/example/hiring/analytics/domain/AnalyticsReport.scala) | Publisher-neutral values cover daily funnel counts, time-to-hire quantiles and daily skill posting activity. Reconciliation compares these existing meanings. |
| [ErasureFailurePolicy.scala](../../analytics/src/main/scala/com/example/hiring/analytics/service/erasure/ErasureFailurePolicy.scala) | Pure classification and bounded retry scheduling provide an existing style precedent. |
| [MongoAnalyticsReportPublisher.scala](../../analytics/src/main/scala/com/example/hiring/analytics/adapter/mongo/MongoAnalyticsReportPublisher.scala) | Publication remains a Mongo adapter responsibility; the verifier receives no publishing capability. |
| [StreamingBatchCoordinatorSpec.scala](../../analytics/src/test/scala/com/example/hiring/analytics/StreamingBatchCoordinatorSpec.scala) | Existing coordinator behavior is the regression baseline for extraction. |
| [HiringAnalyticsStreamingRecoveryIntegrationSpec.scala](../../analytics/src/it/scala/com/example/hiring/analytics/HiringAnalyticsStreamingRecoveryIntegrationSpec.scala) | Existing durable recovery integration is extended for races affected by extraction. |

There is no independently accepted report oracle established by these paths. Reusing production aggregation outputs as expected values would not establish one.

## Actors and observable behavior

### Recovery decisions

1. The resource-managed coordinator captures one immutable observation containing journal state, current deletion evidence, publication receipt/reservation status, source identity and explicitly supplied time.
2. A pure policy returns the next permitted decision or a typed rejection. It does not read the journal, refresh deletion markers, generate IDs, reserve publication or log.
3. The coordinator performs the selected effects in the existing durable order. Existing authorization and activation checks remain prerequisites; policy extraction cannot make missing authorization recoverable.
4. Journal preparation and committed ingestion are replayed idempotently. A matching publication receipt can resolve an uncertain publication outcome; absence of a receipt is not evidence that a publish succeeded.
5. A changed deletion view invalidates any dependent decision revision. Reserve/revalidate according to the current protocol before new report work; stale reservations cannot reveal a report.
6. Commit terminal publication state and its watermark through the existing journal operation. A callback is acknowledged only after the durable terminal state meets the existing acknowledgement contract.
7. Cancellation waits for running driver work and cleanup before releasing ownership. Restart derives decisions from durable state, never from an in-memory assertion that a previous effect probably completed.

The extraction must preserve every existing outcome, including quality-blocked and erasure-pending outcomes; it must not reinterpret them as successful report publication. No change to late-fact admission, expiry, retention or quarantine policy is implicit.

### Independent report verification

The initial caller is a trusted operator using a local command with a narrowly scoped read identity. Candidates and Recruiters gain no analytics access. Admin retains the current service authorization for reading published reports; this specification adds no GraphQL operation or user-selectable lakehouse path.

1. Validate an explicit bounded verification request before reading input. Require a dataset/root identity, selected input coordinates, exact publication generation, observation instant, policy/code identity and evidence limits.
2. Pin the input snapshot identities and the report generation being compared. For Kafka-derived data, retain topic/partition/offset coordinates and the lakehouse snapshot versions that supply their retained facts. A source coordinate alone does not prove the corresponding bytes remain available.
3. Capture a consistent deletion view and the report's visibility/publication state. A pin cannot authorize retaining or re-reading erased data. Validate current deletion controls before reads and before reporting a conclusion.
4. Reject an unavailable generation, expired input, missing partition coverage, incompatible policy/schema, unbounded request or unverifiable deletion view with a distinct sanitized outcome. Do not silently shrink the requested scope.
5. Read only the permitted retained input and the matching report cells. The adapter enforces limits during enumeration and reading; an overflow stops verification and yields an incomplete outcome, never a match.
6. Independently calculate expected cells using small pure Scala collections over the bounded fixture or slice. Encode metric meanings from the owning lakehouse contract rather than calling production Spark transformations.
7. Compare keyed cells and metadata. A missing cell, unexpected cell, wrong count, wrong eligibility, wrong suppression or mismatched quantile is observable. Empty permitted input can match an appropriately empty report; missing evidence cannot.
8. Before issuing a conclusion, verify that the pinned publication and deletion evidence remain applicable. If the selected historical generation cannot be read safely or deletion invalidates the pin, return an unavailable/inconsistent-evidence outcome. Do not compare a newly published report against an old input view.
9. Emit a bounded result with the input/publication identities, policy identity, observation time, counts of compared/mismatched cells and sanitized categories. Subject identifiers, raw records and small-cell values are excluded from diagnostic output.

The verifier never writes Bronze, Silver, Gold, journals, report controls or deletion state. It has no report reservation/publish port and cannot call ordinary batch ingestion on a registered streaming lakehouse. A read-only snapshot may become unverifiable while deletion progresses; that is a valid fail-closed result.

### Metric and evidence semantics

- Funnel counts use the same retained interval, UTC day boundaries and accepted lifecycle meanings as the current HAL contract.
- Duplicate event identities count once; conflicting duplicates and malformed/quarantined records follow the existing exclusion rules. Deliberate conflicting input must not be converted into a clean comparison.
- Late facts, expiry and replay eligibility are evaluated using the pinned observation time and policy identity, not wall-clock time read inside the oracle.
- Time-to-hire eligibility, pairing and quantile conventions follow the active report contract. Synthetic fixtures include hand-computed expected values, ties, ineligible histories and boundary times. Any numeric tolerance must be justified by the existing quantile algorithm and fixed before comparison; do not tune it after observing a mismatch.
- K=10 suppression uses distinct eligible subjects per output cell under the active policy. It is not a claim of anonymity. Compare suppressed structure without emitting underlying low-count values.
- Recruiter deletion/job-close and candidate deletion semantics follow the existing deletion and event contracts. Never query a backup or old source to reconstruct an erased subject for audit completeness.
- Operational workflow command/result streams are not analytical facts; [workflow contracts](durable-hiring-workflows.md) own any future routing changes.

## Boundaries, interfaces, and code style

Follow [engineering quality](../engineering-quality.md) and [schema evolution](../schema-evolution.md).

| Boundary | Responsibility | Forbidden dependency/behavior |
|---|---|---|
| Pure recovery policy | Immutable observations to typed next-step decisions; exhaustive matches; explicit time and identities | Spark, Mongo, clock reads, logging, mutation or persistence |
| Pure report oracle | Bounded input values to expected cells and comparison result | Production aggregation as sole oracle, I/O, external SDKs or unbounded data |
| Analytics services | `F[_]` orchestration, authorization prerequisites, bounded reads, ownership and conclusion validation | Unsafe execution, detached fibers, implicit retry loops or adapter-specific JSON/BSON |
| Spark/Kafka/Mongo adapters | Snapshot/coordinate reads, strict decoding, driver lifecycle and translated failures | Policy hidden in query strings or broad publication capabilities handed to verifier |
| CLI | Typed HOCON configuration, operator-selected request, bounded sanitized rendering, exit outcome | Direct environment precedence, credentials in output, automatic repair |

Use capability-specific names such as `AnalyticsRecoveryPolicy` and `AnalyticsReportReconciliation`; these are proposed internal capabilities, not a mandate for a framework. Keep the current analytics `F[_]` ports and `AnalyticsError` channel for business failures. Represent normal verification results with an explicit ADT such as matched, mismatched or evidence unavailable, so the caller cannot mistake an absent result for success.

Accumulate independent request/configuration validation with `ValidatedNec`; sequence decisions with `Either`. Preserve the main application's `UseCaseIO`/`RepositoryIO` aliases if an authorized service integration is later needed. Use `Resource` for clients and snapshots with owned lifetimes, and bounded FS2 streaming at adapter boundaries. Do not use `null`, unchecked `.get`, mutable global collectors or `collect()` without a proven small upper bound.

Start with in-memory synthetic input and existing read ports. Add a narrow snapshot read port only for a demonstrated missing capability. The production publisher and the verifier may share neutral report value types and validated contract constants, but must not share the aggregation implementation used to derive expected results.

No public API/event shape change is required by the initial slices. If retained verification needs new analytics metadata, specify the one active pre-MVP shape and update producers/consumers/fixtures directly; do not introduce legacy readers or backfills. Incompatible local data is preserved and rejected unless exact reset is separately authorized. Any operational Mongo schema/index addition requires its own versioned, repeatable migration and recovery evidence before the affected slice is ready.

## Decisions and alternatives

| Decision | Alternatives and default | Evidence/owner | Blocked work |
|---|---|---|---|
| Oracle execution | **Use bounded pure Scala calculation first**; distributed oracle only if a justified retained slice exceeds an accepted limit | Architect/Big Data Engineer verify fixture coverage and memory use | Distributed verification is not authorized |
| Snapshot capture | Existing immutable retained snapshots plus revalidation; or a separately authorized existing writer owner captures consistent references under its mutex and hands only immutable references to the read-only verifier | Architect/Security inspect available generation history, deletion semantics and reader behavior | Retained-input adapter until a safe pin is specified |
| Scope and limits | Small deterministic synthetic fixtures first; separately authorized retained slice with row/byte/time limits | Product Manager/operator select slice and limits; Big Data Engineer measures cost | Real-data execution |
| Recurrence | Manual invocation first; scheduled bounded verification only after a demonstrated operational need | Operator supplies cadence, CPU/I/O budget, ownership and alert destination | Scheduler/new persistent audit store |
| Quantile equality | Exact expected convention where reproducible; explicitly justified tolerance only for active approximation behavior | Big Data Engineer and QA pin convention before fixture approval | Numeric acceptance if active convention cannot be independently represented |

These open decisions do not prevent pure recovery extraction or synthetic oracle work. They do prevent claiming the retained-input verifier is implementation ready.

## Acceptance and evidence

Proposed test files below are not existing evidence. Unit paths are under `analytics/src/test/scala/com/example/hiring/analytics/`; integration paths are under `analytics/src/it/scala/com/example/hiring/analytics/`.

| ID | Given / When / Then | Implementation and tests | Planned verification | Actual outcome |
|---|---|---|---|---|
| ARR-01 | Given identical immutable recovery observations, when evaluated repeatedly, then decisions are identical with no I/O; all existing coordinator outcomes retain their meaning. | Proposed `AnalyticsRecoveryPolicySpec`; existing `StreamingBatchCoordinatorSpec` | Pure table/property cases and coordinator regression suite | Not run |
| ARR-02 | Given ingestion/publish uncertainty at each durable boundary, when restarted, then the journal/receipt determines recovery without duplicate publication or early acknowledgement. | Coordinator and `HiringAnalyticsStreamingRecoveryIntegrationSpec` | Fault injection before/after durable commits with reconstructed resources | Not run |
| ARR-03 | Given deletion or a newer publication racing a recovery revision, when resumed, then stale work cannot reveal a report or advance an invalid watermark. | Coordinator, publisher integration | Synchronized deletion/publication race and durable readback | Not run |
| ARR-04 | Given cancelled running Spark work, when ownership would release, then cleanup completes before a second owner enters. | Existing streaming cancellation integration | Resource ordering with controlled running work | Not run |
| ARR-05 | Given bounded synthetic input with independently calculated expected cells, when verified, then valid funnel/time-to-hire/skill cells match. | Proposed `AnalyticsReportReconciliationSpec` | Hand-computed fixtures, including empty input and UTC boundaries | Not run |
| ARR-06 | Given deliberate count, missing/extra-cell, eligibility, suppression and quantile defects, when verified, then every defect is detected. | Proposed oracle/comparison tests | Mutation fixtures; no production aggregation used as expected output | Not run |
| ARR-07 | Given duplicates, conflicts, quarantine, late facts, replay and expiry, when observed at the pinned instant, then expected inclusion follows the active contract. | Proposed oracle tests | Deterministic permutations and boundary instants | Not run |
| ARR-08 | Given unavailable input/generation, mismatched policy, changed deletion view or a limit overflow, when verified, then result is unavailable/incomplete and never matched. | Proposed `AnalyticsReconciliationSnapshotIntegrationSpec` | Real adapters plus controlled concurrent deletion/publication | Not run |
| ARR-09 | Given a retained permitted slice, when verification succeeds, then a second run on the same applicable pins reproduces it and all pins are recorded. | Proposed snapshot adapter/integration | Separately authorized bounded retained-input exercise | Not run |
| ARR-10 | Given verifier credentials and streaming registration, when invoked, then it cannot publish, mutate datasets or bypass ordinary-batch denial; diagnostics exclude sensitive cells. | Proposed CLI authorization/privacy checks | Capability inspection, denied writes and log assertions | Not run |
| ARR-11 | Given a proposed capacity envelope, when reviewed, then workload/resources/cost unknowns are explicit and no local latency experiment or deployed PASS is inferred. | This spec and deployed performance spec | Independent documentation review | Not run |

Future implementation checks: root `sbt test`; Java 17 analytics `sbt test`, applicable `IntegrationTest / test`, and configured `scalafmtCheckAll scalafmtSbtCheck` in each changed build. Serialize SBT; use `SPARK_LOCAL_IP=127.0.0.1` for local Spark where needed. Existing passing runs do not replace final-source checks.

## Capacity and operating cost

The first oracle uses a small deterministic synthetic dataset within an explicit row/byte ceiling. Measure peak driver memory, bytes read and runtime before authorizing larger retained slices. No cloud pricing is assumed.

Capacity planning records dataset age/size, arrival and burst rates, partitions/skew, concurrency, warmup, duration, backlog, file growth, CPU/heap, I/O, shuffle/spill and expected operating hours. It defines requirements for Phase 15; it does not restart local latency tuning. Recurring reconciliation must account for extra snapshot reads, retained audit artifacts, operator effort and its effect on deletion and live workloads. Output artifacts follow ignored `.local/logs/` conventions and their own retention/access rules.

## Ordered implementation handoff

1. **Recovery extraction (ARR-01–04):** Architect and Big Data Engineer enumerate current branches and invariants; Scala Developer extracts pure decisions and preserves existing ports/effect ordering. Exclusive edits: policy/coordinator and affected analytics tests.
2. **Synthetic independent oracle (ARR-05–07):** Big Data Engineer implements neutral input/oracle/comparison values and hand-worked fixtures. No new storage or publisher dependency.
3. **Safe retained snapshot seam (ARR-08–10):** resolve snapshot and scope decisions with Security; implement bounded adapters/CLI and race/authorization integration checks. Real-data execution requires its own scope.
4. **Capacity handoff (ARR-11):** document envelope and monitoring links for the deployed performance owner; keep resource ceilings unknown until selected.
5. **Review and validation:** independent Code Reviewer and Security verdicts, then final independent QA on the final implementation and evidence. Reopen affected criteria after fixes.

## Checkpoint

- Documentation prepared from current coordinator, report, publisher, policy and test seams; no behavior implemented by this document. Documentation delivery checks are recorded in the [central readiness index](../development-milestones.md#specification-ownership-and-readiness).
- Completed implementation criteria: none. All acceptance commands and scenarios above are planned, not executed.
- Next action: independent specification review; then assign recovery extraction within an authorized implementation task.
- Blockers: retained snapshot consistency, actual data scope and limits, any recurring cadence/budget; production remains subject to [production readiness](production-readiness.md).
- Implementation Code Reviewer: pending. Security Engineer: pending. Final independent implementation QA: pending.
