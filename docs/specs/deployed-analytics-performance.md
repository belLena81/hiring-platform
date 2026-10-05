# Deployed Analytics Performance

## Identity, outcome, and scope

- Roadmap: final Phase 15 in [development milestones](../development-milestones.md).
- Status: **draft**. Real environment, representative workload, burst-drain threshold, resource budget and execution authorization remain required inputs.
- Coordinator: Product Manager. Big Data Engineer owns workload and Spark/Delta analysis; Architect owns proposed structural changes; Data Engineer owns operational query effects; Security and Code Reviewer provide independent review; QA owns final qualification.
- Outcome: deployed analytics meets the agreed freshness and burst-recovery targets at an explicit cost, with measured evidence that privacy and recovery contracts still hold.
- This specification authorizes no deployment, paid provisioning, large dataset, tuning experiment or production traffic injection.
- Non-goals: reopening local Phase 7 latency optimization, changing report semantics, relaxing deletion/retention/ownership, replacing the processing engine, introducing schema compatibility layers or claiming production acceptance from accelerated local runs.
- Prerequisites: an authorized real deployment meeting [production readiness](production-readiness.md), applicable [lakehouse](hiring-analytics-lakehouse.md)/[continuous analytics](continuous-hiring-analytics.md) gates and a frozen workload/budget manifest. Those production prerequisites apply whenever production is proposed, not only at Phase 14.
- [Reconciliation and recovery](analytics-reconciliation-recovery.md) supplies correctness/capacity design; [observability](hiring-observability-resilience.md) owns shared metrics/alerts. Their criteria are referenced rather than duplicated.

## Source baseline and evidence boundary

| Source | Relevant implemented seam |
|---|---|
| [SparkStreamingBatchStages.scala](../../analytics/src/main/scala/com/example/hiring/analytics/adapter/spark/SparkStreamingBatchStages.scala) | Spark-backed admission, ingestion and publication are the appropriate execution-plan investigation boundary. |
| [StreamingBatchCost.scala](../../analytics/src/main/scala/com/example/hiring/analytics/adapter/spark/StreamingBatchCost.scala) | Existing cost observations are a starting point for bounded measurement, not proof of an accepted resource budget. |
| [AnalyticsStreamingSettings.scala](../../analytics/src/main/scala/com/example/hiring/analytics/config/AnalyticsStreamingSettings.scala) | Typed streaming settings own runtime tuning inputs; do not add a second environment-variable configuration path. |
| [StreamingBatchCoordinator.scala](../../analytics/src/main/scala/com/example/hiring/analytics/service/streaming/StreamingBatchCoordinator.scala) | Durable ordering, revision, publication and acknowledgement remain invariant under optimization. |
| [HiringAnalyticsStreamingWorkloadMain.scala](../../analytics/src/test/scala/com/example/hiring/analytics/cli/HiringAnalyticsStreamingWorkloadMain.scala) | Existing test workload provides reusable measurement concepts; invoking a local proof is not the deployed acceptance run. |
| [StreamingOpenSilverSelectionSpec.scala](../../analytics/src/test/scala/com/example/hiring/analytics/adapter/spark/StreamingOpenSilverSelectionSpec.scala) | Existing OPEN selection parity evidence must survive changes to projection/join work. |

The roadmap records a historical local `37dc8c` run with 7,500 records over 900 seconds: Bronze p95 **40,084 ms FAIL**, Admin-report p95 50,941 ms, backlog 422, storage 2,844 files / 12,605,172 bytes and maximum maintenance age 83,338 ms. This is preserved as a source-qualified historical result. It is neither a fresh measurement of current source nor a deployed baseline. Separate later functional evidence does not supersede the freshness failure.

Capture a new deployed baseline before any candidate change. The current default trigger period and a passing functional suite cannot establish either target below.

## Desired behavior and measurement contract

### Predeclared workload and environment

The operator and Product Manager approve a manifest before the run. It identifies:

- Source revision/artifact digest, active schemas/policy, sanitized configuration identity and exact environment/storage/checkpoint/report identities.
- Node/driver/executor resources, runtime/JDK/Spark/Delta/Kafka/Mongo versions, network/storage class, limits, partitions, replication where applicable and relevant competing workloads.
- Synthetic seed or authorized data provenance, event mix and aggregate distribution, dataset size/age, existing file layout, subject/skill cardinality, partition skew and concurrency.
- Warmup exclusion rule, steady arrival rate, run duration, scheduled bursts and their sizes/rates, cooldown/observation duration, retry behavior and sample inclusion rules.
- Defined time origin and independently observed Bronze/report visibility, clock synchronization/error bounds, sampling frequency and polling overhead.
- Predeclared backlog/storage/maintenance ceilings from the selected functional workload, burst-drain threshold, resource/cost ceilings and safety stop conditions.

The workload includes duplicates, malformed/conflicting input, late/replayed facts, deletion activity and suppressed-only periods at known rates, so apparent speedups cannot come from silently dropping difficult cases. Report normal-path latency and exceptional/out-of-window outcomes separately with their denominators; neither omitted failures nor suppressed-only intervals can inflate success.

The initial deployed footprint is the smallest authorized one suitable for the target load. Representative does not mean unlimited scale. Material source, policy, topology, dataset or competing-load changes invalidate comparability and require a new paired baseline.

### Freshness, backlog and burst recovery

Preserve the existing source-to-availability definitions owned by HCS. Record original business event time, source publication observation and visibility observation so production clock skew, publisher delay and analytic processing can be distinguished. Freeze the accepted timestamp origin before execution; do not reset event time at Bronze ingestion to improve measured freshness.

- **Bronze availability:** p95 strictly below **30,000 ms** for the declared eligible workload.
- **Authenticated Admin-report freshness:** p95 at most **120,000 ms**, measured through the actual authorized API visibility path rather than a driver log saying publication finished.
- Record p50/p95/p99, sample counts, errors and observation bounds for both. If polling granularity or clock uncertainty prevents proving the target, label the result inconclusive and improve the measurement design.
- Preserve event correlation without logging sensitive subjects/payloads. Observe enough data to confirm actual Bronze/report visibility; timestamps from the same unverified producer are insufficient evidence of end-to-end visibility.
- Record admitted, quarantined, suppressed, expired, late, duplicate, failed and unfinished records with explicit categories. Failure/unfinished results do not vanish from the run summary.
- Backlog is measured from pinned source/durable progress coordinates with per-partition distribution. Boundedness and storage/maintenance health remain required functional gates.
- Burst drain is elapsed time from the predeclared burst end until backlog returns to the declared baseline tolerance and remains there for the declared stability window. Include continuously arriving steady traffic; stopping all traffic is a different experiment.
- Burst size/rate, baseline tolerance, stability window and acceptable drain time are **unresolved** and must be fixed before qualification. Do not choose them from the observed result.
- Measure report visibility during deletion/quality blocking as its contractual unavailable/deferred outcome; do not force publication solely to satisfy a freshness percentile.

### Experiment and candidate behavior

1. Verify authorization, production gates, measurement readiness and workload limits. Abort before injection if environment identity, deletion evidence or budget does not match the manifest.
2. Run and preserve the deployed baseline using the frozen workload. Capture Spark physical plans and operator-level input/output, pruning, shuffle, skew, spill, stage/task time, file count/size, driver/executor CPU/heap and I/O alongside external freshness.
3. Identify one demonstrated bottleneck. Write its cause hypothesis, smallest candidate, expected metric change and correctness risks before implementation. A higher Spark job count alone does not identify the dominant bottleneck.
4. Implement the bounded repair and its focused regression tests. Preserve the admitted input and conflict semantics, including the final `prepared.conflicts` left-anti exclusion in OPEN selection unless a separately proven equivalent preserves every conflicting identity.
5. Re-run the comparable workload under the same environment, input distribution and measurement method. Record failed attempts and repeated-run variation; do not select only the fastest run.
6. Independently compare correctness and privacy, including report cells, quarantine, deletion, replay, generation fencing, watermark/acknowledgement and resource cleanup. Use reconciliation only where its input/deletion pins remain valid; unavailable evidence cannot count as parity.
7. Accept the candidate only if required functional/operational gates and predeclared freshness, drain and resource/cost targets pass. Faster median latency cannot compensate for failed p95 or correctness.
8. If a target fails, retain the evidence and either propose the next bounded measured repair or record the blocking capacity/budget decision. Do not lower the target or claim the historical local result was acceptance.

### Failure and rollback

If the run causes unsafe resource growth, excessive errors or an invalid deletion/ownership state, stop load generation and use the existing orderly shutdown/recovery procedure. Await running driver cleanup before releasing ownership. Record partial samples and stop reason; do not mark an interrupted qualification successful.

Roll back a compatible code/configuration candidate through its approved deployment procedure while preserving checkpoints, journals, publication controls and deletion state. Application rollback does not revert data/schema or retired keys. Any candidate requiring persistent shape changes needs an explicit schema/recovery plan before experimentation; incompatible state must fail closed rather than trigger a convenient reset.

Admin access remains trusted and authorized; no candidate creates a public metrics/report endpoint exposing personal or small-cell data. Candidate and Recruiter request paths remain isolated from analytics capacity changes. Load tests must include evidence that operational transactions and durable outbox work stay within their separately agreed limits.

## Boundaries, code style and contracts

Domain policies remain pure, immutable and deterministic with explicit time/IDs. Services sequence bounded effects and enforce admission/authorization; Spark transformations and execution plans live in adapters. Kafka, Mongo, storage and JSON/BSON concerns remain adapter-owned. Do not hide effectful reads or mutable caches inside ostensibly pure selection/recovery functions.

Preserve analytics `F[_]` services and `AnalyticsError` business-failure channel, typed normal outcomes and `Resource` ownership. Main application changes retain `IO` with `UseCaseIO`/`RepositoryIO`. Accumulate configuration errors with `ValidatedNec`; use typed errors/ADTs and exhaustive matches. Prefer standard library/Spark/Cats operations to a custom framework, speculative scheduler or unsupported dependency.

Keep bounds at both the source/query and application boundaries. Avoid driver `collect`, repeated materialization, unnecessary UDFs and broad scans unless the bounded data and physical evidence justify them. Any cache must have an explicit size/lifetime/invalidation owner and demonstrate correctness under deletion/revision changes. A cache hit cannot bypass a current marker or publication fence.

No public API/event change is required for performance alone. Preserve the seven-field operational event envelope and single active pre-MVP API/analytics shape. If a measured repair requires schema changes, update the owning contracts/fixtures directly and fail closed on incompatible local data absent exact reset authorization. Operational Mongo changes require versioned repeatable migration/recovery checks. See [schema evolution](../schema-evolution.md).

Instrumentation uses bounded labels and sanitized correlation, with measured overhead. High-cardinality subjects, run-specific labels or raw resume/job/event text must not become metric labels. Store detailed local evidence in ignored `.local/logs/` and synthetic datasets under `.local/data/`; summaries reference source/environment identity and artifact digests without embedding secrets.

## Alternatives and decision register

| Decision | Alternatives and selection rule | Owner / evidence | Blocked work |
|---|---|---|---|
| Execution cadence | Existing streaming; compare scheduled batch only where business freshness permits and on a separately authorized non-registered dataset | Product Manager/Architect: target and full compute/idle cost | Cadence change; ordinary batch never bypasses streaming registration |
| Dominant scan/aggregation cost | Projection/pruning and less repeated work first; incremental report recomputation only with measured benefit and correctness design | Big Data Engineer: physical plans and state/deletion/replay comparison | Incremental state/schema work until justified |
| File-layout cost | Targeted compaction/layout versus extra parallelism | Big Data Engineer: file sizes, I/O, shuffle/spill, maintenance load and total run cost | Compaction/partition changes until evidence |
| Capacity increase | Tune within selected footprint versus more compute/partitions | Architect/operator: bottleneck, saturation and priced budget comparison | Provisioning and recurring spend |
| Burst acceptance | Business-selected burst/load/drain/tolerance/stability values | Product Manager/operator before baseline | Burst qualification |
| Repetition/sample design | Repeated comparable windows with fixed minimum sample/duration versus longer continuous observation | QA/Big Data Engineer: variance, clock/poll error and target confidence | Final deployed acceptance |
| Cost limits | Resource ceilings plus actual provider charges and operations estimate | Product Manager/operator: dated pricing and approved spend | Paid runs and cost acceptance |

Do not introduce new concurrency where the shared lakehouse ownership contract serializes writers. Extra executors may improve a single owner's work; multiple uncoordinated writers are not a capacity option.

## Acceptance and evidence

Test names listed as proposed are future work. Existing HCS/HAL criteria and evidence remain in their owning specs.

| ID | Given / When / Then | Implementation/tests or evidence owner | Planned method | Actual outcome |
|---|---|---|---|---|
| DAP-01 | Given a requested run, when readiness is checked, then deployment gates, exact identities, authorization and budget are valid before injection. | Operator manifest and activation checks | Missing/expired/mismatched evidence negative checks | Not run |
| DAP-02 | Given a frozen workload, when baseline executes, then workload/config/plan/resource/sample provenance is complete and historical local failure is kept separate. | Existing workload concepts plus deployed harness | External Bronze/Admin observations and artifact manifest | Not run |
| DAP-03 | Given baseline/candidate samples, when compared, then environment, workload, origin, inclusion and uncertainty rules match; deviations invalidate comparison. | Proposed `AnalyticsWorkloadComparisonSpec` and QA | Deterministic comparison validation and manifest inspection | Not run |
| DAP-04 | Given normal eligible workload, when measured, then Bronze p95 is <30,000 ms and authenticated report p95 is <=120,000 ms with complete denominators. | Deployed harness and Admin API | Repeated predeclared measurement windows | Not run |
| DAP-05 | Given predeclared bursts and continuing normal input, when recovering, then drain time meets the chosen threshold and backlog/storage/maintenance stay bounded. | Workload/drain observations | Actual burst run with tolerance/stability rules fixed beforehand | Not run |
| DAP-06 | Given duplicates/conflicts/late/replay/deletion/suppression, when optimized, then reports and durable outcomes preserve current contracts and conflicting IDs never leak into OPEN selection. | OPEN selection/coordinator/recovery tests and independent reconciliation | Focused parity, deliberate-conflict fixtures and race integration | Not run |
| DAP-07 | Given shutdown, failure or budget stop during a run, when recovery occurs, then driver cleanup precedes ownership release and partial run is not accepted. | Existing cancellation/recovery suites; run control | Controlled stop/fault with durable readback | Not run |
| DAP-08 | Given chosen candidate, when evaluated, then physical-plan evidence identifies the repaired bottleneck and measured resources/cost meet predeclared limits. | Big Data Engineer and operator report | Same-workload plan/resource/cost comparison | Not run |
| DAP-09 | Given shared operational load, when analytics runs, then operational authorization/transaction/outbox gates remain healthy and metrics disclose no personal data. | Main integration/load checks and Security | Agreed operational workload plus bounded telemetry review | Not run |
| DAP-10 | Given candidate rollback or incompatible persisted state, when restarted, then publication/deletion/checkpoint controls remain enforced and no implicit reset occurs. | Affected adapter/config/recovery tests | Supported rollback drill or explicit incompatible-state rejection | Not run |
| DAP-11 | Given final source/config/environment, when closure is requested, then independent code/security review and QA certify the same evidence; failed/unrun targets stay open. | Independent review and qualification record | Evidence applicability audit and final verdicts | Not run |

Future code changes run root `sbt test`, Java 17 analytics unit/integration checks for affected behavior, and configured formatting checks in each changed build. Serialize SBT and preserve source-qualified outcomes. Tests validate correctness; only the authorized deployed workload qualifies latency/drain/cost.

## Workload and cost report

Report both incremental candidate cost and total platform cost: active/idle compute hours, storage growth and retained versions, Kafka retention, Delta checkpoint/log/data size, I/O/egress, backup impact, compaction work, provider calls where applicable and operator effort. Record measurement period, units, price source/date and uncertainty. Unknown prices remain unknown; no invented currency estimate is accepted.

Compare the smallest viable deployment against the target before scaling. Report throughput and cost per admitted event/report interval together with errors, freshness and workload mix. Cheaper processing that omits work, shortens retention or weakens privacy fails acceptance.

## Ordered handoff and checkpoint

1. Product Manager/operator resolve deployed environment, workload, burst and budget decisions; QA reviews measurement design before a baseline is run.
2. Big Data Engineer collects the authorized baseline and physical plans, then proposes one bounded repair with affected DAP/HCS/HAL criteria.
3. Architect/Scala/Data specialists implement only that repair and focused correctness/resource tests; update owning contracts only when required.
4. Run comparable candidate workload and authorized failure/rollback checks. Security and Code Reviewer independently assess source plus actual evidence.
5. Final independent QA qualifies freshness/drain/resources/correctness on the final source/configuration and records explicit FAIL/BLOCKED for unmet gates.

No implementation or deployed criterion is complete. All commands/scenarios above are planned, not run by this specification task. Documentation checks are recorded in the [central readiness index](../development-milestones.md#specification-ownership-and-readiness).

Next action: independent specification review, then leave deployed experiments deferred until prerequisites and authorization exist. Implementation Code Reviewer: pending. Security Engineer: pending. Final independent implementation QA: pending. The 40,084-ms historical local Bronze failure remains visible and is not converted to a pass by this plan.
