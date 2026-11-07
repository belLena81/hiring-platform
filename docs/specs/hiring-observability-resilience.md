# Hiring Observability and Resilience

## Identity and scope

- Roadmap owner: Phase 12, operational observability and resilience.
- Status: **draft**. Signal boundaries and local failure scenarios are defined; production alert thresholds, operator routing, exporter deployment and resource budgets require measured decisions.
- Coordinator: Product Manager. Scala Developer owns instrumentation/runtime changes; Data Engineer owns Mongo query measurements; Big Data Engineer owns broker/workflow measurements. Independent Security Engineer, Code Reviewer and QA review final implementation.
- Outcome: operators can identify whether a hiring request failed, durable background work is delayed, or evidence is missing, and can recover safely with an actionable runbook.
- Authorized current work: specification, plus the [MVP observability slice](#mvp-observability-slice) as MVP scope (implementation still needs its own task request). Non-goals: production monitoring procurement, frontend dashboards, Node tooling, general-purpose event bus, logging hiring content, new automatic replay authority, or claiming resilience from a dashboard alone.
- Dependencies: accepted operational behavior from [Mongo/vector access](mongodb-vector-retrieval-optimization.md), [search/embedding](hiring-search-enhancements.md), [discovery](hiring-discovery-search-quality.md), and [durable workflows](durable-hiring-workflows.md).
- Existing security, telemetry and failure controls remain mandatory before this phase. Existing analytics maintenance/activation gates cannot be postponed into observability work.

## Verified source baseline

| Source | Current fact / remaining need |
|---|---|
| [TelemetryRuntime.scala](../../src/main/scala/com/example/graphQL/cats/infrastructure/telemetry/TelemetryRuntime.scala) | Resource-owned OpenTelemetry setup and HTTP metrics/tracing. Known paths are allowlisted, other routes become `_unmatched`, and query strings are redacted. It does not establish the complete Mongo/provider/workflow signal catalog below. [BacklogSampler.scala](../../src/main/scala/com/example/graphQL/cats/infrastructure/telemetry/BacklogSampler.scala), [BacklogMetrics.scala](../../src/main/scala/com/example/graphQL/cats/infrastructure/telemetry/BacklogMetrics.scala) and [MongoBacklogSources.scala](../../src/main/scala/com/example/graphQL/cats/repository/mongo/MongoBacklogSources.scala) implement only the embedding-work and outbox backlog gauges recorded under "Implemented backlog gauges". |
| [Diagnostics.scala](../../src/main/scala/com/example/graphQL/cats/service/Diagnostics.scala) | Closed event/field enums, public-value validation and classified failures; `emit` suppresses diagnostic errors. Preserve this application boundary. |
| [SafeDiagnostics.scala](../../src/main/scala/com/example/graphQL/cats/infrastructure/logging/SafeDiagnostics.scala) | Structured sanitized output, controlled sensitive metadata, bounded encoded lines and no arbitrary exception-message forwarding. Packaged local destination remains `_logs`; new diagnostic artifacts belong in `.local/logs/`. |
| [OperationalTelemetryService.scala](../../src/main/scala/com/example/graphQL/cats/service/events/OperationalTelemetryService.scala) | Authorized hiring interaction/search event use cases. These business records are distinct from runtime metrics and must not be silently weakened to best-effort export. |
| [OperationalEventKafkaRuntime.scala](../../src/main/scala/com/example/graphQL/cats/infrastructure/kafka/OperationalEventKafkaRuntime.scala) | Resource-owned publisher/consumer and durable receipt/quarantine operations; broader lag/repair observability is a target. |
| [EmbeddingCapability.scala](../../src/main/scala/com/example/graphQL/cats/runtime/EmbeddingCapability.scala), [EmbeddingPipeline.scala](../../src/main/scala/com/example/graphQL/cats/service/search/EmbeddingPipeline.scala) | Optional embedding capability, existing client/worker lifecycle and durable processing. Instrument this ownership rather than starting another worker. |

The earlier runtime safety and mutation reliability tasks (recorded in [hiring platform refinements](hiring-platform-refinements.md#completed-work-recorded-from-removed-specifications)), [logging](../logging.md), and [engineering quality](../engineering-quality.md) remain baseline references. Historical acceptance there is not evidence that the requirements below pass.

## Desired behavior and operator contract

### Request outcomes and health

- Classify request completion, expected rejection, typed repository/provider failure, unexpected failure and cancellation separately. GraphQL HTTP 200 alone is not a successful field/use-case outcome.
- Preserve existing public GraphQL/error/health contracts. No unauthenticated detailed diagnostic endpoint or schema change is authorized by this specification.
- Distinguish service liveness, Mongo transactional readiness, search capability readiness, broker publication progress and provider availability. A live process may have a degraded optional capability.
- Kafka, embedding provider and telemetry collector outage must not make a successfully committed ordinary hiring write appear failed. Mongo transaction/outbox failure must remain a hiring write failure.
- Search that requires an unavailable capability returns its existing sanitized typed failure; instrumentation must not silently switch ranking or filters. Existing approved fallback behavior, if any, is measured as a separate bounded outcome.
- Instrumentation failure cannot replace a use-case result, suppress cancellation, create a retry, or recursively log itself until exhaustion. Resource acquisition and shutdown errors remain owned by the runtime and visible through its safe failure path.
- A missing/stale observation is `unknown`/unobserved, not a zero backlog, successful recovery or healthy state. Surface observation freshness alongside state derived by sampling.

### Signal catalog

All proposed metric names are logical catalog entries. Final instrument names/units must be recorded before implementation, reuse existing conventions, and avoid duplicate semantic instruments.

| Capability | Required observations | Interpretation and boundary |
|---|---|---|
| HTTP/GraphQL | Count and duration by fixed route/use-case class and bounded outcome; rejection/cancellation counts. | Preserve route redaction; caller-supplied operation names are not metric labels. Count one logical result separately from physical attempts. |
| Mongo operations | Duration, failures/conflicts, bounded operation class, examined/returned documents where measured safely. | Driver/repository boundary; do not emit raw filters, queries, connection strings or document contents. Expensive explain is a bounded diagnostic procedure, not per-request work. |
| Search/index readiness | Capability state, readiness observation age, index mismatch/unavailable reason class, bounded search kind, duration and returned-result count. | Index state is not a relevance score; do not expose private filter values or assert an unmeasured freshness SLO. |
| Embedding | Pending work count, oldest pending age, source-to-current-embedding age, attempts, stale-write conflicts, provider duration/timeout, terminal repair count. | Distinguish stale content from an idle fresh worker. Do not read vectors or send content solely to collect metrics. |
| Outbox/Kafka | Pending count, oldest unpublished age, attempts, committed publication outcomes, consumer durable progress/lag, quarantine count and oldest unresolved age. | Never count a send attempt as durable completion. Track unavailable broker samples separately from zero lag. |
| Workflows | Counts by finite workflow/step/state, oldest nonterminal age, overdue deadlines, retries, expired claims, reconciliation/compensation/repair outcomes. | Use durable state as truth; a sleeping/failed process cannot reset pending age. No workflow IDs in labels. |
| Providers | Logical calls and physical attempts, duration, classified timeout/rate-limit/unavailable/rejected outcome, active concurrency. | Provider category/model must come from a bounded configured set; no arbitrary host/error text labels. |
| Runtime/export | Active worker state, bounded queue occupancy, dropped telemetry, exporter failure, shutdown/drain outcome. | Collector failure is independent of business success; distinguish deliberate disabled capability from unexpected stopped worker. |
| Analytics maintenance | Link existing successful maintenance/deferred/cleanup-age/storage signals and their owning gates. | Do not claim a tick proves cleanup progress; this spec does not move Phase 7 maintenance acceptance. |

#### Implemented backlog gauges (embedding pending count/oldest age, outbox pending count/oldest age)

One instrument family serves both capabilities; the only attribute is `hiring.capability` with the closed values `embedding_work` and `operational_event_outbox`. Unimplemented catalog observations (attempts, source-to-current-embedding age, committed publication outcomes, lag, quarantine) remain open.

Pending means claimable now, by each queue's own claim predicate (shared code, not a copy): due `Ready`/`Retry` embedding rows or due `Retryable` outbox entries, plus `Processing`/`InFlight` rows whose lease has expired (for example after a worker crash). Rows under a live lease, not-yet-due retries, and terminal `Failed`/`Published` rows are not pending. Count and oldest age use the same set.

| Instrument | Type | Unit | Meaning | Emitted when |
|---|---|---|---|---|
| `hiring.backlog.pending` | observable gauge (long) | `{item}` | Claimable-now rows per the definition above. Capped at 10000 (`MongoBacklogSources.MaxCounted`): a value of 10000 means "at least 10000". | Fresh sample only; an empty backlog is `0`. |
| `hiring.backlog.oldest_pending_age` | observable gauge (double) | `s` | Approximately the oldest claimable row: each part's `limit(1)` read takes the first row by `availableAt` and reports its age, and the larger of the due and expired-lease parts is reported. Embedding measures from `availableAt` (a retry resets it). Outbox sorts by `availableAt` but measures `occurredAt`, so a retried event with a later `availableAt` can hide an older `occurredAt` behind it; this is an approximation, identical to the readiness `OldestDue` measure, not an exact minimum. | Fresh sample with a non-empty backlog; absent for an empty backlog. |
| `hiring.backlog.sample_age` | observable gauge (double) | `s` | Age of the last successful sample, measured from when the read completed. | Present sample, including a stale one; absent after a failed sample. |

- Observation point and reset semantics: a Resource-owned fiber samples the queues in parallel from MongoDB at a fixed rate of `telemetry.backlog-sample-seconds` (default 15, range 1..3600) using a separate read-only client; each read is cut off after one interval. Gauge callbacks read only process memory. Gauges are absolute values read from durable state, so a restart loses no age and cannot cause a negative rate.
- Unobserved: a failed or timed-out sample deletes the previous sample, so the capability emits no `pending`/`oldest_pending_age`/`sample_age` point rather than `0` or a stale value. A sample older than 3 intervals (freshness window) stops emitting `pending`/`oldest_pending_age` but keeps emitting `sample_age`, which exceeds the window. A failure emits one `BACKLOG_SAMPLE_FAILED` diagnostic per failure episode with only the capability (`worker` field) and a bounded `reason` (`DEADLINE_EXCEEDED` for a timeout, `INTERNAL_ERROR` otherwise); error text and identifiers are never logged, and it is silent until the capability recovers. No production alert or routing is defined here. The local rules (`HiringBacklogOldestPendingTooOld`, `HiringBacklogUnobserved`) are in `docker/observability/alerts.yaml`; their unit tests pass, and on 2026-11-07 the no-data path reached a local ticket receiver through Alertmanager, with the unobserved alert firing 171 s after MongoDB stopped and the age alert staying quiet ([operational alert verification](operational-alert-verification.md), OAV-03 and OAV-04). These are local values on one workstation, not production alerts.
- Bounds: the count is `countDocuments` with `limit(10000)` and `maxTime(500ms)`; each age read is `limit(1)` of one projected timestamp with `maxTime(500ms)`, one per part. The count plan and all four `limit(1)` age-read plans (due and expired-lease parts, both capabilities) are asserted in `MongoBacklogSourcesIntegrationSpec` to be `IXSCAN` on `embedding_work_available_lease` / `event_outbox_claim` with no `COLLSCAN`; observed on a 60-row queue, each age read examined at most one key and no sort stage appeared. No vectors, content or payloads are read; no migration or index was added. Aggregation: per-capability, no cross-label sum is meaningful. Owner: Scala Developer.

For every catalog entry record type (counter/gauge/histogram), unit, observation point, label allowlist, reset semantics, sample interval, freshness window, aggregation and owner. Counter restarts and missing samples must not produce negative rates or false recovery.

### Privacy, cardinality and retention

- Metrics use only finite operation/capability/state/outcome categories. Never label by user, candidate, job, application, workflow, search session, request, trace, free-text query or raw provider status message.
- A single controlled diagnostic record may include an approved masked correlation reference for repair. Access, masking, retention and deletion rules apply even to pseudonymous IDs. Validate/cap values before serialization.
- Keep resumes, vectors, profile/job content, private recruiter filters, credentials/tokens, raw GraphQL documents/variables, event payloads and provider bodies out of logs, traces, alerts and metric exemplars.
- Capture provider/driver errors through a bounded reason ADT; allowlist span names/attributes. Do not bypass `SafeDiagnostics` with direct interpolated logger calls.
- Retain the existing bounded structured-record behavior; define exporter batch/queue/memory bounds and bounded diagnostic query pagination. Telemetry overload drops/samples observability data according to a documented policy and records safe loss counts; it cannot drop durable hiring work.
- Keep new local evidence in ignored `.local/logs/`; sanitized examples belong in tracked docs/fixtures. `_logs` is a known existing destination, not proof that runtime configuration already follows the desired convention. Moving it requires a scoped config/documentation change.
- Review collector/backend/storage/backup access and retention as external data boundaries. Metric aggregation does not justify retaining raw sampled personal payloads.

## Alert and runbook behavior

Each alert has a named operational owner, signal/expression, measured threshold/window, minimum sample rule, no-data policy, severity, cooldown/deduplication key, first diagnostic action, safe recovery action, recovery condition and escalation destination. Roles below are ownership requirements; actual on-call contacts/routing are unresolved deployment inputs.

| Alert family | Required owner | Required diagnosis and safe action |
|---|---|---|
| Mongo failures/latency | Operational database owner | Separate auth/config failure from load/connectivity; inspect bounded safe query evidence, restore dependency; never enable write retries without idempotency evidence. |
| Search not ready | Search/index owner | Check configured index identity and observed readiness; preserve fail-closed mismatch behavior; no automatic destructive index replacement. |
| Embedding freshness/provider outage | Search worker owner | Inspect pending age/current source revisions and provider class; restore/quota-reduce per policy; recover durable work without stale overwrite. |
| Outbox lag/consumer backlog | Event pipeline owner | Check broker/auth/publisher fencing and durable frontier; restore delivery; replay only via authorized guarded procedure. |
| Stuck workflow/compensation | Capability owner | Inspect minimized revision/step/deadline/outcome; reconcile unknown provider effects before retry or compensation. |
| Quarantine/repair backlog | Owning consumer/capability owner | Verify reason and deletion eligibility, correct active contract when appropriate, then perform scoped deduplicated replay. |
| Missing observations/export loss | Runtime/observability owner | Distinguish disabled exporter from stopped service; check bounded export queue and backend access; do not classify absent samples as recovery. |
| Analytics maintenance failure | Analytics runtime owner | Use existing maintenance runbook/evidence, verify cleanup progress and writer ownership; do not change activation/retention rules. |

Start with local, deterministic runbook exercises. Production notifications/escalation transport are a separate deployment choice. Automated repair must be explicitly authorized per action; an alert itself grants no authority to replay data, reset a store, expand ACLs or spend money.

Recovery closes an incident only after pending work converges within its agreed bound, errors cease for the agreed window, and there is evidence no acknowledged work was lost. Merely restarting a worker or seeing one successful request is insufficient.

## Resilience scenarios and observables

| Injected condition | Required behavior | Evidence retained |
|---|---|---|
| Embedding provider timeout/rate limiting | Hiring writes commit with durable work where currently supported; worker retries under its existing owner/budget; freshness degrades visibly. | Accepted transaction IDs in minimized fixture, durable queue trajectory, call attempts, eventual guarded convergence. |
| Broker outage | Mongo commits/outbox persists, publisher reports delay and later drains without duplicated business effects. | Pending age/count, durable event IDs in synthetic fixture, receipts and no lost acknowledged work. |
| Mongo outage/transaction abort | Request fails with sanitized typed failure; required durable work is not falsely acknowledged. | Durable before/after state and error/outcome classification. |
| Worker kill/restart or expired claim | Pending state survives; new owner is fenced correctly; overdue work resumes. | Claim/revision history from synthetic fixtures and resulting state. |
| Cancellation during provider work | Caller cancellation propagates; resources close; uncertain external success is reconciled by workflow policy. | Finalizer/cancellation synchronization and provider observed state. |
| Poison/quarantined input | Durable reason/repair is visible; no raw payload diagnostic and no skip past unrecorded progress. | Quarantine state, acknowledgment frontier and redaction capture. |
| Subject deletion during replay | Replay cannot recreate prohibited data or deliver obsolete subject work. | Deletion barrier/replay denial and retained-copy inventory. |
| Collector/backend outage or slow sink | Export bounds hold; request results and cancellation semantics are preserved; loss/unobserved state is visible. | Queue/drop counts, memory trend and request outcome comparison. |
| Index mismatch/stale search capability | Existing capability failure remains explicit; readiness/freshness signal identifies the dependency. | Index/config identity, safe failure class, no unauthorized result disclosure. |

Use fake providers with explicit synchronization and injected clocks for local tests; use disposable Mongo replica set and Kafka for persistence/progress evidence. An artificial timeout in a fake adapter is not proof of a deployed network recovery procedure. Real provider and deployment drills require scoped credentials, budget, owners and rollback/recovery preparations.

## Interfaces, code style and ownership boundaries

- Extend existing `Diagnostics` and telemetry ports instead of adding a parallel logger/metric framework. New observation inputs use typed capability/outcome/operation enums and validated numeric values.
- Domain policies remain deterministic immutable functions with explicit time inputs. Services return existing `UseCaseIO` (`EitherT[IO, UseCaseError, A]`); repositories preserve `RepositoryIO`. Instrumentation observes outcomes without converting expected failures into thrown exceptions.
- Use `ValidatedNec` for independent configuration validation, `Either` for sequential decisions, typed failures for expected outcomes and `Option` for missing observations. No sentinel health strings, partial `.get`, or `null` business values.
- Adapters own clocks for elapsed duration, SDK calls, metric instruments and protocol error translation. Use monotonic time for durations and UTC `Instant` for observation/freshness timestamps; do not subtract wall-clock values to measure request latency.
- Use a Decorator only when multiple implementations of a real port share identical instrumentation. It must preserve values/errors/cancellation and cannot add retries. A single local measurement does not justify an abstraction layer.
- Observer-style local notifications use resource-owned managed FS2 streams with bounded queues and explicit drop/backpressure semantics. Recoverable business work uses durable outbox/inbox, never an ephemeral observer notification.
- Bridge is appropriate only if two independently varying integration dimensions are demonstrated; selecting an exporter alone does not require it.
- `Resource` owns exporter/client/worker lifetimes. Drain/shutdown has configured finite bounds; never block service release forever on an unavailable collector or leak detached fibers.
- Keep one retry owner per operation: existing repository transaction controller, durable worker, or provider caller as appropriate. Instrument physical attempts and logical calls separately to reveal SDK retries; do not multiply retries through decorators.
- Application configuration follows typed HOCON/PureConfig ownership. Preserve the existing OpenTelemetry SDK auto-configuration boundary explicitly; do not introduce new application-level environment precedence or direct environment reads.
- Dependencies point inward; Mongo/HTTP/Kafka/OpenTelemetry types do not enter domain types. Analytics retains its existing `F[_]` ports and isolated runtime; do not rework Spark to obtain an operational metric.
- No public GraphQL/event schema change is required by basic instrumentation. Any later exposed diagnostic/repair API follows the schema rules in [schema evolution](../schema-evolution.md) and authorization review. Any new operational Mongo metrics summary/index needs its own repeatable migration and query-cost justification.

## Decisions, alternatives and readiness

| Decision | Alternatives / default direction | Evidence and owner | Blocked work |
|---|---|---|---|
| Export destination | Reuse existing OpenTelemetry boundary with local/no-op capture; self-managed collector/backend versus managed service for deployment. | Operations + Security compare access, data residency, retention, egress, idle/storage cost and maintenance. | Production exporter credentials, provisioning and routing. |
| Alert evaluation | Local scripted/runbook checks first; backend alert rules after deployment selection. Local Alertmanager with two local receivers and a logging sink exist (item 8, 2026-11-06); they are not a production route. | Operations demonstrates durable routing, no-data handling and receiver availability. | Production incident delivery/signoff and any production receiver. |
| Thresholds/windows | Derive from accepted workload/SLO and recovery budget; do not pick arbitrary default latency percentiles. | PM + QA + capability owner measure normal/burst/outage baseline. | Production alert tuning and claimed SLO acceptance. |
| Sampling/bounds | Finite queue and bounded diagnostic sampling; always preserve safe aggregate failure counts when feasible. Local policy recorded under [Sampling/bounds](#samplingbounds-local-policy-item-6). | Scala Developer/QA measure request overhead, memory, loss and cardinality budget. | Final instrument configuration (deployed values). |
| Repair exposure | Reuse workflow-owned restricted CLI/Admin boundary; no automatic replay from alert transport. | Security + workflow owner complete DHW repair contract. | Actionable remote repair integration. |
| Local log alignment | Keep known `_logs` baseline visible; align to `.local/logs/` in a scoped configuration slice. | Runtime owner verifies packaged/default/override behavior and docs. | Log-path modification only. |

A choice of monitoring backend or alert service is not approved here. No monitoring frontend or Node toolchain is in scope.

## Acceptance and evidence

Implementers assign capability-named test/source paths when the relevant slice is ready. All rows currently mean **Not implemented / Not run**; source inspection is not runtime acceptance.

| ID | Given / When / Then | Planned verification | Actual outcome |
|---|---|---|---|
| HOR-01 | Given success/rejection/failure/cancellation, when instrumenting, then each logical outcome is classified without changing values/errors/cancellation. | Service/decorator tests using actual Cats Effect cancellation and typed results. | Partial (GraphQL root-field outcomes only; unit level): `GraphQLOperationOutcomesSpec` and `GraphQLOutcomeMetricsSpec` cover success, field error, denied, unauthenticated, rejected and internal classes and that a failing recorder leaves the response unchanged; cancellation and non-GraphQL paths Not run |
| HOR-02 | Given arbitrary paths/operation names and sensitive inputs, when exporting logs/spans/metrics, then allowlists, record-size and label cardinality bounds hold with no sensitive payload. | Capturing-sink redaction and bounded-cardinality tests. | Partial (backlog gauges, GraphQL outcome counter, Mongo command telemetry, runtime metrics; unit level): `BacklogSamplerSpec` asserts the exported attribute set is exactly `hiring.capability`; `GraphQLOutcomeMetricsSpec` and `MongoCommandTelemetrySpec` assert closed attribute sets and that hostile aliases, operation names, filters and documents never appear; HTTP/log coverage Not run |
| HOR-03 | Given Mongo/search/provider/broker dependencies, when one fails, then capability state and observation freshness distinguish failure, optional degradation and no data. | HTTP/GraphQL integration and fake-clock sampling tests. | Partial (backlog gauges and Mongo command telemetry): `BacklogSamplerSpec` covers unobserved-on-failure, timeout, freshness expiry and empty backlog with a fake clock; `MongoCommandTelemetrySpec` classifies Mongo failures (`error.type`) at unit level; other dependencies Not run |
| HOR-04 | Given durable pending work across restart, when measuring age/progress, then its age survives restart and attempts are not reported as completed work. | Replica-set/worker restart and publisher integration. | Not run |
| HOR-05 | Given slow/unavailable telemetry sink, when request load and shutdown occur, then memory/queue/drain bounds hold and business results remain intact. | Bounded load + exporter fault/resource tests. | Not run |
| HOR-06 | Given provider or broker outage, when restored, then accepted durable work converges and the runbook/alert correctly reports recovery. | Local provider/Kafka outage drills with durable before/after counts. | Not run |
| HOR-07 | Given worker kill/cancel/stale claim, when resumed, then no duplicate/lost effects arise and overdue/repair signals match durable state. | Workflow and embedding failure scenarios; DHW evidence links. | Not run |
| HOR-08 | Given quarantined work and deleted subjects, when authorized repair/replay occurs, then scope/deletion guards hold and no raw payload leaks. | Forbidden repair, replay/deletion race and quarantine-drill tests. | Not run |
| HOR-09 | Given each alert and no-data/recovery condition, when exercised, then an owner follows the documented action and evidence confirms resolution. | Independent runbook walkthrough with injected faults; production routing remains separately gated. | Not run as an owner walkthrough. The item 8 drills (runs of record 2026-11-07 on one workstation, the delivery drill on the final files) exercised the alert, delivery and no-data paths with injected faults and recorded resolution: delivery drill PASS 5 of 5 (`20261107T114520Z-delivery`), stalled-backlog drill PASS 3 of 3 (`20261107T063509Z-backlog`), MongoDB-outage drill PASS 2 of 2 (`20261107T064351Z-backlog`); the during-stall case was not run. Production routing and the owner walkthrough remain open. See [operational alert verification](operational-alert-verification.md) |
| HOR-10 | Given measured workload and accepted budgets, when instrumentation is enabled, then latency/throughput/resource/cardinality overhead stays within recorded limits. | Same-seed before/after run with p50/p95/p99, errors and resource counters. | Local part observed 2026-11-03 (one workstation, not deployed, about 7.4 to 7.5 req/s): overlay on versus off, and full telemetry versus SDK disabled (`OTEL_SDK_DISABLED=true`), showed no latency or CPU difference beyond run-to-run variation, with 0 errors in any run; peak memory was higher with full telemetry in one session only, a hint and not a result. See [Item 7 overhead baseline](#item-7-overhead-baseline-2026-11-03). Not run: cardinality, dropped-span counts, and any deployed or higher-rate measurement |
| HOR-11 | Given local config and optional exporter settings, when starting/stopping, then validated settings/resource ownership preserve current runtime contracts. | Config/Resource tests and configured format/root suite checks. | Not run |

## Performance, implementation handoff and checkpoint

Record workload seed, entity/query counts, request mix, concurrency, provider latency/failure distribution, broker load, warm-up/measurement durations, sample cardinality and retention. Compare enabled versus disabled instrumentation on the same workload with p50/p95/p99 latency, throughput, errors, CPU/memory, queue peaks, exported bytes and dropped samples. Set accepted overhead limits from that baseline before performance acceptance; this document invents no latency or spend target.

Compare existing diagnostics plus local capture with a collector and backend. Include always-on compute, sample storage/retention, trace volume, egress, operational burden and licensed/provider charges. Large benchmarks, paid monitoring and production drills need explicit scope/budget. Preserve current Bronze/Admin-report and maintenance targets in their owning specs rather than relabeling local operational tests as analytics SLO evidence.

1. Architect/Scala Developer inventory observation points and fixed-label catalog; Security validates data/retention boundaries (HOR-01–03, HOR-11).
2. Scala Developer/Data Engineer add minimal shared instrumentation only at established ports; collect Mongo/search and durable work observations without per-request scans (HOR-03–04).
3. Scala Developer validates exporter/resource bounds and retry ownership, then adds actionable local runbooks with capability owners (HOR-05, HOR-09).
4. QA runs deterministic provider/broker/worker/quarantine/deletion drills against final code and repeats affected checks after fixes (HOR-06–08).
5. Once workload and deployment decisions are resolved, measure overhead and configure production alert routing/retention with independently reviewed safe examples (HOR-09–10). This is a distinct deployed gate.
6. Run root `sbt test`, configured format checks, and relevant `IntegrationTest / test`; obtain independent Code Reviewer/Security verdicts and final QA. Preserve explicit blocked/unverified status for missing infrastructure.

### MVP observability slice

Recommended 2026-10-29 from a source review of the existing telemetry and placed in MVP scope by the user the same day: it is MVP (local) condition 7 in [development milestones](../development-milestones.md#mvp-observability-slice-decided-2026-10-29). The rest of this phase stays post-MVP. Source facts behind it:

- [GraphQLHttpRoutes.scala](../../src/main/scala/com/example/graphQL/cats/api/http/GraphQLHttpRoutes.scala) returns HTTP 200 for a result with `errors` and records `FIELD_ERROR` only in the `GRAPHQL_COMPLETED` log event. The local 5xx alert and RED dashboard therefore cannot see field-level failures (HOR-01 gap).
- The persisted-query allowlist ([PersistedQueryAllowlist.scala](../../src/main/scala/com/example/graphQL/cats/api/admission/PersistedQueryAllowlist.scala)) holds document hashes only and defaults to `off`, so operation names stay caller-supplied and remain excluded from labels. The bounded label for a GraphQL outcome metric is the schema root field name (not the alias) plus the outcome class.
- Spans exist for the HTTP server, GraphQL fields (`graphql.field.<name>`) and the Voyage client (with header propagation). There are no Mongo spans, no runtime (JVM/Cats Effect) metrics, and no trace context on outbox entries or Kafka records.
- The local alert rules have no minimum-sample guard; `histogram_quantile` and the error ratio are unstable at sparse local traffic.
- Container logging uses an `AsyncAppender` with `neverBlock=false` and a 256-entry queue: no log loss, but a slow stdout delays request handling. Record this as the current overload policy under "Sampling/bounds" rather than change it silently.

Slice order: (1) run the [local observability](../local-observability.md) first-run checklist and record observed names; (2) GraphQL root-field outcome counter (HOR-01, HOR-02); (3) Mongo operation spans and duration at the repository or driver boundary (HOR-02, HOR-03); (4) runtime metrics through the existing OpenTelemetry boundary; (5) minimum-traffic guards on local alerts, thresholds marked provisional; (6) documented `OTEL_*` sampler and exporter batch/queue bounds (HOR-05, local part); (7) overlay on/off overhead baseline (HOR-10, local part); (8) operational alert verification (HOR-09, local part): `promtool` rule tests, a local Alertmanager with two local receivers, and fault drills that follow an alert to the sink. Item 8 is specified in [operational alert verification](operational-alert-verification.md). Its local evidence is recorded after fix round 3 (one workstation: OAV-01 PASS over 25 groups on 2026-11-07; OAV-02 PASS on the final files, run of record 2026-11-07; OAV-03 PASS and OAV-04.1 PASS for the healthy-backlog case on 2026-11-07; the optional during-stall case, the release refusal path, the failed-delete path and the `--scenario all` skip path were not run). Both overlay image scans failed on fixable findings, and the project owner accepted them for local use, with per-image expiry: Alertmanager until 2026-11-20 or the first patched Alertmanager release, whichever is first; collector until 2026-11-20, adopting a patched collector being the bump follow-up (not a safety statement). The Code Reviewer, Security Engineer and Final independent QA verdicts for item 8 are PASS (2026-11-07). MVP condition 7 is met again on 2026-11-07; condition 6's final QA of the whole MVP is not met by this slice. Outbox/Kafka trace links, circuit-breaker state metrics and burn-rate SLO rules follow inside the phase. Tail sampling, metric-storage scaling, production routing and adaptive concurrency wait for deployed traffic (Phases 14–15).

MVP slice items 2–4 (implemented 2026-11-02 in `5193b81`; Code Reviewer and Security Engineer PASS at that commit; QA PASS 2026-11-03 in the condition 7 verdicts below; items 5–7 and the first-run verification of item 1 are separate):

| Item | Implementation | Unit evidence | Not verified |
|---|---|---|---|
| 2 GraphQL outcome counter (HOR-01, HOR-02) | `GraphQLOperationOutcomes` (rule in its scaladoc), `GraphQLFailureCatalog.outcomeOf`, `GraphQLOutcomeMetrics`, counted in `GraphQLHttpRoutes`. Counter `hiring.graphql.root_field.executions`, attributes `graphql.operation.type`, `hiring.graphql.root_field`, `hiring.graphql.outcome`. Counting rule: one count per distinct response key of the selected operation, labelled by schema field name; most severe error class of the key wins (internal, rejected, unauthenticated, denied, field_error); a path-less error applies to all root fields; `@skip`/`@include` fields count only if present in `data` or an error path; introspection collapses to `_introspection`, unknown fields to `_unknown`, parse/invalid/persisted-miss/missing-operation to one `_invalid` rejected count. Typed business payload refusals are not error entries and count as `success` (known limit). | `GraphQLOperationOutcomesSpec`, `GraphQLOutcomeMetricsSpec` | Metric name in Prometheus; dashboard and alert use |
| 3 Mongo operations (HOR-02, HOR-03) | `MongoCommandTelemetry`: driver `CommandListener` plus `ReactiveContextProvider` on both application clients (`hiring`, `backlog_sampler`). Histogram `db.client.operation.duration` and CLIENT spans. Chosen over a repository wrapper because the repositories reach the driver through more than 150 call sites and the listener sees each command exactly once (no double count for transactions or `getMore`). Parent = context current when the operation is subscribed; it is the request span only with `-Dcats.effect.trackFiberContext=true` (`TelemetryRuntime.contextProvider` falls back to the previous provider without it, and `TelemetryRuntime.selectContextStorage`, run first in `Main.run`, selects the standard thread-local context storage when the flag is off, because the fiber-backed storage from `otel4s-oteljava-context-storage` throws on every otel4s instrument call and on `Context.current()` without it; the histogram is recorded with an explicit root context for the same reason). No valid parent (background workers, startup, flag off): histogram only, no span. Decision (container D13): the container does not set the flag, so container Mongo operations have the duration histogram and no parented spans. | `MongoCommandTelemetrySpec` (synthetic driver events, privacy and bounds), `RuntimeMetricsSpec` (fiber-following context), `MongoCommandTelemetryFlagOffSpec` (child JVM without the flag: real client, histogram recorded, otel4s counter works, no span, no exception), `MongoCommandTelemetryIntegrationSpec` (real driver, flag on) | Trace shape in Tempo; container run |
| 4 Runtime metrics | `RuntimeMetrics` registered in `TelemetryRuntime.resource`: `opentelemetry-runtime-telemetry` 2.32.0-alpha (JVM) and `otel4s-instrumentation-metrics` 1.1.0 (Cats Effect). | `RuntimeMetricsSpec` | Overhead of the runtime metrics alone (item 7 measured the overlay as a whole) |

Dependencies added for item 4 (per [engineering quality](../engineering-quality.md)): the capability gap is JVM memory/GC/thread/CPU metrics and Cats Effect runtime and starvation metrics, which hand-written MXBean gauges would duplicate with unstable names. `io.opentelemetry.instrumentation:opentelemetry-runtime-telemetry:2.32.0-alpha` is the official OpenTelemetry Java instrumentation module (Apache-2.0), chosen as the release built against the application's OpenTelemetry API 1.66.0; `alpha` is the label of the whole instrumentation distribution, the emitted `jvm.*` names follow the stable semantic conventions, and the Java API is used in one place (`RuntimeMetrics`). The first candidate `opentelemetry-runtime-telemetry-java17` is deprecated upstream and was not used. `org.typelevel:otel4s-instrumentation-metrics:1.1.0` is the Typelevel module of the same release train as `otel4s-oteljava` (Apache-2.0). Both are isolated in `infrastructure/telemetry/RuntimeMetrics.scala` and registered once in `TelemetryRuntime.resource`.

Advisory and compatibility evidence (2026-11-02, via the OSV API `api.osv.dev/v1/query` and the GitHub Advisory Database API): no known advisories for `io.opentelemetry.instrumentation:opentelemetry-runtime-telemetry:2.32.0-alpha` (also none for its `opentelemetry-instrumentation-api`) or for `org.typelevel:otel4s-instrumentation-metrics_3:1.1.0`. The Maven POM of `opentelemetry-runtime-telemetry:2.32.0-alpha` depends on `opentelemetry-api` 1.66.0, the version the build uses. A point-in-time check; repeat before release.

Counting is bounded in work as well as labels: root-field collection expands each fragment once per document and stops at 1000 root selections (`GraphQLOperationOutcomesSpec` includes a 25-level doubling fragment chain).

#### Item 1 observed (coordinator, 2026-11-02, HEAD 5193b81)

First-run checklist run on a healthy stack with `scripts/generate-local-traffic.sh 120`; local evidence for one run, not a load or deployed result.

| Check | Observed |
|---|---|
| Services | all up, api healthy; Prometheus target `otel-collector` up |
| HTTP metric | `http_server_request_duration_seconds_count` exists with `http_request_method` and `http_response_status_code`; GET 237 all 200; POST 226 x 200 and 226 x 400 (the script's deliberate `{ notAField }`). No `http_route` label. Follow-up (item 5): the series also carry `classifier` (the bounded route template) and `http_phase` (`headers` without status code, `body` with it) |
| Backlog | `hiring_backlog_pending` 0 for `embedding_work` and `operational_event_outbox`; `hiring_backlog_sample_age_seconds` present; no oldest-age series with an empty backlog |
| Mongo stop (about 07:52Z) | both backlog series disappeared (no flat zero); `HiringBacklogUnobserved` pending 07:53:45Z, firing 07:55:48Z |
| Traces | Tempo has `POST /graphql` and `GET /health` traces for `hiring-api` |
| Logs | Loki `{service_name="api"}` 718 lines; 0 matches for JWT/bearer, password, email, Mongo URI, raw GraphQL text; masking enabled |
| Alerts | all four rules load, health ok |
| New names (now observed) | `hiring_graphql_root_field_executions_total{graphql_operation_type,hiring_graphql_root_field,hiring_graphql_outcome}` (seen `_invalid`/`rejected` 226, `_introspection`/`success` 226); `db_client_operation_duration_seconds_{bucket,count,sum}{db_operation_name,db_collection_name,hiring_mongo_client}`; `jvm_*`; `cats_effect_runtime_cpu_starvation_*`; `cats_effect_runtime_wstp_compute_*` |

The "Not verified" cells of items 2–4 for Prometheus names are thereby closed (names observed); trace shape of Mongo spans in Tempo, container run overhead and dashboard/alert use under real GraphQL error traffic remain open.

#### Item 5 alert hygiene (implemented 2026-11-02, static and live-query validation only)

- Why `http_route` is missing: the http4s otel4s server metrics middleware 0.19.0 has no route attribute. `OtelMetrics` labels each sample with the `classifier` string passed to `Metrics(...)`, and `TelemetryRuntime` already passes `routeLabel`, which returns only a path from the fixed public list (`HiringHttpPaths.public`) or `_unmatched`. Chosen: change the dashboard panel to the existing bounded label `classifier`; no application change (an extra `http.route` attribute would duplicate the classifier for no benefit).
- Second defect found while verifying: every request is recorded twice, once at response headers (`http_phase="headers"`, no status code) and once at body completion (`http_phase="body"`). The old alert and dashboard expressions counted both, doubling request rates and the error-ratio denominator and mixing header-time durations into latency. All HTTP expressions now select `http_phase="body"`.
- `HiringApiHighErrorRate` and `HiringApiSlowRequests` are evaluated only when the body request rate excluding `/health` probes (`classifier!="/health"`) over the 1 m window is at least 0.1 req/s (6 per minute). Probes are excluded from the guard and from the ratio and quantile aggregates because idle probes alone measured 0.109 req/s and would pass the floor and dilute both. Below that floor the expression returns no data, so sparse traffic neither fires nor reports healthy; the dashboard panels still draw whatever data exists. Thresholds (5% 5xx, p95 300 ms, backlog age 300 s) and the floor are provisional local values, not SLOs. The [item 7 baseline](#item-7-overhead-baseline-2026-11-03) observed client-side p95 of 22–38 ms at about 7.4 req/s, far below 300 ms, but it does not calibrate the threshold: calibration needs an accepted workload and is not done. `HiringBacklogUnobserved` is deliberately unguarded. **Accepted limitation:** below the guard (6 non-probe requests per minute) a total 5xx outage does not fire; this is a provisional local trade-off to avoid flapping on sparse traffic, to be revisited with a measured workload. Dashboard panels for 5xx ratio, latency and status codes also exclude `/health`; the route panel keeps all routes as the total-requests view.
- Dashboard `hiring-api-red.json` (version 2): per-route panel uses `classifier`; new panel "GraphQL root-field outcomes" (`sum by (hiring_graphql_outcome, hiring_graphql_root_field)` of the counter rate) and "Mongo command duration p95 by operation" (`db_operation_name`, `hiring_mongo_client="hiring"`). Both draw only closed label sets.
- No alert on GraphQL `internal`/`denied` outcomes: this specification does not require one, and a threshold without a measured baseline would be invented.
- Validation: the dashboard JSON parses; every alert and panel expression was parsed and run through the Prometheus HTTP query API of the running stack (all `success`; observed after the probe exclusion at idle, with only `/health` traffic: `/health` body rate 0.109 req/s, non-probe rate 0, the `>= 0.1` guard returns no data, and both guarded alert expressions return no data). `promtool check rules` and `promtool test rules` pass on 2026-11-07 after fix round 3 (8 rules, 25 test groups, [operational alert verification](operational-alert-verification.md) OAV-01); before that, the expressions were checked only by the live Prometheus query API.

#### Sampling/bounds (local policy, item 6)

Set in `compose.observability.yaml` for the `api` service through standard `OTEL_*` variables (read by the OpenTelemetry SDK autoconfiguration under otel4s). Local values; none is a production or SLO value. The overlay as a whole (these bounds included) was measured against the overlay-off configuration at one local workload ([item 7](#item-7-overhead-baseline-2026-11-03)); no individual setting was varied, so the cost of any single bound is unknown.

| Setting | Value | Effect when exceeded |
|---|---|---|
| `OTEL_TRACES_SAMPLER` | `parentbased_always_on` | 100% of root requests sampled locally; an incoming parent's decision is honored. No tail sampling. |
| `OTEL_BSP_MAX_QUEUE_SIZE` | 2048 spans | The span queue is non-blocking: when full, new spans are dropped; requests are never delayed. No dropped-span counter has been observed in this setup (known gap). |
| `OTEL_BSP_MAX_EXPORT_BATCH_SIZE` | 512 | Batch of at most 512 spans per export. |
| `OTEL_BSP_SCHEDULE_DELAY` | 2000 ms | Interval between span batch exports. |
| `OTEL_BSP_EXPORT_TIMEOUT` | 10000 ms | A span batch whose export has not completed is abandoned; its spans are lost, not retried by the processor. |
| `OTEL_EXPORTER_OTLP_TIMEOUT` | 10000 ms | One OTLP request (traces or metrics) is cancelled after 10 s. |
| `OTEL_METRIC_EXPORT_INTERVAL` | 5000 ms (unchanged) | Metrics are aggregated in memory by instrument and attribute set (closed sets), so there is no metric queue to overflow; a failed export loses that interval's export, and cumulative values recover at the next success. |

Overload policy, in order of what is dropped: (1) spans first (queue full or export timeout) - diagnostics only, hiring work unaffected; (2) a failed metric export interval - counters and histograms are cumulative, so only resolution is lost; gauges (backlog) go stale and then disappear through the collector `metric_expiration` of 45 s, which `HiringBacklogUnobserved` reports as unobserved, not as zero; (3) logs are never dropped: the container Logback `AsyncAppender` has `neverBlock=false` and a 256-entry queue, so when stdout (and the Alloy/Loki path behind it) is slower than the log rate, the logging call blocks and request handling is delayed instead of losing log lines. That is the current policy and is kept deliberately; changing it (`neverBlock=true`, loss counters) needs its own decision. Durable hiring work (outbox, embedding work) is never governed by telemetry queues. The exporter being down never fails a request: exports run on SDK background threads.

Observed running (2026-11-03, item 7): with these `OTEL_*` bounds the API started healthy under the overlay in runs 2 and 3 and served the measured workload with 0 failed or 5xx requests. Dropped-span visibility needs SDK self-metrics, which are not scoped, so span loss under overload is not measured.

#### Item 7 overhead baseline (2026-11-03)

Decision: the local overhead of the observability telemetry is recorded as the difference between the same seeded workload with telemetry off and on, on one workstation, in two comparisons run on 2026-11-03:

- Session 1: overlay off versus overlay on. The "off" configuration still runs the OpenTelemetry SDK, so it is not instrumentation-free (see the scope note below).
- Session 2: SDK disabled (`OTEL_SDK_DISABLED=true`) versus full telemetry. This is the instrumentation-off comparison.

Result: in both sessions, no latency or CPU difference distinguishable from run-to-run variation, and 0 errors in any run. Peak container memory was higher with full telemetry in session 2 (both pairs) and not in session 1; memory is a hint, not a result. No overhead limit is set from these runs. Why: the runs are the local part of HOR-10 and give the first observed numbers for telemetry under load; a limit set from runs at this rate would be invented. Why it matters: on this workstation, telemetry shows no measurable latency or CPU cost at about 7.4 to 7.5 req/s. That result does not extend to higher rates, other hardware, or deployed traffic.

Evidence and environment (git-ignored local output, not reproduced here):

- Date 2026-11-03; HEAD `b4e216b` plus uncommitted measurement work (`scripts/seed-local-jobs.sh`, `scripts/seed-local-jobs.py`, `scripts/traffic-lib.sh`, and `generate-local-traffic.sh --mix search`). Code Reviewer and Security Engineer passed these scripts on re-review. Evidence directories `.local/logs/overhead/20261103T132633Z` (session 1) and `.local/logs/overhead/20261103T140057Z` (session 2, same workstation and image).
- One developer workstation running Linux. The `api` container (`compose.yaml`, 2 CPUs, 1 GiB memory limit) and the `mongodb` replica set with persisted local data. The overlay (`compose.observability.yaml`: otel-collector, Tempo, Loki, Alloy, Prometheus, Grafana) runs on the same host. Kafka is off (`KAFKA_ENABLED=false`). A measurement-only compose override seeded the singleton Admin in session 1, because sign-up requires it. For the session 2 SDK-disabled runs, a measurement-only override of the `api` service set `OTEL_SDK_DISABLED=true`; the API started healthy with it.
- Dataset (both sessions; `seed-local-jobs.sh`, idempotent): 1 synthetic recruiter, 8 synthetic candidates, 50 open synthetic jobs.
- Workload (both sessions): `generate-local-traffic.sh 300 --mix search --concurrency 8` with the 8 candidate tokens. Each worker alternates an authenticated `jobs(first: 20, filter: {skills: ["perfseed"]})` and `job(id:)`, paced to 80% of the per-actor query cost budget. Each `jobs` call records a search session, so the database grows during each run. This is not the condition 2 acceptance script.
- Procedure per run: clean restart of the configuration, seed, 60 s warm-up (not recorded), 300 s measured run, and API container CPU and memory sampled with `docker stats --no-stream` every 5 s (50 samples). Run order in both sessions: off, on, on, off (session 2: SDK disabled, full telemetry, full telemetry, SDK disabled).
- Latency is in ms, nearest-rank, successful requests only.

Session 1 (overlay off versus on):

| Run | Overlay | Successful requests | Req/s | p50 | p95 | p99 | CPU mean | CPU max | Memory max |
|---|---|---|---|---|---|---|---|---|---|
| 1 | off | 2211 | 7.37 | 12.4 | 38.4 | 60.7 | 14.4% | 31% | 392 MiB |
| 2 | on | 2233 | 7.41 | 11.1 | 35.8 | 69.4 | 15.1% | 38% | 338 MiB |
| 3 | on | 2260 | 7.52 | 10.3 | 22.1 | 36.4 | 14.2% | 72% | 346 MiB |
| 4 | off | 2260 | 7.51 | 9.8 | 21.7 | 25.7 | 11.1% | 32% | 361 MiB |

Session 1 means of the two runs per configuration: overlay off p50 11.1, p95 30.0, p99 43.2 ms, CPU mean 12.8%; overlay on p50 10.7, p95 29.0, p99 52.9 ms, CPU mean 14.7%.

Session 2 (SDK disabled versus full telemetry; evidence `20261103T140057Z`):

| Run | Configuration | Requests | Req/s | p50 | p95 | p99 | CPU mean | CPU max | Memory max |
|---|---|---|---|---|---|---|---|---|---|
| 1 | SDK disabled | 2254 | 7.48 | 9.7 | 21.7 | 32.5 | 14.0% | 30% | 354 MiB |
| 2 | full telemetry | 2256 | 7.52 | 9.4 | 19.3 | 25.2 | 13.1% | 28% | 375 MiB |
| 3 | full telemetry | 2259 | 7.53 | 10.3 | 22.8 | 28.1 | 12.5% | 67% | 368 MiB |
| 4 | SDK disabled | 2266 | 7.53 | 9.6 | 19.6 | 24.0 | 12.0% | 37% | 351 MiB |

Session 2 means of the two runs per configuration: SDK disabled p50 9.65, p95 20.7, p99 28.3 ms, CPU mean 13.0%; full telemetry p50 9.85, p95 21.0, p99 26.7 ms, CPU mean 12.8%.

CPU percentages are of one core as `docker stats` reports them, against a 2-CPU container limit.

Every run in both sessions had 0 4xx, 0 429, 0 5xx, 0 failed requests and 0 GraphQL errors, and each exceeded the 1000 successful requests that the local procedure aims for before p99 is read as reliable.

Reading the result (session 1):

- Same-configuration p95 varied from 21.7 to 38.4 ms (overlay off) and from 22.1 to 35.8 ms (overlay on). The configurations overlap, so the p95 difference is not an overhead signal. The runs also got faster over the session regardless of configuration.
- Overlay-on CPU mean was about 2 points higher, within the noise. Run 3 reached a 72% CPU maximum; the summary does not show how sustained that peak was, and it is not attributed to telemetry.
- Throughput (7.37–7.52 req/s) is set by the pacing budget and is not an overhead signal.

Reading the result (session 2):

- Full telemetry versus SDK disabled: the means are 9.85 and 9.65 ms at p50, 21.0 and 20.7 ms at p95, 26.7 and 28.3 ms at p99, and CPU 12.8% and 13.0%. The per-run values overlap across the two configurations at p50, p95 and p99, so the differences are not an overhead signal.
- Memory: the peak was higher with full telemetry in both pairs (368 and 375 MiB, against 351 and 354 MiB with SDK disabled). Session 1 showed the opposite direction. The memory difference has no latency or CPU counterpart, so it is a hint for a later measurement, not a result.
- CPU max: run 3 (full telemetry) reached 67%, the highest value in session 2. The sampling does not show how long the peak lasted, and it is not attributed to telemetry.
- Throughput (7.48–7.53 req/s) is pacing-bound and is not an overhead signal.

What the runs do not measure (scope caveat): the API always starts the OpenTelemetry SDK (`OtelJava.autoConfigured` in `src/main/scala/com/example/graphQL/cats/infrastructure/telemetry/TelemetryRuntime.scala`). In session 1 the "off" configuration kept it running: spans and metrics were still recorded (parent-based, always-on sampler by default) and exports to the default localhost endpoint failed, so that comparison is overlay on versus overlay off with the SDK running, not an instrumentation-free baseline. Session 2 set `OTEL_SDK_DISABLED=true`, where the autoconfigured SDK returns no-op providers (no spans, metrics or exports); its comparison is SDK disabled versus full telemetry (instrumentation, 100% parent-based sampling, export to a live collector, backends on the same host). Neither session varied an individual setting or telemetry component, so the cost of any single bound or component is unknown.

Limits: one workstation; about 7.4 to 7.5 req/s, pacing-bound; small dataset; two sessions of four 300 s runs each; local evidence only, not a deployed or production result, and not a basis for SLOs. The 300 ms alert threshold is not calibrated from these runs. No dropped-span counter was observed, so span loss is unknown. The checkpoint verdicts below do not cover the session 2 results.

Remaining for HOR-10: a higher-rate workload before any limit is set, cardinality and dropped-span measurement, and deployed overhead (separate gate).

Checkpoint (2026-11-03). The MVP observability slice (items 1–7, MVP condition 7) is closed with these independent verdicts:

| Scope | Code Reviewer | Security Engineer | QA |
|---|---|---|---|
| Items 2–4 (`5193b81`) | PASS | PASS | PASS 2026-11-03 |
| Items 1, 5, 6 (`097c236`) | PASS | PASS 2026-11-03 (local workstation scope; low notes: log privacy depends on application masking staying on; no dropped-span counter) | PASS 2026-11-03 |
| Item 7 measurement scripts and evidence (2026-11-03) | PASS on re-review | PASS on re-review | PASS 2026-11-03 |

QA evidence: the slice's unit specs 45/45 passed; `MongoCommandTelemetryIntegrationSpec` passed through `scripts/run-local-tests.sh` (all 1824 root unit tests also passed in that run); `scripts/tests/local-traffic-options.sh` 127 checks passed; both dashboards parse; compose OTEL values match the Sampling/bounds table; the item 7 numbers and means recompute. The 2026-11-03 QA did not run `promtool`; the alert expressions then rested on the live Prometheus query validation recorded under item 5. `promtool` checks were added under item 8; the run of record is 2026-11-07 (8 rules, 25 test groups, PASS), and 2026-11-06 was an earlier run. The rest of this phase (HOR acceptance rows beyond the local parts, outbox and Kafka trace links, burn-rate rules, production routing) stays post-MVP and is not covered by these verdicts.
