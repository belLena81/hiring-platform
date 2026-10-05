# Hiring Observability and Resilience

## Identity and scope

- Roadmap owner: Phase 12, operational observability and resilience.
- Status: **draft**. Signal boundaries and local failure scenarios are defined; production alert thresholds, operator routing, exporter deployment and resource budgets require measured decisions.
- Coordinator: Product Manager. Scala Developer owns instrumentation/runtime changes; Data Engineer owns Mongo query measurements; Big Data Engineer owns broker/workflow measurements. Independent Security Engineer, Code Reviewer and QA review final implementation.
- Outcome: operators can identify whether a hiring request failed, durable background work is delayed, or evidence is missing, and can recover safely with an actionable runbook.
- Authorized current work: specification. Non-goals: production monitoring procurement, frontend dashboards, Node tooling, general-purpose event bus, logging hiring content, new automatic replay authority, or claiming resilience from a dashboard alone.
- Dependencies: accepted operational behavior from [Mongo/vector access](mongodb-vector-retrieval-optimization.md), [search/embedding](hiring-search-enhancements.md), [discovery](hiring-discovery-search-quality.md), and [durable workflows](durable-hiring-workflows.md).
- Existing security, telemetry and failure controls remain mandatory before this phase. Existing analytics maintenance/activation gates cannot be postponed into observability work.

## Verified source baseline

| Source | Current fact / remaining need |
|---|---|
| [TelemetryRuntime.scala](../../src/main/scala/com/example/graphQL/cats/infrastructure/telemetry/TelemetryRuntime.scala) | Resource-owned OpenTelemetry setup and HTTP metrics/tracing. Known paths are allowlisted, other routes become `_unmatched`, and query strings are redacted. It does not establish the complete Mongo/provider/workflow signal catalog below. |
| [Diagnostics.scala](../../src/main/scala/com/example/graphQL/cats/service/Diagnostics.scala) | Closed event/field enums, public-value validation and classified failures; `emit` suppresses diagnostic errors. Preserve this application boundary. |
| [SafeDiagnostics.scala](../../src/main/scala/com/example/graphQL/cats/infrastructure/logging/SafeDiagnostics.scala) | Structured sanitized output, controlled sensitive metadata, bounded encoded lines and no arbitrary exception-message forwarding. Packaged local destination remains `_logs`; new diagnostic artifacts belong in `.local/logs/`. |
| [OperationalTelemetryService.scala](../../src/main/scala/com/example/graphQL/cats/service/events/OperationalTelemetryService.scala) | Authorized hiring interaction/search event use cases. These business records are distinct from runtime metrics and must not be silently weakened to best-effort export. |
| [OperationalEventKafkaRuntime.scala](../../src/main/scala/com/example/graphQL/cats/infrastructure/kafka/OperationalEventKafkaRuntime.scala) | Resource-owned publisher/consumer and durable receipt/quarantine operations; broader lag/repair observability is a target. |
| [EmbeddingCapability.scala](../../src/main/scala/com/example/graphQL/cats/runtime/EmbeddingCapability.scala), [EmbeddingPipeline.scala](../../src/main/scala/com/example/graphQL/cats/service/search/EmbeddingPipeline.scala) | Optional embedding capability, existing client/worker lifecycle and durable processing. Instrument this ownership rather than starting another worker. |

[Runtime safety](hiring-runtime-safety.md), [mutation reliability](mutation-reliability.md), [logging](../logging.md), and [engineering quality](../engineering-quality.md) remain baseline references. Historical acceptance there is not evidence that the requirements below pass.

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
- No public GraphQL/event schema change is required by basic instrumentation. Any later exposed diagnostic/repair API follows single-active-shape pre-MVP contract updates and authorization review. Any new operational Mongo metrics summary/index needs its own repeatable migration and query-cost justification.

## Decisions, alternatives and readiness

| Decision | Alternatives / default direction | Evidence and owner | Blocked work |
|---|---|---|---|
| Export destination | Reuse existing OpenTelemetry boundary with local/no-op capture; self-managed collector/backend versus managed service for deployment. | Operations + Security compare access, data residency, retention, egress, idle/storage cost and maintenance. | Production exporter credentials, provisioning and routing. |
| Alert evaluation | Local scripted/runbook checks first; backend alert rules after deployment selection. | Operations demonstrates durable routing, no-data handling and receiver availability. | Production incident delivery/signoff. |
| Thresholds/windows | Derive from accepted workload/SLO and recovery budget; do not pick arbitrary default latency percentiles. | PM + QA + capability owner measure normal/burst/outage baseline. | Production alert tuning and claimed SLO acceptance. |
| Sampling/bounds | Finite queue and bounded diagnostic sampling; always preserve safe aggregate failure counts when feasible. | Scala Developer/QA measure request overhead, memory, loss and cardinality budget. | Final instrument configuration. |
| Repair exposure | Reuse workflow-owned restricted CLI/Admin boundary; no automatic replay from alert transport. | Security + workflow owner complete DHW repair contract. | Actionable remote repair integration. |
| Local log alignment | Keep known `_logs` baseline visible; align to `.local/logs/` in a scoped configuration slice. | Runtime owner verifies packaged/default/override behavior and docs. | Log-path modification only. |

A choice of monitoring backend or alert service is not approved here. No monitoring frontend or Node toolchain is in scope.

## Acceptance and evidence

Implementers assign capability-named test/source paths when the relevant slice is ready. All rows currently mean **Not implemented / Not run**; source inspection is not runtime acceptance.

| ID | Given / When / Then | Planned verification | Actual outcome |
|---|---|---|---|
| HOR-01 | Given success/rejection/failure/cancellation, when instrumenting, then each logical outcome is classified without changing values/errors/cancellation. | Service/decorator tests using actual Cats Effect cancellation and typed results. | Not run |
| HOR-02 | Given arbitrary paths/operation names and sensitive inputs, when exporting logs/spans/metrics, then allowlists, record-size and label cardinality bounds hold with no sensitive payload. | Capturing-sink redaction and bounded-cardinality tests. | Not run |
| HOR-03 | Given Mongo/search/provider/broker dependencies, when one fails, then capability state and observation freshness distinguish failure, optional degradation and no data. | HTTP/GraphQL integration and fake-clock sampling tests. | Not run |
| HOR-04 | Given durable pending work across restart, when measuring age/progress, then its age survives restart and attempts are not reported as completed work. | Replica-set/worker restart and publisher integration. | Not run |
| HOR-05 | Given slow/unavailable telemetry sink, when request load and shutdown occur, then memory/queue/drain bounds hold and business results remain intact. | Bounded load + exporter fault/resource tests. | Not run |
| HOR-06 | Given provider or broker outage, when restored, then accepted durable work converges and the runbook/alert correctly reports recovery. | Local provider/Kafka outage drills with durable before/after counts. | Not run |
| HOR-07 | Given worker kill/cancel/stale claim, when resumed, then no duplicate/lost effects arise and overdue/repair signals match durable state. | Workflow and embedding failure scenarios; DHW evidence links. | Not run |
| HOR-08 | Given quarantined work and deleted subjects, when authorized repair/replay occurs, then scope/deletion guards hold and no raw payload leaks. | Forbidden repair, replay/deletion race and quarantine-drill tests. | Not run |
| HOR-09 | Given each alert and no-data/recovery condition, when exercised, then an owner follows the documented action and evidence confirms resolution. | Independent runbook walkthrough with injected faults; production routing remains separately gated. | Not run |
| HOR-10 | Given measured workload and accepted budgets, when instrumentation is enabled, then latency/throughput/resource/cardinality overhead stays within recorded limits. | Same-seed before/after run with p50/p95/p99, errors and resource counters. | Not run |
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

Checkpoint: specification drafted; no implementation acceptance completed. Future tests/drills listed here have not run. No exporter backend/provider was provisioned. Next action: independent specification review, then finalize the instrument catalog for the first slice. Future Code Reviewer: pending. Future Security Engineer: pending. Future QA: pending. Documentation-delivery evidence and verdicts are tracked separately in the [roadmap readiness index](../development-milestones.md#specification-ownership-and-readiness).
