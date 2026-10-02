# Development Milestones

## Hiring Analytics Lakehouse (Phase 6)

The current analytics milestone is documented in [Hiring Analytics Lakehouse](specs/hiring-analytics-lakehouse.md). It introduces a local bounded Bronze/Silver/Gold batch pipeline, guarded Admin reporting, and a retention-aware erasure worker. Remaining acceptance and runtime gates are tracked in the spec. The roadmap continues through Structured Streaming, then Search Evaluation & Scale, then the post-MVP Discovery Intelligence & Search Quality stage. These are separate milestones; this addition does not change the analytics or event milestones.

Local implementation completion requires source-to-projection evidence, data-quality checks, simulated-clock coverage of retention gate edges, isolated shortened-horizon physical cleanup evidence, independent code/security reviews, and final QA. It does not wait for real calendar retention. Delayed and in-flight outbox replay must be denied by implementation and independently verified; it is not an accepted residual risk.

The account-deletion workflow is Phase 6's cross-system saga candidate. Keep the existing durable receipt, erasure phases, and MongoDB transaction; finish forward recovery across publisher fencing, Kafka retention, Delta erasure, and guarded report publication. The account tombstone and elapsed retention cannot be compensated. Local implementation closure requires restart and failure recovery evidence, an operator-visible path for work that cannot progress, and the lakehouse specification's accelerated physical and simulated-time checks. Real deployed retention, writer exclusion, guarded key retirement, and operational signoff remain prerequisites for production Phase 7 activation. Isolated synthetic test activation requires independently reviewed local evidence with actual shortened physical retention and explicitly simulated calendar boundaries. Do not add a generic saga framework to close this milestone.

## Continuous Hiring Analytics (Phase 7)

Phase 7 implementation is underway in [Continuous Hiring Analytics](specs/continuous-hiring-analytics.md), including the opt-in query/runtime composition, durable streaming journal, and late-fact retention path. Runtime acceptance is not established. Local Phase 6 closure supports isolated synthetic test activation after HAL-01–HAL-14 evidence and independent signoff. Production activation additionally requires genuine deployed Kafka/Delta horizons, external-writer exclusion, guarded HMAC retirement, and operational signoff. The configurable 10-second trigger supports UC11's Bronze p95 under 30 seconds target but does not prove it; both Bronze availability and the 120-second report freshness p95 need workload measurement.

Current local status: observation regression, full analytics unit and executed integration gates pass; six ignored integration bodies are excluded. Graceful TERM/INT and real short-grant expiry proofs pass within their source scopes. The latest complete workload still fails Bronze freshness, although report freshness and backlog meet their limits. Later automatic scenarios, abrupt process recovery, maintenance progress and independent final runtime QA remain pending.

Existing Phase 7 recovery and correctness requirements remain acceptance gates. Further recovery-policy maintainability work is scheduled for Phase 10 and does not defer or weaken Phase 7 acceptance.

### Streaming acceptance refinements

Complete the following bounded work against the existing HCS criteria in the streaming specification. These are planned refinements and validation requirements, not implementation or runtime acceptance claims. Keep source hashes, workload results, and criterion-level evidence in that specification rather than duplicating its execution history here.

1. **Measure and reduce batch cost (HCS-13).** Capture stage durations, Spark action/job counts, rows/files scanned, shuffle/spill, mutex wait, and process resource use. Profile repeated admission analysis and retained-store reads, plus the Silver-to-Gold rebuild, before selecting an optimization. Preserve refreshed deletion checks, durable journal ordering, publication guards, and acknowledgement semantics; do not reuse cached admission across sink mutations. Validate on the same bounded local workload: 500 committed records/minute across three partitions for at least 15 minutes, Bronze availability p95 under 30 seconds, Admin report freshness p95 at most 120 seconds, and peak backlog at most 500. Then execute the bounded burst and backlog recovery checks. A shorter trigger or a component speedup alone is not acceptance.
2. **Finish process recovery evidence (HCS-07, HCS-08, HCS-12).** Actual forked TERM and INT cleanup proofs pass on source 3a6b; real 120-second immutable-grant expiry passes independently on source 95952ad, including original driver/query shutdown and callback/restart denial. Abrupt process termination at durable ingestion/publication boundaries and subsequent restart remain pending. Exercise the documented operator recovery of persistent mutex ownership after verifying all old owners are stopped. Assert convergence without lost acknowledged input, duplicated report contributions, premature watermark advancement, or restored deleted subjects. Verify checkpoint loss/corruption and identity drift fail closed. Retain the existing no-expiry/no-automatic-takeover ownership policy; graceful shutdown and injected adapter failures do not substitute for process-loss evidence.
3. **Complete continuous correctness scenarios (HCS-01–HCS-06, HCS-09, HCS-10).** Run actual committed/aborted Kafka transactions, duplicates, accepted offset gaps, out-of-order/future events, idle periods, closed-day arrivals, bounded replay, and deletion races through the running process to the authorized Admin report. Preserve original late-fact expiry and suppression rules. Keep component-test evidence distinct from continuous execution, real-clock grant expiry, and final independent QA.
4. **Expose maintenance progress (HCS-11–HCS-13).** Record last successful maintenance time, consecutive deferred ticks, oldest pending cleanup, and storage growth using sanitized metadata. Define a maximum acceptable maintenance delay within the existing retention requirements and demonstrate cleanup progress under healthy load and backlog. If contention prevents that bound, repair scheduling within the existing ownership boundary before acceptance. Broader dashboards and alert routing may follow in Phase 10; evidence that maintenance progresses belongs here.
5. **Close the evidence record.** Reconcile the specification's status and HCS table with terminal results on the reviewed source, including scoped checkpoint/privacy and registered-batch-denial evidence. Keep genuine deployed retention, external-writer exclusion, guarded key retirement, and operational signoff as production activation prerequisites. Accelerated local proofs cannot close those prerequisites.

Sequence: complete shutdown ownership, instrument and satisfy healthy-load targets, finish burst/recovery/live scenarios, then obtain final independent reviews and QA. Independent focused checks may run earlier; the acceptance sequence does not defer correctness fixes.

The rationale follows Martin Kleppmann, *Designing Data-Intensive Applications*, first edition (2017): workload and latency measurement (pp. 11–17), fencing and stale owners (pp. 301–304), event-time/late-window policy (pp. 468–471), and recovery/idempotence across processing boundaries (pp. 476–479). The book informs these refinements; the project specification owns their exact contracts.

## Search Evaluation (Phase 8)

This phase adds bounded maintainability work alongside reproducible search relevance evaluation. Extract the shared job/candidate embedding workflow as a functional template and pipeline: pure preparation computes the searchable source and source hash once, while an effectful persistence callback performs the guarded write. Preserve version-guarded writes, retry behavior, document limits, and worker/resource ownership. Validate embedding freshness, stale-write conflicts, and retries. This cleanup does not establish search-quality or performance results.

Evaluate the existing ranking alternatives against the reproducible relevance dataset and baseline. Keep application ranking deterministic and pure, and keep MongoDB query execution in adapters. Verify repeatable result ordering, including ties, across the evaluated alternatives. Add a Strategy abstraction only when a second concrete ranking algorithm needs a shared selection boundary; evaluation alone does not justify a generic framework.

## Scale (Phase 9)

Extend the accepted Phase 7 workload into a measured capacity envelope. Record dataset size and age, event rate, partition/skew distribution, concurrency, warmup, duration, p50/p95/p99 latency, throughput, errors, backlog drain time, file growth, CPU, heap, spill, and I/O. Compare before/after results under the same environment and workload. Phase 7's existing freshness and backlog targets must pass before this broader tuning; they are not deferred here.

Adopt incremental report recomputation, file-layout/compaction changes, or additional parallelism only when physical plans and measurements identify a bottleneck. Compare the smallest suitable streaming footprint with scheduled batch where business freshness permits, including idle compute, storage/retention, I/O, and operating effort. Preserve shared ownership, deletion, replay, and publication contracts. Large benchmarks, paid infrastructure, new persistent schemas, or a processing-engine replacement require separate scope; this plan does not authorize them.

## Observability & Resilience (Phase 10)

Further refine streaming recovery by extracting recovery and revision decisions into pure functions over immutable observations. The coordinator retains effect execution. Preserve authorization, journal ordering, deletion checks, publication fencing, watermark commits, and acknowledgement rules. Validation must cover restart, deletion, and publication races; this refinement is maintainability work and does not replace Phase 7 runtime acceptance.

Add bounded independent report reconciliation, following DDIA's auditing guidance (pp. 530–531). Start with small synthetic fixtures, then a separately authorized retained-input slice: pin its input coordinates, observation time, policy/code identity, deletion view, and published generation so concurrent changes cannot produce a misleading comparison. Independently calculate expected report cells and compare them with the matching publication, accounting for duplicates, quarantine, late facts, expiry, deletion, and suppression. Detect deliberately introduced mismatches and reproduce a matching result without copying the production aggregation as the sole oracle. Respect retained-data limits and current deletion controls; refuse unavailable or incompatible evidence rather than reconstructing erased data. Keep verification isolated from publication and never bypass the permanent ordinary-batch denial on streaming lakehouses. This is a new audit capability, not deferred Phase 7 correctness testing.

Build actionable alerts and operator procedures around the Phase 7 measurements: stale report age, sustained lag, maintenance age, quarantine growth, and unresolved recovery work. Define thresholds, owners, and bounded diagnostic output from measured behavior. Any recurring reconciliation needs an explicit cadence and CPU/I/O budget; do not add an always-on scheduler or new audit store merely to implement the first verifier.

Conditional patterns for this phase:

- **Decorator:** extract shared instrumentation only when multiple provider adapters need it; preserve error semantics and cancellation, and do not duplicate retries.
- **Observer:** use managed streams for new local notifications; retain durable handoffs for work that requires restart recovery.
- **Bridge:** consider only when an integration has two actual, independently varying dimensions.

## Production Hardening (Phase 11)

Consolidate deployed recovery and retention evidence: genuine Kafka earliest-offset passage, Delta data/log reclamation, exclusion of unmanaged writers, guarded key retirement and new-key-only restart, and operator recovery drills. Validate restoration under current deletion controls so an older backup cannot restore erased subjects to reports. Preserve distinct outcomes for local synthetic checks, actual elapsed retention, and deployed operational signoff. These production prerequisites apply whenever production streaming is proposed, including before Phase 11; their placement here does not authorize earlier activation or weaken Phase 6/7 gates.

## Discovery Intelligence & Search Quality (Post-MVP, Phase 12)

Adopt richer typed search criteria alongside discovery requirements, preserving database-side filtering, authorization, and bounded retrieval. Add recursive Composite criteria only if nested Boolean expressions become an accepted requirement. Add personalized or other ranking strategies only when evaluation demonstrates a benefit.

## Interview Scheduling and Workflow Resilience (Phase 10)

Add interview scheduling as a scoped hiring feature when an external calendar integration is introduced. The current `moveApplicationToInterview` status/history write remains a MongoDB transaction. Coordinate the new calendar reservation and candidate/recruiter notifications through a durable application-owned saga with idempotent steps, restart recovery, bounded retries, and observable failure state. Reserve the slot before committing the guarded status change; release it if that change fails. Send notifications after the status commits, and retry or reconcile uncertain delivery because a delivered notification cannot be recalled. Preserve authorization and the permitted application-status transitions.

Acceptance requires evidence that a failed status change releases its reservation, replay cannot create duplicate calendar reservations, an interrupted workflow resumes after restart, and unresolved or uncertain notification deliveries are visible for repair. Use provider idempotency or reconciliation before claiming duplicate-free notification delivery. The calendar provider and notification integration are new feature scope, not part of the existing status mutation.

## Discovery Intelligence & Search Quality (Post-MVP)

This stage extends the existing structured, semantic, and hybrid job search after Search Evaluation & Scale. It does not change the MVP use cases. Proposed capabilities are radius-based job discovery using caller-supplied structured coordinates, richer Atlas lexical search and facets, workload-based MongoDB query-plan and index tuning, and personalized ranking.

Evaluation comes before ranking changes: establish relevance measures and a baseline with bounded, representative data, then compare any personalized ranking against that baseline. Personalization is adopted only when evaluation supports a quality improvement; otherwise, its outcome can be deferred.

Acceptance gates:

- Radius search combines distance with existing filters, preserves candidate visibility rules, and returns deterministic cursor pagination. Coordinates are structured input; geocoding is out of scope.
- Atlas lexical, vector, and hybrid search enhancements are validated in an Atlas-capable environment.
- Index changes have representative query-plan evidence and measured workload results that include read benefits and write/storage tradeoffs.
- Personalized ranking is evaluated against the established baseline before adoption.

These conditional extensions do not change the current search contract. Atlas Search/Vector Search work remains subject to the Atlas-capable validation and cost boundaries above.

Atlas is required for this stage's Atlas Search and Vector Search work. Local MongoDB Community remains appropriate for core workflows and local transaction tests. This roadmap change includes no Atlas provisioning or spend.
