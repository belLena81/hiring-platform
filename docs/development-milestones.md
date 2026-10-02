# Development Milestones

## Hiring Analytics Lakehouse (Phase 6)

The current analytics milestone is documented in [Hiring Analytics Lakehouse](specs/hiring-analytics-lakehouse.md). It introduces a local bounded Bronze/Silver/Gold batch pipeline, guarded Admin reporting, and a retention-aware erasure worker. Remaining acceptance and runtime gates are tracked in the spec. The roadmap continues through Structured Streaming, then Search Evaluation & Scale, then the post-MVP Discovery Intelligence & Search Quality stage. These are separate milestones; this addition does not change the analytics or event milestones.

Local implementation completion requires source-to-projection evidence, data-quality checks, simulated-clock coverage of retention gate edges, isolated shortened-horizon physical cleanup evidence, independent code/security reviews, and final QA. It does not wait for real calendar retention. Delayed and in-flight outbox replay must be denied by implementation and independently verified; it is not an accepted residual risk.

The account-deletion workflow is Phase 6's cross-system saga candidate. Keep the existing durable receipt, erasure phases, and MongoDB transaction; finish forward recovery across publisher fencing, Kafka retention, Delta erasure, and guarded report publication. The account tombstone and elapsed retention cannot be compensated. Local implementation closure requires restart and failure recovery evidence, an operator-visible path for work that cannot progress, and the lakehouse specification's accelerated physical and simulated-time checks. Real deployed retention, writer exclusion, guarded key retirement, and operational signoff remain prerequisites for production Phase 7 activation. Isolated synthetic test activation requires independently reviewed local evidence with actual shortened physical retention and explicitly simulated calendar boundaries. Do not add a generic saga framework to close this milestone.

## Continuous Hiring Analytics (Phase 7)

Phase 7 implementation is underway in [Continuous Hiring Analytics](specs/continuous-hiring-analytics.md), including the opt-in query/runtime composition, durable streaming journal, and late-fact retention path. Runtime acceptance is not established. Local Phase 6 closure supports isolated synthetic test activation after HAL-01–HAL-14 evidence and independent signoff. Production activation additionally requires genuine deployed Kafka/Delta horizons, external-writer exclusion, guarded HMAC retirement, and operational signoff. The configurable 10-second trigger supports UC11's Bronze p95 under 30 seconds target but does not prove it; both Bronze availability and the 120-second report freshness p95 need workload measurement.

Existing Phase 7 recovery and correctness requirements remain acceptance gates. Further recovery-policy maintainability work is scheduled for Phase 10 and does not defer or weaken Phase 7 acceptance.

## Search Evaluation (Phase 8)

This phase adds bounded maintainability work alongside reproducible search relevance evaluation. Extract the shared job/candidate embedding workflow as a functional template and pipeline: pure preparation computes the searchable source and source hash once, while an effectful persistence callback performs the guarded write. Preserve version-guarded writes, retry behavior, document limits, and worker/resource ownership. Validate embedding freshness, stale-write conflicts, and retries. This cleanup does not establish search-quality or performance results.

Evaluate the existing ranking alternatives against the reproducible relevance dataset and baseline. Keep application ranking deterministic and pure, and keep MongoDB query execution in adapters. Verify repeatable result ordering, including ties, across the evaluated alternatives. Add a Strategy abstraction only when a second concrete ranking algorithm needs a shared selection boundary; evaluation alone does not justify a generic framework.

## Observability & Resilience (Phase 10)

Further refine streaming recovery by extracting recovery and revision decisions into pure functions over immutable observations. The coordinator retains effect execution. Preserve authorization, journal ordering, deletion checks, publication fencing, watermark commits, and acknowledgement rules. Validation must cover restart, deletion, and publication races; this refinement is maintainability work and does not replace Phase 7 runtime acceptance.

Conditional patterns for this phase:

- **Decorator:** extract shared instrumentation only when multiple provider adapters need it; preserve error semantics and cancellation, and do not duplicate retries.
- **Observer:** use managed streams for new local notifications; retain durable handoffs for work that requires restart recovery.
- **Bridge:** consider only when an integration has two actual, independently varying dimensions.

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
