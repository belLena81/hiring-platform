# Development Milestones

## Hiring Analytics Lakehouse (Phase 6)

The current analytics milestone is documented in [Hiring Analytics Lakehouse](specs/hiring-analytics-lakehouse.md). It introduces a local bounded Bronze/Silver/Gold batch pipeline, guarded Admin reporting, and a retention-aware erasure worker. Remaining acceptance and runtime gates are tracked in the spec. The roadmap continues through Structured Streaming, then Search Evaluation & Scale, then the post-MVP Discovery Intelligence & Search Quality stage. These are separate milestones; this addition does not change the analytics or event milestones.

Completion requires local source-to-projection evidence, data-quality and retention checks, independent code/security reviews, and final QA. Delayed and in-flight outbox replay must be denied by implementation and independently verified; it is not an accepted residual risk.

The account-deletion workflow is Phase 6's cross-system saga candidate. Keep the existing durable receipt, erasure phases, and MongoDB transaction; finish forward recovery across publisher fencing, Kafka retention, Delta erasure, and guarded report publication. The account tombstone and elapsed retention cannot be compensated. Completion requires restart and failure recovery evidence, an operator-visible path for work that cannot progress, and the lakehouse specification's full retention and review gates. Do not add a generic saga framework to close this milestone.

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

Atlas is required for this stage's Atlas Search and Vector Search work. Local MongoDB Community remains appropriate for core workflows and local transaction tests. This roadmap change includes no Atlas provisioning or spend.
