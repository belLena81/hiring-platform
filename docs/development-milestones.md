# Development Milestones

## Hiring Analytics Lakehouse

The current analytics milestone is documented in [Hiring Analytics Lakehouse](specs/hiring-analytics-lakehouse.md). It introduces a local batch-only Bronze/Silver/Gold pipeline and bounded Admin reporting. The roadmap continues through Structured Streaming, then Search Evaluation & Scale, then the post-MVP Discovery Intelligence & Search Quality stage. These are separate milestones; this addition does not change the analytics or event milestones.

Completion requires local source-to-projection evidence, data-quality and retention checks, independent code/security reviews, and final QA. Delayed and in-flight outbox replay must be denied by implementation and independently verified; it is not an accepted residual risk.

## Discovery Intelligence & Search Quality (Post-MVP)

This stage extends the existing structured, semantic, and hybrid job search after Search Evaluation & Scale. It does not change the MVP use cases. Proposed capabilities are radius-based job discovery using caller-supplied structured coordinates, richer Atlas lexical search and facets, workload-based MongoDB query-plan and index tuning, and personalized ranking.

Evaluation comes before ranking changes: establish relevance measures and a baseline with bounded, representative data, then compare any personalized ranking against that baseline. Personalization is adopted only when evaluation supports a quality improvement; otherwise, its outcome can be deferred.

Acceptance gates:

- Radius search combines distance with existing filters, preserves candidate visibility rules, and returns deterministic cursor pagination. Coordinates are structured input; geocoding is out of scope.
- Atlas lexical, vector, and hybrid search enhancements are validated in an Atlas-capable environment.
- Index changes have representative query-plan evidence and measured workload results that include read benefits and write/storage tradeoffs.
- Personalized ranking is evaluated against the established baseline before adoption.

Atlas is required for this stage's Atlas Search and Vector Search work. Local MongoDB Community remains appropriate for core workflows and local transaction tests. This roadmap change includes no Atlas provisioning or spend.
