# Specifications index

One line per specification, grouped by what the first `- Status:` line says (read 2026-11-04; the spec itself is authoritative). Agents read only the spec named in their handoff, not this folder. Add a line here when a spec is created or its status changes. See [spec-driven development](../spec-driven-development.md) and the [template](../templates/feature-spec.md).

`scripts/check-spec-verdicts.py` gates the specs in **Done**, because it reads a first status line that begins with `done`. Use `done` only when the spec records all three independent verdicts; use `in progress` when a gate is open.

## Open

| Spec | Status |
|---|---|
| [kafka-flow-hardening](kafka-flow-hardening.md) | in progress; publication waves, batched commits, tolerant wire reader with headers, stable producer identity with generations (migration 036), SCRAM-SHA-512, Kafka client metrics |
| [analytics-mongo-persistence](analytics-mongo-persistence.md) | in progress; no final QA PASS recorded after the integration regressions were fixed |
| [hiring-container-runtime](hiring-container-runtime.md) | in review |
| [hiring-discovery-search-quality](hiring-discovery-search-quality.md) | in progress; Phase 10 partial, `.C` cloud rows open |
| [hiring-mongo-schema-consolidation](hiring-mongo-schema-consolidation.md) | in progress |
| [hiring-platform-refinements](hiring-platform-refinements.md) | in progress; each item carries its own status |
| [hiring-search-enhancements](hiring-search-enhancements.md) | in progress; Phase 9 partial |
| [mongodb-vector-retrieval-optimization](mongodb-vector-retrieval-optimization.md) | in progress; `.C` rows deferred, overall partial |
| [analytics-reconciliation-recovery](analytics-reconciliation-recovery.md) | draft |
| [deployed-analytics-performance](deployed-analytics-performance.md) | draft |
| [guided-discovery-and-followup](guided-discovery-and-followup.md) | draft, not started; after the MVP claim |
| [hiring-observability-resilience](hiring-observability-resilience.md) | draft |
| [production-readiness](production-readiness.md) | draft |
| [agent-role-enforcement](agent-role-enforcement.md) | in progress |

## Done (verdict gate applies)

| Spec | Status |
|---|---|
| [agent-workflow-enforcement](agent-workflow-enforcement.md) | done for AWE-01, 02, 03, 05, 07, 08; AWE-04.1 and AWE-06.1 deferred |
| [ai-search-resilience](ai-search-resilience.md) | done locally; AIR-07.1 unverified |
| [analytics-data-model-refactoring](analytics-data-model-refactoring.md) | done 2026-11-01 |
| [analytics-erasure-completion-window](analytics-erasure-completion-window.md) | done 2026-10-23; AEC-05.1 open for an operator run |
| [dependency-circuit-breakers](dependency-circuit-breakers.md) | done (local) |
| [durable-hiring-workflows](durable-hiring-workflows.md) | done (local) for DHW-01 to 34 on the fake-provider path; SES parts deferred |
| [effect-polymorphic-services](effect-polymorphic-services.md) | done (local) 2026-10-14 |
| [embedding-coverage-report](embedding-coverage-report.md) | done (local) |
| [hiring-analytics-lakehouse](hiring-analytics-lakehouse.md) | done (local, Phase 6) |
| [hiring-analytics-verification](hiring-analytics-verification.md) | done 2026-10-13 |
| [hiring-end-to-end-acceptance](hiring-end-to-end-acceptance.md) | done for MVP condition 2 (2026-11-02); F5 to F8 open |
| [hiring-saga-orchestration](hiring-saga-orchestration.md) | done (local) 2026-10-15 |
| [operational-alert-verification](operational-alert-verification.md) | done (MVP observability item 8, 2026-11-07); image acceptances expire 2026-11-20 (per-image rule: Alertmanager also at the first patched Alertmanager release, whichever is first); follow-ups open. Code Reviewer, Security Engineer and Final independent QA PASS (2026-11-07); acceptances are for local use, not a safety statement |

## No `- Status:` line

[analytics-erasure-safety](analytics-erasure-safety.md), [candidate-discovery-and-interview-integrity](candidate-discovery-and-interview-integrity.md), [continuous-hiring-analytics](continuous-hiring-analytics.md), [distributed-data-reliability-improvements](distributed-data-reliability-improvements.md), [duplicate-delivery-containment](duplicate-delivery-containment.md), [embedding-workflow-boundary-reliability](embedding-workflow-boundary-reliability.md), [hiring-event-and-test-reliability](hiring-event-and-test-reliability.md), [hiring-retrieval-publication-reliability](hiring-retrieval-publication-reliability.md), [hiring-search-publication-reliability](hiring-search-publication-reliability.md), [hiring-security-and-runtime-reliability](hiring-security-and-runtime-reliability.md), [hiring-workflow-recovery](hiring-workflow-recovery.md), [job-application-counts](job-application-counts.md).
