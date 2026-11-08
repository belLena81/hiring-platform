---
paths:
  - "src/main/scala/**/repository/**"
  - "src/main/scala/**/infrastructure/**"
  - "src/main/scala/**/api/**"
  - "src/main/resources/**"
  - "src/it/**"
  - "src/test/**/repository/**"
  - "src/test/resources/graphql/**"
  - "analytics/**"
  - "docs/specs/**"
  - "docs/mongodb-design.md"
  - "docs/schema-evolution.md"
  - "docs/big-data-architecture.md"
---

# Persistence and analytics

- Existing SQL schema changes require versioned Flyway migrations; enable tooling explicitly because it is not currently active. Target MongoDB schema/index changes require versioned, repeatable migration steps with recovery and verification. Flyway is not the MongoDB migration mechanism.
- Pre-MVP contract policy: API and analytics schemas have one active shape. Until the owner explicitly asks to start versioning and migration scripts for schema changes (owner decision 2026-11-08; the owner calls that point the first cloud release), implement schema changes directly; do not add migration, backfill, dual-read/write, or legacy-compatibility code for API or analytics changes. If existing local analytics data is incompatible, reset/rebuild only the exact local lakehouse when that reset is explicitly in scope; otherwise preserve it and fail closed. After that request, the normal schema-evolution requirements below apply.
- Schema-changing specs include expand/backfill/verify/contract or a justified smaller plan, old/new compatibility, restart/concurrent-write handling, and recovery evidence. Never edit applied migrations or equate application rollback with data rollback.
- Evolve GraphQL additively where possible with deprecation and local schema/consumer-operation checks. Review semantic changes, enum/nullability changes, and cursor compatibility. Keep database migration identity independent from API and event contracts. The current operational event envelope has one seven-field shape whose schema version (1) travels in the `hiring-schema-version` record header; the producer is strict and readers tolerate unknown additive fields (`docs/schema-evolution.md`, "Operational event evolution policy"); a breaking change bumps the version and requires an explicitly scoped decision consistent with the pre-MVP policy. After that request, version independently where required; do not invent versioned endpoints or tooling before the relevant slice.
- Prefer UUIDs unless existing compatibility requires otherwise. Enforce critical uniqueness in the store rather than preflight reads.
- Derive indexes from candidate/job application lists, recruiter jobs, statuses, authorization predicates, filters and deterministic cursor ordering. Measure read benefit against storage/write cost.
- Optimize query work at both boundaries: push selective filtering, projection, stable ordering, and bounded pagination into the database; avoid N+1 fetches, repeated decoding/hydration, unbounded materialization, and avoidable CPU/allocation work in application code. For performance changes, record representative query plans/counts and a reproducible before/after workload.
- Embed bounded owned values; reference independent entities. Keep applications/events separate. MongoDB transaction tests require replica-set infrastructure.
- Target MongoDB remains operational truth; analytics must not gate transactions. Use durable event publication, explicit active contracts, at-least-once delivery, and idempotent consumers.
- Spark/Delta work includes data quality, replay, late data, recovery, retention, and physical-plan evidence. Distributed processing needs a documented workload benefit.