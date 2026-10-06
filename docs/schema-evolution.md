# Current Contract and Persistence Evolution

The API and analytics schemas have one active shape before full MVP. Implement their schema changes directly; do not add migration, backfill, dual-read/write, or legacy-compatibility code before full MVP. If incompatible local analytics data exists, reset/rebuild only the exact local lakehouse when explicitly in scope; otherwise preserve it and fail closed. After full MVP, apply the schema evolution requirements in `AGENTS.md`.

GraphQL, cursor, operational-event, and embedding contracts therefore have one active shape before MVP. User and job Mongo documents also carry an internal version: Long concurrency revision. This field is not a schema revision and is not exposed through GraphQL or events.

## Aggregate revisions

- New user/job documents start at revision 0. Every successful write to either aggregate advances the revision once in the same atomic Mongo write.
- Compare-and-set writes filter by aggregate _id and expected version; stale snapshots return the existing typed conflict result. Embedding writes use the same revision guard and increment the revision with their embedding update.
- Startup runs migration 001_user_job_revisions before repository reads and gates the durable embedding worker until setup completes. It backfills missing revisions in bounded _id batches, verifies every revision is a non-negative BSON long, and records completion in hiring_migration_ledger only after verification. Once the ledger records Complete, later startups skip the backfill and verification scans.
- The backfill is restartable and safe across concurrent startup instances: each batch updates only documents whose revision is still missing, so it cannot reset a revision advanced by a live write. Verification or write failure aborts startup and leaves the migration resumable. Explicit mongo.reset-on-start still drops all owned collections, including the ledger.
- All binaries using the old document shape must be stopped before this migration runs. The new strict codecs require revisions, and old binaries reject the added field; this change is not compatible with mixed-version application instances.

## Other contracts and data

GraphQL SDL, cursors, operational-event payloads, and embedding metadata describe only the active shape. No legacy API/event readers or compatibility window are maintained. A contract change updates its implementation, SDL fixture, executable operations, and this document together. The aggregate revision is independent from schema, cursor, event, embedding-model, and analytics-run versions.

Critical integrity remains enforced by MongoDB uniqueness and transactional identity, ownership, status, and state predicates. Resetting local data does not relax authorization, lifecycle, duplicate-application, or closed-job invariants.

## Geographic discovery and interview scheduling contracts

Job location has one optional geographic point. API inputs validate finite latitude/longitude; MongoDB stores the active GeoJSON Point shape in longitude/latitude order. Existing jobs keep an absent point. Nearby cursors carry full-precision distance, job identity and a fingerprint of the center, radius, normalized structured filters and ordering; mismatched cursors are rejected rather than interpreted under another query. SDL and executable operations describe the current nearby, facet and scheduling fields.

Interview scheduling accepts UTC times at millisecond precision, matching MongoDB dates. Inputs are normalized before future-start and positive-duration validation and before request fingerprinting; an interval that collapses to zero milliseconds is rejected. Mutation responses, inspection and replay therefore use the same canonical interval.

Interview commands/results have one active capability-specific metadata shape containing workflow/message identity, step, revision, causation, occurrence, deadline and optional result. Scheduling details are fetched from MongoDB. The seven-field operational event envelope is unchanged. Workflow revisions and claim fencing tokens are coordination state, not schema versions.

Operational migrations `006_job_geo_points`, `007_interview_workflow_storage`, `008_interview_subject_cleanup` and `009_interview_inbox_identity` preserve local records and verify their required validators/indexes before enabling discovery or workers. Stop incompatible job/workflow writers before cutover. An invalid stored point or unknown existing index definition fails startup; correct the offending data or definition explicitly and rerun the restartable migration. No analytics reset, legacy reader or dual writer is introduced. Scheduling acceptance uses the existing application `updatedAt` field for a transactional write lock and adds no application status or persistence field.

Current evidence and pending gates are maintained in the [discovery specification](specs/hiring-discovery-search-quality.md) and [workflow specification](specs/durable-hiring-workflows.md).

Run sbt test for the Docker-independent contract suite and sbt 'IntegrationTest / test' for disposable MongoDB, migration, transaction, and HTTP checks. Current acceptance and evidence are tracked in [the active specification](specs/pre-mvp-contract-reset.md).
