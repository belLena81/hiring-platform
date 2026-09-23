# Current Contract and Persistence Evolution

The GraphQL, cursor, operational-event, and embedding contracts have one active shape before MVP. User and job Mongo documents also carry an internal version: Long concurrency revision. This field is not a schema revision and is not exposed through GraphQL or events.

## Aggregate revisions

- New user/job documents start at revision 0. Every successful write to either aggregate advances the revision once in the same atomic Mongo write.
- Compare-and-set writes filter by aggregate _id and expected version; stale snapshots return the existing typed conflict result. Embedding writes use the same revision guard and increment the revision with their embedding update.
- Startup runs migration 001_user_job_revisions before repository reads and gates the durable embedding worker until setup completes. It backfills missing revisions in bounded _id batches, verifies every revision is a non-negative BSON long, and records completion in hiring_migration_ledger only after verification.
- The backfill is restartable and safe across concurrent startup instances: each batch updates only documents whose revision is still missing, so it cannot reset a revision advanced by a live write. Verification or write failure aborts startup and leaves the migration resumable. Explicit mongo.reset-on-start still drops all owned collections, including the ledger.
- All binaries using the old document shape must be stopped before this migration runs. The new strict codecs require revisions, and old binaries reject the added field; this change is not compatible with mixed-version application instances.

## Other contracts and data

GraphQL SDL, cursors, operational-event payloads, and embedding metadata describe only the active shape. No legacy API/event readers or compatibility window are maintained. A contract change updates its implementation, SDL fixture, executable operations, and this document together. The aggregate revision is independent from schema, cursor, event, embedding-model, and analytics-run versions.

Critical integrity remains enforced by MongoDB uniqueness and transactional identity, ownership, status, and state predicates. Resetting local data does not relax authorization, lifecycle, duplicate-application, or closed-job invariants.

Run sbt test for the Docker-independent contract suite and sbt 'IntegrationTest / test' for disposable MongoDB, migration, transaction, and HTTP checks. Current acceptance and evidence are tracked in [the active specification](specs/pre-mvp-contract-reset.md).
