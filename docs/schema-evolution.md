# Current Contract and Persistence Evolution

## Authentication receipt protection and trusted Admin seed


Receipt fingerprints are keyed by `auth.jwt.receipt-fingerprint-secret` (`AUTH_RECEIPT_FP_SECRET`, at least 32 bytes), falling back to the JWT secret when unset. Pre-MVP there is no receipt conversion: older receipt shapes expire by TTL. Changing the receipt key invalidates existing authentication receipt matches, so use new idempotency keys. Public GraphQL `bootstrapAdmin` is removed under the pre-MVP single-active-shape policy; trusted `auth.admin-seed` startup configuration owns provisioning and never resets an existing singleton. Existing users and non-authentication mutation receipts are preserved. See [acceptance evidence](specs/hiring-security-and-runtime-reliability.md).

## Candidate residence integrity

Migration `016_candidate_residence_integrity` supplements the unchanged `002_candidate_search_profile_verification` ledger. Stop incompatible user writers before cutover. The user validator requires display/canonical city fields to be both absent or both present; a bounded restartable audit verifies their existing canonical values using the application's normalization. It changes only its ledger, preserving user data and revisions. Failed batches retain the preceding checkpoint and block startup until explicit repair. Completed proof and the exact installed validator are checked before the audit is skipped; validator drift is not silently overwritten. No public or stored field shape changes, normalization backfill or data reset is introduced. Evidence is tracked in [candidate discovery and interview integrity](specs/candidate-discovery-and-interview-integrity.md).

## Deleted account embedding cleanup

Migration `014_deleted_account_embeddings` removes `embedding` and `embeddingMeta` from existing Deleted-account tombstones in guarded batches of at most 500. Stop older writers before initialization. Both fields are unset atomically; successful removal is the restart checkpoint. Existing active accounts and every other tombstone field remain unchanged. This privacy maintenance operation deliberately preserves aggregate revisions as an exception to ordinary aggregate writes. Existing validation stays enabled; malformed data or an unsupported ledger blocks startup without bypassing validation or repairing revisions implicitly.

Completion requires acknowledged writes and absence verification. Every completed startup checks for a residual matching document and fails closed if fields reappear. Recovery from Running resumes the remaining records. A Complete ledger with reintroduced data requires explicit maintenance repair before startup; application rollback cannot restore purged vectors. Follow-up evidence belongs to [retrieval and publication reliability](specs/hiring-retrieval-publication-reliability.md#account-erasure-and-recovery-follow-up).

## Workflow reliability cutovers

The October 7 reliability slice adds `012_attributable_producer_registrations` and `013_hiring_workflow_integrity`; applied migrations remain unchanged. Stop incompatible operational and analytics writers before cutover. Migration 012 transfers attribution before removing old arrays, verifies strict validators and preserves unresolved generations without TTL. Analytics accepts the single registry-backed shape only after the completed version-one proof.

After migration 013 audits existing commands/outbox rows and installs equivalent strict validators, startup verifies the exact definitions before skipping completed 010 command and 003 outbox audits. Validator drift fails closed; startup does not silently restore a changed validator. Migration `015_interview_cleanup_integrity` separately audits cleanup records in bounded restartable batches and installs the strict cleanup validator. Only its exact completed proof, configured physical topic pair and validator permit skipping the completed 011 cleanup scan. Maintenance audits still check raw BSON with durable checkpoints.

Operational event payloads now have one minimized active shape, validated by typed payload constructors and the wire decoder. The seven envelope fields and event names are unchanged. Incompatible existing event bytes are preserved and terminally failed during claim, retaining subject attribution for deletion. Invalid control metadata fails closed. There is no legacy reader, event backfill or automatic lakehouse reset. Stop incompatible writers before this cutover and update consumers together; see [event and test reliability](specs/hiring-event-and-test-reliability.md).

Run the explicit `MongoWorkflowIntegrityAudit` maintenance entrypoint for bounded resumable checks. Failed audit batches retain their previous checkpoint for repair and rerun; a completed run starts a fresh audit next time. Canonical UUID identities are required at cutover, so incompatible existing command rows require explicit repair before proof completion. No application rollback is a database rollback; preserve the database and stopped-writer recovery evidence.


The API and analytics schemas have one active shape before full MVP. Implement their schema changes directly; do not add migration, backfill, dual-read/write, or legacy-compatibility code before full MVP. If incompatible local analytics data exists, reset/rebuild only the exact local lakehouse when explicitly in scope; otherwise preserve it and fail closed. After full MVP, apply the schema evolution requirements in `AGENTS.md`.

GraphQL, cursor, operational-event, and embedding contracts therefore have one active shape before MVP. User and job Mongo documents also carry an internal version: Long concurrency revision. This field is not a schema revision and is not exposed through GraphQL or events.

## Aggregate revisions

- New user/job documents start at revision 0. Every successful write to either aggregate advances the revision once in the same atomic Mongo write.
- Compare-and-set writes filter by aggregate _id and expected version; stale snapshots return the existing typed conflict result. Embedding writes use the same revision guard and increment the revision with their embedding update.
- Startup runs migration 001_user_job_revisions before repository reads and gates the durable embedding worker until setup completes. It backfills missing revisions in bounded _id batches, verifies every revision is a non-negative BSON long, and records completion in hiring_migration_ledger only after verification. Once the ledger records Complete, later startups skip the backfill and verification scans.
- The backfill is restartable and safe across concurrent startup instances: each batch updates only documents whose revision is still missing, so it cannot reset a revision advanced by a live write. Verification or write failure aborts startup and leaves the migration resumable. Explicit mongo.reset-on-start still drops all owned collections, including the ledger.
- All binaries using the old document shape must be stopped before this migration runs. The new strict codecs require revisions, and old binaries reject the added field; this change is not compatible with mixed-version application instances.

## Other contracts and data

The search/publication reliability slice changes internal application-admission ports to a projected ID/status/revision snapshot and adds validated workflow polling configuration; persisted job revisions, public GraphQL/event shapes and applied migrations remain unchanged. Embedding provider model mismatches now fail before use. Candidate outbox indexes are measured on disposable namespaces and require a new repeatable migration only if adoption gates pass; a rejected candidate changes no production index.

GraphQL SDL, cursors, operational-event payloads, and embedding metadata describe only the active shape. No legacy API/event readers or compatibility window are maintained. A contract change updates its implementation, SDL fixture, executable operations, and this document together. The aggregate revision is independent from schema, cursor, event, embedding-model, and analytics-run versions.

Critical integrity remains enforced by MongoDB uniqueness and transactional identity, ownership, status, and state predicates. Resetting local data does not relax authorization, lifecycle, duplicate-application, or closed-job invariants.

## Embedding coverage report contract

The Admin-only `embeddingCoverage(expectedModel: String)` query and its `EmbeddingCoverage*`, `EmbeddingFreshness`, `EmbeddingRepairState`, `EmbeddingFailureReason` and `EmbeddingObservedModel` types are an additive, read-only API change. They add no persisted field, index, migration, event or cursor, and reuse the existing `VECTOR_SEARCH_UNAVAILABLE`, `UNAUTHORIZED` and `VALIDATION_FAILED` codes. The SDL snapshot, the validated `embedding-coverage.graphql` operation, `docs/api.md` and the [specification](specs/embedding-coverage-report.md) change together. Enum labels use SCREAMING_CASE while the internal states keep their existing representation. Each entity page and its queue rows are read at one point in time under snapshot read concern; the report as a whole is a sequence of such pages, not a transaction. The root is an expensive root with fixed complexity, so one coverage scan runs per request. The scan adds no write and no index.

## Geographic discovery and interview scheduling contracts

Job location has one optional geographic point. API inputs validate finite latitude/longitude; MongoDB stores the active GeoJSON Point shape in longitude/latitude order. Existing jobs keep an absent point. Nearby cursors carry full-precision distance, job identity and a fingerprint of the center, radius, normalized structured filters and ordering; mismatched cursors are rejected rather than interpreted under another query. SDL and executable operations describe the current nearby, facet and scheduling fields.

Interview scheduling accepts UTC times at millisecond precision, matching MongoDB dates. Inputs are normalized before future-start and positive-duration validation and before request fingerprinting; an interval that collapses to zero milliseconds is rejected. Mutation responses, inspection and replay therefore use the same canonical interval.

Interview commands/results have one active capability-specific metadata shape containing workflow/message identity, step, revision, causation, occurrence, deadline and optional result. Scheduling details are fetched from MongoDB. The seven-field operational event envelope is unchanged. Workflow revisions and claim fencing tokens are coordination state, not schema versions.

Operational migrations `006_job_geo_points`, `007_interview_workflow_storage`, `008_interview_subject_cleanup` and `009_interview_inbox_identity` preserve local records and verify their required validators/indexes before enabling discovery or workers. Stop incompatible job/workflow writers before cutover. An invalid stored point or unknown existing index definition fails startup; correct the offending data or definition explicitly and rerun the restartable migration. No analytics reset, legacy reader or dual writer is introduced. Scheduling acceptance uses the existing application `updatedAt` field for a transactional write lock and adds no application status or persistence field.

Current evidence and pending gates are maintained in the [discovery specification](specs/hiring-discovery-search-quality.md) and [workflow specification](specs/durable-hiring-workflows.md).

Run sbt test for the Docker-independent contract suite and sbt 'IntegrationTest / test' for disposable MongoDB, migration, transaction, and HTTP checks. Current acceptance and evidence are tracked in [the active specification](specs/pre-mvp-contract-reset.md).

## Interview workflow recovery maintenance

Operational migrations `010_interview_workflow_attempts` and `011_interview_publication_fencing` preserve workflow rows, permanent subject tombstones and conservative execution budgets. They gate startup, are restartable in bounded batches and reject malformed stored contracts. Completed 010 and 011 scans are skipped only after their respective exact 013 and 015 audited proofs and validators are verified. Notification lookup has one active participant-aware shape; no legacy dual interpreter is introduced. Cleanup rows lacking the new proof metadata, including legacy Complete rows, are reopened for broker fencing and fresh retention barriers.

Before activation stop old interview writers and revoke their nontransactional topic-write credentials. Configure separate command/result publishers and a prefix-scoped fencer; verify migrations, fencing and physical retention on the target environment before declaring erasure complete. Rolling back the application does not roll back migrated budgets or restore the old cleanup proof. Existing local data is preserved; a deployed maintenance migration is outside this local refactoring task. See [current acceptance criteria](specs/hiring-workflow-recovery.md).
