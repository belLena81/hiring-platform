# Data migrations and contract evolution

These are rules for future data changes, not a statement that migration infrastructure already exists. Foundation serves health/readiness through a resource-managed MongoDB connection. Unused PostgreSQL/Doobie adapters and their source-only user schema have been removed at the user's request; no database data is changed. No hiring data is migrated or written. Flyway is not active. The first served GraphQL schema has deterministic SDL and executable operation fixtures under `src/test/resources/graphql/`. Add migration tooling with the first relevant persistence slice.

## Keep version boundaries separate

| Boundary | Version/evolution mechanism | Owner |
|---|---|---|
| PostgreSQL stored data | Ordered immutable applied Flyway migrations | Data Engineer |
| MongoDB documents/indexes | Versioned migration history; document shape version when mixed shapes require it | Data Engineer |
| GraphQL public contract | Additive evolution, explicit deprecation, schema/operation compatibility checks | Architect and Scala Developer |
| Domain events and analytical data | Event `schemaVersion`, compatible readers, replayable transformations | Big Data Engineer with Data Engineer |

Application release numbers do not prove database, GraphQL, or event compatibility. Do not apply one global version field to every boundary. Domain status enums remain valid invariants; evolution requires a deliberate data and client impact review.

## Migration slice contract

Extend the current feature spec with source/target schema, supported application versions, data volumes, invariants, compatibility direction, migration owner, execution order, and recovery criteria. Include exact local commands once implemented and explicitly identify any destructive or irreversible step.

1. **Prepare:** inventory current shapes and conflicting records. Define preflight validation, required permissions, expected counts, locking/index-build behavior, disk headroom and batch limits. Establish backup/restore or another justified recovery mechanism before irreversible changes to valuable data.
2. **Expand:** introduce compatible fields, readers/writers, collections/tables or indexes while the previous application remains supported. Validate old and new readers against transitional data. Do not assume an additive DB field guarantees compatible business behavior.
3. **Backfill:** use deterministic transformations, bounded batches and resumable progress. Specify concurrent-write handling, retries, and idempotency where needed; prevent concurrent migration runners from applying the same work. A rerun must not double-apply changes.
4. **Verify and switch:** reconcile counts, uniqueness, references and domain invariants; exercise representative queries and both application versions. Record the actual data/schema version and successful checkpoints before switching reads/writes.
5. **Contract:** remove the old shape only after supported consumers and data have migrated and the rollback window is closed. Destructive work must be explicitly authorized; if it is outside the task, stop before it while completing independent preparation.

For an empty disposable development database, a smaller migration may be sufficient. State why and still provide deterministic setup and verification; do not create an elaborate production rollout for nonexistent data.

### Store-specific constraints

- PostgreSQL: add new ordered Flyway files; do not edit migrations already applied to shared/persistent environments. Use transactions where supported, and explicitly handle operations that cannot be transactional. Budget lock time and index creation cost. A unique constraint needs a policy for existing duplicates before it can succeed.
- MongoDB: track migration ID/status and prevent overlapping runners; use predicates/checkpoints that make partial progress restartable. Document shape versions are needed only when readers must distinguish shapes. Define index creation/replacement and rollback separately from document updates. The account setup records `user-profile-one-of-v1` for the tagged role-specific profile backfill and `user-email-canonical-sparse-v1` for replacing the legacy non-sparse `users_emailCanonical_unique` index with its sparse unique form; each step is idempotent and setup records it only after success. Transaction-dependent behavior requires replica-set integration tests.
- A PostgreSQL-to-MongoDB transition is a separate migration project: map identifiers/types/UTC precision, extract and transform records, reconcile invariants, define concurrent-write/cutover strategy, and verify recovery. Do not casually add dual writes or claim cross-store atomicity.

Recovery must distinguish application rollback, resumable retry, restore, and forward repair. Never claim an irreversible data transformation can be undone by deploying the previous binary. Test failure mid-batch and restart on a disposable copy, and record what remains unverified for real data.

## GraphQL schema and API evolution

Prefer one evolving GraphQL schema over automatically introducing `/v1` and `/v2`. Keep existing supported operations working while introducing replacements. When the first usable schema is implemented, export its deterministic SDL and keep representative client operations as fixtures; validate and execute them locally on contract-changing slices.

- Review removed/renamed fields or arguments, changed types/defaults, new required inputs, enum/union changes, nullability, error payloads, pagination/cursor formats, and authorization behavior. Even a syntactically additive change may affect exhaustive client handling or business semantics.
- Adding a required input or weakening an output's non-null guarantee can break clients. Tightening output guarantees requires proof that every resolver path satisfies them. Review input/output direction separately; do not use a blanket rule that all nullability changes are safe.
- Deprecate old fields/arguments with a reason and replacement, update examples, identify supported consumers and a removal criterion. If there are no consumers yet, state that evidence and keep the smaller evolution path. Do not fabricate a fixed deprecation period.
- For unavoidable breaking changes, document the consumer migration and compatibility window or a coordinated cutover; use explicit versioned alternatives only when justified. Keep cursor readers compatible across that window or explicitly version and validate cursors without bypassing ownership checks.
- A schema diff alone is insufficient. Run representative old operations against the new schema with variables and relevant errors/permissions. Validate expected response semantics and query counts; keep old persisted data compatible with the application serving those requests.

### Hiring GraphQL typed IDs and signed cursor cutover

The current Hiring GraphQL contract intentionally uses typed scalar IDs for jobs and applications in both inputs and outputs: `Job.id` is `JobID!` and `Application.id` is `ApplicationID!`. Clients that treated these output fields as plain `ID` should refresh generated types from the served SDL before this change is released.

Pagination cursors are signed v2 values. A cursor contains the cursor version, connection kind, pagination key, and an HMAC-SHA256 signature derived from the configured JWT HS256 secret with a cursor-specific derivation label. The server validates the signature before decoding payload fields. Valid signed cursors for the wrong connection return `WRONG_CURSOR_KIND`; malformed, tampered, unsigned, missing-version, or old-version cursors return `INVALID_CURSOR`.

This is a coordinated breaking security change: old unsigned cursors are not accepted. Consumers must discard stored pagination cursors and restart pagination from the first page after deploying against this contract. Runtime configuration must provide a valid `AUTH_JWT_HS256_SECRET`; `disabled`, missing, blank, or short values fail configuration validation.

Login and signup now pass through a configured process-local fixed-window limiter keyed by remote address and operation. Consumers should treat HTTP 429 with `Retry-After` as a transport-level retry signal for account operations. Public signup no longer exposes canonical-name collisions as `NAME_TAKEN`; unauthenticated registration collisions return generic `REGISTRATION_FAILED` to avoid account enumeration.

## Events and analytical schemas

Events remain immutable facts with envelope versions. Define producer/consumer compatibility, defaults and unknown-version handling; never silently reinterpret historical payloads. Test old fixtures with new readers and supported new payloads with old readers where the rollout requires it. Keep upcasting/normalization pure and versioned, preserve replay input, and quarantine unsupported records with a bounded recovery path.

For derived Delta/Silver/Gold changes, document rebuild/backfill, checkpoint compatibility, late-data policy, and reconciliation. Retention/deletion obligations still apply to replay data. Analytics migration cannot become a synchronous dependency of hiring transactions.

## Local release evidence

Data Engineer verifies migration results, Architect/Scala Developer verify API compatibility, Big Data Engineer verifies events/analytics where relevant, Security Engineer reviews access/privacy consequences, and QA checks final observable behavior independently. Record commands, versions, fixture sizes, failures/restarts, verification results, and remaining risks in the spec. All required checks run locally because this project has no CI/CD pipeline; publishing or running a live migration remains separately authorized work.
