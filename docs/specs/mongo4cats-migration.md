# Mongo4cats Mongo Boundary Migration

## Summary

Replace the raw MongoDB reactive-streams layer with `mongo4cats-core` and
`mongo4cats-circe` version `0.7.18`. The migration covers client lifecycle,
database access, typed collections, sessions and transactions, repositories,
operational events, setup, indexes, Atlas Search administration, and
integration tests.

This is an intentional BSON rewrite. Existing documents migrate to a new
schema version through a resumable shadow-collection cutover. Domain models
remain free of MongoDB and Circe dependencies.

## Current State

- Mongo access uses `mongodb-driver-reactivestreams`, `fs2-reactive-streams`,
  and a local `PublisherBridge`.
- `MongoHiringCodecs` manually encodes and validates BSON `Document` values.
- `MongoHiringRepositories`, `MongoOperationalEventRepositories`,
  `MongoEmbeddingWorkRepository`, and `MongoHiringSetup` use raw reactive
  collections and sessions.
- The existing setup ledger, index checks, transaction behavior, outbox
  semantics, and Atlas Search readiness checks are required behavior.
- The shared `SourceHash.sha256` utility is already used for migration hashing.

## Decisions And Constraints

- Use `mongo4cats-core` and `mongo4cats-circe` `0.7.18`.
- Migrate the entire Mongo boundary in one final cutover; do not leave a
  mixed raw-driver repository layer.
- Rewrite BSON to schema version `2` rather than preserve the old shape.
- Preserve service, GraphQL, and repository protocol interfaces.
- Keep the domain layer free of database, BSON, Circe, and driver types.
- Preserve transaction atomicity, authorization predicates, query ordering,
  uniqueness constraints, outbox bytes, and existing error semantics.
- Retain legacy collections after cutover; never delete them automatically.
- Keep the custom migration ledger. Do not add Mongock or another migration
  framework.
- Atlas Search operations may use Mongo commands through mongo4cats where the
  library has no dedicated search-index API.

## Implementation Changes

### Dependencies And Runtime

- Add `mongo4cats-core` and `mongo4cats-circe`.
- Remove direct reactive-streams and `fs2-reactive-streams` dependencies once
  source references are gone.
- Replace `MongoDatabaseProbe.clientResource` with
  `mongo4cats.client.MongoClient.create[IO](MongoClientSettings)`.
- Preserve pool limits, timeouts, UUID representation, diagnostics, and
  readiness behavior.
- Acquire databases and typed collections effectfully through `Resource`.
- Replace raw `org.bson.Document` with `mongo4cats.bson.Document`.
- Delete `PublisherBridge.scala` and all publisher conversion tests.

### Persistence DTOs And Codecs

Replace `MongoHiringCodecs` with persistence DTOs and Circe-derived mongo4cats
codec providers for:

- users and credentials;
- jobs;
- applications and application events;
- embedding work;
- search sessions;
- operational outbox records;
- consumer receipts and quarantine records;
- migration ledger and account registry documents.

Use `deriveCirceCodecProvider` for each DTO. Add explicit Circe codecs for
enums, typed identifiers, profile unions, `_id`, and other special fields.
Use UUID BSON values for identifiers, BSON dates for `Instant`, and BSON binary
values for immutable envelope bytes.

Each DTO must contain `schemaVersion = 2` where it represents an application
document. DTO-to-domain mapping must return `Either` and convert decoding or
invariant failures into sanitized repository errors.

Operational event envelope bytes must remain byte-for-byte identical after the
rewrite. The domain models themselves must not acquire Circe or Mongo imports.

### Typed Queries And Transactions

Introduce a small adapter layer around mongo4cats `Filter`, `Update`, `Sort`,
`Index`, `Aggregate`, query builders, and typed collection acquisition.

Centralize helpers for:

- `_id` filters;
- cursor predicates and deterministic sort order;
- optimistic version guards;
- bounded aggregation results;
- duplicate-key and decoding error translation;
- repository error mapping.

Replace raw `ClientSession` with `mongo4cats.client.ClientSession[IO]`.
Implement a cancellation-safe transaction resource that starts, aborts,
commits, and retries transactions while preserving current conflict and
duplicate-write behavior.

### Repositories

Migrate user, account, job, application, embedding-work, semantic-search,
search-session, outbox, consumer-receipt, and quarantine repositories to typed
DTO collections.

Preserve atomicity for:

- application plus initial history;
- status plus history;
- domain write plus operational outbox;
- embedding-related version updates.

Use typed projection DTOs for aggregation results. Remove all
`PublisherBridge.first` and `PublisherBridge.collectWithin` calls. Keep the
service and repository protocol contracts unchanged.

### Schema Rewrite Migration

Add a forward-only migration such as `hiring-mongo-circe-schema-v2`.

1. Acquire the migration lease and verify descriptor/checksum.
2. Detect an empty database, an already-v2 database, or legacy documents.
3. For legacy data, create deterministic shadow collections.
4. Read legacy documents in bounded `_id` keyset batches.
5. Decode, validate, and transform identifiers, timestamps, profiles,
   embeddings, references, events, and search sessions.
6. Preserve operational envelope bytes exactly.
7. Checkpoint after each successful batch.
8. Build and verify all v2 indexes on shadow collections.
9. Verify counts, identifiers, references, role/profile invariants, duplicate
   application constraints, pagination indexes, and outbox consistency.
10. Rename legacy collections to retained backup names and shadow collections
    to canonical names.
11. Re-run index and validator verification.
12. Mark the migration `Applied`.

The ledger must represent `Pending`, `Copying`, `Verifying`, `CuttingOver`,
`Applied`, and recoverable `Failed` states. Empty databases create v2
collections directly. Failed or incomplete cutovers must keep readiness
unavailable and must not silently expose partial data.

Use `SourceHash.sha256` for migration descriptors and checksums.

### Setup And Atlas Search

- Rewrite `MongoHiringSetup` against mongo4cats database and collection APIs.
- Preserve the current fail-closed behavior for incompatible named indexes.
- Recreate and verify all named indexes using mongo4cats index/query APIs.
- Use mongo4cats `runCommand` and aggregate results for Atlas Search
  administration where dedicated methods are unavailable.
- Preserve configured index names, readiness polling, queryability checks, and
  the no-drop policy.
- Run setup before exposing typed repositories from `MongoHiringRuntime`.

## Testing And Acceptance Criteria

### Unit Tests

- Every persistence DTO round-trips through its Circe/mongo4cats codec.
- UUID, date, binary payload, enum, profile, and optional-field behavior is
  verified.
- Malformed documents produce typed decoding failures.
- Operational envelope bytes remain byte-for-byte stable.
- Transaction runner commit, abort, retry, and cancellation behavior is
  covered.
- Filter helpers and cursor ordering match current query behavior.

### Replica-Set Integration Tests

- Empty databases initialize directly as v2.
- Representative legacy fixtures migrate successfully.
- Re-running migration is idempotent.
- Concurrent migration runners cannot overlap.
- Mid-batch failure resumes from the checkpoint.
- Invalid legacy data fails closed without cutover.
- Cutover preserves counts, references, uniqueness, and index definitions.
- Existing user, job, application, history, race, rollback, outbox,
  embedding, search-session, receipt, and quarantine tests remain valid.
- Readiness remains unavailable until setup completes.

### Required Verification

```text
sbt compile
sbt test
sbt 'IntegrationTest / test'
git diff --check
python3 scripts/check-skills.py
```

The final source audit must find no production references to
`PublisherBridge`, `org.reactivestreams`, or direct reactive Mongo client
types. The unrelated unstaged `CursorCodecSpec` change must be preserved and
reconciled separately; the final migration gate requires the complete unit
suite to pass.

## Delivery Order

1. Add the migration spec, dependencies, and typed client/database resource.
2. Add persistence DTOs, Circe codecs, and domain mappers.
3. Add typed query, projection, and transaction helpers.
4. Migrate repositories and operational event persistence.
5. Rewrite setup, indexes, Atlas Search commands, and the v1-to-v2 cutover.
6. Migrate unit, replica-set, and Compose integration tests.
7. Update MongoDB design and schema-evolution documentation.
8. Run independent Data Engineer, Code Reviewer, Security Engineer, and QA
   reviews before declaring the migration complete.

