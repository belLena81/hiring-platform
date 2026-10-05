# Hiring Mongo Persistence Boundary

## Current state — 2026-10-05

Mongo adapters now use mongo4cats; `MongoHiringSetup` delegates to migration, validation, ordinary-index, and Atlas-index owners. The Java `Document` codec bridge in `MongoHiringPersistenceCodecs` and grouped `MongoHiringMigrations` still leave HM-02/HM-04 open. The original compilation blockers below are historical: later [repository composition](hiring-repository-composition.md) records passing root unit and Mongo integration suites. Those later checks do not close the remaining design criteria. Phase 8 owns subsequent Mongo/vector optimization and measured adapter refinement.

The remaining task evidence is retained as a historical record unless explicitly identified as a current source fact. Roadmap sequencing follows [development milestones](../development-milestones.md).

## Status

Implementation is in place with HM-02 and HM-04 still partial. Original verification blockers are preserved below; later root integration evidence is linked in the current-state section.

## Original source baseline and goal

- The root build currently uses the MongoDB Reactive Streams driver directly, with hand-written `PublisherBridge` conversion and Mongo access spread across repositories, runtime, setup, and integration fixtures.
- Persistence records already have generated BSON codecs, but encoding and decoding pass through Java `Document` using `BsonDocumentReader`/`BsonDocumentWriter`.
- `MongoHiringSetup` contains collection setup, index creation, validators, migration checkpoints, and Atlas Search index provisioning in one large object.
- The goal is a narrow adapter refactor to mongo4cats and shared persisted-name constants without changing hiring behavior or stored-document shape.

## Contracts and Non-goals

- Persisted collection/field names, BSON types and widths, missing-versus-null behavior, defaults, unknown-field handling, query predicates, write ordering, repository error mapping, and transaction outcomes remain unchanged.
- Mongo client, database, session, collection operations, and bounded result streams are owned by mongo4cats/Cats Effect/FS2. Java driver access is limited to specialized Atlas Search operations that lack a wrapper API.
- Setup retains all migration identifiers/checkpoints, reset-off-by-default behavior, validators, index verification, and fail-closed behavior. Index definitions become data and collection setup is split by responsibility.
- No public GraphQL/API changes, data migration, dual-shape compatibility, index recreation, or unrelated repository cleanup is in scope.

## Acceptance Criteria

| ID | Acceptance criterion | Evidence |
|---|---|---|
| HM-01 | Production repositories, runtime, and setup use mongo4cats clients, databases, sessions, collection effects, and bounded FS2 reads; `PublisherBridge` is removed from production Mongo access. | PASS: `sbt compile`; source search found no `PublisherBridge` or Reactive Streams Mongo client references in production or integration source. Test/integration behavior blocked at Test compile (see below). |
| HM-02 | Typed persistence codecs preserve the existing BSON contract and strict malformed-data behavior without the Java `BsonDocumentReader`/`Writer` round-trip. | PARTIAL: derived `Stored*` codecs are configured and existing BSON-facing shape retained, but `MongoHiringPersistenceCodecs` still bridges Java `Document` with `BsonDocumentReader`/`Writer`. Removing that bridge requires converting repository APIs from heterogeneous `Document` collections to typed persistence collections; this remains open. |
| HM-03 | Production collection and persisted field names are centralized in `MongoCollections` and `MongoFields`; raw names remain only in intentional schema-contract fixtures or Atlas syntax. | PASS: production query/update/sort/index DSL arguments and persisted `Document` readers use constants. Source scans found no remaining raw persisted-field reader literals outside Atlas syntax. Atlas command/schema keys and test fixture BSON literals remain intentionally literal. |
| HM-04 | Collection setup is split by responsibility and ordinary index definitions are represented as data, while migration, validator, reset, and index-safety behavior remains unchanged. | PARTIAL: `MongoHiringSetup` is now a small orchestrator; setup migrations, Atlas search readiness, validators, and declarative ordinary index specs are separate modules. Migration steps remain grouped in `MongoHiringMigrations` rather than per collection. |
| HM-05 | Semantic-search score decoding is checked and maps invalid/missing values to the existing typed stored-data failure. | IMPLEMENTED: both job and candidate result decoders validate score presence/type; valid job hits with missing and string scores now have regression cases. Those test sources could not compile due unrelated test API drift. |

## Verification Results and Blockers

- `sbt compile` passed on Java 17 after the final field-name changes.
- `sbt test` stops during `Test / compile` with nine errors in the dirty GraphQL/service-port/analytics fixture boundary (`HiringGraphQLResolverSupportSpec`, `RequestContextSpec`, `AnalyticsReportingServiceSpec`, and `JobServiceSpec`), not Mongo production source. No test body ran. The removed `PublisherBridgeSpec` was an obsolete bridge-specific test. `IntegrationTest / test` was previously attempted and is blocked by the same test compilation boundary.
- A narrowed Mongo test-source compile also fails because existing `MongoHiringCodecsSpec` and `ServiceFixtures` refer to missing `AccountValueFixtures` and old `User.apply` arity. No Mongo test body ran.
- `git diff --check` passed. Static search found no `asInstanceOf[Number]`, `PublisherBridge`, raw Reactive Streams Mongo client references, persisted-field literal readers, or raw field arguments to production Mongo query/update/sort/index DSL calls.
- Mongo transaction and Atlas/live behavior remain unverified because test compilation did not reach execution.
- Independent Code Reviewer: FAIL because HM-02 and the per-collection part of HM-04 remain open. Security Engineer: PASS by static review; Mongo integration behavior was not exercised. QA: BLOCKED for full acceptance because HM-02/HM-04 remain partial and test execution did not pass compilation. HM-03 passed the independent static recheck.

## Verification Plan

- Codec unit cases cover exact BSON names/types/widths, absent and malformed values, unknown fields where retained, and numeric score validation.
- Root `sbt test` covers the Docker-independent suite. `sbt 'IntegrationTest / test'` covers disposable MongoDB, replica-set transaction, setup/migration, bounded stream, and concurrency behavior.
- Run configured formatting/lint checks, dependency eviction review, `git diff --check`, source searches for `PublisherBridge`, Reactive Streams collection/session types, raw collection/field strings, and unchecked numeric casts.
- Independent Code Reviewer, Security Engineer, and QA verdicts are required. Missing Docker or Atlas infrastructure is reported as blocked/unverified, not passing evidence.
