# Analytics Mongo Persistence Codecs

## Identity and scope

- Status: implementation present; acceptance blocked by failing Mongo integration tests.
- User outcome: analytics Mongo adapters use typed mongo4cats collections with mongo4cats-circe codecs instead of mixing case-class collections, raw `Document`, and generic `Json` collections.
- Scope: analytics production Mongo repositories and their codec tests/fixtures. Keep the operational schema and analytics workflow behavior unchanged.
- Non-goals: root application Mongo persistence, data migration, query/index changes, retention policy changes, or edits to unrelated working tree changes.

## Contracts

- Persisted collection names, field names, BSON kinds, absent/null behavior, enum names, and validated failure outcomes remain stable.
- Preserve report snapshot extension fields and the exact state/lease/CAS/transaction semantics. Retain raw BSON only for collection-wide audit reads and where the Mongo wrapper lacks a required session/bulk operation or BSON fidelity cannot be expressed through Circe.
- Typed collection records are adapter-owned and use mongo4cats-circe derived codecs. No domain or service model becomes a persistence model.

## Acceptance

- AM-01: Fixed-shape analytics Mongo collections use typed case-class collections and mongo4cats-circe codecs; generic `BsonDecoder`/`BsonValueDecoder`, manual `Json` BSON codec, and redundant cursor readers are removed.
- AM-02: Report reservation/publication, deletion claiming/progress/barriers, active-deletion marker reads, lock behavior, and HMAC authorization/preparation retain persisted names, defaults, typed failures, session behavior, CAS predicates, ordering, bounded FS2 demand, and unknown report extension fields.
- AM-03: Codec tests cover exact stored field names and BSON types, valid round trips, missing/null optionals, wrong types/numeric widths, malformed state/identity values, and report extension preservation. Mongo integration tests exercise the affected repositories against disposable MongoDB.
- AM-04: Analytics Java 17 compile, unit, integration compile/test, formatter, and diff checks are run; unavailable infrastructure is recorded as unverified.

## Evidence checkpoint

- Source review confirmed three production styles in analytics: mongo4cats `Document`, raw reactive `Document`, and mongo4cats `Json` plus a custom JSON BSON codec. The report publisher also preserves unknown BSON fields; the key-retirement audit scans dynamic collection shapes.
- Fixed-shape production records now use typed mongo4cats collections and mongo4cats-circe derived codecs. `BsonDecoder.scala`, generic `BsonValueDecoder`, and the generic Json collection codec/read helpers are removed. Raw BSON remains for collection-wide key-retirement audit and query/update expressions; `.underlying` remains for session-bound driver operations and typed bulk writes that the wrapper does not expose.
- `CirceJsonMapper` in mongo4cats-circe 0.7.18 narrows small `Long` values to BSON Int32 and accepts BSON Int32 when decoding Long. A narrow typed codec wrapper widens declared Long fields on writes and validates declared Int32/Int64 fields on reads. Codec specs cover both numeric widths and small Long values.
- Typed reads use field projections where only part of a record is needed. Report snapshot writes use `$set`/`$unset` to retain operator extension fields, and metadata reads exclude the arbitrary report payload.
- Independent Security Engineer review: PASS. It noted an existing unbounded HMAC authorization list read; this refactor does not change that behavior.
- Independent Code Reviewer: PASS after the width-preserving wrapper and projections. Independent Security Engineer: PASS; it noted an existing unbounded HMAC authorization list read, unchanged by this refactor. QA's earlier BLOCKED result is superseded by the final integration run, which exposed regressions requiring remediation.
- Added codec specs for exact field names/types, Long-width preservation, absent/null optionals, wrong types, and malformed values. Added a replica-set integration case for HMAC authorization/preparation records, including small nested Long values.
- Java 17 production compile succeeded during `IntegrationTest / test`; integration sources also compiled. The integration run executed 25 tests: 10 passed, 15 failed. Failures included typed erasure marker reads producing `MalformedMarker`, typed report reads producing `InvalidConfiguration("analytics report record is malformed")`, and the new HMAC preparation round trip producing `InvalidConfiguration("HMAC key retirement preparation is unavailable or malformed")`. These Mongo-boundary failures are not accepted and need diagnosis/fix. Other failures included `KafkaProducerFencerIntegrationSpec` loading a missing class from the unrelated dirty Spark tree. The prior root `sbt test` attempt also failed compilation in dirty checkout sources. Do not treat integration evidence as passing.
- A focused rerun of the active marker spec was attempted to expose the parser cause, but incremental test compilation stopped on a pre-existing stale TASTy warning promoted to an error. Focused `scalafmtOnly` completed successfully for touched Mongo sources and tests. `git diff --check` passed.
- Focused `scalafmtOnly` completed successfully for the touched Mongo sources and tests. `git diff --check` passed at handoff.
