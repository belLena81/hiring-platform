# Hiring Analytics Lakehouse

## Identity and scope

- Status: in progress
- User outcome: Admin can query bounded, aggregate hiring analytics produced from local Kafka events without coupling request handling to Spark.
- Authorized scope: a local Scala 2.13 Spark/Delta analytics module, Compose integration, Bronze/Silver/Gold datasets, an Admin GraphQL read model, account-deletion erasure requests, tests, and canonical documentation.
- Non-goals: cloud deployment, Structured Streaming, a dashboard, current-state enrichment, changes to the operational event envelope, or a 365-day historical analytics store.
- Known security exception: a failed operational outbox record repaired after the 31-day deletion marker expires can reintroduce deleted-subject analytics data. The user explicitly accepted this residual risk; Security signoff remains blocked.

## Source facts and decisions

- `hiring.operational-events` is the current unversioned seven-field source envelope. Spark consumes that exact shape with declarative parsing and does not reimplement the Scala Circe decoder.
- GraphQL mutations run through `MutationReceiptRepository`, which supplies a receipt-owned `MutationWriteContext`; the current account deletion joins it to tombstone the account, close open recruiter jobs, and append `JOB_CLOSED` outbox events atomically.
- `deleteMyAccount` requires a non-noop context for the analytics-aware path. A new erasure-request port joins the same session before account deletion; direct repository deletion remains a non-analytics repository operation.
- Bronze and quarantine retain raw records for seven days. Silver, Gold, and published Mongo snapshots retain 30 days. Gold is rebuilt only from retained Silver data.
- Silver pseudonymizes only allowed identifier fields with a dedicated HMAC secret. Gold exposes only groups with at least ten contributing subjects/events.

## Acceptance and evidence

| ID | Given / When / Then | Evidence | Actual outcome |
|---|---|---|---|
| HAL-01 | A bounded Kafka range is run twice | Bronze deduplicates by topic/partition/offset; Silver deduplicates by event ID; manifests reconcile counts | Transformation tests pass; durable range runner is not implemented |
| HAL-02 | Lifecycle, duplicate-hire, malformed, or conflicting events arrive | Canonical lifecycle facts aggregate; invalid/conflicting input quarantines and prevents publish | Transformation tests pass for malformed/conflicting input and duplicate-hire exclusion; durable quarantine/publish wiring is not implemented |
| HAL-03 | An active Admin queries published analytics | Service-level stored Admin authorization returns bounded aggregate-only results and sanitized failures | `AnalyticsReportingServiceSpec`: 3 passed |
| HAL-04 | A GraphQL account deletion succeeds or rolls back | Receipt, erasure request, account tombstone, closed jobs, and close-event outbox rows are atomic | `UserAccountServiceSpec`: 15 passed; Mongo transaction integration remains unverified |
| HAL-05 | A deletion marker is active | Actor and candidate payload references are purged, blocked from retained-range replay, and retained Gold is rebuilt | Not implemented; accepted delayed-replay residual risk remains |
| HAL-06 | Retention passes | Bronze/quarantine expire at seven days; Silver/Gold/snapshots and HMAC deletion markers expire at 30/31 days | Constants covered by transformation tests; deployment retention enforcement is not implemented |
| HAL-07 | A local deterministic workload executes | Dataset counts, physical plan, shuffle/file counts, duration, memory, storage, and reconciliation are reported | Not run; no workload harness or local lakehouse deployment |

## Checkpoint and review

- Current checks: root `Test/compile`; focused root service tests (18 passed); isolated Spark transformation tests (6 passed) under Java 17; `git diff --check`.
- Required final gates: Code Reviewer, Security Engineer, and QA. Security remains BLOCKED for the accepted delayed-replay residual risk unless the product decision changes.
