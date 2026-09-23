# Hiring Analytics Architecture

## Boundary

MongoDB remains the operational source of truth. Kafka carries immutable operational events to the local analytics runtime; no GraphQL mutation waits for Spark, Delta, or an analytics projection.

The current local event contract is the unversioned `hiring.operational-events` envelope implemented in `OperationalEvent.scala`. Analytics consumes that exact seven-field shape. A future envelope evolution is a separate compatibility slice.

## Target Lakehouse

```text
Kafka retained range -> Bronze -> Silver -> Gold -> Mongo analytics projection -> Admin GraphQL
```

- The batch module reads an explicit, bounded Kafka offset range, preserves Kafka record coordinates, parses analytical facts, deduplicates, and quarantines malformed/conflicting input. A quality-blocked range cannot advance Gold or the published Mongo snapshot. K=10 suppression is checked per output cell using distinct pseudonymized subjects; it is not a formal anonymity guarantee. See the [implementation specification](specs/hiring-analytics-lakehouse.md) for exact metric, retry, and acceptance contracts.
- Delta Bronze, Silver, quarantine, Gold, and run-manifest datasets are written beneath a caller-selected local lakehouse root. The opt-in `analytics-batch` Compose profile persists that root at `.local/data/analytics/`.
- The batch does not yet publish Gold results into MongoDB. GraphQL reports therefore remain dependent on a separately populated, bounded Mongo snapshot until the publish path is implemented.

The first batch metrics are systemwide lifecycle transition activity, bounded time-to-hire percentiles, and job-created skill posting activity. `CANDIDATE_HIRED` is not a second lifecycle fact; it remains available for its own event use cases. Recruiter, job, and location enrichment are deferred until an event-time dimension policy exists.

## Erasure and limits

Account deletion writes a durable erasure request in the same receipt-owned Mongo transaction as the account tombstone and recruiter job-close events. The runnable batch reads up to 100,000 pending requests from Mongo before mutation, fails closed if the collection/read/UUID validation fails or the limit is exceeded, removes matching rows from existing Silver, and rebuilds Gold before processing the requested Kafka range. The batch still does not publish or hide the GraphQL Mongo snapshot, purge raw Bronze/quarantine/outbox data, coordinate publisher drain, or complete/expire erasure requests; concurrent deletion races also remain open.

Raw Kafka records cannot be selectively deleted and expire on broker retention. Erasure therefore remains active through a subject-scoped publisher fence/drain and the maximum source retention window; raw Delta rows without extractable subjects require purging the affected raw scope. A permanent Deleted-account tombstone denies delayed/manual outbox replay, while the HMAC marker covers bounded Kafka and analytical retention and is matched across active/retiring keys. These controls, generation-guarded snapshot publication, restricted Kafka write ACLs, and physical Delta reclamation require implementation and independent security verification before release.

## Local execution

`analytics-batch` is an opt-in Compose profile, not part of the default stack. It connects through the internal Kafka listener and Mongo service, and requires a caller-supplied run ID, partition, and inclusive/exclusive offset bounds. The local Delta mount is ignored by Git. See the [README batch command](../README.md#local-analytics-batch) for invocation; range discovery and workload measurement are explicit operator work, not hidden startup behavior. The acceptance workload is a fixed-seed, 100,000-record local range; its environment and measured resource use are recorded without claiming a performance SLO.

## Non-goals

Structured Streaming, cloud storage, Databricks deployment, search evaluation, and reports beyond the 30-day retained window are not part of this slice.
