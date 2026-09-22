# Hiring Analytics Architecture

## Boundary

MongoDB remains the operational source of truth. Kafka carries immutable operational events to the local analytics runtime; no GraphQL mutation waits for Spark, Delta, or an analytics projection.

The current local event contract is the unversioned `hiring.operational-events` envelope implemented in `OperationalEvent.scala`. Analytics consumes that exact seven-field shape. A future envelope evolution is a separate compatibility slice.

## Target Lakehouse

```text
Kafka retained range -> Bronze -> Silver -> Gold -> Mongo analytics projection -> Admin GraphQL
```

- The batch module reads an explicit, bounded Kafka offset range, preserves Kafka record coordinates, parses analytical facts, deduplicates, quarantines malformed/conflicting input, and applies K=10 suppression.
- Delta Bronze, Silver, quarantine, Gold, and run-manifest datasets are written beneath a caller-selected local lakehouse root. The opt-in `analytics-batch` Compose profile persists that root at `.local/data/analytics/`.
- The batch does not publish Gold results into MongoDB. GraphQL reports therefore remain dependent on a separately populated, bounded Mongo snapshot.

The first batch metrics are systemwide lifecycle transition activity, bounded time-to-hire percentiles, and job-created skill posting activity. `CANDIDATE_HIRED` is not a second lifecycle fact; it remains available for its own event use cases. Recruiter, job, and location enrichment are deferred until an event-time dimension policy exists.

## Erasure and limits

Account deletion writes a durable erasure request in the same receipt-owned Mongo transaction as the account tombstone and recruiter job-close events. The erasure worker, HMAC marker checks, purge, Gold rebuild, and snapshot republish are not implemented yet.

Raw Kafka records cannot be selectively deleted and expire on broker retention. The proposed 31-day HMAC deletion marker must be implemented before release. A failed operational outbox record repaired after that marker expires can reintroduce data; this accepted product risk blocks security approval.

## Local execution

`analytics-batch` is an opt-in Compose profile, not part of the default stack. It connects through the internal Kafka listener and requires a caller-supplied run ID, partition, and inclusive/exclusive offset bounds. The local Delta mount is ignored by Git. See the [README batch command](../README.md#local-analytics-batch) for invocation; range discovery and workload measurement are explicit operator work, not hidden startup behavior.

## Non-goals

Structured Streaming, cloud storage, Databricks deployment, search evaluation, and reports beyond the 30-day retained window are not part of this slice.
