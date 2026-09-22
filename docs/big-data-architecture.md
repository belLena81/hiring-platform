# Hiring Analytics Architecture

## Boundary

MongoDB remains the operational source of truth. Kafka carries immutable operational events to the local analytics runtime; no GraphQL mutation waits for Spark, Delta, or an analytics projection.

The current local event contract is the unversioned `hiring.operational-events` envelope implemented in `OperationalEvent.scala`. Analytics consumes that exact seven-field shape. A future envelope evolution is a separate compatibility slice.

## Target Lakehouse

```text
Kafka retained range -> Bronze -> Silver -> Gold -> Mongo analytics projection -> Admin GraphQL
```

- The delivered transformation module preserves Kafka record coordinates, parses analytical facts, deduplicates, and applies K=10 suppression in memory.
- Bronze/Delta persistence, HMAC pseudonymization, retention enforcement, manifests, and atomic projection publication remain implementation work.

The first batch metrics are systemwide lifecycle transition activity, bounded time-to-hire percentiles, and job-created skill posting activity. `CANDIDATE_HIRED` is not a second lifecycle fact; it remains available for its own event use cases. Recruiter, job, and location enrichment are deferred until an event-time dimension policy exists.

## Erasure and limits

Account deletion writes a durable erasure request in the same receipt-owned Mongo transaction as the account tombstone and recruiter job-close events. The erasure worker, marker checks, purge, and Gold rebuild are not implemented yet.

Raw Kafka records cannot be selectively deleted and expire on broker retention. The proposed 31-day HMAC deletion marker must be implemented before release. A failed operational outbox record repaired after that marker expires can reintroduce data; this accepted product risk blocks security approval.

## Non-goals

Structured Streaming, cloud storage, Databricks deployment, search evaluation, and reports beyond the 30-day retained window are not part of this slice.
